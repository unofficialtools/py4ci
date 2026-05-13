"""
This file defines the ci logic

TODO:
docker integration: docker run alpine sh -c 'ls > task.ls'
display tags and make editable
"""

import datetime
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import tomllib
import traceback
import uuid

# group_id is stored as a tag too (it's a uuid4 hex). We don't want to display
# it as a user-facing tag pill, so filter anything that looks like a uuid.
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

from fabric import Connection
from py4web import HTTP, action, request
from pydal import Field
from pydal.validators import IS_IN_SET, IS_JSON
from pydal.tools.tags import Tags

now = datetime.datetime.utcnow
never = datetime.datetime(2000, 1, 1, 0, 0, 0)

DEFAULT_TIMEOUT = "180s"
# this is a relative path on workers where work fill bbe done
RUNS_FOLDER = "ci_runs"

STATUSES = (
    "queued",  # waiting to be assigned a worked and start
    "skipped",  # preepmted by another run
    "starting",  # assigned a worker and waiting to issue commands
    "jammed",  # failed to start
    "started",  # started running on worker
    "timeout",  # run too long without being "done"
    "stopping",  # used requested it be stopped
    "stopped",  # it was killed because of user request
    "done",  # run reported that it is completed
    "broken",  # done but missing task.status
    "success",  # task success
    "failure",  # task failure
)

# statuses that keep a worker busy
BUSY_STATUSES = ("starting", "started", "stopping", "done")
# statuses that require a follow-up action with worker
ACTION_STATUSES = ("done", "started", "stopping")
# statuses that requred any followup action
NON_TERMINAL_STATUSES = ("queued", "done", "starting", "started", "stopping")
# statuses that do not have any follow up action
TERMINAL_STATUSES = ("skipped", "stopped", "jammed", "broken", "success", "failure")

CONNECT_ARGS = {
    "allow_agent": True,
    "look_for_keys": True,
}


_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 24 * 3600, "w": 7 * 24 * 3600}
# Each chunk is "<number><unit>", e.g. "1h", "30m", "1.5h".
_DURATION_CHUNK_RE = re.compile(r"(\d+(?:\.\d+)?)\s*([smhdw])", re.IGNORECASE)


def delta(value):
    """Parse a duration into a ``datetime.timedelta``.

    Accepts ints/floats (seconds) and strings such as ``"30s"``, ``"1.5h"``,
    ``"1h30m"`` (with arbitrary whitespace). Returns a ``timedelta`` rounded
    to whole seconds. Raises ``ValueError`` on garbage input so config errors
    are surfaced loudly instead of silently treated as zero seconds.
    """
    if isinstance(value, (int, float)):
        return datetime.timedelta(seconds=int(value))
    if not isinstance(value, str):
        raise ValueError(f"unsupported duration value: {value!r}")
    stripped = value.strip()
    if not stripped:
        raise ValueError("empty duration string")
    # Plain integer (e.g. "180") still means seconds.
    if stripped.lstrip("-").isdigit():
        return datetime.timedelta(seconds=int(stripped))
    total = 0.0
    consumed = 0
    for match in _DURATION_CHUNK_RE.finditer(stripped):
        # Tolerate whitespace between chunks but reject stray characters.
        gap = stripped[consumed:match.start()].strip()
        if gap:
            raise ValueError(f"invalid duration: {value!r}")
        total += float(match.group(1)) * _DURATION_UNITS[match.group(2).lower()]
        consumed = match.end()
    if consumed == 0 or stripped[consumed:].strip():
        raise ValueError(f"invalid duration: {value!r}")
    return datetime.timedelta(seconds=int(total))


class Remote:
    def __init__(self, host, folder="remote_task_runner", callback=None):
        self.user, self.host = host.split("@")
        self.folder = folder
        self.callback = callback

    def start(self, code, input_data=None, payloads=None):
        local_tmp_dir = f"/tmp/{self.folder}"
        os.makedirs(local_tmp_dir, exist_ok=True)
        local_task_sh = f"{local_tmp_dir}.task.sh"
        with open(local_task_sh, "w") as fp:
            fp.write(code.strip() + "\n")
        local_input = f"{local_tmp_dir}.task.input.json"
        with open(local_input, "w") as fp:
            json.dump(input_data or {}, fp)
        # Build the manager script locally and ship it as a file so we don't
        # have to worry about quoting it through `echo`. The manager:
        #   - puts itself in its own process group (setsid) so we can kill
        #     the whole group from the server, taking grandchildren with us
        #   - records the manager's PGID in task.pgid for that kill
        #   - backgrounds task.sh, records its PID in task.pid
        #   - writes 'success' or 'failure' to task.status from $rc
        #   - POSTs back to the callback once
        local_manager_sh = f"{local_tmp_dir}.task.manager.sh"
        manager_lines = [
            "#!/bin/sh",
            "set -u",
            # re-exec ourselves in a new session so we own the process group
            "if [ -z \"${_PY4CI_SESSION:-}\" ]; then",
            "  _PY4CI_SESSION=1 exec setsid \"$0\" \"$@\"",
            "fi",
            "echo $$ > task.pgid",
            "./task.sh > task.log 2>&1 &",
            "task_pid=$!",
            "echo \"$task_pid\" > task.pid",
            "wait \"$task_pid\"",
            "rc=$?",
            "if [ \"$rc\" -eq 0 ]; then echo 'success' > task.status; else echo 'failure' > task.status; fi",
        ]
        if self.callback:
            manager_lines.append(f"curl -X POST --retry 5 {self.callback} || true")
        with open(local_manager_sh, "w") as fp:
            fp.write("\n".join(manager_lines) + "\n")
        with Connection(
            self.host, user=self.user, connect_kwargs=CONNECT_ARGS
        ) as connection:
            # kill anything left over from a prior run (should not happen).
            connection.run(
                f"[ -f {self.folder}/task.pgid ] && kill -- -$(cat {self.folder}/task.pgid) 2>/dev/null"
                " || true",
                hide=True,
            )
            connection.run(f"mkdir -p {self.folder}", hide=True)
            connection.run(f"rm -rf {self.folder}/* || true", hide=True)
            connection.put(local_task_sh, f"{self.folder}/task.sh")
            connection.put(local_input, f"{self.folder}/task.input.json")
            connection.put(local_manager_sh, f"{self.folder}/task.manager.sh")
            for payload in payloads or []:
                payload_dir = os.path.dirname(payload.remote_name)
                connection.run(f"mkdir -p {self.folder}/{payload_dir}", hide=True)
                connection.put(
                    payload.local_name, f"{self.folder}/{payload.remote_name}"
                )
            with connection.cd(self.folder):
                connection.run("chmod +x task.sh task.manager.sh", hide=True)
                connection.run("dtach -n task.socket -E ./task.manager.sh", hide=True)
        os.unlink(local_task_sh)
        os.unlink(local_input)
        os.unlink(local_manager_sh)

    def tail_log(self, max_bytes=64 * 1024):
        """Return the last max_bytes of task.log from the worker (best-effort).
        Used for live log-following while a run is in progress."""
        try:
            with Connection(
                self.host, user=self.user, connect_kwargs=CONNECT_ARGS
            ) as connection:
                res = connection.run(
                    f"tail -c {max_bytes} {self.folder}/task.log 2>/dev/null || true",
                    hide=True,
                    warn=True,
                )
                return res.stdout if res and res.stdout else ""
        except Exception:
            return ""

    def finish(self):
        files = {}
        with Connection(
            self.host, user=self.user, connect_kwargs=CONNECT_ARGS
        ) as connection:
            with connection.cd(self.folder):
                # Kill the whole process group; success means the manager was
                # still running (=> we hit the run's wall-clock timeout).
                killed = connection.run(
                    "[ -f task.pgid ] && kill -- -$(cat task.pgid)",
                    warn=True,
                    hide=True,
                )
                status = "timeout" if not killed.failed else "done"
                # Pull back log + status + structured output (if any).
                for filename in ("task.log", "task.status", "task.output.json"):
                    local = f"/tmp/{self.folder}.{filename}"
                    try:
                        connection.get(f"{self.folder}/{filename}", local)
                    except Exception:
                        pass
                    else:
                        with open(local) as fp:
                            files[filename] = fp.read()
                        os.unlink(local)
            connection.run(f"rm -rf {self.folder}", hide=True)
        # Decide the final status from what we got back.
        if "task.status" in files:
            status = files["task.status"].strip()
        elif status != "timeout":
            status = "broken"
        log = files.get("task.log", "").replace("\x00", "\\x00")
        # output_data: tasks may write structured JSON to task.output.json.
        # If it's missing or malformed, leave it as None — don't fail the run.
        results = None
        raw_out = files.get("task.output.json")
        if raw_out:
            try:
                results = json.loads(raw_out)
            except (ValueError, TypeError):
                results = {"_parse_error": "task.output.json was not valid JSON"}
        return status, log, results


CONFIG_TOP_KEYS = {"administrators", "workers", "tasks", "variables"}

WORKER_FIELDS = {"host", "queues"}

# (name, required). Anything outside this set is rejected to surface typos.
TASK_FIELDS = {
    "enabled",
    "description",
    "queues",
    "tags",
    "period",
    "debounce",
    "priority",
    "timeout",
    "command",
    "authorized_users",
    "triggered_by",
    "retries",
    "retry_backoff",
    "retention",
}

# Allowed transitions between run statuses. Anything not in this map is
# rejected with a state-machine error before the row is updated.
ALLOWED_TRANSITIONS = {
    None: {"queued"},
    "queued": {"queued", "skipped", "starting"},
    "starting": {"queued", "starting", "started", "jammed"},
    "started": {"started", "stopping", "done", "timeout"},
    "stopping": {"stopping", "stopped", "done"},
    "done": {"done", "success", "failure", "broken"},
    # terminal states (success/failure/broken/skipped/stopped/jammed/timeout)
    # are only allowed to transition to "queued" (manual re-run).
    "success":  {"queued"},
    "failure":  {"queued"},
    "broken":   {"queued"},
    "skipped":  {"queued"},
    "stopped":  {"queued"},
    "jammed":   {"queued"},
    "timeout":  {"queued"},
}


_VAR_REF_RE = re.compile(r"\$\{:([^}]+)\}")


class CI:
    def __init__(
        self,
        db,
        app_base_url="http://127.0.0.1:8000/py4ci",
        config_path=None,
        github_webhook_secret="",
    ):
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), "ci_config"
        )
        """read the configuration file with description of workers and tasks"""
        self.db = db
        self.app_base_url = app_base_url
        self.github_webhook_secret = github_webhook_secret or ""
        self.config_errors = []
        self.reload_config()
        self.define_tables()

    def reload_config(self):
        """Reload every .toml file under config_path. Collects validation
        errors into ``self.config_errors`` rather than raising, so a typo in
        one task only disables that task — the rest keep working and the UI
        can show the errors in the Reload-config flash.
        """
        self.config = {
            "administrators": [],
            "workers": {},
            "tasks": {},
            "variables": {},
        }
        errors = []
        for root, _, filenames in os.walk(self.config_path):
            for filename in sorted(filenames):
                if not filename.endswith(".toml"):
                    continue
                path = os.path.join(root, filename)
                rel = os.path.relpath(path, self.config_path)
                try:
                    with open(path, "rb") as fp:
                        config = tomllib.load(fp)
                except (OSError, tomllib.TOMLDecodeError) as err:
                    errors.append(f"{rel}: {err}")
                    continue
                self._merge_config(rel, config, errors)
        # variable substitution; rejected tasks land in errors[]
        variables = {str(k): str(v) for k, v in self.config["variables"].items()}
        self._substitute_variables(variables, errors)
        # final validation on tasks/workers; bad rows are dropped from the
        # config so the loop never tries to dispatch them.
        self._validate_workers(errors)
        self._validate_tasks(errors)
        self.config_errors = errors

    def _merge_config(self, rel, config, errors):
        """Merge one parsed TOML file into self.config; report unknown keys."""
        unknown = set(config) - CONFIG_TOP_KEYS
        if unknown:
            errors.append(
                f"{rel}: unknown top-level keys: {', '.join(sorted(unknown))}"
            )
        admins = config.get("administrators")
        if admins is not None:
            if not isinstance(admins, list):
                errors.append(f"{rel}: 'administrators' must be a list")
            else:
                self.config["administrators"] += [str(a) for a in admins]
        for section in ("workers", "tasks", "variables"):
            value = config.get(section)
            if value is None:
                continue
            if not isinstance(value, dict):
                errors.append(f"{rel}: '{section}' must be a table")
                continue
            self.config[section].update(value)

    def _substitute_variables(self, variables, errors):
        """Replace every ${:name} in every task command. Tasks referencing
        unknown variables are removed from the config and reported."""
        bad_tasks = []
        for name, task in self.config["tasks"].items():
            cmd = task.get("command", "")
            if not isinstance(cmd, str):
                errors.append(f"task '{name}': 'command' must be a string")
                bad_tasks.append(name)
                continue
            missing = []

            def _replace(match):
                key = match.group(1).strip()
                if key not in variables:
                    missing.append(key)
                    return match.group(0)
                return variables[key]

            new_cmd = _VAR_REF_RE.sub(_replace, cmd)
            if missing:
                errors.append(
                    f"task '{name}': unresolved variables: {', '.join(sorted(set(missing)))}"
                )
                bad_tasks.append(name)
            else:
                task["command"] = new_cmd
        for name in bad_tasks:
            self.config["tasks"].pop(name, None)

    def _validate_workers(self, errors):
        bad = []
        for name, worker in self.config["workers"].items():
            if not isinstance(worker, dict):
                errors.append(f"worker '{name}': must be a table")
                bad.append(name)
                continue
            unknown = set(worker) - WORKER_FIELDS
            if unknown:
                errors.append(
                    f"worker '{name}': unknown fields: {', '.join(sorted(unknown))}"
                )
                bad.append(name)
                continue
            host = worker.get("host")
            if not isinstance(host, str) or "@" not in host:
                errors.append(f"worker '{name}': 'host' must be 'user@host'")
                bad.append(name)
                continue
            queues = worker.get("queues") or ["default"]
            if not isinstance(queues, list) or not all(isinstance(q, str) for q in queues):
                errors.append(f"worker '{name}': 'queues' must be a list of strings")
                bad.append(name)
                continue
            worker["queues"] = queues
        for name in bad:
            self.config["workers"].pop(name, None)

    def _validate_tasks(self, errors):
        bad = []
        for name, task in self.config["tasks"].items():
            if not isinstance(task, dict):
                errors.append(f"task '{name}': must be a table")
                bad.append(name)
                continue
            unknown = set(task) - TASK_FIELDS
            if unknown:
                errors.append(
                    f"task '{name}': unknown fields: {', '.join(sorted(unknown))}"
                )
                bad.append(name)
                continue
            if not isinstance(task.get("command", ""), str) or not task.get("command", "").strip():
                errors.append(f"task '{name}': missing or empty 'command'")
                bad.append(name)
                continue
            # validate durations early
            for key in ("period", "debounce", "timeout", "retry_backoff", "retention"):
                if key in task and task[key] is not None:
                    try:
                        delta(task[key])
                    except ValueError as err:
                        errors.append(f"task '{name}': invalid {key}: {err}")
                        bad.append(name)
                        break
            else:
                if "retries" in task and not (
                    isinstance(task["retries"], int) and task["retries"] >= 0
                ):
                    errors.append(
                        f"task '{name}': 'retries' must be a non-negative integer"
                    )
                    bad.append(name)
        for name in bad:
            self.config["tasks"].pop(name, None)

    def define_tables(self):
        """define the required database tables"""
        db = self.db
        db.define_table(
            "task_run",
            Field("name", writable=False),
            Field(
                "status",
                default="queued",
                options=STATUSES,
                requires=IS_IN_SET(STATUSES),
            ),
            Field("description", "text", writable=True),
            Field("worker", writable=False),
            Field("priority", "integer", default=0),  # higher comes first
            Field("timeout", "float", default=delta(DEFAULT_TIMEOUT).total_seconds()),
            Field("trigger_event", "json", requires=IS_JSON()),
            Field("ancestors", "list:integer", writable=False),
            Field("descendants", "list:integer", writable=False),
            Field("queued_timestamp", "datetime", writable=False),
            Field("scheduled_timestamp", "datetime", writable=True),
            Field("start_timestamp", "datetime", writable=False),
            Field("stop_timestamp", "datetime", writable=False),
            # output_log is kept (legacy) for backward compatibility and as a
            # fallback when log_path can't be written. New runs write the log
            # to disk and store only the path + size here.
            Field("output_log", "text", writable=False),
            Field("output_data", "json", writable=False),
            Field("log_path", writable=False, readable=False),
            Field("log_size", "integer", default=0, writable=False),
            Field("group_id", writable=False),
            Field("callback_token", writable=False, readable=False),
            # Retry bookkeeping.
            Field("attempt", "integer", default=1, writable=False),
            Field("max_attempts", "integer", default=1, writable=False),
        )
        self.run_tags = Tags(db.task_run)
        # Single-loop guard. The loop thread/process holds this row open by
        # heartbeating expires_at; a fresh loop refuses to start if a still
        # un-expired row exists.
        db.define_table(
            "ci_loop_lock",
            Field("owner", writable=False),
            Field("acquired_at", "datetime", writable=False),
            Field("expires_at", "datetime", writable=False),
        )
        db.commit()

    # ----- run state machine ----------------------------------------

    def _transition(self, run, **updates):
        """Wrapper around update_record that validates the status transition.

        Updates without a 'status' key go through unchanged. An illegal
        transition is logged but not raised — we'd rather keep the loop alive
        than crash on a weird-state run, so we fall back to the current
        status and stamp ``output_log`` with a note.
        """
        if "status" in updates:
            old = run.status
            new = updates["status"]
            allowed = ALLOWED_TRANSITIONS.get(old, set())
            if new != old and new not in allowed:
                print(f"illegal transition {old} -> {new} on run {run.id}; ignoring")
                updates.pop("status")
        if updates:
            run.update_record(**updates)

    # ----- log file storage -----------------------------------------

    def _log_dir(self):
        path = os.path.join(os.path.dirname(__file__), "ci_logs")
        os.makedirs(path, exist_ok=True)
        return path

    def _log_path_for(self, run_id):
        return os.path.join(self._log_dir(), f"run{run_id}.log")

    def write_log(self, run, text):
        """Persist a run's log to disk and stamp the row with path + size.

        Falls back to storing the log in the row's output_log column if disk
        IO fails (e.g. read-only filesystem in a test environment).
        """
        text = text or ""
        path = self._log_path_for(run.id)
        try:
            with open(path, "w", encoding="utf-8", errors="replace") as fp:
                fp.write(text)
            run.update_record(log_path=path, log_size=len(text), output_log=None)
        except OSError as err:
            print(f"log write failed for run {run.id}: {err}; storing in DB")
            run.update_record(output_log=text, log_path=None, log_size=len(text))

    def read_log(self, run):
        """Return a run's log text from disk if a path is set, otherwise from
        the legacy ``output_log`` column. Returns empty string if neither."""
        if run.log_path:
            try:
                with open(run.log_path, "r", encoding="utf-8", errors="replace") as fp:
                    return fp.read()
            except OSError:
                return ""
        return run.output_log or ""

    def _delete_log_file(self, run):
        if run.log_path and os.path.exists(run.log_path):
            try:
                os.unlink(run.log_path)
            except OSError:
                pass

    # ----- single-loop lock -----------------------------------------

    def acquire_loop_lock(self, owner, ttl_seconds=60):
        """Try to claim the singleton loop lock. Returns True on success."""
        db = self.db
        now_ = now()
        row = db(db.ci_loop_lock.id > 0).select().first()
        if row and row.expires_at and row.expires_at > now_ and row.owner != owner:
            return False
        if not row:
            db.ci_loop_lock.insert(
                owner=owner,
                acquired_at=now_,
                expires_at=now_ + datetime.timedelta(seconds=ttl_seconds),
            )
        else:
            row.update_record(
                owner=owner,
                acquired_at=now_,
                expires_at=now_ + datetime.timedelta(seconds=ttl_seconds),
            )
        db.commit()
        return True

    def heartbeat_loop_lock(self, owner, ttl_seconds=60):
        """Extend the lock; returns True if we still own it."""
        db = self.db
        row = db(db.ci_loop_lock.id > 0).select().first()
        if not row or row.owner != owner:
            return False
        row.update_record(expires_at=now() + datetime.timedelta(seconds=ttl_seconds))
        db.commit()
        return True

    def release_loop_lock(self, owner):
        db = self.db
        row = db(db.ci_loop_lock.id > 0).select().first()
        if row and row.owner == owner:
            row.delete_record()
            db.commit()

    def busy_workers(self):
        """returns list of names of busy workers"""
        db = self.db
        query = db.task_run.status.belongs(BUSY_STATUSES)
        return [
            row.worker
            for row in db(query).select(db.task_run.worker, orderby="<random>")
        ]

    def available_worker(self, queues=None):
        """returns list of addresses of available workers matching one of the specified queues"""
        if not queues:
            queues = ["default"]
        elif isinstance(queues, str):
            queues = [queues]
        queues = set(queues)
        busy_workers = self.busy_workers()
        workers = self.config["workers"]
        for name, worker in workers.items():
            if not name in busy_workers and queues & set(worker["queues"]):
                return name
        return None

    def create_run(
        self,
        name,
        trigger_event=None,
        extra_tags=None,
        ancestors=None,
        scheduled_timestamp=None,
    ):
        """creates a new task given it name and a trigger event"""
        extra_tags = list(extra_tags) if extra_tags else []
        ancestors = list(ancestors) if ancestors else []
        now_ = now()
        db = self.db
        task = self.config["tasks"].get(name)

        # cannot create a run of a non-existant task
        if not task:
            return None

        # check if must skip because of debouncing
        if task.get("debounce"):
            # find prev task
            prev_task = (
                db(db.task_run.name == name)
                .select(orderby=~db.task_run.id, limitby=(0, 1))
                .first()
            )
            # if there is a previous task that actually started, debounce
            # against its start time. If start_timestamp is None (e.g. the run
            # never started) skip the debounce check rather than crashing.
            if (
                prev_task
                and prev_task.stop_timestamp
                and prev_task.start_timestamp
                and not scheduled_timestamp
            ):
                if prev_task.start_timestamp + delta(task["debounce"]) > now_:
                    return None

        # mark all queued task with the same name as skipped
        query = (db.task_run.name == name) & (db.task_run.status == "queued")
        db(query).update(status="skipped")

        # group runs by group_id
        ancestor_runs = db(db.task_run.id.belongs(ancestors)).select()
        gids = list(set(a.group_id for a in ancestor_runs))
        group_id = gids[0] if len(gids) == 1 and gids[0] else str(uuid.uuid4())
        extra_tags.append(group_id)

        # create a new task record
        max_attempts = max(1, int(task.get("retries", 0)) + 1)
        run_id = db.task_run.insert(
            name=name,
            description=task.get("description"),
            trigger_event=trigger_event,
            queued_timestamp=now_,
            scheduled_timestamp=scheduled_timestamp or now_,
            status="queued",
            priority=task.get("priority") or 0,
            timeout=delta(task.get("timeout") or DEFAULT_TIMEOUT).total_seconds(),
            ancestors=ancestors,
            group_id=group_id,
            attempt=1,
            max_attempts=max_attempts,
        )
        for tag in (task.get("tags") or []) + extra_tags:
            self.run_tags.add(run_id, str(tag))
        return run_id

    def try_start_run(self, run):
        """try start the run specified by the if a worker is available"""
        if run.status != "queued":
            return
        task = self.config["tasks"].get(run.name)
        if not task:
            return
        # pass queues as a list so available_worker doesn't accidentally split
        # a bare string into a set of its characters
        worker = self.available_worker(task.get("queues") or ["default"])
        if not worker:
            return
        self.assign_run_to_worker(run, worker)

    def assign_run_to_worker(self, run, worker_name):
        """assign the run to the worker (does not check queue match)"""
        if run.status != "queued":
            return
        # mint a one-time token the worker must present when it calls back
        callback_token = secrets.token_urlsafe(32)
        self._transition(
            run,
            status="starting",
            worker=worker_name,
            start_timestamp=now(),
            callback_token=callback_token,
        )
        self.db.commit()
        try:
            task = self.config["tasks"][run.name]
            code = f"export CI_RUN_ID={run.id}\n" + task["command"]
            callback = f"{self.app_base_url}/api/done/{run.id}/{callback_token}"
            input_data = self._assemble_input_data(run)
            worker = self.config["workers"][worker_name]
            self._transition(run, status="started", start_timestamp=now())
            self.write_log(run, "")  # truncate prior log on retry
            self.db.commit()
            Remote(worker["host"], f"{RUNS_FOLDER}/run{run.id}", callback).start(
                code, input_data
            )
        except Exception:
            tb = traceback.format_exc()
            print(tb)
            self.write_log(run, tb)
            self._transition(run, status="jammed", start_timestamp=now())
            self.db.commit()

    def try_finish_run(self, run):
        if run.status not in ("done", "starting", "started", "stopping", "timeout"):
            return
        try:
            worker = self.config["workers"][run.worker]
            status, log, data = Remote(
                worker["host"], f"{RUNS_FOLDER}/run{run.id}"
            ).finish()
            if run.status == "stopping":
                status = "stopped"
        except Exception:
            tb = traceback.format_exc()
            print(tb)
            status, log, data = "jammed", tb, None

        # record the event (log to disk, structured output to the row)
        self._transition(run, status=status, stop_timestamp=now(), output_data=data)
        self.write_log(run, log)

        # If this attempt failed and we have retries left, re-queue. We use
        # a new run record so the history is preserved, copying the attempt
        # counter forward and applying retry_backoff.
        task = self.config["tasks"].get(run.name)
        retry_failed = status in ("failure", "broken", "timeout", "jammed", "stopped")
        if (
            task
            and retry_failed
            and run.attempt < (run.max_attempts or 1)
            and task.get("retries", 0) > 0
        ):
            self._queue_retry(run, task)

        # if periodic, schedule next task (independent of retries)
        if task and task.get("period"):
            t1 = now()
            t0 = run.scheduled_timestamp or t1
            dt = delta(task["period"])
            cycles = math.ceil((t1 - t0).total_seconds() / dt.total_seconds())
            scheduled_timestamp = t0 + cycles * dt
            self.create_run(run.name, scheduled_timestamp=scheduled_timestamp)

        # if successful schedule dependent runs
        if status == "success":
            self._trigger_dependant_runs(run)

        self.db.commit()

    def _queue_retry(self, run, task):
        """Create a follow-on run continuing the same retry chain."""
        db = self.db
        backoff = delta(task.get("retry_backoff") or "0s")
        scheduled = now() + backoff
        next_attempt = (run.attempt or 1) + 1
        new_id = db.task_run.insert(
            name=run.name,
            description=task.get("description"),
            trigger_event={"retry_of": run.id},
            queued_timestamp=now(),
            scheduled_timestamp=scheduled,
            status="queued",
            priority=task.get("priority") or 0,
            timeout=delta(task.get("timeout") or DEFAULT_TIMEOUT).total_seconds(),
            ancestors=[run.id],
            group_id=run.group_id,
            attempt=next_attempt,
            max_attempts=run.max_attempts or 1,
        )
        for tag in task.get("tags") or []:
            self.run_tags.add(new_id, str(tag))
        self.run_tags.add(new_id, f"retry:{next_attempt}")
        if run.group_id:
            self.run_tags.add(new_id, run.group_id)
        run.update_record(descendants=(run.descendants or []) + [new_id])

    def _trigger_dependant_runs(self, run):
        db = self.db
        trigger_event = {"run_completion": run.id}
        # for every task
        for name, task in self.config["tasks"].items():
            triggered_by = task.get("triggered_by") or []
            # check if the completed run is triggering the task
            ancestor_task_names = [
                trigger["task"] for trigger in triggered_by if "task" in trigger
            ]
            triggered = run.name in ancestor_task_names
            if not triggered:
                continue

            # find the most recent completed possible ancestors
            ancestor_runs = [
                db((db.task_run.status == "success") & (db.task_run.name == aname))
                .select(orderby=~db.task_run.id, limitby=(0, 1))
                .first()
                for aname in ancestor_task_names
            ]
            # if we find one for each expected one
            ancestors = [ancestor.id for ancestor in ancestor_runs if ancestor]
            if len(ancestors) != len(ancestor_task_names):
                continue

            # create a new run
            drun_id = self.create_run(
                name, trigger_event, extra_tags=[], ancestors=ancestors
            )
            if not drun_id:
                continue

            # update all links
            for ancestor in ancestors:
                arun = db.task_run(ancestor)
                arun.update_record(descendants=(arun.descendants or []) + [drun_id])

    def _assemble_input_data(self, run):
        db = self.db
        data = {}
        data["trigger_event"] = run.trigger_event
        data["ancestor_runs"] = ancestor_runs = {}
        for ancestor in run.ancestors or []:
            arun = db.task_run(ancestor)
            if arun:
                ancestor_runs[arun.name] = arun.as_dict()
        return data

    # ----- step orchestration ---------------------------------------

    def step(self):
        """Single tick of the CI loop. Each sub-step is broken out so the
        flow is easy to read and individual phases can be tested.
        """
        self._requeue_orphan_starting()
        next_timeout = self._finish_or_timeout_running()
        self._dispatch_queued()
        self._prune_old_runs()
        return next_timeout

    def _requeue_orphan_starting(self):
        """A run still in 'starting' at the top of a tick means the previous
        tick crashed between mint-token and remote.start(). Re-queue it."""
        db = self.db
        db(db.task_run.status == "starting").update(status="queued")

    def _finish_or_timeout_running(self):
        """Walk ACTION_STATUSES rows: finish the ones the worker reported
        done, time out the ones past their wall-clock budget. Returns the
        next expected timeout time (used by the loop to size its sleep)."""
        db = self.db
        now_ = now()
        next_timeout = now_ + delta("1h")
        runs = db(db.task_run.status.belongs(ACTION_STATUSES)).select(
            orderby="<random>"
        )
        for run in runs:
            if run.start_timestamp is None:
                # malformed state — let the next tick decide
                continue
            expire = run.start_timestamp + delta(run.timeout)
            if run.status in ("done", "stopping") or expire < now_:
                print("finishing", run.id, run.status)
                self.try_finish_run(run)
            elif run.status == "started":
                next_timeout = min(next_timeout, expire)
        return next_timeout

    def _dispatch_queued(self):
        """Pick queued runs whose scheduled time has arrived and try to
        assign each to an available worker, oldest+highest-priority first.
        Runs referencing tasks the config no longer knows about are
        marked 'skipped' so the audit trail is preserved.
        """
        db = self.db
        now_ = now()
        query = db.task_run.scheduled_timestamp == None
        query |= db.task_run.scheduled_timestamp < now_
        query &= db.task_run.status == "queued"
        runs = db(query).select()

        def sort_key(run):
            return ((run.queued_timestamp or now_) - now_).total_seconds() - (
                run.priority or 0
            )

        runs.sort(sort_key)
        for run in runs:
            if run.name not in self.config["tasks"]:
                self._transition(run, status="skipped", stop_timestamp=now_)
                continue
            print("starting", run.id, run.status)
            self.try_start_run(run)

    def _prune_old_runs(self):
        """Delete terminal runs whose task declares a `retention` and whose
        stop_timestamp is older than that retention. No-op for tasks without
        a `retention` setting (runs are kept forever).

        Per-task makes sense here: a heartbeat task that fires every minute
        wants a short retention, while a release pipeline wants its history
        kept indefinitely.
        """
        db = self.db
        now_ = now()
        to_delete = []
        for task_name, task in self.config["tasks"].items():
            retention = task.get("retention")
            if not retention:
                continue
            try:
                cutoff = now_ - delta(retention)
            except ValueError:
                continue
            query = (
                (db.task_run.name == task_name)
                & db.task_run.status.belongs(TERMINAL_STATUSES)
                & (db.task_run.stop_timestamp < cutoff)
            )
            to_delete.extend(
                db(query).select(db.task_run.id, db.task_run.log_path)
            )
        if not to_delete:
            return
        ids = []
        for row in to_delete:
            ids.append(row.id)
            if row.log_path and os.path.exists(row.log_path):
                try:
                    os.unlink(row.log_path)
                except OSError:
                    pass
        tag_table = self.run_tags.tag_table
        db(tag_table.record_id.belongs(ids)).delete()
        db(db.task_run.id.belongs(ids)).delete()

    def post_run_done(self, run_id, token):
        """called by workers to report a run is completed (token-authenticated)"""
        run = self.db.task_run(run_id)
        if not run:
            return False
        expected = run.callback_token or ""
        if not expected or not hmac.compare_digest(str(expected), str(token or "")):
            return False
        # invalidate the token so the URL can't be replayed against this run
        run.update_record(status="done", callback_token=None)
        return True

    def verify_github_signature(self, raw_body, signature_header):
        """verify GitHub's X-Hub-Signature-256 header against the configured secret"""
        if not self.github_webhook_secret:
            # if no secret is configured we refuse to authenticate the request
            return False
        if not signature_header or not signature_header.startswith("sha256="):
            return False
        expected = hmac.new(
            self.github_webhook_secret.encode("utf-8"),
            raw_body if isinstance(raw_body, bytes) else raw_body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature_header.split("=", 1)[1])

    def post_git(self, data):
        """connect to github webhook and update record when receive notification"""
        try:
            url = data["repository"]["ssh_url"]
            branch = data["ref"].split("/", 2)[-1]
            commit = data["head_commit"]["id"]
        except KeyError as err:
            return str(err)
        # if there any task that references this repo
        triggered = False
        for name, task in self.config["tasks"].items():
            # skip disabled tasks
            if not task.get("enabled"):
                continue
            # check all triggers
            for trigger in task.get("triggered_by") or []:
                # if matching submit; tolerate a missing/None "branches" entry
                if (
                    trigger.get("ssh_url") == url
                    and branch in (trigger.get("branches") or [])
                ):
                    # create a task and pass it this data
                    if self.create_run(name, data, extra_tags=["commit:" + commit]):
                        triggered = True
        return f"No matching task for {url} {branch}" if not triggered else ""

    LIST_PAGE_SIZE = 100

    def _attach_tags(self, rows):
        """Mutate each run dict in `rows` to include a `tags` list (excluding
        the internal group_id uuid that is stored as a tag for indexing)."""
        if not rows:
            return
        tag_table = self.run_tags.tag_table
        ids = [r["id"] for r in rows]
        tag_rows = self.db(tag_table.record_id.belongs(ids)).select(
            tag_table.record_id, tag_table.tagpath
        )
        by_run = {}
        for tr in tag_rows:
            tag = tr.tagpath.strip("/")
            if _UUID_RE.match(tag):
                continue
            by_run.setdefault(tr.record_id, []).append(tag)
        for r in rows:
            r["tags"] = by_run.get(r["id"], [])

    def get_runs(
        self,
        after_id=0,
        names=None,
        tags=None,
        statuses=None,
        workers=None,
        words=None,
        ids=None,
        group_ids=None,
        latest=False,
        limit=None,
    ):
        """allows seraching for runs"""
        db = self.db

        def split(value, sep=None):
            return value.split(sep) if isinstance(value, str) else value

        if words:
            words = split(words)
            ids = []
            names = []
            group_ids = []
            statuses = []
            workers = []
            tags = []
            latest = False
            for word in words:
                if word.isdigit():
                    ids.append(int(word))
                elif word in self.config["tasks"]:
                    names.append(word)
                elif word.startswith("group:"):
                    group_ids.append(word[6:])
                elif word in STATUSES:
                    statuses.append(word)
                elif word in self.config["workers"]:
                    workers.append(word)
                elif word == "latest":
                    latest = True
                else:
                    tags.append(word)
        if ids:
            query = db.task_run.id.belongs(list(map(int, split(ids, ","))))
        elif not after_id:
            query = db.task_run.id > 0
        else:
            query = db.task_run.id < after_id
        if names:
            query &= db.task_run.name.belongs(names)
        if group_ids:
            query &= db.task_run.group_id.belongs(group_ids)
        if statuses:
            query &= db.task_run.status.belongs(split(statuses, ","))
        if workers:
            query &= db.task_run.worker.belongs(split(workers, ","))
        if tags:
            query &= self.run_tags.find(split(split(tags, ",")))
        fields = [f for f in db.task_run if not f.name in ("output_data", "output_log")]
        if latest:
            fname = db.task_run.name
            groups = db(fname).select(fname, distinct=True)
            rows = None
            for group in groups:
                new_rows = db(fname == group.name)(query).select(
                    *fields, orderby=~db.task_run.id, limitby=(0, 1)
                )
                rows = new_rows if rows is None else (rows | new_rows)
            rows = rows.sort(lambda row: row.name)
        else:
            page_size = limit if limit is not None else self.LIST_PAGE_SIZE
            rows = db(query).select(
                *fields, orderby=~db.task_run.id, limitby=(0, page_size)
            )
        return rows

    def expose_api(self, *uses):
        """this is mostly an example but can be called in controller"""
        db = self.db

        # Worker callback: authenticated by a per-run one-time token in the URL.
        # No session fixture is involved — workers don't have one.
        @action("api/done/<run_id:int>/<token>", method="POST")
        @action.uses(db)
        def post_run_done(run_id, token):
            if not self.post_run_done(run_id, token):
                raise HTTP(403)
            return ""

        # GitHub webhook: verify HMAC signature on the raw request body before
        # parsing JSON, so a forged payload can't trigger runs.
        @action("api/gitpost", method="POST")
        @action.uses(db)
        def post_git():
            raw = request.body.read()
            signature = request.headers.get("X-Hub-Signature-256", "")
            if not self.verify_github_signature(raw, signature):
                raise HTTP(401)
            try:
                payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            except ValueError:
                raise HTTP(400)
            return self.post_git(payload)

        @action("api/runs", method=["GET", "POST"])
        @action.uses(db, *uses)
        def get_runs():
            # accept both query params and a JSON body (POST) so callers with
            # long id lists don't bump against URL-length limits
            data = dict(request.query or {})
            if request.method == "POST":
                body = request.json or {}
                data.update(body)
            page_size = self.LIST_PAGE_SIZE
            rows = self.get_runs(limit=page_size + 1, **data).as_list()
            has_more = len(rows) > page_size
            rows = rows[:page_size]
            self._attach_tags(rows)
            return {"runs": rows, "has_more": has_more}

        @action("api/runs/<run_id:int>")
        @action.uses(db, *uses)
        def get_run(run_id=None):
            run = self.db.task_run(run_id)
            return run.as_dict() if run else {}


# Manual integration tests for Remote and CI live in apps/py4ci/tests/test_ci.py.

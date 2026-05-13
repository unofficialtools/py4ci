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


def delta(value):
    units = {"s": 1, "m": 60, "h": 3600, "d": 24 * 3600, "w": 7 * 24 * 3600}
    if isinstance(value, str) and any(key in value for key in units):
        value = int(value[:-1].strip()) * units.get(value[-1])
    return datetime.timedelta(seconds=int(value))


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
        # Build the manager script locally and ship it as a file so we don't have
        # to worry about quoting it through `echo`. We background task.sh and
        # capture its real PID with $! — using $$ would record the manager's
        # PID instead, which kill may not propagate to the child.
        local_manager_sh = f"{local_tmp_dir}.task.manager.sh"
        manager_lines = [
            "#!/bin/sh",
            "set -u",
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
            # try kill the task if still running (should never happen)
            connection.run(
                f"[[ -f {self.folder}/task.pid ]] && kill -9 `cat {self.folder}/task.pid` || true",
                hide=True,
            )
            # make the folder
            connection.run(f"mkdir -p {self.folder}", hide=True)
            # delete everything in it in case it existed
            connection.run(f"rm -rf {self.folder}/* || true", hide=True)
            # copy the script that needs to run
            connection.put(local_task_sh, f"{self.folder}/task.sh")
            # copy the input data
            connection.put(local_input, f"{self.folder}/task.input.json")
            # copy the manager script
            connection.put(local_manager_sh, f"{self.folder}/task.manager.sh")
            for payload in payloads or []:
                payload_dir = os.path.dirname(payload.remote_name)
                connection.run(f"mkdir -p {self.folder}/{payload_dir}", hide=True)
                connection.put(
                    payload.local_name, f"{self.folder}/{payload.remote_name}"
                )
            # run the manager script with dtach so it survives disconnect
            with connection.cd(self.folder):
                connection.run("chmod +x task.sh task.manager.sh", hide=True)
                connection.run("dtach -n task.socket -E ./task.manager.sh", hide=True)
        os.unlink(local_task_sh)
        os.unlink(local_input)
        os.unlink(local_manager_sh)

    def is_running(self):
        with Connection(
            self.host, user=self.user, connect_kwargs=CONNECT_ARGS
        ) as connection:
            res = connection.run(
                f"pgrep -F {self.folder}/task.pid > /dev/null",
                hide=True,
                warn=True,
            )
            return res.return_code == 0

    def finish(self):
        files = {}
        with Connection(
            self.host, user=self.user, connect_kwargs=CONNECT_ARGS
        ) as connection:
            with connection.cd(self.folder):
                # if we succeed in killing it, then it was still running
                if connection.run(
                    "[[ -f task.pid ]] && kill -9 `cat task.pid`", warn=True, hide=True
                ).failed:
                    status = "done"
                else:
                    status = "timeout"
                connection.run("./task.cleanup.sh", warn=True, hide=True)
                # retrieve the expected files
                for filename in ["task.log", "task.status"]:
                    local = f"/tmp/{self.folder}.{filename}"
                    try:
                        connection.get(f"{self.folder}/{filename}", local)
                    except Exception:
                        pass
                    else:
                        with open(local) as fp:
                            files[filename] = fp.read()
                        os.unlink(local)
            # cleanup
            connection.run(f"rm -rf {self.folder}", hide=True)
            print("cleaned")
        results = None
        if "task.status" in files:
            status = files["task.status"].strip()
        elif status != "timeout":
            status = "broken"
        log = files.get("task.log", "").replace("\x00", "\\x00")
        return status, log, results


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
        self.reload_config()
        self.define_tables()

    def reload_config(self):
        """load or reload the config file"""
        self.config = {
            "administrators": [],
            "workers": {},
            "tasks": {},
            "variables": {},
        }
        for root, _, filenames in os.walk(self.config_path):
            for filename in filenames:
                if not filename.endswith(".toml"):
                    continue
                path = os.path.join(root, filename)
                # tomllib requires the file be opened in binary mode
                with open(path, "rb") as fp:
                    config = tomllib.load(fp)
                if "administrators" in config:
                    self.config["administrators"] += config["administrators"]
                if "workers" in config:
                    self.config["workers"].update(config["workers"])
                if "tasks" in config:
                    self.config["tasks"].update(config["tasks"])
                if "variables" in config:
                    self.config["variables"].update(config["variables"])
        for _, task in self.config["tasks"].items():
            for key, value in self.config.get("variables", {}).items():
                task["command"] = task["command"].replace("${:%s}" % key, str(value))

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
            Field("output_log", "text", writable=False),
            Field("output_data", "json", writable=False),
            Field("group_id", writable=False),
            Field("callback_token", writable=False, readable=False),
        )
        self.run_tags = Tags(db.task_run)
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
        run.update_record(
            status="starting",
            worker=worker_name,
            start_timestamp=now(),
            callback_token=callback_token,
        )
        self.db.commit()
        try:
            # move input tasks
            task = self.config["tasks"][run.name]
            code = f"export CI_RUN_ID={run.id}\n" + task["command"]
            callback = f"{self.app_base_url}/api/done/{run.id}/{callback_token}"
            input_data = self._assemble_input_data(run)
            worker = self.config["workers"][worker_name]
            run.update_record(status="started", start_timestamp=now(), output_log=None)
            self.db.commit()
            Remote(worker["host"], f"{RUNS_FOLDER}/run{run.id}", callback).start(
                code, input_data
            )
        except Exception:
            tb = traceback.format_exc()
            print(tb)
            run.update_record(status="jammed", start_timestamp=now(), output_log=tb)
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

        # record the event
        run.update_record(
            status=status, stop_timestamp=now(), output_log=log, output_data=data
        )

        # if periodic, schedule next task
        task = self.config["tasks"].get(run.name)
        if task and task.get("period"):
            t1 = now()
            t0 = run.scheduled_timestamp or t1
            dt = delta(task["period"])
            cycles = math.ceil((t1 - t0).total_seconds() / dt.total_seconds())
            scheduled_timestamp = t0 + cycles * dt
            self.create_run(run.name, scheduled_timestamp=scheduled_timestamp)

        # if succesfull schedule dependent runs
        if status == "success":
            self._trigger_dependant_runs(run)

        # commit the work
        self.db.commit()

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

    def step(self):
        """
        function to be run every time a new actions occurs:
        - run created
        - run done
        - worker done
        """
        db = self.db
        # re-enqueue all task that failed to start in previous step
        db(db.task_run.status == "starting").update(status="queued")
        # collect and cleanup tasks that are done or timeout
        runs = db(db.task_run.status.belongs(ACTION_STATUSES)).select(
            orderby="<random>"
        )
        now_ = now()
        next_timeout = now_ + delta(3600)
        for run in runs:
            expire = run.start_timestamp + delta(run.timeout)
            if run.status in ("done", "stopping") or expire < now_:
                print("finishing", run.id, run.status)
                self.try_finish_run(run)
            # compute the time when the next timeout is expected
            elif run.status == "started":
                next_timeout = min(next_timeout, expire)

        # start queued tasks unless scheduled in the future
        query = db.task_run.scheduled_timestamp == None
        query |= db.task_run.scheduled_timestamp < now_
        query &= db.task_run.status == "queued"
        runs = db(query).select()

        # sort runs so oldest is first, 100 priority is a 100 seconds advantage
        def func(run):
            return ((run.queued_timestamp or now_) - now_).total_seconds() - (
                run.priority or 0
            )

        runs.sort(func)

        # loop over selected runs and start them
        for run in runs:
            if run.name not in self.config["tasks"]:
                # the task was removed from the config; preserve the audit
                # trail by marking the run skipped rather than deleting it
                run.update_record(status="skipped", stop_timestamp=now_)
                continue
            print("starting", run.id, run.status)
            self.try_start_run(run)
        return next_timeout

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

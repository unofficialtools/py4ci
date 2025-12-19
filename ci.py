"""
This file defines the ci logic

TODO:
docker integration: docker run alpine sh -c 'ls > task.ls'
display tags and make editable
"""

import datetime
import json
import math
import os
import socket
import time
import traceback
import uuid

import yaml
from fabric import Connection
from py4web import action, request
from pydal import DAL, Field
from pydal.validators import IS_IN_SET
from pydal.tools.tags import Tags

now = datetime.datetime.utcnow
never = datetime.datetime(2000, 1, 1, 0, 0, 0)

DEFAULT_TIMEOUT = 180
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
    "broken",  # done but missing task.output.json
    "success",  # found task.output.json
    "failure",  # possible value set by status in task.outout.json
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
        # TODO: write triggers info into remote
        with Connection(self.host, user=self.user, connect_kwargs=CONNECT_ARGS) as connection:
            # try kill the task if still running (should never happen)
            connection.run(
                f"[[ -f {self.folder}/task.pid ]] && kill -9 `cat {self.folder}/task.pid` || true",
                hide=True,
            )
            # make the folder
            connection.run(f"mkdir -p {self.folder}", hide=True)
            # delete everything in it in case it existed
            connection.run(f"rm -rf {self.folder}/* | true", hide=True)
            # copy the script that needs to run
            connection.put(local_task_sh, f"{self.folder}/task.sh")
            # copy the input data
            connection.put(local_input, f"{self.folder}/task.input.json")
            for payload in payloads or []:
                payload_dir = os.path.dirname(payload.remote_name)
                connection.run(f"mkdir -p {self.folder}/{payload_dir}", hide=True)
                connection.put(
                    payload.local_name, f"{self.folder}/{payload.remote_name}"
                )
            # create a manager script and run it with dtach do does die on disconnect
            with connection.cd(self.folder):
                connection.run("chmod +x task.sh", hide=True)
                script = " ; ".join(
                    [
                        "(echo $$ > task.pid) || true",
                        "(exec ./task.sh > task.log) || true",
                    ]
                )
                if self.callback:
                    script += f" ; curl -X POST {self.callback}"
                connection.run(f"echo '{script}' > task.manager.sh", hide=True)
                connection.run("chmod +x task.manager.sh", hide=True)
                connection.run("dtach -n task.socket -E ./task.manager.sh", hide=True)
        os.unlink(local_task_sh)
        os.unlink(local_input)

    def is_running(self):
        with Connection(self.host, user=self.user, connect_kwargs=CONNECT_ARGS) as connection:
            res = connection.run(
                f"ps -ef | grep `cat {self.folder}/task.pid` | grep -v 'grep' | wc -l",
                hide=True,
            )
            return res.stdout.strip() == "2"

    def finish(self):
        files = {}
        with Connection(self.host, user=self.user, connect_kwargs=CONNECT_ARGS) as connection:
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
                for filename in ["task.log", "task.output.json"]:
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
        if "task.output.json" in files:
            results = json.loads(files["task.output.json"])
            files["task.output.json"] = files["task.output.json"]
            status = "success"
            if results.get("status") == "failure":  # maybe account for more states
                status = "failure"
        elif status != "timeout":
            status = "broken"
        log = files.get("task.log", "").replace("\x00", "\\x00")
        return status, log, results


class CI:
    def __init__(self, db, app_base_url="http://127.0.0.1:8000/ci", config_path=None):
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), "ci_config"
        )
        """read the configuration file with description of workers and tasks"""
        self.db = db
        self.app_base_url = app_base_url
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
                if not filename.endswith(".yaml"):
                    continue
                path = os.path.join(root, filename)
                with open(path) as fp:
                    config = yaml.load(fp, Loader=yaml.Loader)
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
                task["command"] = task["command"].replace("${:%s}" % key, value)

    def define_tables(self):
        """define the required database tables"""
        db = self.db
        db.define_table(
            "task_run",
            Field("name", writable=False),
            Field("status", default="queued", options=STATUSES, requires=IS_IN_SET(STATUSES)),
            Field("description", "text", writable=True),
            Field("worker", writable=False),
            Field("priority", "integer", default=0),  # higher comes first
            Field("timeout", "integer", default=DEFAULT_TIMEOUT),
            Field("trigger_event", "json"),
            Field("ancestors", "list:integer", writable=False),
            Field("descendants", "list:integer", writable=False),
            Field("queued_timestamp", "datetime", writable=False),
            Field("scheduled_timestamp", "datetime", writable=True),
            Field("start_timestamp", "datetime", writable=False),
            Field("stop_timestamp", "datetime", writable=False),
            Field("output_log", "text", writable=False),
            Field("output_data", "json", writable=False),
            Field("group_id", writable=False),
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
        queues = set(queues if queues else ["default"])
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
        extra_tags=[],
        ancestors=[],
        scheduled_timestamp=None,
    ):
        """creates a new task given it name and a trigger event"""
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
            # if there is a previous task
            if prev_task and prev_task.stop_timestamp and not scheduled_timestamp:
                # and the next run is in the future
                if prev_task.start_timestamp + delta(task["debounce"]) > now_:
                    # do not create a new run
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
            timeout=task.get("timeout") or DEFAULT_TIMEOUT,
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
        worker = self.available_worker(task.get("queues", "default"))
        if not worker:
            return
        self.assign_run_to_worker(run, worker)

    def assign_run_to_worker(self, run, worker_name):
        """assign the run to the worker (does not check queue match)"""
        if run.status != "queued":
            return
        run.update_record(status="starting", worker=worker_name, start_timestamp=now())
        self.db.commit()
        try:
            # move input tasks
            task = self.config["tasks"][run.name]
            code = f"export CI_RUN_ID={run.id}\n" + task["command"]
            callback = f"{self.app_base_url}/api/done/{run.id}"
            input_data = self._assemble_input_data(run)
            worker = self.config["workers"][worker_name]
            Remote(worker["host"], f"{RUNS_FOLDER}/run{run.id}", callback).start(code, input_data)
            status, log = "started", None
        except Exception:
            tb = traceback.format_exc()
            print(tb)
            status, log = "jammed", tb
        run.update_record(status=status, start_timestamp=now(), output_log=log)
        self.db.commit()

    def try_finish_run(self, run):
        if run.status not in ("done", "started", "stopping", "timeout"):
            return
        try:
            worker = self.config["workers"][run.worker]
            status, log, data = Remote(worker["host"], f"{RUNS_FOLDER}/run{run.id}").finish()
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
            ancestor_runs[arun.name] = arun.as_json()
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
                run.delete_record()
                continue
            print("starting", run.id, run.status)
            self.try_start_run(run)
        return next_timeout

    def post_run_done(self, run_id):
        """called by workers to report a run is completed"""
        task = self.db.task_run(run_id)
        if task and task.status in ("starting", "started"):
            task.update_record(status="done")

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
                # if matching submit
                if trigger.get("ssh_url") == url and branch in trigger.get("branches"):
                    # create a task and pass it this data
                    if self.create_run(name, data, extra_tags=["commit:" + commit]):
                        triggered = True
        return f"No matching task for {url} {branch}" if not triggered else ""

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
                    statues.append(word)
                elif word in self.config["workers"]:
                    workers.append(word)
                elif word == latest:
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
            rows = db(query).select(*fields, orderby=~db.task_run.id, limitby=(0, 100))
        return rows

    def expose_api(self, *uses):
        """this is mostly an example but can be called in controller"""
        db = self.db

        @action("api/done/<run_id:int>", method="POST")
        @action.uses(db, *uses)
        def post_run_done(run_id):
            self.post_run_done(run_id)

        @action("api/gitpost", method="POST")
        @action.uses(db, *uses)
        def post_git():
            return self.post_git(request.json)

        @action("api/runs", method="GET")
        @action.uses(db, *uses)
        def get_runs():
            data = dict(request.query or {})
            return {"runs": self.get_runs(**data).as_list()}

        @action("api/runs/<run_id:int>")
        @action.uses(db, *uses)
        def get_run(run_id=None):
            run = self.db.task_run(run_id)
            return run.as_json() if run else {}


def test_remote(host="user@domain"):
    def check(res, status, msg, data):
        print("checking repose with status", status)
        try:
            assert res[0] == status
            assert msg in res[1]
            assert res[2] == data
        except:
            print(res, (status, msg, data))
            raise

    payloads = [
        {
            "local_name": os.path.join(os.path.dirname(__file__), "data/payload.zip"),
            "remote_name": "a/b/c.zip",
        }
    ]
    Remote(host).start("echo 'test1' && sleep 2 && echo '{}' > task.output.json")
    assert Remote(host).is_running()
    time.sleep(4)
    check(
        Remote(host).finish(),
        "success",
        "test1",
        {},
    )

    Remote(host).start("echo 'test2' && sleep 10 && echo {} > task.output.json")
    assert Remote(host).is_running()
    time.sleep(2)
    check(
        Remote(host).finish(),
        "timeout",
        "test2",
        None,
    )

    Remote(host).start("echo 'test3' && sleep 2 && false")
    assert Remote(host).is_running()
    time.sleep(4)
    check(
        Remote(host).finish(),
        "broken",
        "test3",
        None,
    )

    Remote(host).start(
        "echo 'test4' && sleep 2 && echo '{\"status\": \"failure\"}' > task.output.json"
    )
    assert Remote(host).is_running()
    time.sleep(4)
    check(
        Remote(host).finish(),
        "failure",
        "test4",
        {"status": "failure"},
    )
    print("done!")

def test_ci():
    db = DAL(
        "sqlite://storage.sqlite",
        folder=os.path.join(os.path.dirname(__file__), "databases"),
    )
    ci = CI(db=db)
    ci.step()
    ci.create_run("task1")
    ci.step()
    for run in db(db.task_run).select():
        print(run.id, run.status)
    print("sleeping")
    time.sleep(10)
    ci.step()
    for run in db(db.task_run).select():
        print(run.id, run.status)
    ci.create_run("task1")
    ci.step()
    for run in db(db.task_run).select():
        print(run.id, run.status)
    print("sleeping")
    time.sleep(1)
    while db(db.task_run.status.belongs(NON_TERMINAL_STATUSES)).count():
        time.sleep(1)
        ci.step()
    for run in db(db.task_run).select():
        print(run.id, run.status)
    db.commit()


if __name__ == "__main__":
    if True:
        test_remote()
    if False:
        test_ci()

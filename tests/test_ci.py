"""Manual/integration tests for py4ci. Not loaded by the app at runtime."""

import os
import time

from pydal import DAL

from ..ci import CI, NON_TERMINAL_STATUSES, Remote


def test_remote(host="user@domain"):
    def check(res, status, msg, data):
        try:
            assert res[0] == status
            assert msg in res[1]
            assert res[2] == data
        except AssertionError:
            print(res, (status, msg, data))
            raise

    Remote(host).start("echo 'test1' && sleep 2 && echo 'success' > task.status")
    assert Remote(host).is_running()
    time.sleep(4)
    check(Remote(host).finish(), "success", "test1", {})

    Remote(host).start("echo 'test2' && sleep 10 && echo 'success' > task.status")
    assert Remote(host).is_running()
    time.sleep(2)
    check(Remote(host).finish(), "timeout", "test2", None)

    Remote(host).start("echo 'test3' && sleep 2 && false")
    assert Remote(host).is_running()
    time.sleep(4)
    check(Remote(host).finish(), "broken", "test3", None)

    Remote(host).start("echo 'test4' && sleep 2 && echo 'failure' > task.status")
    assert Remote(host).is_running()
    time.sleep(4)
    check(Remote(host).finish(), "failure", "test4", {"status": "failure"})
    print("done!")


def test_ci():
    db = DAL(
        "sqlite://storage.sqlite",
        folder=os.path.join(os.path.dirname(__file__), "..", "databases"),
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
    test_remote()

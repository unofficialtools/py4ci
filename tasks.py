import os
import sys
import threading
import time
import traceback

from .common import db
from .controllers import ci
from .settings import APP_FOLDER

_LOG_PATH = os.path.join(APP_FOLDER, "ci.log")
_loop_thread = None
_loop_lock = threading.Lock()


def main_step():
    try:
        ci.step()
        db.commit()
    except Exception:
        print(traceback.format_exc())
        db.rollback()


def loop(logfile=None):
    """Run the CI step loop forever. If logfile is provided, redirect stdout/stderr to it."""
    if logfile is not None:
        sys.stdout = logfile
        sys.stderr = logfile
    while True:
        main_step()
        time.sleep(5)
        if logfile is not None:
            logfile.flush()


def start_background_loop():
    """Start the loop in a daemon thread. Idempotent — safe to call multiple times.

    Uses a thread (not multiprocessing) so DAL connections remain per-thread and
    the loop dies with the host process. Open the log in append mode so reloads
    do not truncate prior history.
    """
    global _loop_thread
    with _loop_lock:
        if _loop_thread is not None and _loop_thread.is_alive():
            return _loop_thread
        logfile = open(_LOG_PATH, "a", buffering=1)
        thread = threading.Thread(
            target=loop, kwargs={"logfile": logfile}, daemon=True, name="py4ci-loop"
        )
        thread.start()
        _loop_thread = thread
        return thread


if __name__ == "__main__":
    with open(_LOG_PATH, "a", buffering=1) as fp:
        loop(logfile=fp)

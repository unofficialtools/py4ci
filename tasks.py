import os
import signal
import socket
import sys
import threading
import time
import traceback
import uuid

from .common import db
from .controllers import ci
from .settings import APP_FOLDER

_LOG_PATH = os.path.join(APP_FOLDER, "ci.log")
_loop_thread = None
_loop_lock = threading.Lock()
_stop_event = threading.Event()

# unique to this process/thread instantiation — used as the lock owner so
# we can tell our heartbeats apart from a stale row owned by a crashed peer.
_LOOP_OWNER = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

LOOP_TICK_SECONDS = 5
LOOP_LOCK_TTL_SECONDS = 30  # short enough to recover quickly after a crash


def main_step():
    try:
        ci.step()
        db.commit()
    except Exception:
        print(traceback.format_exc())
        db.rollback()


def loop(logfile=None, owner=_LOOP_OWNER, stop_event=None):
    """Run the CI step loop until ``stop_event`` is set.

    Holds a single-process lock in the ci_loop_lock table; if another
    instance already owns the lock and is still heartbeating, this loop
    just waits and retries until the other side releases or expires.
    """
    if logfile is not None:
        sys.stdout = logfile
        sys.stderr = logfile
    stop_event = stop_event or _stop_event
    print(f"py4ci loop starting (owner={owner})")
    while not stop_event.is_set():
        if not ci.acquire_loop_lock(owner, ttl_seconds=LOOP_LOCK_TTL_SECONDS):
            # someone else owns the lock — wait and retry
            if logfile is not None:
                logfile.flush()
            stop_event.wait(timeout=LOOP_TICK_SECONDS)
            continue
        try:
            main_step()
            ci.heartbeat_loop_lock(owner, ttl_seconds=LOOP_LOCK_TTL_SECONDS)
        except Exception:
            print(traceback.format_exc())
        if logfile is not None:
            logfile.flush()
        stop_event.wait(timeout=LOOP_TICK_SECONDS)
    print("py4ci loop draining; releasing lock")
    try:
        ci.release_loop_lock(owner)
    except Exception:
        print(traceback.format_exc())


def start_background_loop():
    """Start the loop in a daemon thread. Idempotent."""
    global _loop_thread
    with _loop_lock:
        if _loop_thread is not None and _loop_thread.is_alive():
            return _loop_thread
        logfile = open(_LOG_PATH, "a", buffering=1)
        thread = threading.Thread(
            target=loop,
            kwargs={"logfile": logfile, "stop_event": _stop_event},
            daemon=True,
            name="py4ci-loop",
        )
        thread.start()
        _loop_thread = thread
        return thread


def stop_background_loop(timeout=None):
    """Signal the loop to drain and join the thread (used in shutdown hooks
    and tests)."""
    _stop_event.set()
    thread = _loop_thread
    if thread is not None:
        thread.join(timeout=timeout)


def _install_signal_handlers(stop_event):
    def _handler(signum, _frame):
        print(f"received signal {signum}; draining loop")
        stop_event.set()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            # signal() only works on the main thread of the main interpreter
            pass


if __name__ == "__main__":
    _install_signal_handlers(_stop_event)
    with open(_LOG_PATH, "a", buffering=1) as fp:
        loop(logfile=fp, stop_event=_stop_event)

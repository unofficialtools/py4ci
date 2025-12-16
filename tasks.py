import datetime
import os
import sys
import time
import traceback

from .settings import APP_FOLDER
from .common import db
from .controllers import ci

logfile = open(os.path.join(APP_FOLDER, "ci.log"), "w")

def main_step():
    try:
        ci.step()
        db.commit()
    except Exception:
        print(traceback.format_exc())
        db.rollback()


def loop():
    sys.stdout = logfile
    sys.stderr = logfile
    while True:
        main_step()
        time.sleep(5)
        logfile.flush()

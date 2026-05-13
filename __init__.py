# check compatibility
import py4web

assert py4web.check_compatible("1.20240501.1")

# by importing controllers you expose the actions defined in it
from . import controllers

# expose db (defined in common.py; models.py is intentionally empty — CI tables
# are created by CI.define_tables() inside controllers).
from .common import db

# Optionally run the CI loop inside this process as a background daemon thread.
# For production prefer running `python -m apps.py4ci.tasks` under systemd or
# supervisor so a single loop owns the task_run table; multiple py4web workers
# would otherwise each start their own loop and race on the same rows.
from . import settings

if settings.RUN_CI_LOOP_INPROCESS:
    from .tasks import start_background_loop

    start_background_loop()

# optional parameters
__version__ = "0.0.0"
__author__ = "you <you@example.com>"
__license__ = "anything you want"

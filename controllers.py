import datetime
import fnmatch
import json
import os
import uuid

from py4web import HTTP, URL, abort, action, redirect, request, response
from py4web.utils.form import Form
from pydal.validators import IS_IN_SET
from yatl.helpers import A

from . import settings
from .ci import CI
from .common import (
    T,
    auth,
    authenticated,
    cache,
    db,
    flash,
    logger,
    session,
    unauthenticated,
)

now = datetime.datetime.utcnow

ci = CI(db, app_base_url=settings.APP_BASE_URL)
ci.expose_api()

is_testing = os.environ.get("PY4WEB_TESTING") == "true"
requires_login = auth if is_testing else auth.user


@action("index")
@action.uses("index.html", auth)
def index():
    """The landing page"""
    if auth.user_id:
        redirect(URL("main"))
    return dict()


@action("main")
@action.uses("main.html", requires_login)
def main():
    """To search and view runs"""
    return dict(is_admin=is_admin())


def is_admin():
    """is this an administrator"""
    if is_testing:
        return True
    if auth.user_id:
        user = auth.get_user()
        emails = ci.config.get("administrators", [])
        for email in emails:
            if email == user.get("username") or email == user.get("email"):
                return True
    return False


def has_access(run):
    """checks whether the logged in user has read permissions"""
    if is_testing:
        return True
    if auth.user_id:
        user = auth.get_user()
        if is_admin():
            return True
        task = run and ci.config["tasks"].get(run.name)
        if task:
            emails = task.get("authorized_users", ["*"])
            if "*" in emails:
                return True
            if not run:
                return False
            for email in emails:
                email = str(email).lower()
                if email in [
                    str(user.get("username")).lower(),
                    str(user.get("email")).lower(),
                ]:
                    return True
    return False


@action("run/run<run_id:int>.output_log.txt")
@action.uses(db, requires_login)
def run_log(run_id):
    """get the log file if allowed"""
    run = db.task_run(run_id)
    if not run or not has_access(run):
        raise HTTP(404)
    response.content_type = "text"
    return run.output_log if run else ""


@action("run/run<run_id:int>.output_data.json")
@action.uses(db, requires_login)
def get_output_data(run_id):
    """get the json output if allowed"""
    run = db.task_run(run_id)
    if not run or not has_access(run):
        raise HTTP(404)
    response.content_type = "application/json"
    return json.dumps(run.output_data) if run else ""


@action("run/run<run_id:int>.trigger_event.json")
@action.uses(db, requires_login)
def get_trigger_event(run_id):
    """get the json output if allowed"""
    run = db.task_run(run_id)
    if not run or not has_access(run):
        raise HTTP(404)
    response.content_type = "application/json"
    return json.dumps(run.trigger_event) if run else ""


@action("create_run", method=["GET", "POST"])
@action.uses("create_run.html", requires_login)
def create_run():
    """Page to create a new run"""
    if not is_admin():
        raise HTTP(404)
    db.task_run.name.writable = True
    db.task_run.name.requires = IS_IN_SET(list(ci.config["tasks"]))
    db.task_run.queued_timestamp.default = now()
    db.task_run.scheduled_timestamp.default = now()
    db.task_run.group_id.default = str(uuid.uuid4())
    form = Form(db.task_run)
    if form.accepted:
        redirect(URL("main"))
    return dict(form=form)


@action("update_run/<run_id:int>", method=["GET", "POST"])
@action.uses("update_run.html", requires_login)
def update_run(run_id):
    """Page to edit an existent run"""
    if not is_admin():
        raise HTTP(404)
    form = Form(db.task_run, run_id)
    if form.accepted:
        redirect(URL("main"))
    return dict(form=form)


@action("api/rerun/<run_id:int>", method="POST")
@action.uses(requires_login)
def rerun(run_id):
    """Page reruns an run"""
    if not is_admin():
        raise HTTP(404)
    query = (db.task_run.id == run_id) & (
        ~db.task_run.status.belongs(["started", "starting"])
    )
    updated = db(query).update(status="queued")
    return {"updated": updated}


@action("api/config/reload", method="POST")
@action.uses(requires_login)
def config_reload():
    """Page reruns an run"""
    if not is_admin():
        raise HTTP(404)
    ci.reload_config()
    return {}


@action("api/config", method="GET")
@action.uses(requires_login)
def config_get():
    """Page reruns an run"""
    if not is_admin():
        raise HTTP(404)
    return ci.config


@action("users")
@action.uses("users.html", auth)
def users():
    if not is_admin():
        raise HTTP(404)
    users = db(db.auth_user).select(
        db.auth_user.id, db.auth_user.username, db.auth_user.sso_id, db.auth_user.email
    )
    return locals()

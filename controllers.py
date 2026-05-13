import datetime
import json
import os
import uuid

from py4web import HTTP, URL, action, redirect, request, response
from py4web.utils.form import Form
from py4web.utils.url_signer import URLSigner
from pydal.validators import IS_IN_SET

from . import settings
from .ci import CI, RUNS_FOLDER, Remote
from .common import auth, db, flash, groups, session

ADMIN_TAG = "admin"

now = datetime.datetime.utcnow

ci = CI(
    db,
    app_base_url=settings.APP_BASE_URL,
    github_webhook_secret=settings.GITHUB_WEBHOOK_SECRET,
)
ci.expose_api()

# CSRF-style signer for admin POST endpoints. The signed URL is rendered into
# main.html once per page load and reused by JS for every admin action.
admin_url_signer = URLSigner(session)

# Bypass auth for tests only when (a) we are running in development mode AND
# (b) the caller passes the matching shared secret.
_testing_secret = settings.TESTING_BYPASS_SECRET
is_testing = (
    settings.MODE == "development"
    and bool(_testing_secret)
    and os.environ.get("PY4WEB_TESTING") == _testing_secret
)
requires_login = auth if is_testing else auth.user


def is_admin():
    """Is the current request from a py4ci administrator?

    Source of truth is the `admin` Tag on the user row. ``sync_admin_tags()``
    keeps that tag in sync with the ``administrators`` list in TOML, so
    removing someone from the TOML on the next reload demotes them.
    """
    if is_testing:
        return True
    if not auth.user_id:
        return False
    if groups is not None and ADMIN_TAG in (groups.get(auth.user_id) or []):
        return True
    # First-time-login path: if the user matches the TOML list but the tag
    # hasn't been attached yet (e.g. nobody has run sync_admin_tags() since
    # they registered), promote them now.
    user = auth.get_user() or {}
    emails = ci.config.get("administrators", []) or []
    for email in emails:
        if email == user.get("username") or email == user.get("email"):
            if groups is not None:
                groups.add(auth.user_id, ADMIN_TAG)
            return True
    return False


def sync_admin_tags():
    """Make the auth_user.admin tag exactly equal to the TOML administrators.

    Called from config_reload and once at module import. Anyone whose
    username/email is in ``administrators`` gets the tag; anyone with the tag
    whose identity is no longer in ``administrators`` loses it.
    """
    if groups is None:
        return
    wanted = {str(a).lower() for a in (ci.config.get("administrators") or [])}
    rows = db(db.auth_user).select(
        db.auth_user.id, db.auth_user.username, db.auth_user.email
    )
    for user in rows:
        identities = {
            str(user.username or "").lower(),
            str(user.email or "").lower(),
        } - {""}
        has_tag = ADMIN_TAG in (groups.get(user.id) or [])
        should = bool(identities & wanted)
        if should and not has_tag:
            groups.add(user.id, ADMIN_TAG)
        elif has_tag and not should:
            groups.remove(user.id, ADMIN_TAG)


# Initial sync so the tag state matches the TOML the first time the app
# imports. Safe to call repeatedly.
try:
    sync_admin_tags()
except Exception:
    # Non-fatal: if auth_user isn't ready yet, the first config_reload will
    # do this work.
    pass


def has_access(run):
    """Whether the current user may view this run's logs and details."""
    if is_testing:
        return True
    if not auth.user_id:
        return False
    if is_admin():
        return True
    user = auth.get_user() or {}
    task = run and ci.config["tasks"].get(run.name)
    if not task:
        return False
    emails = task.get("authorized_users", ["*"])
    if "*" in emails:
        return True
    for email in emails:
        email = str(email).lower()
        if email in (
            str(user.get("username") or "").lower(),
            str(user.get("email") or "").lower(),
        ):
            return True
    return False


# ---------- pages ----------

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
    return dict(
        is_admin=is_admin(),
        admin_url_signer=admin_url_signer,
    )


# ---------- per-run text/json endpoints ----------

@action("run/run<run_id:int>.output_log.txt")
@action.uses(db, requires_login)
def run_log(run_id):
    """Plain-text run log (read from the on-disk log file if available)."""
    run = db.task_run(run_id)
    if not run or not has_access(run):
        raise HTTP(404)
    response.content_type = "text"
    return ci.read_log(run)


@action("run/run<run_id:int>.output_data.json")
@action.uses(db, requires_login)
def get_output_data(run_id):
    run = db.task_run(run_id)
    if not run or not has_access(run):
        raise HTTP(404)
    response.content_type = "application/json"
    return json.dumps(run.output_data) if run else ""


@action("run/run<run_id:int>.trigger_event.json")
@action.uses(db, requires_login)
def get_trigger_event(run_id):
    run = db.task_run(run_id)
    if not run or not has_access(run):
        raise HTTP(404)
    response.content_type = "application/json"
    return json.dumps(run.trigger_event) if run else ""


@action("api/runs/<run_id:int>/log_tail", method="GET")
@action.uses(db, requires_login)
def run_log_tail(run_id):
    """Live-tail the worker's log while a run is still in progress.

    The client polls this endpoint and tracks the byte offset it has
    already received. Terminal runs serve directly from the stored log;
    in-progress runs SSH to the worker for the latest bytes.
    """
    run = db.task_run(run_id)
    if not run or not has_access(run):
        raise HTTP(404)
    try:
        offset = int(request.query.get("offset", "0"))
    except (TypeError, ValueError):
        offset = 0
    if run.status in ("starting", "started", "stopping", "done"):
        worker = ci.config["workers"].get(run.worker)
        text = ""
        if worker:
            try:
                text = Remote(
                    worker["host"], f"{RUNS_FOLDER}/run{run.id}"
                ).tail_log()
            except Exception:
                text = ""
        live = True
    else:
        text = ci.read_log(run)
        live = False
    if offset and offset <= len(text):
        chunk = text[offset:]
    else:
        chunk = text
    return {
        "status": run.status,
        "live": live,
        "chunk": chunk,
        "size": len(text),
    }


# ---------- create / edit run pages ----------

@action("create_run", method=["GET", "POST"])
@action.uses("create_run.html", requires_login)
def create_run():
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
    """Edit only the fields users actually want to change. Everything else is
    hidden — the run's other state is bookkeeping owned by the loop.
    """
    if not is_admin():
        raise HTTP(404)
    editable = {"description", "priority", "timeout", "scheduled_timestamp"}
    for field in db.task_run:
        if field.name == "id" or field.name not in editable:
            field.readable = False
            field.writable = False
        else:
            field.writable = True
    form = Form(db.task_run, run_id)
    if form.accepted:
        redirect(URL("main"))
    return dict(form=form)


# ---------- admin POST endpoints (CSRF-protected via URLSigner) ----------

@action("api/admin/rerun", method="POST")
@action.uses(db, requires_login, admin_url_signer.verify())
def admin_rerun():
    if not is_admin():
        raise HTTP(403)
    body = request.json or {}
    try:
        run_id = int(body.get("run_id"))
    except (TypeError, ValueError):
        raise HTTP(400)
    query = (db.task_run.id == run_id) & (
        ~db.task_run.status.belongs(["started", "starting"])
    )
    updated = db(query).update(status="queued")
    return {"updated": updated}


@action("api/admin/config/reload", method="POST")
@action.uses(db, requires_login, admin_url_signer.verify())
def admin_config_reload():
    if not is_admin():
        raise HTTP(403)
    ci.reload_config()
    sync_admin_tags()
    return {"errors": ci.config_errors}


# ---------- read-only admin info ----------

@action("api/config", method="GET")
@action.uses(requires_login)
def config_get():
    if not is_admin():
        raise HTTP(404)
    return ci.config


@action("api/workers", method="GET")
@action.uses(db, requires_login)
def api_workers():
    """List configured workers and what each is currently running. Used by
    the workers page; visible to logged-in users (no sensitive data)."""
    if not is_admin():
        raise HTTP(404)
    current = {}
    busy_statuses = ("starting", "started", "stopping")
    rows = db(db.task_run.status.belongs(busy_statuses)).select()
    for r in rows:
        if r.worker:
            current.setdefault(r.worker, []).append(
                {"id": r.id, "name": r.name, "status": r.status}
            )
    out = []
    for name, worker in (ci.config.get("workers") or {}).items():
        out.append({
            "name": name,
            "host": worker.get("host"),
            "queues": worker.get("queues") or [],
            "running": current.get(name, []),
        })
    return {"workers": out, "config_errors": ci.config_errors}


@action("workers")
@action.uses("workers.html", requires_login)
def workers_page():
    if not is_admin():
        raise HTTP(404)
    return dict(is_admin=is_admin())


@action("readme")
@action.uses(requires_login)
def readme():
    """Serve the bundled README.md so the dashboard can render it client-side."""
    path = os.path.join(settings.APP_FOLDER, "README.md")
    try:
        with open(path, "r", encoding="utf-8") as fp:
            response.content_type = "text/markdown; charset=utf-8"
            return fp.read()
    except OSError:
        raise HTTP(404)


@action("users")
@action.uses("users.html", requires_login)
def users():
    if not is_admin():
        raise HTTP(404)
    users = db(db.auth_user).select(
        db.auth_user.id, db.auth_user.username, db.auth_user.sso_id, db.auth_user.email
    )
    return dict(users=users)

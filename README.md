# py4ci

**py4ci** is a small, self-contained Continuous Integration server written in
Python on top of [py4web](https://py4web.com). It dispatches shell commands
to remote Linux hosts over SSH, captures their output, and chains the results
together into pipelines.

It is designed as a lightweight alternative to systems like Jenkins or Bamboo
for deployments in the range of one worker on the same machine up to about a
thousand workers. It is single-process, single-database, and easy to read
end to end.

py4ci emphasises:

- minimal operational complexity (one process, one database, one config dir);
- zero-configuration workers (no agent — only `dtach` and `curl` on the worker);
- explicit, reproducible execution environments (you write the shell command);
- straightforward horizontal scaling (add a worker to a TOML file).

![Screenshot](static/media/screenshot01.png)

---

## Table of contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Installation](#installation)
- [Running the CI loop](#running-the-ci-loop)
- [Server settings (`settings.py`)](#server-settings-settingspy)
- [Configuration files (`ci_config/*.toml`)](#configuration-files-ci_configtoml)
  - [How files are loaded](#how-files-are-loaded)
  - [`administrators`](#administrators)
  - [`variables`](#variables)
  - [`workers`](#workers)
  - [`tasks`](#tasks)
  - [Triggers](#triggers)
  - [Time-based fields](#time-based-fields)
- [GitHub webhooks](#github-webhooks)
- [GitHub single sign-on](#github-single-sign-on)
- [Web UI](#web-ui)
- [Security model](#security-model)
- [Database](#database)

---

## What it does

You describe **tasks** in TOML — each task is essentially a shell script plus
some metadata (a name, a list of queues it can run on, a timeout, optionally
a periodic schedule, optionally a list of triggers).

You describe **workers** in TOML — each worker is an `ssh user@host` plus a
list of queue names it serves.

py4ci then runs an event loop that:

1. Watches for triggers (an HTTP POST to `/api/gitpost` from GitHub, the
   successful completion of an ancestor task, or a scheduled time arriving);
2. Creates a **run** record in its database for each triggered task;
3. Picks an idle worker whose queue list overlaps the task's queue list;
4. Copies a small shell script to the worker over SSH and starts it under
   `dtach` so it survives disconnect;
5. Waits for the worker to POST back to `/api/done/<run_id>/<token>` with
   a per-run callback token;
6. Pulls back the task's log and status file over SSH and stores them.

The web UI lets you search and inspect runs, see their full log, re-run a
failed run, and edit a run's metadata (timeout, schedule, etc.).

---

## Architecture

```
   ┌────────────────────────┐         ┌───────────────────────┐
   │   py4web (this app)    │         │    worker (Linux)     │
   │ ─────────────────────  │  ssh    │ ───────────────────── │
   │  controllers/ci.py     │ ──────► │ ci_runs/runN/task.sh  │
   │  • CI loop thread      │  scp    │ ci_runs/runN/task.log │
   │  • REST API            │ ◄──     │ ci_runs/runN/task.pid │
   │  • Web UI              │         │ (dtach detached)      │
   │  • SQLite db           │ ◄────── │ HTTP POST callback    │
   │                        │   curl  │  /api/done/N/<token>  │
   └────────────────────────┘         └───────────────────────┘
```

- The **CI loop** runs every five seconds and is responsible for transitioning
  runs through their state machine: `queued → starting → started →
  done/timeout/jammed → success/failure/broken`. By default the loop runs
  inside a daemon thread of the py4web process. For production, set
  `RUN_CI_LOOP_INPROCESS = False` in `settings.py` and run the loop as a
  separate process under `systemd` or `supervisor`:

      python -m apps.py4ci.tasks

- The **dispatch** mechanism is plain SSH. The server writes a small
  `task.manager.sh` to the worker, then runs
  `dtach -n task.socket -E ./task.manager.sh` so the script survives the
  SSH session ending. The manager script backgrounds `task.sh`, records its
  PID, waits for completion, writes `task.status` (`success` or `failure`),
  and POSTs back to the server.

- The **callback** carries a one-time URL-safe token (`secrets.token_urlsafe`)
  that the server minted when it started the run. The token is stored on the
  run row, included in the callback URL, and invalidated as soon as the
  callback is honored.

- All run metadata lives in a single SQLite table (`task_run`).

---

## Installation

Worker requirements (only):

- `dtach`
- `curl`
- a Linux account the server can reach with `ssh -A user@host` (no password).

Server requirements:

- Python 3.11+ (uses the stdlib `tomllib`),
- the packages in `requirements.txt` (`py4web`, `fabric`).

The simplest way is via [uv](https://github.com/astral-sh/uv):

```bash
mkdir apps
cd apps
git clone <py4ci-repo-url> py4ci
cd ..
uv run --with-requirements apps/py4ci/requirements.txt py4web run apps
```

Then visit `http://127.0.0.1:8000/py4ci/` in a browser.

---

## Running the CI loop

The CI loop is what actually drives runs forward. There are two ways to host
it:

1. **In-process daemon thread (default in development).** With
   `RUN_CI_LOOP_INPROCESS = True` (the default when `PY4WEB_MODE=development`),
   the loop starts automatically inside the py4web process. This is the
   simplest option but only safe when you run exactly one py4web worker —
   otherwise each py4web worker would start its own loop and they'd race on
   the same `task_run` rows.

2. **Separate process (production).** Set `RUN_CI_LOOP_INPROCESS = False` and
   run the loop yourself:

       python -m apps.py4ci.tasks

   under `systemd`, `supervisor` or similar. Output is appended to
   `ci.log` inside the app directory.

---

## Server settings (`settings.py`)

The only py4ci-specific settings you typically touch are:

| Setting | Meaning |
|---|---|
| `APP_BASE_URL` | The public URL of py4web as workers see it. Used to build the callback URL workers POST to when a run finishes. |
| `RUN_CI_LOOP_INPROCESS` | If `True`, start the CI loop as a daemon thread in the py4web process. Default: `True` in development mode, otherwise `False`. |
| `GITHUB_WEBHOOK_SECRET` | Shared secret used to verify `X-Hub-Signature-256` on `/api/gitpost`. Without this set, the endpoint refuses every request. |
| `TESTING_BYPASS_SECRET` | When `MODE == "development"` AND `PY4WEB_TESTING` env var matches this value, every request is treated as an admin. Don't set this in production. |
| `OAUTH2GITHUB_CLIENT_ID` / `OAUTH2GITHUB_CLIENT_SECRET` | Enables "sign in with GitHub" — see below. |
| `SMTP_*` | Standard py4web auth email settings, used only if you let users register with email/password. |

The rest of the standard py4web settings (DB, session, password complexity,
…) apply unchanged. See `settings.py` for the full list.

---

## Configuration files (`ci_config/*.toml`)

Everything else — administrators, workers, tasks, substitution variables —
lives in TOML files under `apps/py4ci/ci_config/`. py4ci does not have an
admin UI for editing this configuration; edit the files on disk and use
**Reload config** in the web UI to pick up changes without restarting.

### How files are loaded

- At startup (and whenever you click **Reload config**), py4ci recursively
  walks `ci_config/` and reads every file ending in `.toml`.
- File names and directory structure are **not significant** — only the
  top-level keys (`administrators`, `workers`, `tasks`, `variables`) matter.
- Configuration is **merged** across files:
  - `administrators` lists are concatenated;
  - `workers`, `tasks` and `variables` tables are merged key-by-key (later
    files override earlier ones for the same name).
- After all files are loaded, py4ci performs variable substitution: for each
  variable `key = "value"` in the `variables` table, any occurrence of
  `${:key}` inside a task's `command` is replaced with `value`.
- This means you can split things however you like (one file per task, one
  file per project, one big file…) and you can keep secrets in a separate
  file that's `.gitignore`d.

### `administrators`

```toml
# in file ci_config/administrators.toml
administrators = ["alice", "bob@example.com"]
```

- Each entry is matched against the logged-in user's **username** or
  **email**. The first time a logged-in user matches, they are promoted to
  the `admin` role via the pydal `Tags` table, so subsequent checks no
  longer need to consult this list.
- Administrators can see and edit every run, can create new runs from the
  web UI, and can reload the configuration.
- Non-administrators only see runs whose task lists them in
  `authorized_users`.

### `variables`

```toml
[variables]
build_target = "main"
some_secret  = "abracadabra"
```

- Variables are textually substituted into every task's `command` string.
- Reference them as `${:name}` in the command. Example:

      command = """
      git checkout ${:build_target}
      echo "$SOME_SECRET=${:some_secret}"
      """

- Substitution happens at config-load time, not per-run, so changing a
  variable requires a config reload.
- Referencing an undefined variable (e.g. `${:nope}`) is a configuration
  error: it surfaces in the **Reload config** flash and the task is
  rejected until the typo is fixed.

### `workers`

A worker is a name, a target host, and the list of queues it can serve.

```toml
[workers.worker01]
host   = "ci@127.0.0.1"
queues = ["default"]

[workers.fast01]
host   = "ci@192.168.1.23"
queues = ["default", "fast", "gpu"]
```

- `host` must be in `user@hostname` form. The user is the account py4web
  uses to SSH into the worker; the local server account must be able to
  `ssh user@hostname` without entering a password (use `ssh-agent` and
  `ssh -A`, or install your public key on the worker).
- A single physical machine can host multiple workers — just register them
  under different names. Each worker only runs one task at a time, so two
  workers on the same host gives you a concurrency of 2 there.
- `queues` is the list of queue names this worker can serve. A task can run
  on a worker if `set(task.queues) & set(worker.queues)` is non-empty.

### `tasks`

A task is the static description of a unit of work.

```toml
[tasks.my-heartbeat]
enabled         = true
description     = "log the date every hour"
queues          = ["default"]
tags            = ["heartbeat"]
period          = "1h"
debounce        = "1m"
priority        = 0
timeout         = "180s"
command         = """
date
"""
authorized_users = ["alice"]
```

| Key | Type | Meaning |
|---|---|---|
| `enabled` | bool | If `false`, GitHub webhook triggers are ignored (periodic / ancestor / manual triggers still work). |
| `description` | str | Free-text description shown in the UI. |
| `queues` | list[str] | Queues this task can run on. Defaults to `["default"]`. |
| `tags` | list[str] | Free-text labels. Shown as pills in the UI and searchable. |
| `period` | duration | If set, the task is automatically re-queued every `period`. See [Time-based fields](#time-based-fields). |
| `debounce` | duration | After a run starts, suppress new triggers for this task for `debounce`. Useful for noisy webhooks. |
| `priority` | int | Tasks with higher priority are picked from the queue first. `100` priority is worth a 100-second head start over a `0`-priority task. |
| `timeout` | duration | Maximum wall-clock time for the run. After this, the run is marked `timeout` and the process is killed. Default `180s`. |
| `command` | str | The shell script to execute on the worker. Run with `sh`, with the environment variable `CI_RUN_ID` set to the run's id. Use a TOML triple-quoted string for multiple lines. `${:var}` substitution applies. |
| `authorized_users` | list[str] | py4ci usernames/emails that may view this task's runs. Use `["*"]` to allow any logged-in user. Administrators always have access. |
| `triggered_by` | array of tables | See [Triggers](#triggers). |

The shell command receives a JSON file at `task.input.json` in its working
directory, containing the trigger event and the recent results of any
ancestor runs:

```json
{
  "trigger_event": { ... GitHub payload, or {"run_completion": <id>} ... },
  "ancestor_runs": { "task-name": { "id": 42, "status": "success", ... } }
}
```

### Triggers

A task is triggered (i.e. a new run is created and queued) in one of three
ways:

1. **Periodic.** Set `period = "<duration>"`. py4ci re-queues the task that
   long after the previous scheduled time.

2. **GitHub push.** Add a `[[tasks.<name>.triggered_by]]` block matching a
   repository URL and one or more branches:

       [tasks.deploy]
       enabled = true
       command = "..."

       [[tasks.deploy.triggered_by]]
       ssh_url  = "git@github.com:acme/web.git"
       branches = ["main", "staging"]

   When py4ci receives a verified webhook with a matching `ssh_url` and a
   ref ending in one of the listed branches, it creates a run and tags it
   `commit:<sha>`. `enabled = true` is required for webhook triggers.

3. **Successful ancestor run.** Reference another task by name:

       [tasks.smoke-tests]
       command = "..."

       [[tasks.smoke-tests.triggered_by]]
       task = "deploy"

   When `deploy` completes with status `success`, py4ci creates a
   `smoke-tests` run, links the ancestor's id into `ancestors`, links the
   descendant's id back into the ancestor's `descendants`, and assigns the
   same `group_id` so the whole pipeline is searchable by one tag.

A single task can have multiple `[[tasks.x.triggered_by]]` entries combining
any of the above.

### Time-based fields

`period`, `debounce` and `timeout` accept either a plain integer (seconds)
or a string with a unit suffix:

| Suffix | Unit |
|---|---|
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days |
| `w` | weeks |

Examples: `"30s"`, `"5m"`, `"2h"`, `"7d"`, `"1w"`.

---

## GitHub webhooks

py4ci exposes `POST /<app>/api/gitpost` for GitHub webhook delivery.

1. In your repository on GitHub, **Settings → Webhooks → Add webhook**.
2. **Payload URL**: `https://<your-app-base-url>/py4ci/api/gitpost`.
3. **Content type**: `application/json`.
4. **Secret**: choose a strong random string. **Set the same value as
   `GITHUB_WEBHOOK_SECRET` in `settings.py`** (or in the environment as
   `PY4CI_GITHUB_WEBHOOK_SECRET`).
5. **Which events?** "Just the push event" is enough.

py4ci verifies the `X-Hub-Signature-256` header on every webhook delivery
using HMAC-SHA256 with the configured secret. **If `GITHUB_WEBHOOK_SECRET`
is empty, the endpoint refuses every request with 401** — this is on
purpose, so you don't accidentally accept anonymous triggers.

On every verified push, py4ci compares the repository's `ssh_url` and the
pushed ref's branch against every task's `triggered_by` entries. Each
matching task gets a new run, tagged `commit:<sha>`.

---

## GitHub single sign-on

py4ci uses py4web's built-in OAuth2 plugin for GitHub. To enable it:

1. **Register an OAuth App on GitHub.**
   - Visit
     [github.com/settings/developers → OAuth Apps → New OAuth App](https://github.com/settings/developers).
   - **Application name**: anything (e.g. `py4ci`).
   - **Homepage URL**: `https://<your-app-base-url>/py4ci/`.
   - **Authorization callback URL**: must exactly match
     `https://<your-app-base-url>/py4ci/auth/plugin/oauth2github/callback`
   - GitHub will issue a **Client ID** and **Client Secret**.

2. **Configure py4ci.** In `settings.py` (or, better, in a
   `settings_private.py` you don't check in):

       OAUTH2GITHUB_CLIENT_ID     = "Iv1.xxxxxxxxxxxxxxxx"
       OAUTH2GITHUB_CLIENT_SECRET = "ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

   You may also want to disable the default email/password flow if GitHub is
   your only sign-in:

       DEFAULT_LOGIN_ENABLED = False

3. **Restart py4web.** A **Sign in with GitHub** button will appear on the
   `/py4ci/auth/login` page. The first time a user signs in, py4ci creates
   a local `auth_user` row populated from their GitHub profile (username,
   email, etc.).

4. **Promote them to admin.** Add their GitHub username or email to
   `administrators.toml`:

       administrators = ["alice", "alice@example.com"]

   The next time they sign in, py4ci sees the match, attaches the `admin`
   tag to their user row, and from then on the check is tag-based (so
   removing them from the TOML file does **not** demote them automatically
   — remove the `admin` tag from `auth_user_tag_groups` if you need to).

5. **(Optional) Promote others without TOML edits.** Once at least one
   admin exists, that admin can manage other users' admin tag directly via
   the database. A future version of the UI will expose this; today it's a
   one-liner against `auth_user_tag_groups`.

---

## Web UI

- `/py4ci/main` — main dashboard. Left sidebar lists runs (most recent first),
  with status swatch, name, queued/start/stop times and tag pills. The search
  box accepts plain words (matched as tags), a task name, a status name
  (e.g. `failure`), a worker name, a run id, `latest`, or `group:<id>`.
- Selecting a run shows its details on the right: status, history, ancestors,
  descendants, output log (auto-detects HTML output and renders it in a
  sandboxed iframe, otherwise linkifies URLs in plain text).
- The action buttons in the detail header — Trigger event, Logs, Data,
  Re-run, Edit — all open in a modal so the dashboard stays in context.
- The toolbar buttons **New run** / **Reload config** / **Refresh** are
  admin-only.

---

## Security model

- **Web UI access** is gated by py4web auth. Per-task access is further
  restricted by the task's `authorized_users` list (with `["*"]` meaning
  any logged-in user; admins always have access).
- **Worker callbacks** (`POST /api/done/<run_id>/<token>`) authenticate via
  a 256-bit URL-safe token minted per run by the server and invalidated as
  soon as the callback succeeds. There is no shared secret to leak; if a
  worker is compromised, only the token for its current run is exposed.
- **GitHub webhooks** (`POST /api/gitpost`) authenticate via
  `X-Hub-Signature-256` HMAC. Empty `GITHUB_WEBHOOK_SECRET` means **no
  webhook traffic is accepted**.
- **SSH** is the trust boundary between the server and workers; py4ci
  assumes the server account has passwordless SSH access to each worker and
  that the worker's filesystem under `ci_runs/` is private to that user.

---

## Database

All run state lives in one SQLite table, `task_run`. Notable columns:

| Column | Purpose |
|---|---|
| `name` | task name (foreign-keyless reference to `ci_config/tasks.toml`) |
| `status` | one of `queued`, `skipped`, `starting`, `started`, `jammed`, `timeout`, `stopping`, `stopped`, `done`, `broken`, `success`, `failure` |
| `worker` | the worker name that ran (or is running) this run |
| `priority`, `timeout` | copied from the task definition at create time |
| `trigger_event` | JSON of the event that created the run |
| `ancestors`, `descendants` | run-id lists |
| `queued_timestamp`, `scheduled_timestamp`, `start_timestamp`, `stop_timestamp` | the run's timeline |
| `output_log`, `output_data` | what the worker produced |
| `group_id` | pipeline grouping (a UUID shared across triggered runs) |
| `callback_token` | short-lived secret for the worker's done callback |

Tags (run tags and admin tags) live in companion tables managed by pydal's
`Tags` tool.

SQLite is fine for "small to medium" deployments; for higher throughput
swap the `DB_URI` in `settings.py` for Postgres and bump `DB_POOL_SIZE`.

# py4ci Review — TODO

Findings from a code review of `__init__.py`, `tasks.py`, `ci.py`, `controllers.py`, `common.py`, `settings.py`, `models.py`, `static/js/main.js`, and `templates/main.html`. Ordered by severity.

## Critical bugs

- [ ] **`__init__.py` spawns a child process at import time — unsafe.**
  ```python
  process = multiprocessing.Process(target=loop)
  process.start()
  ```
  - On macOS/Windows the start method is `spawn`, which re-imports `__init__.py` in the child — triggering another `process.start()`, recursively forking. Needs an `if __name__ == "__main__"` guard or (better) start the worker outside the app import.
  - Not marked `daemon=True`, so it leaks if py4web is restarted ungracefully.
  - The forked child shares the parent's DAL connection (`pool_size=1`, SQLite); concurrent use across processes will corrupt or lock the DB.
  - If py4web is run with multiple workers, each spawns its own loop process — multiple CI loops competing on the same `task_run` table without locking.

- [ ] **`ci.py:325` queue matching is broken when `queues` defaults to a string.**
  ```python
  worker = self.available_worker(task.get("queues", "default"))
  ```
  In `available_worker`: `queues = set(queues if queues else ["default"])` → `set("default")` becomes `{'d','e','f','a','u','l','t'}`. Pass `["default"]`, or change the call site to `task.get("queues") or ["default"]`.

- [ ] **`ci.py:259-316` mutable default args + caller mutation.**
  ```python
  def create_run(self, name, trigger_event=None, extra_tags=[], ancestors=[], ...):
      ...
      extra_tags.append(group_id)  # mutates the shared default!
  ```
  Calls without `extra_tags` reuse the same list across invocations — `group_id`s accumulate forever. Use `extra_tags=None` and copy inside. Same for `ancestors`.

- [ ] **`ci.py:550-555` `get_runs` has typo + dead branch.**
  ```python
  elif word in STATUSES:
      statues.append(word)         # NameError — should be 'statuses'
  ...
  elif word == latest:             # 'latest' is False here; compares word == False
      latest = True
  ```
  Searching by status raises `NameError`; the `latest` keyword is unreachable. Should be `"latest"`.

- [ ] **`api/done/<run_id>` and `api/gitpost` are unauthenticated and unsigned.**
  Anyone who reaches the public URL can mark any run as `done` or inject fake GitHub events. The worker callback URL should carry an HMAC token tied to the run, and the GitHub webhook should verify `X-Hub-Signature-256`.

- [ ] **`tasks.py:11` log file opened with `"w"` at module import.**
  Every reload truncates `ci.log`. Also, the file is opened in the parent and then `sys.stdout`/`sys.stderr` are reassigned only in `loop()` (the child process on Linux fork). On `spawn` platforms the parent ends up holding an unused file handle while the child opens its own. Use `"a"` (append) and open inside `loop()`.

## Real but lower-severity bugs

- [ ] **`ci.py:96`** `rm -rf {folder}/* | true` — pipes to `true` instead of suppressing errors. Should be `|| true`.

- [ ] **`ci.py:111`** `((echo $$ > task.pid) || true)` — `$$` is the subshell parent's PID (the manager), not `task.sh`. Killing it may not propagate. Use `exec` or capture `$!` after backgrounding.

- [ ] **`ci.py:131`** `is_running` checks `wc -l == "2"` — fragile and platform-dependent. `pgrep -F task.pid` is cleaner.

- [ ] **`ci.py:285`** debounce uses `prev_task.start_timestamp + delta(...)` — if the previous run never started (e.g. jammed), `start_timestamp` is `None` and this raises.

- [ ] **`ci.py:510`** `branch in trigger.get("branches")` — if `branches` is missing, returns `None` → TypeError. Guard with `or []`.

- [ ] **`ci.py:208`** `task["command"].replace("${:%s}" % key, value)` — if `value` is not a string (yaml int), `replace` raises. Coerce with `str(value)`.

- [ ] **`ci.py:436`** `arun.as_json()` returns a JSON *string* nested in a dict that is then JSON-encoded. Should be `arun.as_dict()`.

- [ ] **`ci.py:481`** `run.delete_record()` silently destroys history when a task name is removed from config. Probably want to mark `skipped` instead so the audit trail survives.

- [ ] **`controllers.py:31-32`** `is_testing = os.environ.get("PY4WEB_TESTING") == "true"` makes every route bypass auth and treat all users as admin. Easy to leave on in prod; consider gating via `settings.MODE == "development"` or rejecting it unless a known secret is also set.

- [ ] **`controllers.py:184`** `@action.uses("users.html", auth)` not `requires_login` — `auth` alone permits anonymous (just performs the check manually after). Use `requires_login` for consistency with the rest of the admin pages.

- [ ] **`controllers.py:191`** `return locals()` leaks every local variable into the template (including `db`, fixture objects). Return an explicit `dict(users=users)`.

## JavaScript / template issues

- [ ] **`main.js:108`** Uses Vue 2 syntax (`new Vue({el, data, methods})`). CLAUDE.md mandates Vue 3 `createApp().mount()`. Either the bundled `vue.min.js` is Vue 2 (inconsistent with the project guideline) or the code is broken against Vue 3.

- [ ] **Hardcoded URLs in `main.js`** (`../api/runs`, `../run/run<id>...`, `../api/rerun/<id>`, `../api/config/reload`). CLAUDE.md: "do NOT hardcode paths in JS. Pass URLs from controller via template variables."

- [ ] **`main.js:35-40`** Timezone code does `new Date(date+"Z")` — if the backend ever serializes with a trailing `Z`, you get `…ZZ` which `Date` parses as Invalid Date. Safer: parse with `new Date(date)` (server is UTC), then use `toLocaleString`.

- [ ] **`main.js:68`** Magic `100` couples client to server's `limitby=(0, 100)`. Have the server return `has_more` explicitly.

- [ ] **`main.js:99`** `axios.get("../api/runs?ids=" + run_ids.join(","))` — long polling list can blow URL length limits; switch to POST or batch.

- [ ] **`main.html:68`** `<a v-on:click="rerun(run)">` — `run` is undefined in this scope; should be `rerun(selected_run)`.

- [ ] **`main.html:91,95`** `v-bind:href="window.location.pathname + '#' + r.id"` — but the `_search` code parses hash as `words;run_id`, so a single `#<id>` won't be selected.

## Architectural suggestions

- [ ] **Move the loop out of `__init__.py`.** Use the built-in `Scheduler` (`USE_SCHEDULER`) or a separate process started by systemd / supervisor. Spawning subprocesses inside a WSGI app import is a long-term footgun.

- [ ] **Use `URLSigner` for `/api/done/<run_id>`** so workers prove the callback came from a run the controller actually started.

- [ ] **Use `Tags` for admin membership** rather than parsing emails from `administrators.yaml`; the project endorses that pattern and you already use `Tags` for `run_tags`.

- [ ] **Don't shell-out via Fabric to manage processes**; consider `dtach`/`tmux` ↔ proper job control or a small worker daemon over a socket.

- [ ] **`models.py` is empty** — the CI tables are defined inside `CI.define_tables()` instead. That's fine, but it means `__init__.py:17`'s `from .models import db` imports nothing useful; `db` lives in `common.py`. Either move the table defs into `models.py` or drop the misleading import comment.

- [ ] **Move `test_remote` and `test_ci`** out of `ci.py` into a `tests/` directory so they aren't shipped/imported with the app.

---

**Most urgent fixes:** process spawning in `__init__.py`, queue matching in `available_worker`, mutable defaults in `create_run`, `NameError` in `get_runs`, and auth on the worker/GitHub callback endpoints.

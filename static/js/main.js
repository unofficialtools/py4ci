"use strict";

// PY4CI_URLS is injected by the template. Expected keys:
//   runs                  -> GET/POST /api/runs
//   run_template          -> "/run/run{id}.{kind}"
//   log_tail_template     -> "/api/runs/{id}/log_tail"
//   update_template       -> "/update_run/{id}"
//   readme                -> "/readme"
// Admin-only (only injected when is_admin is true):
//   admin_rerun           -> signed POST /api/admin/rerun
//   admin_config_reload   -> signed POST /api/admin/config/reload
const URLS = window.PY4CI_URLS || {};

function runUrl(id, kind) {
    return URLS.run_template.replace("{id}", id).replace("{kind}", kind);
}
function logTailUrl(id) {
    return URLS.log_tail_template.replace("{id}", id);
}

function parseHash() {
    var parts = decodeURIComponent(window.location.hash.substring(1)).split(";");
    return { words: parts[0] || "", run_id: parts[1] || "" };
}

function fmtd(date) {
    if (!date) return "";
    var d = new Date(date.endsWith("Z") ? date : date + "Z");
    if (isNaN(d.getTime())) return "";
    var pad = function (n) { return n < 10 ? "0" + n : "" + n; };
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate())
        + " " + pad(d.getHours()) + ":" + pad(d.getMinutes());
}

// Strip ANSI SGR/CSI/OSC escape codes so colorized CI output (pytest, cargo,
// npm, etc.) renders as readable plain text instead of escape-soup.
const ANSI_RE = /\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_]/g;
function stripAnsi(s) {
    return (s || "").replace(ANSI_RE, "");
}

const RUNNING_STATUSES = new Set(["queued", "starting", "started", "stopping", "done"]);

const EMPTY_MODAL = {
    open: false,
    type: "",
    title: "",
    content: "",
    url: "",
    confirmLabel: "",
    confirm: null,
};

const App = {
    data() {
        return {
            loading: false,
            runs: [],
            words: parseHash().words,
            graph: "loading...",
            have_more: false,
            selected_run: null,
            ancestor_runs: [],
            descendant_runs: [],
            modal: Object.assign({}, EMPTY_MODAL),
            // Active log-tail bookkeeping.
            log_size: 0,
            log_polling_for: null,
        };
    },
    methods: {
        fmtd: fmtd,
        fmtt: function (date) { return fmtd(date).substring(11, 16); },

        select(run) {
            history.pushState(
                null, "",
                window.location.pathname + "#" + this.words + ";" + run.id
            );
            this.selected_run = run;
            this.ancestor_runs = [];
            this.descendant_runs = [];
            if (run.ancestors && run.ancestors.length > 0) {
                this._fetchRuns(run.ancestors).then((rows) => {
                    this.ancestor_runs = rows;
                });
            }
            if (run.descendants && run.descendants.length > 0) {
                this._fetchRuns(run.descendants).then((rows) => {
                    this.descendant_runs = rows;
                });
            }
            run.output_log = "";
            this.log_size = 0;
            this._tailLog(run);
        },

        _tailLog(run) {
            // Pull a chunk from the server (file for terminal runs, live tail
            // from the worker for in-progress runs). The poller (`update`)
            // keeps calling this so we get tail behavior without SSE.
            if (!run) return;
            this.log_polling_for = run.id;
            var params = { offset: this.log_size };
            axios.get(logTailUrl(run.id), { params: params }).then((res) => {
                if (this.selected_run !== run) return;  // user moved on
                var chunk = (res.data && res.data.chunk) || "";
                if (chunk) {
                    run.output_log = (run.output_log || "") + chunk;
                    this.log_size = res.data.size || (this.log_size + chunk.length);
                }
            }).catch(() => { /* network blip; next tick will retry */ });
        },

        _fetchRuns(ids) {
            return axios.post(URLS.runs, { ids: ids.join(",") })
                .then((res) => res.data.runs);
        },

        search() {
            var hash = parseHash();
            history.pushState(
                null, "",
                window.location.pathname + "#" + this.words + ";" + hash.run_id
            );
            this._search();
        },

        _search() {
            this.selected_run = null;
            this.runs = [];
            this.search_more();
        },

        search_more() {
            this.loading = true;
            this.have_more = false;
            var params = { words: this.words };
            if (this.runs.length) {
                params.after_id = this.runs[this.runs.length - 1].id;
            }
            axios.get(URLS.runs, { params: params }).then((res) => {
                this.loading = false;
                res.data.runs.forEach((run) => { run.output_log = ""; });
                this.runs = this.runs.concat(res.data.runs);
                this.have_more = !!res.data.has_more;
                if (!this.selected_run) {
                    if (this.runs.length > 0) this.selected_run = this.runs[0];
                    var run_id = parseHash().run_id;
                    var selected = this.runs.filter((r) => String(r.id) === String(run_id))[0];
                    if (selected) this.select(selected);
                    else if (this.selected_run) this._tailLog(this.selected_run);
                }
            });
        },

        addFilter(prefix, value) {
            // Append a chip to the search box. If the same token is already
            // there, clicking it toggles it off.
            var token = prefix ? prefix + ":" + value : value;
            var parts = (this.words || "").split(/\s+/).filter(Boolean);
            var idx = parts.indexOf(token);
            if (idx >= 0) parts.splice(idx, 1);
            else parts.push(token);
            this.words = parts.join(" ");
            this.search();
        },

        // ---- modal -------------------------------------------------
        openModal(opts) {
            this.modal = Object.assign({}, EMPTY_MODAL, opts, { open: true });
            document.documentElement.classList.add("modal-is-open");
        },
        closeModal() {
            this.modal = Object.assign({}, EMPTY_MODAL);
            document.documentElement.classList.remove("modal-is-open");
            this._editFirstLoad = false;
        },
        onDialogBackdropClick(e) {
            if (e.target.tagName === "DIALOG") this.closeModal();
        },

        // ---- action handlers --------------------------------------
        viewTriggerEvent(run) {
            axios.get(this.triggerEventUrl(run.id)).then((res) => {
                this.openModal({
                    type: "json",
                    title: "Trigger event — #" + run.id + " " + run.name,
                    content: JSON.stringify(res.data, null, 2),
                });
            });
        },
        viewLogs(run) {
            axios.get(this.outputLogUrl(run.id)).then((res) => {
                var text = typeof res.data === "string" ? res.data : String(res.data);
                var clean = stripAnsi(text);
                this.openModal({
                    type: this.isHtmlOutput(clean) ? "html" : "text",
                    title: "Output log — #" + run.id + " " + run.name,
                    content: clean,
                });
            });
        },
        viewData(run) {
            axios.get(this.outputDataUrl(run.id)).then((res) => {
                this.openModal({
                    type: "json",
                    title: "Output data — #" + run.id + " " + run.name,
                    content: JSON.stringify(res.data, null, 2),
                });
            });
        },
        rerun(run) {
            if (!run || !URLS.admin_rerun) return;
            this.openModal({
                type: "confirm",
                title: "Re-run #" + run.id,
                content: 'Re-run "' + run.name + '"? It will be re-queued.',
                confirmLabel: "Re-run",
                confirm: () => {
                    axios.post(URLS.admin_rerun, { run_id: run.id }).then(() => {
                        run.status = "queued";
                        run.output_log = "";
                        this.log_size = 0;
                        this.closeModal();
                    });
                },
            });
        },
        editRun(run) {
            this._editFirstLoad = true;
            this.openModal({
                type: "iframe",
                title: "Edit run — #" + run.id + " " + run.name,
                url: URLS.update_template.replace("{id}", run.id),
            });
        },
        viewHelp() {
            axios.get(URLS.readme).then((res) => {
                var src = typeof res.data === "string" ? res.data : String(res.data);
                var html = (typeof window.marked !== "undefined")
                    ? window.marked.parse(src, { gfm: true, breaks: false })
                    : "<pre>" + src
                        .replace(/&/g, "&amp;")
                        .replace(/</g, "&lt;")
                        .replace(/>/g, "&gt;") + "</pre>";
                this.openModal({
                    type: "markdown",
                    title: "py4ci — Help",
                    content: html,
                });
            });
        },
        onEditFrameLoad() {
            if (this._editFirstLoad) {
                this._editFirstLoad = false;
                return;
            }
            this.closeModal();
            this.update();
        },

        reload_config() {
            if (!URLS.admin_config_reload) return;
            this.openModal({
                type: "confirm",
                title: "Reload config",
                content: "Re-read ci_config/*.toml from disk?",
                confirmLabel: "Reload",
                confirm: () => {
                    axios.post(URLS.admin_config_reload).then((res) => {
                        var errs = (res.data && res.data.errors) || [];
                        if (errs.length) {
                            this.openModal({
                                type: "text",
                                title: "Config errors",
                                content: errs.join("\n"),
                            });
                        } else {
                            this.closeModal();
                        }
                    });
                },
            });
        },

        refresh_page() {
            window.location.reload();
        },

        update() {
            // Refresh in-flight rows (status/timestamps may have changed).
            var mapped = {};
            var run_ids = this.runs.filter((run) => {
                mapped[run.id] = run;
                return RUNNING_STATUSES.has(run.status);
            }).map((run) => run.id);
            if (run_ids.length > 0) {
                this._fetchRuns(run_ids).then((rows) => {
                    rows.forEach((run) => {
                        for (var key in run) {
                            if (key === "output_log") continue;
                            mapped[run.id][key] = run[key];
                        }
                    });
                });
            }
            // Live-tail the currently selected run if it's still active.
            if (this.selected_run && RUNNING_STATUSES.has(this.selected_run.status)) {
                this._tailLog(this.selected_run);
            }
        },

        triggerEventUrl(id) { return runUrl(id, "trigger_event.json"); },
        outputLogUrl(id) { return runUrl(id, "output_log.txt"); },
        outputDataUrl(id) { return runUrl(id, "output_data.json"); },
        hashFor(id) { return window.location.pathname + "#" + this.words + ";" + id; },

        isHtmlOutput(log) {
            if (!log) return false;
            return /^\s*(<!doctype html|<html\b|<body\b|<head\b)/i.test(log);
        },

        linkifyLog(log) {
            var clean = stripAnsi(log);
            if (!clean) return "";
            var escaped = clean
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#39;");
            return escaped.replace(
                /\b(https?:\/\/[^\s<>"'`]+[^\s<>"'`.,;:!?)\]}])/g,
                function (url) {
                    return '<a href="' + url + '" target="_blank" rel="noopener noreferrer">' + url + "</a>";
                }
            );
        },
    },
    mounted() {
        this._search();
        setInterval(this.update, 10000);
        window.addEventListener("hashchange", () => { window.location.reload(); });
        document.addEventListener("keydown", (e) => {
            if (e.key === "Escape" && this.modal.open) this.closeModal();
        });
    },
};

window.init = function () {
    var app = Vue.createApp(App);
    app.config.compilerOptions.isCustomElement = function (tag) {
        return tag.indexOf("-") !== -1;
    };
    var vm = app.mount("#vue");
    window.app = { vue: vm };
    return window.app;
};
window.init();

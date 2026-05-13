"use strict";

// PY4CI_URLS is injected by the template (main.html) so we never hardcode
// route paths in JS. Expected keys:
//   runs               -> /api/runs
//   run_template       -> "/run/run{id}.{kind}"  ({id} and {kind} substituted)
//   rerun_template     -> "/api/rerun/{id}"
//   config_reload      -> /api/config/reload
const URLS = window.PY4CI_URLS || {};

function runUrl(id, kind) {
    return URLS.run_template.replace("{id}", id).replace("{kind}", kind);
}

function rerunUrl(id) {
    return URLS.rerun_template.replace("{id}", id);
}

function parseHash() {
    var parts = decodeURIComponent(window.location.hash.substring(1)).split(";");
    return { words: parts[0] || "", run_id: parts[1] || "" };
}

function fmtd(date) {
    if (!date) return "";
    // The server emits naive UTC; parse as ISO and let toLocaleString format
    // it in the user's local zone. Avoids the "ZZ" double-suffix trap.
    var d = new Date(date.endsWith("Z") ? date : date + "Z");
    if (isNaN(d.getTime())) return "";
    var pad = function (n) { return n < 10 ? "0" + n : "" + n; };
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate())
        + " " + pad(d.getHours()) + ":" + pad(d.getMinutes());
}

const EMPTY_MODAL = {
    open: false,
    type: "",       // 'text' | 'json' | 'iframe' | 'confirm'
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
            if (!run.output_log) {
                axios.get(runUrl(run.id, "output_log.txt")).then((res) => {
                    run.output_log = res.data;
                });
            }
        },

        // Use POST for run-id lookups so we don't blow URL-length limits when
        // the polling list grows. The server endpoint accepts both.
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
                // server now tells us explicitly whether there are more rows
                this.have_more = !!res.data.has_more;
                if (!this.selected_run) {
                    if (this.runs.length > 0) this.selected_run = this.runs[0];
                    var run_id = parseHash().run_id;
                    var selected = this.runs.filter((r) => String(r.id) === String(run_id))[0];
                    if (selected) this.select(selected);
                }
            });
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
            // close when the user clicks the dialog backdrop (outside <article>)
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
                this.openModal({
                    type: this.isHtmlOutput(text) ? "html" : "text",
                    title: "Output log — #" + run.id + " " + run.name,
                    content: text,
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
            if (!run) return;
            this.openModal({
                type: "confirm",
                title: "Re-run #" + run.id,
                content: 'Re-run "' + run.name + '"? It will be re-queued.',
                confirmLabel: "Re-run",
                confirm: () => {
                    axios.post(rerunUrl(run.id)).then(() => {
                        run.status = "queued";
                        run.output_log = "";
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
            // Fetch the bundled README and render it client-side with marked.
            // If marked isn't loaded yet (older cached main.html), fall back
            // to plain text inside the modal so help is always reachable.
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
            // The edit form POSTs back to update_run/<id>, then redirects to
            // /main. The first iframe load is the form itself; any subsequent
            // load means the form was submitted, so close + refresh.
            if (this._editFirstLoad) {
                this._editFirstLoad = false;
                return;
            }
            this.closeModal();
            this.update();
        },

        reload_config() {
            if (confirm("reload?")) {
                axios.post(URLS.config_reload);
            }
        },

        refresh_page() {
            window.location.reload();
        },

        update() {
            var mapped = {};
            var run_ids = this.runs.filter((run) => {
                mapped[run.id] = run;
                return ["queued", "starting", "started", "stopping", "done"].indexOf(run.status) >= 0;
            }).map((run) => run.id);
            if (run_ids.length === 0) return;
            this._fetchRuns(run_ids).then((rows) => {
                rows.forEach((run) => {
                    for (var key in run) mapped[run.id][key] = run[key];
                });
            });
        },

        triggerEventUrl(id) { return runUrl(id, "trigger_event.json"); },
        outputLogUrl(id) { return runUrl(id, "output_log.txt"); },
        outputDataUrl(id) { return runUrl(id, "output_data.json"); },
        hashFor(id) { return window.location.pathname + "#" + this.words + ";" + id; },

        // Treat output as HTML if it starts (after whitespace) with a
        // recognizable HTML opener — covers DOCTYPE, <html>, <body>, etc.
        isHtmlOutput(log) {
            if (!log) return false;
            return /^\s*(<!doctype html|<html\b|<body\b|<head\b)/i.test(log);
        },

        // Plain-text output: HTML-escape, then turn http(s) URLs into
        // clickable anchors. Returned HTML is safe to v-html because the
        // user content was escaped before we injected the anchor markup.
        linkifyLog(log) {
            if (!log) return "";
            var escaped = log
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
    // <flash-alerts> is a web component from utils.js; don't try to resolve
    // it as a Vue component.
    app.config.compilerOptions.isCustomElement = function (tag) {
        return tag.indexOf("-") !== -1;
    };
    var vm = app.mount("#vue");
    window.app = { vue: vm };
    return window.app;
};
window.init();

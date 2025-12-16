"use strict";

window.init = function() {
    var app={};
    app.data = {};
    app.methods = {};
	app.data.loading = false;
    app.data.runs = [];
    app.data.words = "";
	app.data.graph = "loading...";
    app.data.have_more = false;	
    app.data.selected_run = null;
    app.data.ancestor_runs = [];
    app.data.descendant_runs = [];

    app.methods.select = function(run) {
		window.location.hash = app.vue.words+";"+run.id;
		app.vue.selected_run = run;
		app.vue.ancestor_runs = [];
		app.vue.descendant_runs = [];		
		if(run.ancestors && run.ancestors.length > 0) {
			var runs = run.ancestors.map(function(id){return ''+id;}).join(",");
			axios.get("../api/runs?ids="+runs).then(function(res){app.vue.ancestor_runs = res.data.runs;});
		}
		if(run.descendants && run.descendants.length > 0) {
			var runs = run.descendants.map(function(id){return ''+id;}).join(",");
			axios.get("../api/runs?ids="+runs).then(function(res){app.vue.descendant_runs = res.data.runs;});
		}
		if (!run.output_log) {
			axios.get("../run/run"+run.id+".output_log.txt").then(function(res){
				run.output_log = res.data;
			});
		}
    };
	app.methods.fmtd = function(date) {
		if (!date) return "";
		var offset = (new Date()).getTimezoneOffset();
		var utc = new Date(date+"Z");
		var local = new Date(utc.getTime() - offset*60000);
		return local.toISOString().substring(0,16).replace("T", " ");
	};
	app.methods.fmtt = function(date) {
		return app.methods.fmtd(date).substring(11,16);
	};
    app.methods.search = function() {		
		var run_id = window.location.hash.substring(1).split(";")[1];
		window.location.hash = app.vue.words+";"+run_id;
		app.methods._search();
    };
    app.methods._search = function() {		
		app.vue.selected_run = null;
		app.vue.runs = [];
		app.vue.search_more();
    };
    
    app.methods.search_more = function(callback) {
		app.vue.loading = true;
		app.vue.have_more = false;
		var run_id = 0;
		var url = "../api/runs?words=" + app.vue.words;
		if (app.vue.runs.length) {
			url = url + "&after_id=" + app.vue.runs[app.vue.runs.length - 1].id;
		}
		axios.get(url).then(function(res){
			app.vue.loading = false;
			res.data.runs.map(function(run){ run.output_log = ""; });
			app.vue.runs = app.vue.runs.concat(res.data.runs);
			app.vue.have_more = res.data.runs.length == 100;
			if (!app.vue.selected_run) {
				if (app.vue.runs.length > 0) app.vue.selected_run = app.vue.runs[0];
				// select a run if specificed in the hash 
				var run_id = window.location.hash.substring(1).split(";")[1];
				var selected_run = app.vue.runs.filter(function(run) { return run.id==run_id;})[0];
				if(selected_run) app.methods.select(selected_run);
			}
		});
    };
    app.methods.rerun = function(run) {
		if (confirm("rerun?")) {
			axios.post("../api/rerun/"+run.id).then(function(){
				run.status="queued";
				run.output_log="";
			});
		}		
    };	
    app.methods.reload_config = function(run) {
		if (confirm("reload?")) {
			axios.post("../api/config/reload");
		}
    };
    app.update = function() {
		var mapped = {};
		var run_ids = app.vue.runs.filter(function(run){
			mapped[run.id] = run;
			return ["queued","starting","started","stopping","done"].indexOf(run.status)>=0;
		}).map(function(run){return run.id;});

		if (run_ids.length>0) {
			axios.get("../api/runs?ids="+run_ids.join(",")).then(function(res){
				res.data.runs.map(function(run){
					for(var key in run) {
						mapped[run.id][key] = run[key];
					}					
				});
			});
		}
    };
    app.vue = new Vue({el:"#vue", data:app.data, methods:app.methods});
	app.vue.words = decodeURIComponent(window.location.hash.substring(1).split(";")[0]);
    app.vue._search();
    setInterval(app.update, 10000);
    return app;
};
window.app = window.init();

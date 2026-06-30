/* aichat-webix generative-UI tool registry.
   One global: aichatWebixWidgets = { register, has, template, mountWidget, destroyWidget, helpers }.
   Per-widget logic lives in webapp/aichat-webix/views/widgets/*.js; each widget
   file calls aichatWebixWidgets.register(toolName, {buildConfig,renderPill})
   at module load. This file owns only the registry plumbing and shared helpers. */

var aichatWebixWidgets = (function () {
    // toolName -> { buildConfig(item, sendToolResult, helpers), renderPill(item) }
    var registry = {};

    function register(toolName, def) {
        if (!toolName || !def || typeof def.buildConfig !== "function") {
            console.warn("[aichat-webix] invalid widget registration:", toolName, def);
            return;
        }
        registry[toolName] = def;
    }

    function has(toolName) {
        return !!registry[toolName];
    }

    function classFor(name) {
        // Kept for backwards-compat with existing CSS rules.
        if (name === "ConfirmAction") return "is-confirm";
        if (name === "PickOne") return "is-pickone";
        if (name === "AskUser") return "is-askuser";
        if (name === "PickMany") return "is-pickmany";
        if (name === "AskDate") return "is-askdate";
        if (name === "AskForm") return "is-askform";
        if (name === "PickRow") return "is-pickrow";
        if (name === "AskNumber") return "is-asknumber";
        if (name === "AskLongText") return "is-asklongtext";
        return "";
    }

    function template(item) {
        var cls = classFor(item.name);
        var prompt = (item.input && item.input.prompt) || "";
        if (item.result) {
            return "<div class=\"aichat-webix-widget " + cls + "\" data-row-id=\"" + item.id + "\">"
                + "<div class=\"aichat-webix-widget-card\">"
                + "<div class=\"aichat-webix-widget-prompt\">" + webix.template.escape(prompt) + "</div>"
                + renderPill(item)
                + "</div></div>";
        }
        return "<div class=\"aichat-webix-widget " + cls + "\" data-row-id=\"" + item.id + "\">"
            + "<div class=\"aichat-webix-widget-card\">"
            + "<div class=\"aichat-webix-widget-prompt\">" + webix.template.escape(prompt) + "</div>"
            + "<div class=\"aichat-webix-widget-host\" id=\"" + item.id + "-widget\"></div>"
            + "</div></div>";
    }

    // Universal pill renderer: if {_skipped:true}, show "skipped"; else
    // delegate to the per-widget renderPill registered alongside buildConfig.
    // A "(server error)" suffix is appended whenever item._serverError is
    // truthy, applied uniformly across all widgets and the skipped pill.
    function renderPill(item) {
        var serverErr = item._serverError
            ? "<span class=\"server-error\">(server error)</span>"
            : "";
        if (item.result && item.result._skipped === true) {
            return helpers.skippedPill() + serverErr;
        }
        var def = registry[item.name];
        if (def && typeof def.renderPill === "function") {
            return def.renderPill(item) + serverErr;
        }
        return "<span class=\"aichat-webix-widget-pill\"><span class=\"label\">answered</span>"
            + serverErr + "</span>";
    }

    function mountWidget(_refId, item, $state, sendToolResult) {
        if (!item || item.kind !== "widget" || item.result) return;
        var box = document.getElementById(item.id + "-widget");
        if (!box) return;
        if (box.children.length > 0 && $state.widgetViews[item.id]) return;
        if ($state.widgetViews[item.id]) {
            try { $state.widgetViews[item.id].destructor(); } catch (_) { /* ignore */ }
            delete $state.widgetViews[item.id];
        }
        var def = registry[item.name];
        if (!def) {
            box.innerHTML = "<div class=\"aichat-webix-widget-error\">Unknown widget: "
                + webix.template.escape(item.name) + "</div>";
            return;
        }
        try {
            var cfg = def.buildConfig(item, sendToolResult, helpers);
            var view = webix.ui(cfg, box);
            $state.widgetViews[item.id] = view;
        } catch (e) {
            console.warn("[aichat-webix] widget mount failed:", e);
            box.innerHTML = "<div class=\"aichat-webix-widget-error\">Could not render widget: "
                + webix.template.escape(e.message || String(e)) + "</div>";
        }
    }

    function destroyWidget(rowId, $state) {
        if (!$state) return;
        var v = $state.widgetViews[rowId];
        if (!v) return;
        try { v.destructor && v.destructor(); } catch (_) { /* ignore */ }
        delete $state.widgetViews[rowId];
    }

    // Shared helpers passed to every widget's buildConfig.
    var helpers = {
        // Defensively normalize a model-supplied value that should be an array
        // of objects (options/fields/columns/rows). The model sometimes emits
        // it as a JSON string or a non-array, which would throw on .map/.forEach
        // and fail the whole widget ("Could not render widget"). Returns a clean
        // array of objects (possibly empty).
        coerceArray: function (raw) {
            var a = raw;
            if (typeof a === "string") {
                try { a = JSON.parse(a); } catch (e) { return []; }
            }
            if (!Array.isArray(a)) return [];
            return a.filter(function (x) { return x && typeof x === "object"; });
        },
        // Returns a Webix view config for a single Skip button. Widgets place
        // this in their cols/rows so Skip is consistent across the family.
        skipRow: function (item, sendToolResult) {
            return {
                view: "button",
                value: "Skip",
                css: "aichat-webix-skip-btn",
                height: 28,
                click: function () {
                    try {
                        if (helpers.guardAlreadyAnswered(item.id)) return;
                        sendToolResult(item.id, item.toolUseId, { _skipped: true });
                    } catch (e) {
                        console.warn("[aichat-webix] Skip click failed:", e);
                        try { webix.message({ type: "error", text: "Could not submit selection." }); } catch (_) { /* ignore */ }
                    }
                },
            };
        },
        // HTML for the "skipped" answered-state pill.
        skippedPill: function () {
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">skipped</span>"
                + "</span>";
        },
        // Primary-styled button factory.
        submitButton: function (label, onClick) {
            return {
                view: "button",
                value: label || "Submit",
                css: "webix_primary",
                height: 34,
                click: onClick,
            };
        },
        // Defensive double-submit guard. Returns true if the thread row's
        // item.result is already set.
        guardAlreadyAnswered: function (rowId) {
            try {
                var lists = (webix.ui && webix.ui.views) || {};
                var keys = Object.keys(lists);
                for (var i = 0; i < keys.length; i += 1) {
                    var v = lists[keys[i]];
                    if (!v || !v.config || typeof v.config.id !== "string") continue;
                    if (v.config.id.indexOf(":thread") < 0) continue;
                    if (v.getItem && v.getItem(rowId)) {
                        return !!v.getItem(rowId).result;
                    }
                }
            } catch (_) { /* ignore */ }
            return false;
        },
    };

    var publicAPI = {
        register: register,
        has: has,
        template: template,
        mountWidget: mountWidget,
        destroyWidget: destroyWidget,
        helpers: helpers,
        // Internal — exposed so tests/diagnostics can inspect.
        _registry: registry,
    };

    return publicAPI;
})();

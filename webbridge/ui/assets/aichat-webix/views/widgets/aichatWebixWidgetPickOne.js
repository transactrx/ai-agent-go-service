/* PickOne widget. One button per option + a Skip button.
   Result shape: {value:string} | {_skipped:true}. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; PickOne widget skipped.");
        return;
    }
    aichatWebixWidgets.register("PickOne", {
        buildConfig: function (item, sendToolResult, helpers) {
            var options = helpers.coerceArray(item.input && item.input.options);
            var rows = options.map(function (opt) {
                return {
                    view: "button",
                    value: opt.label || opt.value,
                    css: "webix_primary",
                    height: 32,
                    click: function () {
                        try {
                            if (helpers.guardAlreadyAnswered(item.id)) return;
                            sendToolResult(item.id, item.toolUseId, { value: opt.value });
                        } catch (e) {
                            console.warn("[aichat-webix] PickOne click failed:", e);
                            try { webix.message({ type: "error", text: "Could not submit selection." }); } catch (_) { /* ignore */ }
                        }
                    },
                };
            });
            rows.push(helpers.skipRow(item, sendToolResult));
            return { rows: rows, height: options.length * 36 + 32 };
        },
        renderPill: function (item) {
            var val = (item.result && item.result.value) || "";
            var opts = (item.input && item.input.options) || [];
            if (!Array.isArray(opts)) opts = [];
            var match = opts.filter(function (o) { return o.value === val; })[0];
            var labelTxt = match ? match.label : val;
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">picked:</span>"
                + "<span class=\"value\">" + webix.template.escape(labelTxt) + "</span></span>";
        },
    });
})();

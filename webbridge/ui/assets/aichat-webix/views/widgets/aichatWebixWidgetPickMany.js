/* PickMany widget. Checkbox list + Submit + Skip.
   Result shape: {values:[string]} | {_skipped:true}. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; PickMany widget skipped.");
        return;
    }

    aichatWebixWidgets.register("PickMany", {
        buildConfig: function (item, sendToolResult, helpers) {
            var options = helpers.coerceArray(item.input && item.input.options);
            var minPicks = (typeof item.input.minPicks === "number") ? item.input.minPicks : 1;
            var maxPicks = (typeof item.input.maxPicks === "number") ? item.input.maxPicks : options.length;
            var selected = {}; // value -> true

            var checkboxes = options.map(function (opt) {
                return {
                    view: "checkbox",
                    labelRight: opt.label || opt.value,
                    labelWidth: 0,
                    height: 24,
                    on: {
                        onChange: function (newVal) {
                            if (newVal) selected[opt.value] = true;
                            else delete selected[opt.value];
                        },
                    },
                };
            });

            var submit = helpers.submitButton("Submit", function () {
                if (helpers.guardAlreadyAnswered(item.id)) return;
                var values = Object.keys(selected);
                if (values.length < minPicks || values.length > maxPicks) {
                    webix.message({
                        type: "error",
                        text: "Pick between " + minPicks + " and " + maxPicks + " option(s).",
                    });
                    return;
                }
                sendToolResult(item.id, item.toolUseId, { values: values });
            });

            return {
                rows: checkboxes.concat([
                    { cols: [submit, helpers.skipRow(item, sendToolResult)], height: 34 },
                ]),
                height: checkboxes.length * 24 + 40,
            };
        },
        renderPill: function (item) {
            var vals = (item.result && item.result.values) || [];
            var opts = (item.input && item.input.options) || [];
            if (!Array.isArray(opts)) opts = [];
            var labels = vals.map(function (v) {
                var m = opts.filter(function (o) { return o.value === v; })[0];
                return m ? m.label : v;
            });
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">picked:</span>"
                + "<span class=\"value\">" + webix.template.escape(labels.join(", ")) + "</span></span>";
        },
    });
})();

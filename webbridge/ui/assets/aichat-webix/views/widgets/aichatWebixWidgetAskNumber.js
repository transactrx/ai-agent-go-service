/* AskNumber widget. Slider (mode:"slider", needs min/max) or numeric input
   (mode:"input"). + Submit + Skip.
   Result shape: {value:number} | {_skipped:true}. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; AskNumber widget skipped.");
        return;
    }

    aichatWebixWidgets.register("AskNumber", {
        buildConfig: function (item, sendToolResult, helpers) {
            var mode = (item.input && item.input.mode) || "input";
            var min = (typeof item.input.min === "number") ? item.input.min : 0;
            var max = (typeof item.input.max === "number") ? item.input.max : 100;
            var step = (typeof item.input.step === "number") ? item.input.step : 1;
            var inputId = item.id + "-num";

            // App-standard input height from the active skin, so the numeric
            // input matches every other Webix form (no flex-stretch).
            var controlHeight = (webix.skin && webix.skin.$active
                && webix.skin.$active.inputHeight) || 30;
            var SLIDER_HEIGHT = 50; // slider needs room for its value title + track

            var isSlider = mode === "slider";
            var control;
            if (isSlider) {
                control = {
                    view: "slider", id: inputId, min: min, max: max, step: step,
                    value: Math.round((min + max) / 2),
                    title: "<b>#value#</b>",
                    height: SLIDER_HEIGHT,
                };
            } else {
                control = {
                    view: "text", type: "number", id: inputId,
                    attributes: { min: min, max: max, step: step },
                    height: controlHeight,
                };
            }

            var submit = helpers.submitButton("Submit", function () {
                if (helpers.guardAlreadyAnswered(item.id)) return;
                var raw = $$(inputId).getValue();
                var num = Number(raw);
                if (isNaN(num)) { webix.message({ type: "error", text: "Enter a number." }); return; }
                if (num < min || num > max) {
                    webix.message({ type: "error", text: "Value must be between " + min + " and " + max + "." });
                    return;
                }
                sendToolResult(item.id, item.toolUseId, { value: num });
            });

            return {
                rows: [
                    control,
                    { height: 16 }, // breathing room so the control isn't flush against the buttons
                    { cols: [submit, helpers.skipRow(item, sendToolResult)], height: 34 },
                ],
                margin: 0,
                padding: 0,
                height: (isSlider ? SLIDER_HEIGHT : controlHeight) + 16 + 34,
            };
        },
        renderPill: function (item) {
            var v = (item.result && item.result.value);
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">value:</span>"
                + "<span class=\"value\">" + webix.template.escape(String(v)) + "</span></span>";
        },
    });
})();

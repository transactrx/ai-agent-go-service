/* AskLongText widget. Multi-line textarea + Submit + Skip.
   Result shape: {text:string} | {_skipped:true}. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; AskLongText widget skipped.");
        return;
    }

    aichatWebixWidgets.register("AskLongText", {
        buildConfig: function (item, sendToolResult, helpers) {
            var placeholder = (item.input && item.input.placeholder) || "Type a long-form answer...";
            var minLen = (typeof item.input.minLength === "number") ? item.input.minLength : 0;
            var maxLen = (typeof item.input.maxLength === "number") ? item.input.maxLength : Infinity;
            var areaId = item.id + "-area";

            var submit = helpers.submitButton("Submit", function () {
                if (helpers.guardAlreadyAnswered(item.id)) return;
                var t = ($$(areaId).getValue() || "").trim();
                if (t.length < minLen) {
                    webix.message({ type: "error", text: "Need at least " + minLen + " characters." });
                    return;
                }
                if (t.length > maxLen) {
                    webix.message({ type: "error", text: "Stay under " + maxLen + " characters." });
                    return;
                }
                if (t === "") { webix.message({ type: "error", text: "Empty answer." }); return; }
                sendToolResult(item.id, item.toolUseId, { text: t });
            });

            return {
                rows: [
                    { view: "textarea", id: areaId, placeholder: placeholder, height: 120 },
                    { cols: [submit, helpers.skipRow(item, sendToolResult)], height: 34 },
                ],
                height: 170,
            };
        },
        renderPill: function (item) {
            var t = (item.result && item.result.text) || "";
            var short = t.length > 60 ? t.slice(0, 57) + "..." : t;
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">answered:</span>"
                + "<span class=\"value\">" + webix.template.escape(short) + "</span></span>";
        },
    });
})();

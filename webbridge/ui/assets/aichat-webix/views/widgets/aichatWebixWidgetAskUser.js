/* AskUser widget. Single-line text input + Send + Skip.
   Result shape: {text:string} | {_skipped:true}. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; AskUser widget skipped.");
        return;
    }

    function submit(input, item, sendToolResult, helpers) {
        if (helpers.guardAlreadyAnswered(item.id)) return;
        if (!input) return;
        var text = (input.getValue() || "").trim();
        if (text === "") return;
        sendToolResult(item.id, item.toolUseId, { text: text });
    }

    aichatWebixWidgets.register("AskUser", {
        buildConfig: function (item, sendToolResult, helpers) {
            var placeholder = (item.input && item.input.placeholder) || "Your answer...";
            var inputId = item.id + "-askuser-input";
            // App-standard input height from the active skin.
            var rowHeight = (webix.skin && webix.skin.$active
                && webix.skin.$active.inputHeight) || 30;
            return {
                rows: [
                    {
                        cols: [
                            {
                                view: "text",
                                id: inputId,
                                placeholder: placeholder,
                                on: {
                                    onAfterRender: function () {
                                        var node = this.getInputNode();
                                        if (!node || node.__aichatAskUserBound) return;
                                        node.__aichatAskUserBound = true;
                                        var self = this;
                                        webix.event(node, "keydown", function (e) {
                                            if ((e.key === "Enter" || e.keyCode === 13) && !e.shiftKey) {
                                                e.preventDefault();
                                                submit(self, item, sendToolResult, helpers);
                                            }
                                        });
                                    },
                                },
                            },
                            {
                                view: "button",
                                value: "Send",
                                width: 80,
                                css: "webix_primary",
                                click: function () { submit($$(inputId), item, sendToolResult, helpers); },
                            },
                            helpers.skipRow(item, sendToolResult),
                        ],
                        height: rowHeight,
                    },
                ],
                height: rowHeight,
            };
        },
        renderPill: function (item) {
            var t = (item.result && item.result.text) || "";
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">answered:</span>"
                + "<span class=\"value\">" + webix.template.escape(t) + "</span></span>";
        },
    });
})();

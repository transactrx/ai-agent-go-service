/* ConfirmAction widget. Buttons: Approve / Reject / Skip.
   Result shape: {approved:true} | {approved:false} | {_skipped:true}. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; ConfirmAction widget skipped.");
        return;
    }
    aichatWebixWidgets.register("ConfirmAction", {
        buildConfig: function (item, sendToolResult, helpers) {
            return {
                rows: [
                    {
                        cols: [
                            {
                                view: "button",
                                value: "Approve",
                                css: "webix_primary",
                                click: function () {
                                    if (helpers.guardAlreadyAnswered(item.id)) return;
                                    sendToolResult(item.id, item.toolUseId, { approved: true });
                                },
                            },
                            {
                                view: "button",
                                value: "Reject",
                                click: function () {
                                    if (helpers.guardAlreadyAnswered(item.id)) return;
                                    sendToolResult(item.id, item.toolUseId, { approved: false });
                                },
                            },
                            helpers.skipRow(item, sendToolResult),
                        ],
                        height: 36,
                    },
                ],
                height: 36,
            };
        },
        renderPill: function (item) {
            var approved = !!(item.result && item.result.approved);
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">answered:</span>"
                + "<span class=\"value " + (approved ? "is-approved" : "is-rejected") + "\">"
                + (approved ? "approved" : "rejected")
                + "</span></span>";
        },
    });
})();

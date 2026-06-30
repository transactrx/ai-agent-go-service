/* aichat-webix thread factory + dispatch template + sub-view mount orchestration.
   One global: aichatWebixThread = { build, dispatchTemplate, mountPendingSubViews, bindAll }. */

var aichatWebixThread = (function () {
    function dispatchTemplate(refId, item) {
        if (!item) return "";
        // "Show tool steps" toggle: tool_call/tool_result rows render an EMPTY
        // template (which collapses to zero height) when hidden, instead of being
        // removed via Webix list.filter. list.filter reorders hidden rows to the
        // end of the store, so revealing them later would drop them BELOW the
        // final answer and out of order. Rendering empty keeps each row in its
        // original chronological slot, so revealing shows them exactly where they
        // happened.
        var wnd = $$(refId);
        var showToolSteps = !!(wnd && wnd.$state && wnd.$state.showToolSteps);
        if (!aichatWebixTools.toolStepRowVisible(item, showToolSteps)) return "";
        switch (item.kind) {
        case "welcome":
        case "user":
        case "assistant":
        case "error":
        case "working":
            return aichatWebixBubbles.template(item);
        case "reasoning":
        case "tool_call":
        case "tool_result":
            return aichatWebixTools.template(item);
        case "attachment":
            return aichatWebixAttachments.template(item);
        case "widget":
            return aichatWebixWidgets.template(item);
        default:
            return "";
        }
    }

    function build(refId) {
        return {
            view: "list",
            id: refId + ":thread",
            css: "aichat-webix-thread",
            type: { height: "auto" },
            select: false,
            data: [],
            template: function (item) { return dispatchTemplate(refId, item); },
        };
    }

    function mountPendingSubViews(refId) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return;
        var list = $$(refId + ":thread");
        if (!list) return;
        var $state = wnd.$state;
        var sendToolResult = wnd.$sendToolResult; // wired by aichatWebixWnd
        list.data.each(function (item) {
            if (!item) return;
            if (item.kind === "attachment" && item.attachmentKind === "webix-chart") {
                aichatWebixAttachments.mountChart(refId, item, $state);
            } else if (item.kind === "widget" && !item.result) {
                aichatWebixWidgets.mountWidget(refId, item, $state, sendToolResult);
            }
        });
    }

    function bindAll(refId, listView) {
        aichatWebixTools.bindToggles(refId, listView);
        aichatWebixAttachments.bindExpandClicks(refId, listView);
    }

    return {
        build: build,
        dispatchTemplate: dispatchTemplate,
        mountPendingSubViews: mountPendingSubViews,
        bindAll: bindAll,
    };
})();

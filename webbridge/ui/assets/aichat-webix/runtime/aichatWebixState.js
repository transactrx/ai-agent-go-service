/* aichat-webix runtime state.
   One global: aichatWebixState = { createState, destroyAllSubViews }.
   No Webix view references at module scope — pure data + cleanup helpers.
   Each panel open starts a fresh session; no cross-reload persistence. */

var aichatWebixState = (function () {

    function createState(workflowId, indexName) {
        return {
            workflowId: workflowId || "",
            indexName: indexName || "",
            requestId: null,
            assistantRowId: null,
            sessionId: "",
            handle: null,
            pendingAttachments: [],
            attachmentsByToolUseId: {},
            toolCallStartByUseId: {},
            charts: {},
            widgetViews: {},
            aceEditors: {},
            expandWnd: null,
            showToolSteps: false,
            workingRowId: null,
            workingStartedAt: 0,
            workingTimer: null,
            stickToBottom: true,
            scrollNode: null,
        };
    }

    function destroyAllSubViews($state) {
        if (!$state) return;
        Object.keys($state.charts || {}).forEach(function (k) {
            try { $state.charts[k] && $state.charts[k].destructor && $state.charts[k].destructor(); } catch (_) { /* ignore */ }
        });
        Object.keys($state.widgetViews || {}).forEach(function (k) {
            try { $state.widgetViews[k] && $state.widgetViews[k].destructor && $state.widgetViews[k].destructor(); } catch (_) { /* ignore */ }
        });
        Object.keys($state.aceEditors || {}).forEach(function (k) {
            var arr = $state.aceEditors[k] || [];
            arr.forEach(function (ed) { try { ed && ed.destroy && ed.destroy(); } catch (_) { /* ignore */ } });
        });
        if ($state.expandWnd) { try { $state.expandWnd.close(); } catch (_) { /* ignore */ } }
        if ($state.workingTimer) { try { clearInterval($state.workingTimer); } catch (_) { /* ignore */ } }
        $state.charts = {};
        $state.widgetViews = {};
        $state.aceEditors = {};
        $state.expandWnd = null;
        $state.workingRowId = null;
        $state.workingTimer = null;
    }

    return { createState: createState, destroyAllSubViews: destroyAllSubViews };
})();

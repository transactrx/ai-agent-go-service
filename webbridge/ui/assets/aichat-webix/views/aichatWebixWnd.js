/* aichat-webix window — the public entry point.
   All helpers (aichatWebixSendMessage, aichatWebixCancelStream, ...) use the
   `aichatWebix*` prefix so they don't collide with same-named top-level
   functions in the legacy webapp/aichat/views/aichatViewerWnd.js (sendMessage,
   cancelStream, newSession). Without this prefix the later-loaded script
   wins in the global scope and the legacy chat clicks invoke MY handlers. */

// Base chat-window title; the Search name is appended as " - <searchName>"
// so the chat header mirrors its parent Search window for every workflow.
var AICHAT_WEBIX_TITLE_BASE = "AI Assistant";

// aichatWebixLogChart mirrors the frame reducer's logChart so the WS-level
// callbacks (onError/onClose) can trace assistant/chart-bubble lifecycle with
// the same greppable "aichat-chart:" prefix. On by default; set
// window.AICHAT_CHART_DEBUG = false to silence.
function aichatWebixLogChart(msg) {
    try { if (typeof window !== "undefined" && window.AICHAT_CHART_DEBUG === false) return; } catch (_) { /* ignore */ }
    try { console.log("aichat-chart: " + msg); } catch (_) { /* ignore */ }
}

// sessionDetails is accepted for signature compatibility with the legacy
// displayAIChatViewerWnd(refId, sessionDetails, opts) call shape used in app.js,
// but the Webix engine reads session info from webix.storage.local + opts only.
function displayAIChatWebixWnd(refId, _sessionDetails, opts) {
    var workflowId = (opts && opts.workflowId) || "";
    var indexName = (opts && opts.indexName) || "";
    var searchName = (opts && opts.searchName) || "";
    var title = AICHAT_WEBIX_TITLE_BASE + (searchName ? " - " + searchName : "");

    var wnd = webix.ui({
        id: refId,
        view: "window",
        width: 800,
        height: 700,
        move: true,
        escHide: false,
        resize: true,
        toFront: true,
        position: "center",
        css: "aichat-webix-root",
        on: {
            onViewResize: function () { aichatWebixAdjustCharts(refId); },
            onDestruct: function () {
                try {
                    if (wnd.$state) {
                        if (wnd.$state.handle && wnd.$state.handle.cancel) wnd.$state.handle.cancel();
                        aichatWebixState.destroyAllSubViews(wnd.$state);
                    }
                } catch (_) { /* ignore */ }
            },
        },
        head: {
            view: "toolbar",
            cols: [
                { view: "label", template: function () { return "&nbsp;" + webix.template.escape(title); } },
                {
                    view: "checkbox",
                    id: refId + ":showToolSteps",
                    css: "aichat-show-tool-steps",
                    labelRight: "Show details",
                    value: 0,
                    width: 160,
                    labelWidth: 0,
                    labelRightWidth: 140,
                    on: {
                        // Static label (no Hide/Show relabel — that read as weird
                        // on a checkbox). Checked = reveal the tool_call /
                        // tool_result diagnostic cards; unchecked (default) = hide
                        // them so the user sees only reasoning + the final answer.
                        onChange: function (newVal) {
                            var w = $$(refId);
                            if (w && w.$state) w.$state.showToolSteps = !!newVal;
                            aichatWebixApplyToolStepVisibility(refId);
                        },
                    },
                },
                { view: "button", label: "New session", width: 120, click: function () { aichatWebixNewSession(refId); } },
                // Toolbar Cancel removed (2026-06-04): duplicate of the
                // composer's Send→Stop swap, which is the single abort control.
                {
                    view: "icon",
                    icon: "mdi mdi-tune",
                    tooltip: "AI Prompt Admin",
                    hidden: !aichatPromptAdminAllowed(),
                    // Target THIS chat's workflow (e.g. eprescribeSearch), not a
                    // hardcoded one — the admin edits that workflow's prompt.
                    click: function () { displayAichatPromptAdminWnd(workflowId); },
                },
                // Shared menu-aware maximize/restore toggle (commons/utils.js).
                getMaximizeWindowIcon(),
                { view: "icon", icon: "mdi mdi-close", click: function () { this.getTopParentView().close(); } },
            ],
        },
        body: {
            view: "form",
            padding: 0,
            elements: [
                aichatWebixThread.build(refId),
                aichatWebixComposer.build(refId, {
                    onSend: function (text, attachments) { aichatWebixSendMessage(refId, text, attachments); },
                    onCancel: function () { aichatWebixCancelStream(refId); },
                }),
            ],
        },
    });

    wnd.$state = aichatWebixState.createState(workflowId, indexName);
    wnd.$sendToolResult = function (rowId, toolUseId, result) {
        aichatWebixSendToolResultClient(refId, rowId, toolUseId, result);
    };

    wnd.show();

    // Wire delegated click handlers + seed welcome row after the view is in the DOM.
    webix.delay(function () {
        var list = $$(refId + ":thread");
        if (list) {
            aichatWebixThread.bindAll(refId, list);
            list.data.attachEvent("onStoreUpdated", function (id, _obj, mode) {
                if (mode === "delete") {
                    aichatWebixAttachments.destroyChart(id, wnd.$state);
                    aichatWebixWidgets.destroyWidget(id, wnd.$state);
                    var aces = wnd.$state.aceEditors[id];
                    if (aces) {
                        aces.forEach(function (ed) { try { ed.destroy && ed.destroy(); } catch (_) { /* ignore */ } });
                        delete wnd.$state.aceEditors[id];
                    }
                }
                if (mode === "add" || mode === "update" || mode === "paint") {
                    // Cheap when stickToBottom is false (early return); cheap when
                    // already at the bottom (scrollTop assignment is idempotent).
                    aichatWebixScrollIfSticky(refId);
                }
            });
            list.attachEvent("onAfterRender", function () {
                aichatWebixThread.mountPendingSubViews(refId);
                // Re-resolve scrollNode after every render (Webix may regenerate the
                // inner scroll container on layout changes). Cheap: hits memoized
                // node if still valid.
                aichatWebixWireScroll(refId);
            });
            // Seed welcome row if list is empty.
            if (list.count() === 0) list.add({ id: webix.uid(), kind: "welcome" });
            aichatWebixWireScroll(refId);
        }
        aichatWebixComposer.bindChipStripClicks(refId);
    });
}

function aichatWebixSendMessage(refId, text, attachments) {
    var wnd = $$(refId);
    if (!wnd || !wnd.$state) return;
    var list = $$(refId + ":thread");
    if (!list) return;
    var $state = wnd.$state;

    // The user is hitting Send — they want to see their own message and the
    // response. Re-arm stick-to-bottom in case they had scrolled up earlier.
    $state.stickToBottom = true;

    // Remove welcome row if present.
    list.data.each(function (it) { if (it && it.kind === "welcome") list.remove(it.id); });

    list.add({ id: webix.uid(), kind: "user", text: text });
    // Working indicator goes on immediately so the user sees activity before
    // token + WS handshake complete. The frame reducer updates the label and
    // removes the row on complete/error.
    var workingRowId = webix.uid();
    list.add({ id: workingRowId, kind: "working", label: "Thinking...", elapsedSec: 0 });
    $state.workingRowId = workingRowId;
    $state.workingStartedAt = Date.now();
    if ($state.workingTimer) { try { clearInterval($state.workingTimer); } catch (_) { /* ignore */ } }
    $state.workingTimer = setInterval(function () {
        if (!$state.workingRowId || !list.getItem($state.workingRowId)) {
            try { clearInterval($state.workingTimer); } catch (_) { /* ignore */ }
            $state.workingTimer = null;
            return;
        }
        var sec = Math.max(0, Math.round((Date.now() - $state.workingStartedAt) / 1000));
        list.updateItem($state.workingRowId, { elapsedSec: sec });
    }, 1000);

    // Tag this stream. Its async teardown (onClose/onError) fires on the
    // WebSocket's own schedule and may land AFTER the next turn has already
    // started streaming — a previous turn's late close would otherwise null
    // $state.assistantRowId mid-stream, orphaning the in-flight bubble so the
    // streamed prose renders as a stray visible bubble instead of collapsing
    // into a reasoning row (the "split bubble" race). Each send installs a new
    // token; teardown only mutates shared $state while it is still the active
    // stream, so a stale stream's close becomes a no-op.
    var streamToken = {};
    $state.activeStreamToken = streamToken;
    var isStaleStream = function () { return $state.activeStreamToken !== streamToken; };

    $state.handle = aichatWebixService.openStream({
        message: text,
        sessionId: $state.sessionId,
        workflowId: $state.workflowId,
        indexName: $state.indexName,
        attachments: attachments,
        onEvent: function (frame) {
            if (isStaleStream()) return; // a newer send owns the thread now
            aichatWebixFrames.applyFrame(refId, frame);
        },
        onError: function (err) {
            if (isStaleStream()) { aichatWebixLogChart("ws onError: ignored (stale stream)"); return; }
            aichatWebixRemoveWorking(list, $state);
            list.add({ id: webix.uid(), kind: "error", code: err.code, message: err.message });
            aichatWebixComposer.toggleSending(refId, false);
            aichatWebixLogChart("ws onError: clearing assistantRowId=" + $state.assistantRowId
                + " code=" + (err && err.code));
            $state.handle = null;
            $state.requestId = null;
            $state.assistantRowId = null;
        },
        onClose: function () {
            if (isStaleStream()) { aichatWebixLogChart("ws onClose: ignored (stale stream — next turn owns $state)"); return; }
            aichatWebixRemoveWorking(list, $state);
            aichatWebixComposer.toggleSending(refId, false);
            aichatWebixLogChart("ws onClose: clearing assistantRowId=" + $state.assistantRowId);
            $state.handle = null;
            $state.requestId = null;
            $state.assistantRowId = null;
        },
    });
}

// Shared with the frame reducer's removeWorking — here for the WS-level
// error/close callbacks that bypass applyFrame.
function aichatWebixRemoveWorking(list, $state) {
    if ($state.workingTimer) {
        try { clearInterval($state.workingTimer); } catch (_) { /* ignore */ }
        $state.workingTimer = null;
    }
    var id = $state.workingRowId;
    $state.workingRowId = null;
    $state.workingStartedAt = 0;
    if (id && list.getItem(id)) {
        try { list.remove(id); } catch (_) { /* ignore */ }
    }
}

function aichatWebixCancelStream(refId) {
    var wnd = $$(refId);
    if (!wnd || !wnd.$state) return;
    if (wnd.$state.handle && wnd.$state.handle.cancel) wnd.$state.handle.cancel();
}

function aichatWebixNewSession(refId) {
    var wnd = $$(refId);
    if (!wnd) return;
    if (wnd.$state.handle && wnd.$state.handle.cancel) wnd.$state.handle.cancel();
    aichatWebixState.destroyAllSubViews(wnd.$state);
    wnd.$state = aichatWebixState.createState(wnd.$state.workflowId, wnd.$state.indexName);
    var list = $$(refId + ":thread");
    if (list) {
        list.clearAll();
        list.add({ id: webix.uid(), kind: "welcome" });
    }
    aichatWebixComposer.toggleSending(refId, false);
}

function aichatWebixSendToolResultClient(refId, rowId, toolUseId, result) {
    var wnd = $$(refId);
    if (!wnd || !wnd.$state) return;
    if (!wnd.$state.handle) {
        // Stream already closed (complete/error/onClose nulled the handle).
        // Without feedback this looks like the Submit button "did nothing".
        console.warn("[aichat-webix] tool_result drop: stream closed", {
            rowId: rowId, toolUseId: toolUseId, result: result,
        });
        try { webix.message({ type: "error", text: "Conversation ended. Start a new session to continue." }); } catch (_) { /* ignore */ }
        return;
    }
    var list = $$(refId + ":thread");
    if (list) {
        // Optimistic UI update — flip widget into "answered" pill state.
        list.updateItem(rowId, { result: result });
        aichatWebixWidgets.destroyWidget(rowId, wnd.$state);
    }
    wnd.$state.handle.send({
        event: "tool_result_from_client",
        data: { toolCallId: toolUseId, result: result },
    });
}

// Re-render the tool-step rows (tool_call / tool_result) so they reflect the
// current $state.showToolSteps. The thread template renders those rows empty
// (zero height) when hidden, so on toggle we only need to refresh them — and we
// refresh ONLY those rows (not the whole list) so mounted charts/widgets in
// other rows stay intact. We never reorder the thread, so hidden rows keep their
// original position and reappear in chronological order when revealed (Webix
// list.filter, by contrast, parks hidden rows at the end of the store, which
// pushed revealed steps below the final answer and out of order).
// New tool rows streamed while the toggle is OFF render hidden automatically via
// the template, so the frame reducer doesn't need to call this on every add.
function aichatWebixApplyToolStepVisibility(refId) {
    var wnd = $$(refId);
    if (!wnd || !wnd.$state) return;
    var list = $$(refId + ":thread");
    if (!list) return;
    list.data.each(function (it) {
        if (it && (it.kind === "tool_call" || it.kind === "tool_result")) {
            list.refresh(it.id);
        }
    });
}

function aichatWebixAdjustCharts(refId) {
    var wnd = $$(refId);
    if (!wnd || !wnd.$state) return;
    Object.keys(wnd.$state.charts || {}).forEach(function (k) {
        var c = wnd.$state.charts[k];
        try { c && c.adjust && c.adjust(); } catch (_) { /* ignore */ }
    });
}

// Stick-to-bottom auto-scroll wiring.
// Webix's onAfterRender doesn't reliably fire on internal scroll-container
// resizes, so we attach a native scroll listener to the actual scrollable DOM
// node inside the list. The listener flips $state.stickToBottom based on
// whether the user is within 30px of the bottom. The frame reducer reads
// $state.stickToBottom and only auto-scrolls when true.
function aichatWebixWireScroll(refId) {
    var wnd = $$(refId);
    if (!wnd || !wnd.$state) return;
    var list = $$(refId + ":thread");
    if (!list || !list.$view) return;

    var scrollNode = aichatWebixFindScrollNode(list.$view);
    if (!scrollNode) return;
    // If we already wired this exact node, nothing to do.
    if (wnd.$state.scrollNode === scrollNode && scrollNode.__aichatScrollBound) return;
    wnd.$state.scrollNode = scrollNode;
    if (scrollNode.__aichatScrollBound) return;
    scrollNode.__aichatScrollBound = true;

    webix.event(scrollNode, "scroll", function () {
        var atBottom = (scrollNode.scrollTop + scrollNode.clientHeight) >= (scrollNode.scrollHeight - 30);
        wnd.$state.stickToBottom = atBottom;
    });
}

// Walks the list view's DOM to find the element that actually scrolls
// (overflow-y auto/scroll AND scrollHeight > clientHeight). Falls back to
// the outer element if no inner scroller exists yet.
function aichatWebixFindScrollNode(root) {
    if (!root) return null;
    var queue = [root];
    while (queue.length) {
        var el = queue.shift();
        if (!el || el.nodeType !== 1) continue;
        try {
            var cs = el.ownerDocument.defaultView.getComputedStyle(el);
            var oy = cs.overflowY;
            if ((oy === "auto" || oy === "scroll") && el.scrollHeight > el.clientHeight + 1) {
                return el;
            }
        } catch (_) { /* ignore */ }
        for (var i = 0; i < el.childNodes.length; i += 1) queue.push(el.childNodes[i]);
    }
    return root;
}

// Scroll the list to the bottom, but ONLY when stickToBottom is true.
// Called by the frame reducer on every delta + add-row mutation. Cheap when
// already at bottom (no-op).
function aichatWebixScrollIfSticky(refId) {
    var wnd = $$(refId);
    if (!wnd || !wnd.$state) return;
    if (!wnd.$state.stickToBottom) return;
    var node = wnd.$state.scrollNode;
    if (!node) {
        // Lazy-resolve on first call if the listener hasn't bound yet.
        aichatWebixWireScroll(refId);
        node = wnd.$state.scrollNode;
    }
    if (!node) return;
    node.scrollTop = node.scrollHeight;
}

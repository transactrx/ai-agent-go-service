/* aichat-webix WS-frame reducer.
   One global: aichatWebixFrames = { applyFrame }.
   applyFrame(refId, frame) mutates the thread list and per-window $state. */

var aichatWebixFrames = (function () {
    function parseData(frame) {
        if (frame == null) return {};
        if (frame.data == null) return {};
        if (typeof frame.data !== "string") return frame.data;
        try { return JSON.parse(frame.data); } catch (_) { return {}; }
    }

    // logChart emits a greppable "aichat-chart:" diagnostic for every point that
    // creates, mutates, or reconciles an assistant/chart bubble. It traces the
    // full client-side chart lifecycle so we can tell, after the fact, exactly
    // which frame produced (or duplicated) a chart bubble. On by default; set
    // window.AICHAT_CHART_DEBUG = false to silence. Mirrors the server's
    // "chart-trace:" logs.
    function logChart(msg) {
        try { if (typeof window !== "undefined" && window.AICHAT_CHART_DEBUG === false) return; } catch (_) { /* ignore */ }
        try { console.log("aichat-chart: " + msg); } catch (_) { /* ignore */ }
    }

    // Matches an inline markdown chart image (![alt](.../chart/<uuid>.png...)).
    var CHART_IMG_RE = /!\[[^\]]*\]\([^)\s]*chart\/[0-9a-fA-F-]{8,}\.png[^)\s]*\)/;
    function hasChartImage(text) { return !!text && CHART_IMG_RE.test(text); }

    // trailingAssistantRowId returns the id of the last non-"working" row when
    // that row is an assistant bubble, else null. A late or duplicate `complete`
    // frame (or an empty-finalText complete followed by a real one) can clear
    // $state.assistantRowId while the turn's already-streamed assistant bubble is
    // still the last row; reconciling with it keeps `complete` idempotent instead
    // of appending a duplicate bubble.
    function trailingAssistantRowId(list) {
        var rows = [];
        list.data.each(function (o) { rows.push({ id: o.id, kind: o.kind }); });
        for (var i = rows.length - 1; i >= 0; i--) {
            if (rows[i].kind === "working") continue;
            return rows[i].kind === "assistant" ? rows[i].id : null;
        }
        return null;
    }

    function applyFrame(refId, frame) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return;
        var list = $$(refId + ":thread");
        if (!list) return;
        var $state = wnd.$state;
        var data = parseData(frame);

        switch (frame.event) {
        case "start":
            $state.requestId = data.requestId || null;
            if (data.sessionId) {
                $state.sessionId = data.sessionId;
            }
            // Streaming on — composer swaps Send→Stop (single abort control;
            // the old toolbar Cancel was removed as a duplicate).
            aichatWebixComposer.toggleSending(refId, true);
            ensureWorking(refId, list, $state, "Thinking...");
            break;

        case "delta": {
            if (!$state.assistantRowId) {
                var newId = webix.uid();
                // _streaming marks a bubble whose chart links are not yet
                // server-validated; the assistant template suppresses chart
                // images while streaming so a half-formed/fabricated ![chart](…)
                // never renders as a broken <img>. Cleared on `complete`, when
                // the validated finalText arrives and the real chart renders.
                // Add assistant row BEFORE the working row so working stays at bottom.
                if ($state.workingRowId && list.getItem($state.workingRowId)) {
                    var idx = list.getIndexById($state.workingRowId);
                    list.add({ id: newId, kind: "assistant", text: "", _streaming: true }, idx);
                } else {
                    list.add({ id: newId, kind: "assistant", text: "", _streaming: true });
                }
                $state.assistantRowId = newId;
                logChart("delta: opened assistant bubble id=" + newId);
            }
            var row = list.getItem($state.assistantRowId);
            if (!row) return;
            var nextText = (row.text || "") + (data.text || "");
            // Log only the delta that first introduces a chart image, not every token.
            if (!hasChartImage(row.text || "") && hasChartImage(nextText)) {
                logChart("delta: chart image now present in assistant bubble id=" + $state.assistantRowId);
            }
            list.updateItem($state.assistantRowId, { text: nextText });
            aichatWebixScrollIfSticky(refId);
            ensureWorking(refId, list, $state, "Writing...");
            break;
        }

        case "tool_call": {
            // Convert any in-flight assistant text to a reasoning row (preserve streaming).
            if ($state.assistantRowId) {
                var cur = list.getItem($state.assistantRowId);
                if (cur && (cur.text || "").trim() !== "") {
                    logChart("tool_call: name=" + data.name + " -> converting in-flight assistant bubble id="
                        + $state.assistantRowId + " (textLen=" + (cur.text || "").length
                        + ", chart=" + hasChartImage(cur.text) + ") to a reasoning row");
                    list.updateItem($state.assistantRowId, {
                        kind: "reasoning",
                        _expanded: true,
                        label: cur.label || undefined,
                    });
                } else if (cur) {
                    logChart("tool_call: name=" + data.name + " -> removing empty in-flight bubble id=" + $state.assistantRowId);
                    list.remove($state.assistantRowId);
                }
                $state.assistantRowId = null;
            } else {
                // No active assistant bubble. If the trailing row is a non-empty
                // assistant bubble, the prose the model streamed before this tool
                // call was orphaned — it will render as a stray visible bubble
                // instead of collapsing into a reasoning row. Flag it so we can
                // see exactly when/why assistantRowId was already cleared.
                var orphanId = trailingAssistantRowId(list);
                if (orphanId) {
                    var orphan = list.getItem(orphanId);
                    if (orphan && (orphan.text || "").trim() !== "") {
                        logChart("tool_call: name=" + data.name + " WARNING orphaned assistant bubble id=" + orphanId
                            + " (textLen=" + (orphan.text || "").length + ", chart=" + hasChartImage(orphan.text)
                            + ") — assistantRowId was already null, prose will stay a visible bubble");
                    }
                }
            }
            var rowId = webix.uid();
            var isWidget = aichatWebixWidgets.has(data.name);
            list.add({
                id: rowId,
                kind: isWidget ? "widget" : "tool_call",
                name: data.name,
                input: data.input,
                toolUseId: data.toolUseId,
                _expanded: false,
            });
            $state.toolCallStartByUseId[data.toolUseId] = Date.now();
            webix.delay(function () { aichatWebixThread.mountPendingSubViews(refId); });
            var toolLabel = data.name ? ("Using tool: " + data.name + "...") : "Using tool...";
            ensureWorking(refId, list, $state, toolLabel);
            break;
        }

        case "tool_result": {
            var start = $state.toolCallStartByUseId[data.toolUseId];
            var elapsed = start ? (Date.now() - start) : null;
            // If a widget row matches by toolUseId, attach the result there.
            var widgetRowId = findWidgetByToolUseId(list, data.toolUseId);
            if (widgetRowId) {
                var existing = list.getItem(widgetRowId) || {};
                // Destroy the mounted widget BEFORE updateItem replaces the host DOM.
                aichatWebixWidgets.destroyWidget(widgetRowId, $state);
                if (existing.result) {
                    // Client already answered optimistically (Skip click or button
                    // click set result before the server roundtrip landed). Preserve
                    // the client-side result. Only flag _serverError so the pill
                    // can show the server's view as a suffix without overwriting.
                    if (data.isError) {
                        list.updateItem(widgetRowId, { _serverError: true });
                    }
                } else {
                    // No optimistic answer — set result from the server payload.
                    list.updateItem(widgetRowId, {
                        result: data.output,
                        _serverError: !!data.isError,
                    });
                }
            } else {
                list.add({
                    id: webix.uid(),
                    kind: "tool_result",
                    output: data.output,
                    isError: data.isError,
                    elapsedMs: elapsed,
                    _expanded: false,
                });
            }
            // Propagate to any attachment row sharing this toolUseId (chart cache invalidation).
            var attRowId = $state.attachmentsByToolUseId[data.toolUseId];
            if (attRowId && list.getItem(attRowId)) {
                webix.delay(function () { aichatWebixThread.mountPendingSubViews(refId); });
            }
            ensureWorking(refId, list, $state, "Thinking...");
            break;
        }

        case "attachment": {
            var existingId = data.toolUseId && $state.attachmentsByToolUseId[data.toolUseId];
            if (existingId && list.getItem(existingId)) {
                // Destroy the mounted chart BEFORE updateItem replaces the host DOM.
                aichatWebixAttachments.destroyChart(existingId, $state);
                list.updateItem(existingId, { attachmentKind: data.kind, payload: data.payload });
                logChart("attachment: updated existing attachment row id=" + existingId
                    + " kind=" + data.kind + " toolUseId=" + data.toolUseId);
                webix.delay(function () { aichatWebixThread.mountPendingSubViews(refId); });
                ensureWorking(refId, list, $state, "Thinking...");
                break;
            }
            var aRowId = webix.uid();
            if (data.toolUseId) $state.attachmentsByToolUseId[data.toolUseId] = aRowId;
            logChart("attachment: added attachment row id=" + aRowId
                + " kind=" + data.kind + " toolUseId=" + data.toolUseId);
            list.add({
                id: aRowId,
                kind: "attachment",
                attachmentKind: data.kind,
                toolUseId: data.toolUseId,
                payload: data.payload,
            });
            webix.delay(function () { aichatWebixThread.mountPendingSubViews(refId); });
            ensureWorking(refId, list, $state, "Thinking...");
            break;
        }

        case "complete":
            removeWorking(list, $state);
            logChart("complete: received finalText=" + (data.finalText ? data.finalText.length : 0)
                + " assistantRowId=" + ($state.assistantRowId || "null"));
            // The server may rewrite the final answer (e.g. correcting/stripping
            // chart image URLs the model fabricated or corrupted). When a
            // finalText is present, it is authoritative: overwrite the streamed
            // bubble so the user sees the corrected answer. The visible prose is
            // unchanged — only chart links inside ![chart](...) are fixed.
            if (data.finalText) {
                var targetId = ($state.assistantRowId && list.getItem($state.assistantRowId))
                    ? $state.assistantRowId
                    : null;
                // assistantRowId can be cleared out-of-band (a duplicate/extra
                // complete frame, or an empty-finalText complete) while the turn's
                // streamed bubble is still the last row. Reconcile with it instead
                // of appending a duplicate.
                if (!targetId) {
                    var trailing = trailingAssistantRowId(list);
                    if (trailing) {
                        targetId = trailing;
                        logChart("complete: reconciled with trailing assistant bubble id=" + trailing
                            + " (DUPLICATE PREVENTED; assistantRowId was cleared before complete)");
                    }
                }
                if (targetId) {
                    // _streaming:false — finalText is server-validated, so the
                    // assistant template now renders chart images for this bubble.
                    list.updateItem(targetId, { text: data.finalText, _streaming: false });
                    $state.assistantRowId = targetId;
                    logChart("complete: branch=update id=" + targetId
                        + " finalLen=" + data.finalText.length + " hasChartImg=" + hasChartImage(data.finalText));
                } else {
                    var fId = webix.uid();
                    list.add({ id: fId, kind: "assistant", text: data.finalText, _streaming: false });
                    $state.assistantRowId = fId;
                    logChart("complete: branch=add id=" + fId
                        + " finalLen=" + data.finalText.length + " hasChartImg=" + hasChartImage(data.finalText));
                }
            }
            aichatWebixComposer.toggleSending(refId, false);
            // Capture before clearing so the ace upgrade still has the row id.
            // Clearing assistantRowId is CRITICAL: without it, the next turn's
            // deltas append to the previous assistant bubble instead of
            // creating a new one.
            var completedAssistantRowId = $state.assistantRowId;
            $state.assistantRowId = null;
            $state.requestId = null;
            $state.handle = null;
            if (completedAssistantRowId) {
                aichatWebixBubbles.upgradeCodeBlocks(completedAssistantRowId, $state);
            }
            break;

        case "error":
            removeWorking(list, $state);
            list.add({
                id: webix.uid(),
                kind: "error",
                code: data.code,
                message: data.message,
            });
            aichatWebixComposer.toggleSending(refId, false);
            $state.assistantRowId = null;
            $state.requestId = null;
            $state.handle = null;
            break;

        default:
            break;
        }
    }

    function findWidgetByToolUseId(list, toolUseId) {
        var found = null;
        list.data.each(function (it) {
            if (it.kind === "widget" && it.toolUseId === toolUseId) found = it.id;
        });
        return found;
    }

    // ensureWorking keeps a "Working..." indicator row as the LAST row.
    // If absent, creates it + starts a 1s timer that refreshes elapsedSec.
    // If present, updates its label in place and re-pins it to the bottom
    // (since other rows just got appended). Idempotent.
    function ensureWorking(_refId, list, $state, label) {
        var now = Date.now();
        if (!$state.workingRowId) {
            $state.workingStartedAt = now;
            var rowId = webix.uid();
            $state.workingRowId = rowId;
            list.add({ id: rowId, kind: "working", label: label || "Thinking...", elapsedSec: 0 });
            // Start the per-second elapsed updater.
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
            return;
        }
        // Already exists. Move to bottom if not already last, and update label.
        if (list.getLastId() !== $state.workingRowId) {
            list.move($state.workingRowId, list.count() - 1);
        }
        var sec2 = Math.max(0, Math.round((now - $state.workingStartedAt) / 1000));
        list.updateItem($state.workingRowId, { label: label || "Thinking...", elapsedSec: sec2 });
    }

    function removeWorking(list, $state) {
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

    return { applyFrame: applyFrame };
})();

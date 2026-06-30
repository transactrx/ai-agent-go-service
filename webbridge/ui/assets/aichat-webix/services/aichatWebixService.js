/* aichat-webix WS transport — mints a token via POST aichatviewer/token, opens
   WS at aichatviewer/stream?token=..., ships first frame with attachments[], and
   exposes handle.send(clientFrame) for tool_result_from_client.

   One global: aichatWebixService = { openStream }. */

var aichatWebixService = (function () {
    var TOKEN_URL = "aichatviewer/token";

    function streamWsUrl(token) {
        var proto = window.location.protocol === "https:" ? "wss:" : "ws:";
        var path = window.location.pathname.replace(/\/[^/]*$/, "/");
        return proto + "//" + window.location.host + path + "aichatviewer/stream?token="
            + encodeURIComponent(token);
    }

    function openStream(opts) {
        var ws = null;
        var handle = {
            requestId: null,
            cancel: function () { try { if (ws) ws.close(); } catch (_) { /* ignore */ } },
            send: function (clientFrame) {
                try {
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify(clientFrame));
                    }
                } catch (_) { /* ignore */ }
            },
        };

        var clientTimeZone = "";
        try { clientTimeZone = (Intl.DateTimeFormat().resolvedOptions() || {}).timeZone || ""; } catch (_) { /* unsupported */ }
        webix.ajax().headers({ "Content-Type": "application/json" })
            .post(TOKEN_URL, JSON.stringify({ timeZone: clientTimeZone }))
            .then(function (resp) {
                var json = resp.json() || {};
                var token = json.token;
                if (!token) {
                    opts.onError && opts.onError({ code: "auth-error", message: "Could not obtain stream token" });
                    opts.onClose && opts.onClose();
                    return;
                }
                ws = new WebSocket(streamWsUrl(token));

                ws.addEventListener("open", function () {
                    // Event-wrapped first frame matches the React engine's protocol.
                    // The Go server's StreamChat reads `attachments` only from this
                    // shape (ClientFrame.UserMessage.Attachments); the legacy flat
                    // shape's `attachments` field is silently dropped. Each
                    // attachment carries inline base64 `data` so the agent (NATS-only)
                    // can feed Bedrock as native image/document content blocks.
                    // sessionId is required: the Webix engine opens a fresh WS per
                    // send, so the previous session's id only flows back to the
                    // API trigger via this field. Without it the trigger mints a
                    // new uuid and chat memory loads zero history.
                    var msg = (typeof opts.message === "string") ? opts.message : (opts.message != null ? String(opts.message) : "");
                    ws.send(JSON.stringify({
                        event: "user_message",
                        data: {
                            text: msg,
                            sessionId: opts.sessionId || "",
                            workflowId: opts.workflowId || "",
                            indexName: opts.indexName || "",
                            // timeZone travels on the WS frame (not just the token POST
                            // body) because the portal proxy can strip the token body;
                            // the frame is the proven channel that also carries
                            // workflowId/indexName reliably.
                            timeZone: clientTimeZone,
                            attachments: opts.attachments || [],
                        },
                    }));
                });

                ws.addEventListener("message", function (ev) {
                    var frame;
                    try { frame = JSON.parse(ev.data); } catch (_) { return; }
                    if (!frame || !frame.event) return;
                    if (frame.event === "start") {
                        try { handle.requestId = (frame.data || {}).requestId || null; } catch (_) { /* ignore */ }
                    }
                    var dataStr = (typeof frame.data === "string") ? frame.data : JSON.stringify(frame.data || {});
                    opts.onEvent && opts.onEvent({ event: frame.event, id: String(frame.sequence || 0), data: dataStr });
                });

                ws.addEventListener("error", function () {
                    opts.onError && opts.onError({ code: "network-error", message: "WebSocket error" });
                });

                ws.addEventListener("close", function () {
                    opts.onClose && opts.onClose();
                });
            })
            .fail(function (xhr) {
                var msg = "Could not obtain stream token";
                try { msg = (JSON.parse(xhr.responseText || "{}").message) || msg; } catch (_) { /* ignore */ }
                opts.onError && opts.onError({ code: "auth-error", message: msg });
                opts.onClose && opts.onClose();
            });

        return handle;
    }

    return { openStream: openStream };
})();

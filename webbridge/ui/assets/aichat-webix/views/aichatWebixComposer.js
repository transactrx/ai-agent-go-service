/* aichat-webix composer: textarea + paperclip uploader + chip strip + Send/Stop.
   Files dropped or picked upload via aichatWebixUpload.uploadFile and stage as
   pendingAttachments on the window's $state until Send is clicked.

   One global: aichatWebixComposer = { build, toggleSending, bindChipStripClicks }.
   build(refId, {onSend, onCancel}) returns a Webix view config. */

var aichatWebixComposer = (function () {
    var ACCEPT = "image/*,application/pdf,text/csv,text/plain,application/json";

    function build(refId, callbacks) {
        var inputId = refId + ":input";
        var sendBtnId = refId + ":sendBtn";
        var stopBtnId = refId + ":stopBtn";
        var uploaderId = refId + ":uploader";
        var chipStripId = refId + ":chips";

        return {
            css: "aichat-webix-composer",
            rows: [
                {
                    id: chipStripId,
                    view: "template",
                    autoheight: true,
                    template: function () { return renderChipStrip(refId); },
                    borderless: true,
                },
                {
                    height: 50,
                    paddingX: 4,
                    paddingY: 4,
                    margin: 4,
                    cols: [
                        {
                            id: uploaderId,
                            view: "uploader",
                            type: "icon",
                            icon: "mdi mdi-paperclip",
                            label: "",
                            width: 42,
                            css: "aichat-webix-attach-btn",
                            multiple: true,
                            autosend: false,
                            accept: ACCEPT,
                            on: {
                                onBeforeFileAdd: function (file) {
                                    handleFile(refId, file && file.file ? file.file : file);
                                    return false;
                                },
                            },
                        },
                        {
                            view: "textarea",
                            id: inputId,
                            placeholder: "Ask the agent...  (Enter = send, Shift+Enter = new line, drop a file)",
                            on: {
                                onAfterRender: function () {
                                    var node = this.getInputNode();
                                    if (!node || node.__aichatEnterBound) return;
                                    node.__aichatEnterBound = true;
                                    var self = this;
                                    webix.event(node, "keydown", function (e) {
                                        if ((e.key === "Enter" || e.keyCode === 13) && !e.shiftKey) {
                                            e.preventDefault();
                                            triggerSend(refId, callbacks);
                                        }
                                    });
                                    // Paste support: if the clipboard has image items (screenshots,
                                    // copied images from other apps), route them through the same
                                    // upload path as drag-drop and the paperclip picker.
                                    webix.event(node, "paste", function (e) {
                                        var cd = e.clipboardData;
                                        if (!cd) return;
                                        var items = cd.items || [];
                                        var imgFiles = [];
                                        for (var i = 0; i < items.length; i += 1) {
                                            var it = items[i];
                                            if (it && it.kind === "file" && /^image\//.test(it.type || "")) {
                                                var f = it.getAsFile && it.getAsFile();
                                                if (f) {
                                                    // Pasted images have no useful name; synthesize one.
                                                    if (!f.name || f.name === "image.png") {
                                                        try {
                                                            f = new File([f], "pasted-" + Date.now() + "." + (f.type.split("/")[1] || "png"), { type: f.type });
                                                        } catch (_) { /* File ctor not always available; keep original */ }
                                                    }
                                                    imgFiles.push(f);
                                                }
                                            }
                                        }
                                        if (imgFiles.length > 0) {
                                            e.preventDefault();
                                            imgFiles.forEach(function (f) { handleFile(refId, f); });
                                        }
                                    });
                                    bindDragDrop(self.$view, refId);
                                },
                            },
                        },
                        {
                            rows: [
                                {},
                                {
                                    view: "button",
                                    id: sendBtnId,
                                    label: "Send",
                                    css: "webix_primary",
                                    width: 80,
                                    height: 34,
                                    click: function () { triggerSend(refId, callbacks); },
                                },
                                {
                                    view: "button",
                                    id: stopBtnId,
                                    label: "Stop",
                                    width: 80,
                                    height: 34,
                                    hidden: true,
                                    click: function () { callbacks.onCancel && callbacks.onCancel(); },
                                },
                                {},
                            ],
                            width: 80,
                        },
                    ],
                },
            ],
        };
    }

    function handleFile(refId, file) {
        var wnd = $$(refId);
        if (!wnd || !file) return;
        var $state = wnd.$state;
        var id = webix.uid();
        var chip = {
            id: id,
            name: file.name,
            mediaType: file.type || "application/octet-stream",
            size: file.size || 0,
            status: "running",
            error: null,
            url: null,
            _file: file,
        };
        $state.pendingAttachments.push(chip);
        refreshChips(refId);
        // Run upload + base64 read in parallel — mirrors React's HttpUploadAdapter.send.
        // The bytes are needed by the agent (NATS-only, can't HTTP-fetch the URL) so
        // it can feed Bedrock as native image/document content blocks.
        Promise.all([aichatWebixUpload.uploadFile(file), aichatWebixUpload.fileToBase64(file)])
            .then(function (results) {
                var uploaded = results[0];
                var dataBase64 = results[1];
                chip.status = "complete";
                chip.url = uploaded.url;
                chip.mediaType = uploaded.mediaType || chip.mediaType;
                chip.size = uploaded.size || chip.size;
                chip.dataBase64 = dataBase64;
                refreshChips(refId);
            })
            .catch(function (err) {
                chip.status = "error";
                chip.error = err && err.message ? err.message : String(err);
                console.warn("[aichat-webix] upload failed:", err);
                refreshChips(refId);
            });
    }

    function refreshChips(refId) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return;
        var strip = $$(refId + ":chips");
        if (!strip) return;
        // autoheight on the strip view means the row sizes itself from the
        // template's HTML output (renderChipStrip returns "" when no chips).
        strip.refresh();
        // resize() forces the parent layout to recompute child heights,
        // otherwise the new autoheight isn't applied until something else triggers it.
        try { strip.resize(); } catch (_) { /* ignore */ }
        var parent = strip.getParentView && strip.getParentView();
        if (parent && parent.resize) { try { parent.resize(); } catch (_) { /* ignore */ } }
    }

    function renderChipStrip(refId) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return "";
        var chips = wnd.$state.pendingAttachments || [];
        if (chips.length === 0) return "";
        var html = "<div class=\"aichat-webix-chipstrip\">";
        chips.forEach(function (c) {
            var cls = c.status === "running" ? "is-running" : (c.status === "error" ? "is-error" : "");
            var retry = c.status === "error"
                ? "<span class=\"aichat-webix-chip-retry\" data-aichat-chip-retry=\"" + c.id + "\" title=\"Retry\">&#8635;</span>"
                : "";
            html += "<span class=\"aichat-webix-chip " + cls + "\">"
                + "<span>&#128206;</span>"
                + "<span class=\"aichat-webix-chip-name\" title=\"" + webix.template.escape(c.error || c.name) + "\">"
                + webix.template.escape(c.name) + "</span>"
                + retry
                + "<span class=\"aichat-webix-chip-x\" data-aichat-chip-remove=\"" + c.id + "\" title=\"Remove\">&times;</span>"
                + "</span>";
        });
        html += "</div>";
        return html;
    }

    function triggerSend(refId, callbacks) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return;
        if (wnd.$state.handle) return;
        var input = $$(refId + ":input");
        var text = (input.getValue() || "").trim();
        var pending = wnd.$state.pendingAttachments || [];
        var anyRunning = pending.some(function (c) { return c.status === "running"; });
        if (anyRunning) return;
        var attachments = pending.filter(function (c) { return c.status === "complete"; }).map(function (c) {
            return { url: c.url, mediaType: c.mediaType, filename: c.name, size: c.size, data: c.dataBase64 };
        });
        if (!text && attachments.length === 0) return;

        callbacks.onSend && callbacks.onSend(text, attachments);

        input.setValue("");
        wnd.$state.pendingAttachments = [];
        refreshChips(refId);
        toggleSending(refId, true);
    }

    function toggleSending(refId, sending) {
        var send = $$(refId + ":sendBtn");
        var stop = $$(refId + ":stopBtn");
        if (sending) { if (send) send.hide(); if (stop) stop.show(); }
        else { if (stop) stop.hide(); if (send) send.show(); }
    }

    function bindDragDrop(rootNode, refId) {
        if (!rootNode || rootNode.__aichatDragBound) return;
        rootNode.__aichatDragBound = true;
        ["dragenter", "dragover"].forEach(function (evt) {
            webix.event(rootNode, evt, function (e) { e.preventDefault(); e.stopPropagation(); });
        });
        webix.event(rootNode, "drop", function (e) {
            e.preventDefault(); e.stopPropagation();
            var files = (e.dataTransfer && e.dataTransfer.files) || [];
            for (var i = 0; i < files.length; i += 1) handleFile(refId, files[i]);
        });
    }

    function bindChipStripClicks(refId) {
        var strip = $$(refId + ":chips");
        if (!strip || !strip.$view) return;
        var node = strip.$view;
        if (node.__aichatChipBound) return;
        node.__aichatChipBound = true;
        webix.event(node, "click", function (e) {
            var t = e.target || e.srcElement;
            while (t && t !== node && !t.getAttribute) t = t.parentNode;
            if (!t || t === node) return;
            var rmId = t.getAttribute("data-aichat-chip-remove");
            var rtId = t.getAttribute("data-aichat-chip-retry");
            if (rmId) {
                removeChip(refId, rmId);
            } else if (rtId) {
                retryChip(refId, rtId);
            }
        });
    }

    function removeChip(refId, id) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return;
        // String/number coercion: the id from data-attributes is a string, but
        // webix.uid() returns a number. Use loose-eq via String() comparison.
        var key = String(id);
        wnd.$state.pendingAttachments = wnd.$state.pendingAttachments.filter(function (c) { return String(c.id) !== key; });
        refreshChips(refId);
    }

    function retryChip(refId, id) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return;
        var key = String(id);
        var chip = wnd.$state.pendingAttachments.filter(function (c) { return String(c.id) === key; })[0];
        if (!chip || !chip._file) return;
        chip.status = "running";
        chip.error = null;
        refreshChips(refId);
        Promise.all([aichatWebixUpload.uploadFile(chip._file), aichatWebixUpload.fileToBase64(chip._file)])
            .then(function (results) {
                var uploaded = results[0];
                var dataBase64 = results[1];
                chip.status = "complete";
                chip.url = uploaded.url;
                chip.mediaType = uploaded.mediaType || chip.mediaType;
                chip.size = uploaded.size || chip.size;
                chip.dataBase64 = dataBase64;
                refreshChips(refId);
            })
            .catch(function (err) {
                chip.status = "error";
                chip.error = err && err.message ? err.message : String(err);
                refreshChips(refId);
            });
    }

    return {
        build: build,
        toggleSending: toggleSending,
        bindChipStripClicks: bindChipStripClicks,
    };
})();

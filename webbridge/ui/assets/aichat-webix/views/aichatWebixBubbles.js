/* aichat-webix bubble templates + ace code-block upgrade + error message map.
   One global: aichatWebixBubbles = { template, upgradeCodeBlocks, errorMessages }. */

var aichatWebixBubbles = (function () {
    // Ported verbatim from webapp/aichat/views/aichatViewerWnd.js so the three
    // engines stay decoupled (no cross-file import).
    var errorMessages = {
        cancelled: "Request cancelled.",
        timeout: "The agent took too long to respond.",
        "llm-error": "AI service error. Try again.",
        "tool-error": "A tool call failed.",
        "policy-deny": "Request denied by policy.",
        "policy-error": "Policy evaluation error.",
        "max-iterations": "The agent gave up after too many steps.",
        "memory-load-error": "Conversation memory error.",
        "memory-append-error": "Conversation memory error.",
        "render-error": "Render error.",
        "executor-failed": "Internal error. Please retry.",
        panic: "Internal error. Please retry.",
        shutdown: "Service shutting down. Retry shortly.",
        "http-error": "Network error. Please retry.",
        "network-error": "Network error. Please retry.",
        "endpoint-not-found": "Workflow not found on the server.",
        "bad-body": "Malformed request.",
        "bad-request": "Invalid request.",
        "stream-init-failed": "Could not start the AI stream.",
        "auth-error": "Authentication failed.",
        "account-id-missing": "Missing account context.",
        "unknown-tool": "The agent tried to use an unknown tool.",
        "index-not-allowed": "Search target not permitted.",
        "dsl-injection-blocked": "Unsafe query blocked.",
    };

    function safeMarkdown(text) {
        try {
            return marked.parse(text || "", { gfm: true, breaks: false });
        } catch (e) {
            console.warn("[aichat-webix] marked.parse failed:", e);
            return webix.template.escape(text || "");
        }
    }

    // While a bubble is still streaming (_streaming), any image link in it is the
    // model's own un-validated URL (any host/format); only the finalText delivered
    // on `complete` is server-validated. Until then, replace every markdown image
    // with a "loading chart" spinner placeholder so a half-formed/fabricated link
    // never renders as a broken <img> and the user sees a clear loading state; on
    // `complete` the validated chart image takes its place.
    // Matching by image syntax (not a chart-URL pattern) is deliberate — the model
    // uses several URL shapes (S3 .png, quickchart.io/chart/render/...), so a
    // host-specific regex would let some through. The reasoning template keeps its
    // own plain "(chart)" neutralizer (aichatWebixTools) — intentionally decoupled.
    var IMG_MD_RE = /!\[([^\]]*)\]\([^)]*\)/g;
    function loadingChartPlaceholders(text) {
        if (!text) return text;
        return text.replace(IMG_MD_RE, function () {
            return "<span class=\"aichat-webix-chart-loading\">"
                + "<span class=\"aichat-webix-chart-spinner-sm\"></span>"
                + "loading chart…</span>";
        });
    }

    function template(item) {
        switch (item.kind) {
        case "welcome":
            return "<div class=\"aichat-webix-welcome\">"
                + "<div class=\"aichat-webix-welcome-title\">AI Assistant</div>"
                + "<div class=\"aichat-webix-welcome-body\">"
                + "Ask a question about PowerLine claims, providers, or trends.<br/>"
                + "Attach a file or image with the &#128206; button or by dragging it onto the composer."
                + "</div></div>";
        case "user":
            return "<div class=\"aichat-webix-row is-user\">"
                + "<div class=\"aichat-webix-bubble is-user\">" + webix.template.escape(item.text || "") + "</div>"
                + "</div>";
        case "assistant": {
            // Render chart images only once the bubble is finalized (validated by
            // the server on `complete`); while streaming, show a loading placeholder.
            // A finalized chart link loads natively: the chart-redirect route serves
            // the real image (302 -> S3) or a gray "Chart unavailable" placeholder
            // image for a missing/fabricated id, so the <img> always renders cleanly.
            var assistantText = item._streaming ? loadingChartPlaceholders(item.text) : item.text;
            return "<div class=\"aichat-webix-row is-assistant\">"
                + "<div class=\"aichat-webix-bubble is-assistant\">"
                + "<div class=\"aichat-webix-md\" id=\"" + item.id + "-md\">" + safeMarkdown(assistantText) + "</div>"
                + "</div></div>";
        }
        case "error": {
            var code = item.code || "error";
            var msg = errorMessages[code] || item.message || "";
            return "<div class=\"aichat-webix-row is-error\">"
                + "<div class=\"aichat-webix-bubble is-error\">"
                + "<b>" + webix.template.escape(code) + "</b>: " + webix.template.escape(msg)
                + "</div></div>";
        }
        case "working": {
            // Claude Desktop-style indicator: spinning icon + dynamic label + elapsed seconds.
            // item.label is updated by the frame reducer as the stream progresses
            // (delta -> "Writing...", tool_call -> "Using tool: X...").
            var workingLabel = webix.template.escape(item.label || "Thinking...");
            var elapsedTxt = (typeof item.elapsedSec === "number" && item.elapsedSec >= 0) ? (item.elapsedSec + "s") : "";
            return "<div class=\"aichat-webix-working\">"
                + "<span class=\"aichat-webix-working-icon\"></span>"
                + "<span class=\"aichat-webix-working-label\">" + workingLabel + "</span>"
                + (elapsedTxt ? "<span class=\"aichat-webix-working-elapsed\">" + elapsedTxt + "</span>" : "")
                + "</div>";
        }
        default:
            return "";
        }
    }

    function upgradeCodeBlocks(assistantRowId, $state) {
        if (!assistantRowId || typeof ace === "undefined") return;
        var root = document.getElementById(assistantRowId + "-md");
        if (!root) return;
        var blocks = root.querySelectorAll("pre > code");
        var editors = [];
        blocks.forEach(function (codeEl, i) {
            try {
                var pre = codeEl.parentElement;
                var holder = document.createElement("div");
                holder.style.height = "200px";
                holder.id = assistantRowId + "-ace-" + i;
                pre.parentElement.replaceChild(holder, pre);
                var editor = ace.edit(holder.id);
                editor.setReadOnly(true);
                editor.setShowPrintMargin(false);
                editor.setValue(codeEl.textContent, -1);
                var lang = (codeEl.className.match(/language-(\w+)/) || [])[1];
                if (lang === "json") editor.session.setMode("ace/mode/json");
                else if (lang === "javascript") editor.session.setMode("ace/mode/javascript");
                else if (lang === "sql") editor.session.setMode("ace/mode/sql");
                else editor.session.setMode("ace/mode/text");
                editors.push(editor);
            } catch (e) {
                console.warn("[aichat-webix] ace upgrade failed:", e);
            }
        });
        if ($state && editors.length) {
            $state.aceEditors[assistantRowId] = editors;
        }
    }

    return {
        template: template,
        upgradeCodeBlocks: upgradeCodeBlocks,
        errorMessages: errorMessages,
        loadingChartPlaceholders: loadingChartPlaceholders,
    };
})();

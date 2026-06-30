/* aichat-webix templates for reasoning / tool_call / tool_result rows.
   One global: aichatWebixTools = { template, bindToggles }.
   Toggle is wired by the thread mount step — no inline window callbacks. */

var aichatWebixTools = (function () {
    function safeMarkdown(text) {
        try { return marked.parse(text || "", { gfm: true, breaks: false }); }
        catch (e) { console.warn("[aichat-webix] marked.parse failed:", e); return webix.template.escape(text || ""); }
    }

    // Reasoning rows show the model's intermediate thinking, which often contains
    // a half-formed image link — e.g. a placeholder/fabricated chart URL the model
    // writes BEFORE actually rendering the chart, in any host/format (S3
    // chart/<uuid>.png OR a raw quickchart.io/chart/render/... URL). Only the final
    // answer's chart URL is server-validated; an unvalidated image link renders as
    // a broken <img>. Neutralize EVERY markdown image in reasoning so it shows as
    // plain text — the real chart still renders in the final answer bubble (which
    // is NOT neutralized). Matching by image syntax (not a chart-URL pattern) is
    // deliberate: the model uses several URL shapes, so a host-specific regex
    // misses some and lets a broken image through.
    var IMG_MD_RE = /!\[([^\]]*)\]\([^)]*\)/g;
    function neutralizeChartImages(text) {
        if (!text) return text;
        return text.replace(IMG_MD_RE, function (_m, alt) { return "*(" + (alt || "chart") + ")*"; });
    }

    function escapeJson(v) {
        var s = (typeof v === "string") ? v : JSON.stringify(v || {}, null, 2);
        return webix.template.escape(s);
    }

    function template(item) {
        var expanded = !!item._expanded;
        var arrow = expanded ? "&#9660;" : "&#9654;"; // down arrow / right arrow
        var display = expanded ? "block" : "none";
        switch (item.kind) {
        case "reasoning": {
            var label = webix.template.escape(item.label || "reasoning");
            return "<div class=\"aichat-webix-tool is-reasoning\" data-row-id=\"" + item.id + "\">"
                + "<div class=\"aichat-webix-tool-card\">"
                + "<div class=\"aichat-webix-tool-head\" data-aichat-toggle=\"" + item.id + "\">"
                + arrow + " " + label
                + "</div>"
                + "<div class=\"aichat-webix-tool-body\" style=\"display:" + display + "\">"
                + safeMarkdown(neutralizeChartImages(item.text))
                + "</div></div></div>";
        }
        case "tool_call": {
            var name = webix.template.escape(item.name || "");
            return "<div class=\"aichat-webix-tool\" data-row-id=\"" + item.id + "\">"
                + "<div class=\"aichat-webix-tool-card\">"
                + "<div class=\"aichat-webix-tool-head\" data-aichat-toggle=\"" + item.id + "\">"
                + arrow + " calling tool: " + name
                + "</div>"
                + "<pre class=\"aichat-webix-tool-body\" style=\"display:" + display + "\">"
                + escapeJson(item.input)
                + "</pre></div></div>";
        }
        case "tool_result": {
            var elapsed = item.elapsedMs ? " in " + item.elapsedMs + "ms" : "";
            return "<div class=\"aichat-webix-tool\" data-row-id=\"" + item.id + "\">"
                + "<div class=\"aichat-webix-tool-card\">"
                + "<div class=\"aichat-webix-tool-head\" data-aichat-toggle=\"" + item.id + "\">"
                + arrow + " tool returned" + elapsed
                + "</div>"
                + "<pre class=\"aichat-webix-tool-body\" style=\"display:" + display + "\">"
                + escapeJson(item.output)
                + "</pre></div></div>";
        }
        default:
            return "";
        }
    }

    // Pure visibility predicate for the "Show tool steps" toggle. Tool-step
    // diagnostic cards (tool_call / tool_result) are hidden unless the user opts
    // in; everything else (reasoning, answer, attachments, widgets, working) is
    // always shown. Kept here (not in the window) so it is unit-testable without
    // the Webix UI stack.
    function toolStepRowVisible(item, showToolSteps) {
        if (!item) return true;
        if (showToolSteps) return true;
        return item.kind !== "tool_call" && item.kind !== "tool_result";
    }

    // Called by the thread's onAfterRender to wire toggle clicks (idempotent).
    // refId is accepted (matching aichatWebixAttachments.bindExpandClicks signature)
    // but unused — toggles only need listView to call updateItem.
    function bindToggles(_refId, listView) {
        var node = listView.$view;
        if (!node || node.__aichatToolsBound) return;
        node.__aichatToolsBound = true;
        webix.event(node, "click", function (e) {
            var target = e.target || e.srcElement;
            while (target && target !== node && !target.getAttribute) target = target.parentNode;
            if (!target || target === node) return;
            var rowId = target.getAttribute("data-aichat-toggle");
            if (!rowId) return;
            var item = listView.getItem(rowId);
            if (!item) return;
            listView.updateItem(rowId, { _expanded: !item._expanded });
        });
    }

    return {
        template: template,
        bindToggles: bindToggles,
        neutralizeChartImages: neutralizeChartImages,
        toolStepRowVisible: toolStepRowVisible,
    };
})();

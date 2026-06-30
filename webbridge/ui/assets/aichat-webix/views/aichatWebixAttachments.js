/* aichat-webix templates for attachment rows: image, file chip, webix-chart.
   One global: aichatWebixAttachments = { template, mountChart, destroyChart, expandChart, bindExpandClicks }. */

var aichatWebixAttachments = (function () {
    function template(item) {
        var kind = item.attachmentKind || "";
        if (kind === "image") {
            var payload = item.payload || {};
            var src = payload.url || "";
            var name = webix.template.escape(payload.filename || "");
            return "<div class=\"aichat-webix-attachment-row\">"
                + "<div class=\"aichat-webix-image-bubble\">"
                + "<img src=\"" + webix.template.escape(src) + "\" alt=\"" + name + "\"/>"
                + "</div></div>";
        }
        if (kind === "file") {
            var p = item.payload || {};
            var url = webix.template.escape(p.url || "#");
            var fname = webix.template.escape(p.filename || "download");
            var mt = p.mediaType ? "<span class=\"aichat-webix-file-meta\">" + webix.template.escape(p.mediaType) + "</span>" : "";
            return "<div class=\"aichat-webix-attachment-row\">"
                + "<a class=\"aichat-webix-file-chip\" href=\"" + url + "\" download=\"" + fname + "\">"
                + "<span>&#128206;</span><span>" + fname + "</span>" + mt
                + "</a></div>";
        }
        if (kind === "webix-chart") {
            return "<div class=\"aichat-webix-attachment-row\">"
                + "<div class=\"aichat-webix-chart-bubble\">"
                + "<div class=\"aichat-webix-chart-toolbar\">"
                + "<span class=\"aichat-webix-chart-expand\" data-aichat-expand-chart=\"" + item.id + "\" title=\"Expand\">&#8663;</span>"
                + "</div>"
                + "<div class=\"aichat-webix-chart-host\" id=\"" + item.id + "-chart\"></div>"
                + "</div></div>";
        }
        // Fallback — render as a collapsible JSON card via the tools template style
        var expanded = !!item._expanded;
        var arrow = expanded ? "&#9660;" : "&#9654;";
        var display = expanded ? "block" : "none";
        return "<div class=\"aichat-webix-tool\" data-row-id=\"" + item.id + "\">"
            + "<div class=\"aichat-webix-tool-card\">"
            + "<div class=\"aichat-webix-tool-head\" data-aichat-toggle=\"" + item.id + "\">"
            + arrow + " attachment: " + webix.template.escape(kind)
            + "</div>"
            + "<pre class=\"aichat-webix-tool-body\" style=\"display:" + display + "\">"
            + webix.template.escape(JSON.stringify(item.payload || {}, null, 2))
            + "</pre></div></div>";
    }

    function mountChart(_refId, item, $state) {
        if (!item || item.attachmentKind !== "webix-chart") return;
        var box = document.getElementById(item.id + "-chart");
        if (!box || !item.payload) return;
        if (box.children.length > 0 && $state.charts[item.id]) return;
        if ($state.charts[item.id]) {
            try { $state.charts[item.id].destructor(); } catch (_) { /* ignore */ }
            delete $state.charts[item.id];
        }
        try {
            var spec = Object.assign({}, item.payload, { container: box });
            var chart = webix.ui(spec);
            $state.charts[item.id] = chart;
        } catch (e) {
            console.warn("[aichat-webix] chart mount failed:", e);
            box.innerHTML = "<div class=\"aichat-webix-chart-error\">Could not render chart: "
                + webix.template.escape(e.message || String(e)) + "</div>";
        }
    }

    function destroyChart(rowId, $state) {
        if (!$state) return;
        var c = $state.charts[rowId];
        if (!c) return;
        try { c.destructor && c.destructor(); } catch (_) { /* ignore */ }
        delete $state.charts[rowId];
    }

    function expandChart(refId, rowId) {
        var wnd = $$(refId);
        if (!wnd || !wnd.$state) return;
        var list = $$(refId + ":thread");
        var item = list && list.getItem(rowId);
        if (!item || !item.payload) return;
        if (wnd.$state.expandWnd) { try { wnd.$state.expandWnd.close(); } catch (_) { /* ignore */ } wnd.$state.expandWnd = null; }
        var modal = webix.ui({
            view: "window",
            modal: true,
            move: true,
            resize: true,
            width: 900,
            height: 600,
            position: "center",
            head: {
                view: "toolbar",
                cols: [
                    { view: "label", template: function () { return "&nbsp;Chart"; } },
                    { view: "icon", icon: "mdi mdi-close", click: function () { this.getTopParentView().close(); } },
                ],
            },
            body: Object.assign({}, item.payload),
            on: { onDestruct: function () { wnd.$state.expandWnd = null; } },
        });
        wnd.$state.expandWnd = modal;
        modal.show();
    }

    function bindExpandClicks(refId, listView) {
        var node = listView.$view;
        if (!node || node.__aichatExpandBound) return;
        node.__aichatExpandBound = true;
        webix.event(node, "click", function (e) {
            var t = e.target || e.srcElement;
            while (t && t !== node && !t.getAttribute) t = t.parentNode;
            if (!t || t === node) return;
            var rowId = t.getAttribute("data-aichat-expand-chart");
            if (!rowId) return;
            expandChart(refId, rowId);
        });
    }

    return {
        template: template,
        mountChart: mountChart,
        destroyChart: destroyChart,
        expandChart: expandChart,
        bindExpandClicks: bindExpandClicks,
    };
})();

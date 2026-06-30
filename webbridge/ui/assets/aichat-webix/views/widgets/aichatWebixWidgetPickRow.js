/* PickRow widget. Webix datatable; click a row to submit; Skip below.
   Result shape: {row:{...}} | {_skipped:true}. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; PickRow widget skipped.");
        return;
    }

    aichatWebixWidgets.register("PickRow", {
        buildConfig: function (item, sendToolResult, helpers) {
            var cols = helpers.coerceArray(item.input && item.input.columns).map(function (c) {
                return { id: c.id, header: c.label || c.id, width: c.width || 120 };
            });
            var rows = helpers.coerceArray(item.input && item.input.rows);
            var tableId = item.id + "-table";

            return {
                rows: [
                    {
                        view: "datatable",
                        id: tableId,
                        columns: cols,
                        data: rows,
                        autoheight: true,
                        select: "row",
                        on: {
                            onItemClick: function (id) {
                                if (helpers.guardAlreadyAnswered(item.id)) return;
                                var row = this.getItem(id);
                                if (!row) return;
                                sendToolResult(item.id, item.toolUseId, { row: row });
                            },
                        },
                    },
                    { cols: [{}, helpers.skipRow(item, sendToolResult)], height: 34 },
                ],
                height: Math.min(rows.length, 10) * 30 + 70,
            };
        },
        renderPill: function (item) {
            var row = (item.result && item.result.row) || {};
            var display = row.id || JSON.stringify(row);
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">picked:</span>"
                + "<span class=\"value\">" + webix.template.escape(String(display)) + "</span></span>";
        },
    });
})();

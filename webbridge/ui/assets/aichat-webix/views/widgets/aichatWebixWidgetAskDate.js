/* AskDate widget. Single calendar (mode:"single") or two calendars
   (mode:"range") + Submit + Skip.
   Result shape: {date:"YYYY-MM-DD"} | {from,to} | {_skipped:true}.

   UX notes:
   - Both calendars are PRE-FILLED with sensible defaults (single: today;
     range: 7-day window ending today) so Submit always has a value and never
     surfaces a confusing "pick both dates" toast. setValue is also re-applied
     post-mount via webix.delay because Webix occasionally drops the `value`
     config on calendars nested inside a cols layout.
   - A status label above the calendar(s) reflects the current selection
     live (onChange) so the user can SEE what's selected.
   - When the LLM passes min/max bounds, those bounds are shown in the status
     line so the user knows why earlier/later dates are disabled. */
(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; AskDate widget skipped.");
        return;
    }

    function toIso(d) {
        if (!d) return "";
        return d.toISOString().slice(0, 10);
    }

    function startOfToday() {
        var d = new Date();
        d.setHours(0, 0, 0, 0);
        return d;
    }

    function clamp(d, minDate, maxDate) {
        if (minDate && d < minDate) return minDate;
        if (maxDate && d > maxDate) return maxDate;
        return d;
    }

    function rangeHint(minDate, maxDate) {
        if (!minDate && !maxDate) return "";
        return " <span style=\"color:#888;font-weight:normal\">(allowed "
            + (minDate ? toIso(minDate) : "any")
            + " – "
            + (maxDate ? toIso(maxDate) : "any")
            + ")</span>";
    }

    // Webix calendar's minDate/maxDate default to "today" when not provided
    // (or when set to null/undefined), which means the user can only ever
    // click on today — every other date renders with .webix_cal_day_disabled.
    // We work around this by explicitly passing very wide bounds so the
    // user can freely navigate and click any date in a reasonable window.
    var EPOCH_PAST = new Date(1900, 0, 1);
    var EPOCH_FUTURE = new Date(2100, 11, 31);

    aichatWebixWidgets.register("AskDate", {
        buildConfig: function (item, sendToolResult, helpers) {
            var mode = (item.input && item.input.mode) || "single";
            // Intentionally IGNORE the LLM-supplied min/max. Real domain
            // constraints should come from workflow/policy config, not
            // per-call LLM input where the model habitually over-restricts.
            var minDate = EPOCH_PAST;
            var maxDate = EPOCH_FUTURE;
            var singleCalId = item.id + "-cal";
            var fromCalId = item.id + "-cal-from";
            var toCalId = item.id + "-cal-to";
            var statusId = item.id + "-cal-status";
            var hint = rangeHint(minDate, maxDate);

            // Defaults: today for single; (today - 6, today) for range.
            var today = clamp(startOfToday(), minDate, maxDate);
            var weekAgo = new Date(today);
            weekAgo.setDate(weekAgo.getDate() - 6);
            weekAgo = clamp(weekAgo, minDate, maxDate);

            // Captured values — single source of truth for Submit. Updated on
            // every calendar onChange. Survives Webix view churn that can
            // make $$(calId).getValue() unreliable at click time.
            var capturedFrom = weekAgo;
            var capturedTo = today;
            var capturedSingle = today;

            function renderStatus() {
                if (mode === "range") {
                    return "Selected: from <b>" + (toIso(capturedFrom) || "?") + "</b> to <b>" + (toIso(capturedTo) || "?") + "</b>" + hint;
                }
                return "Selected: <b>" + (toIso(capturedSingle) || "?") + "</b>" + hint;
            }

            function refreshStatus() {
                var s = $$(statusId);
                if (s) s.setHTML(renderStatus());
            }

            var calCfg = function (id, defaultDate) {
                return {
                    view: "calendar",
                    id: id,
                    width: 240,
                    height: 220,
                    value: defaultDate,
                    date: defaultDate,
                    // Always pass explicit wide bounds — see EPOCH_PAST/FUTURE
                    // comment. Skipping these props lets Webix default both
                    // to today which makes ~all dates unselectable.
                    minDate: minDate,
                    maxDate: maxDate,
                    on: {
                        onChange: function () {
                            try {
                                var v = this.getValue();
                                if (id === fromCalId) capturedFrom = v;
                                else if (id === toCalId) capturedTo = v;
                                else if (id === singleCalId) capturedSingle = v;
                            } catch (_) { /* ignore */ }
                            refreshStatus();
                        },
                    },
                };
            };

            var statusRow = {
                id: statusId,
                view: "template",
                css: "aichat-webix-askdate-status",
                autoheight: true,
                borderless: true,
                template: mode === "range"
                    ? "Selected: from <b>" + toIso(weekAgo) + "</b> to <b>" + toIso(today) + "</b>" + hint
                    : "Selected: <b>" + toIso(today) + "</b>" + hint,
            };

            var submit = helpers.submitButton("Submit", function () {
                try {
                    if (helpers.guardAlreadyAnswered(item.id)) return;
                    if (mode === "range") {
                        var f = capturedFrom;
                        var t = capturedTo;
                        if (!f || !t) { webix.message({ type: "error", text: "Pick both dates." }); return; }
                        if (toIso(f) > toIso(t)) { webix.message({ type: "error", text: "From must be on or before To." }); return; }
                        sendToolResult(item.id, item.toolUseId, { from: toIso(f), to: toIso(t) });
                    } else {
                        var d = capturedSingle;
                        if (!d) { webix.message({ type: "error", text: "Pick a date." }); return; }
                        sendToolResult(item.id, item.toolUseId, { date: toIso(d) });
                    }
                } catch (e) {
                    console.warn("[aichat-webix] AskDate Submit failed:", e);
                    try { webix.message({ type: "error", text: "Could not submit selection." }); } catch (_) { /* ignore */ }
                }
            });

            // Post-mount: re-apply setValue (Webix occasionally drops the
            // value config when calendars are inside a cols layout).
            // setValue fires onChange which refreshes captured* + status.
            webix.delay(function () {
                if (mode === "range") {
                    if ($$(fromCalId)) $$(fromCalId).setValue(weekAgo);
                    if ($$(toCalId)) $$(toCalId).setValue(today);
                } else if ($$(singleCalId)) {
                    $$(singleCalId).setValue(today);
                }
                refreshStatus();
            });

            if (mode === "range") {
                return {
                    rows: [
                        statusRow,
                        { cols: [calCfg(fromCalId, weekAgo), calCfg(toCalId, today)] },
                        { cols: [submit, helpers.skipRow(item, sendToolResult)], height: 34 },
                    ],
                    height: 290,
                };
            }
            return {
                rows: [
                    statusRow,
                    calCfg(singleCalId, today),
                    { cols: [submit, helpers.skipRow(item, sendToolResult)], height: 34 },
                ],
                height: 290,
            };
        },
        renderPill: function (item) {
            var r = item.result || {};
            var txt = r.date ? r.date : (r.from && r.to ? (r.from + " → " + r.to) : "");
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">picked:</span>"
                + "<span class=\"value\">" + webix.template.escape(txt) + "</span></span>";
        },
    });
})();

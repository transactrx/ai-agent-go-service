/* AskForm widget. Webix form with N typed fields + Submit + Skip.
   Field types: text, select, date, number, checkbox.
   Result shape: {<field.name>: value, ...} | {_skipped:true}. */

/* Pure, unit-testable height math for the AskForm widget. No DOM / canvas /
   webix references here so it can be exercised in a Node vm sandbox.
   Controls are NOT given a custom size — they render at the app's standard
   Webix input height, which the DOM layer reads from the active skin
   (webix.skin.$active.inputHeight) and passes in as `controlHeight`. This file
   only sums those known heights so the chat-list row reserves the right space.
   Height model (px):
     label block        = lines*LABEL_LINE_HEIGHT + LABEL_PAD_BOTTOM
     non-checkbox field = labelHeight + controlHeight
     checkbox field     = max(controlHeight, labelHeight)   (label sits inline-right)
     total              = Σ fieldHeight + FIELD_GAP*(n-1) + BUTTON_ROW + OUTER_GAP */
var aichatWebixAskForm = (function () {
    var LABEL_LINE_HEIGHT = 18;
    var LABEL_PAD_BOTTOM = 4;
    var FIELD_GAP = 12;     // gap row between successive fields
    var FORM_PADDING = 8;   // inset on all sides so content isn't flush to the panel edge
    var BUTTON_ROW = 34;
    var OUTER_GAP = 10;     // gap between the form and the Submit/Skip row

    // Defensively normalize the tool's `fields` into a clean array of field
    // objects. The model (or API serialization) sometimes sends it as a JSON
    // string or a non-array; without this, fields.forEach throws and the whole
    // widget fails to render.
    function coerceFields(raw) {
        var f = raw;
        if (typeof f === "string") {
            try { f = JSON.parse(f); } catch (e) { return []; }
        }
        if (!Array.isArray(f)) return [];
        return f.filter(function (x) { return x && typeof x === "object"; });
    }

    function labelLineCount(labelPixelWidth, availWidth) {
        if (!(availWidth > 0)) return 1;
        return Math.max(1, Math.ceil(labelPixelWidth / availWidth));
    }

    function labelHeight(lines) {
        return (lines || 1) * LABEL_LINE_HEIGHT + LABEL_PAD_BOTTOM;
    }

    function fieldHeight(spec) {
        var lines = (spec && spec.lines) || 1;
        var ch = (spec && spec.controlHeight) || 0;
        if (spec && spec.checkbox) {
            return Math.max(ch, labelHeight(lines));
        }
        return labelHeight(lines) + ch;
    }

    function computeFormHeight(specs) {
        specs = specs || [];
        var sum = 0;
        for (var i = 0; i < specs.length; i += 1) sum += fieldHeight(specs[i]);
        var gaps = specs.length > 1 ? FIELD_GAP * (specs.length - 1) : 0;
        return sum + gaps + (2 * FORM_PADDING) + BUTTON_ROW + OUTER_GAP;
    }

    return {
        LABEL_LINE_HEIGHT: LABEL_LINE_HEIGHT,
        LABEL_PAD_BOTTOM: LABEL_PAD_BOTTOM,
        FIELD_GAP: FIELD_GAP,
        FORM_PADDING: FORM_PADDING,
        BUTTON_ROW: BUTTON_ROW,
        OUTER_GAP: OUTER_GAP,
        coerceFields: coerceFields,
        labelLineCount: labelLineCount,
        labelHeight: labelHeight,
        fieldHeight: fieldHeight,
        computeFormHeight: computeFormHeight,
    };
})();

(function () {
    if (typeof aichatWebixWidgets === "undefined") {
        console.warn("[aichat-webix] aichatWebixWidgets not loaded; AskForm widget skipped.");
        return;
    }

    var LABEL_FONT = "500 13px -apple-system, BlinkMacSystemFont, 'Segoe UI', "
        + "Roboto, Helvetica, Arial, sans-serif";
    var HOST_FALLBACK_WIDTH = 700;
    var HOST_WIDTH_INSET = 8; // shrink avail width so we round line count UP (never under-count)
    var CHECKBOX_BOX_WIDTH = 24; // the box sits left of the wrapping label, stealing width

    // Canvas text width measurement; context cached across calls.
    function measureTextWidth(text, font) {
        var c = measureTextWidth._c || (measureTextWidth._c = document.createElement("canvas"));
        var ctx = c.getContext("2d");
        ctx.font = font;
        return ctx.measureText(text || "").width;
    }

    // Available content width from the live host div (already inside card
    // padding). Falls back to a sane default if not measurable yet.
    function hostAvailWidth(item) {
        var host = document.getElementById(item.id + "-widget");
        var w = host ? host.clientWidth : 0;
        if (!(w > 0)) w = HOST_FALLBACK_WIDTH;
        // Subtract the inset plus the form's left+right padding so the wrap
        // line-count reflects the real text width (over-count is safe, never clip).
        return Math.max(120, w - HOST_WIDTH_INSET - (2 * aichatWebixAskForm.FORM_PADDING));
    }

    // Wrapping question label as a borderless template, sized to its lines.
    function labelElement(f, lines) {
        var req = f.required ? "<span class=\"req\">*</span>" : "";
        return {
            view: "template",
            borderless: true,
            css: "aichat-webix-askform-label-host",
            template: "<div class=\"aichat-webix-askform-label\">"
                + webix.template.escape(f.label || "") + req + "</div>",
            height: lines * aichatWebixAskForm.LABEL_LINE_HEIGHT
                + aichatWebixAskForm.LABEL_PAD_BOTTOM,
        };
    }

    // Bare (label-less) input rendered at the app's standard input height
    // (controlHeight, from the active Webix skin) so it matches every other
    // form in the app. Field spacing is added separately as gap rows.
    function controlElement(f, controlHeight) {
        var common = {
            name: f.name,
            label: "",
            labelWidth: 0,
            height: controlHeight,
        };
        switch (f.type) {
        case "text":
            return Object.assign({ view: "text", placeholder: f.placeholder || "" }, common);
        case "select":
            return Object.assign({
                view: "richselect",
                options: aichatWebixAskForm.coerceFields(f.options).map(function (o) { return { id: o.value, value: o.label }; }),
            }, common);
        case "date":
            return Object.assign({ view: "datepicker", format: "%Y-%m-%d" }, common);
        case "number":
            return Object.assign({
                view: "text",
                type: "number",
                attributes: {
                    min: (typeof f.min === "number") ? f.min : undefined,
                    max: (typeof f.max === "number") ? f.max : undefined,
                },
            }, common);
        default:
            return Object.assign({ view: "text", placeholder: "(unsupported type: " + f.type + ")" }, common);
        }
    }

    // Checkbox keeps its label to the right of the box (wraps via CSS),
    // sized to whichever is taller: the box or the wrapped label. The label is
    // escaped (author-supplied tool input) with the required "*" appended as
    // HTML, mirroring labelElement.
    function checkboxElement(f, lines, controlHeight) {
        var req = f.required ? " <span class=\"req\">*</span>" : "";
        return {
            view: "checkbox",
            name: f.name,
            labelRight: webix.template.escape(f.label || "") + req,
            label: "",
            labelWidth: 0,
            css: "aichat-webix-askform-checkbox",
            height: Math.max(controlHeight, aichatWebixAskForm.labelHeight(lines)),
        };
    }

    function readValues(formId, fields) {
        var form = $$(formId);
        if (!form) return null;
        var vals = form.getValues() || {};
        var out = {};
        fields.forEach(function (f) {
            var v = vals[f.name];
            if (f.type === "number" && typeof v === "string" && v !== "") v = Number(v);
            if (f.type === "date" && v instanceof Date) v = v.toISOString().slice(0, 10);
            if (f.type === "checkbox") v = !!v;
            out[f.name] = v;
        });
        return out;
    }

    aichatWebixWidgets.register("AskForm", {
        buildConfig: function (item, sendToolResult, helpers) {
            var input = item.input;
            if (typeof input === "string") {
                try { input = JSON.parse(input); } catch (e) { input = {}; }
            }
            input = input || {};
            var fields = aichatWebixAskForm.coerceFields(input.fields);
            var formId = item.id + "-form";
            var availW = hostAvailWidth(item);
            // App-standard input height from the active skin — keeps these
            // controls visually identical to every other Webix form.
            var controlHeight = (webix.skin && webix.skin.$active
                && webix.skin.$active.inputHeight) || 30;

            var elements = [];
            var specs = [];
            fields.forEach(function (f, i) {
                var isCheckbox = f.type === "checkbox";
                var measureText = (f.label || "") + (f.required ? " *" : "");
                // A checkbox label wraps in the space left of the box, not the
                // full host width, so it has less room per line.
                var avail = isCheckbox ? Math.max(40, availW - CHECKBOX_BOX_WIDTH) : availW;
                var lines = aichatWebixAskForm.labelLineCount(
                    measureTextWidth(measureText, LABEL_FONT), avail);
                specs.push({ lines: lines, checkbox: isCheckbox, controlHeight: controlHeight });
                if (i > 0) elements.push({ height: aichatWebixAskForm.FIELD_GAP });
                if (isCheckbox) {
                    elements.push(checkboxElement(f, lines, controlHeight));
                } else {
                    elements.push(labelElement(f, lines));
                    elements.push(controlElement(f, controlHeight));
                }
            });

            var submit = helpers.submitButton("Submit", function () {
                if (helpers.guardAlreadyAnswered(item.id)) return;
                var out = readValues(formId, fields);
                // Surface required-field-missing via webix.message.
                for (var i = 0; i < fields.length; i += 1) {
                    var f = fields[i];
                    if (f.required && (out[f.name] === undefined || out[f.name] === "" || out[f.name] === null)) {
                        webix.message({ type: "error", text: "Field required: " + (f.label || f.name) });
                        return;
                    }
                }
                sendToolResult(item.id, item.toolUseId, out);
            });

            return {
                rows: [
                    {
                        view: "form",
                        id: formId,
                        elements: elements,
                        borderless: true,
                        padding: aichatWebixAskForm.FORM_PADDING,
                        margin: 0,
                    },
                    { cols: [submit, helpers.skipRow(item, sendToolResult)], height: 34 },
                ],
                margin: aichatWebixAskForm.OUTER_GAP,
                padding: 0,
                height: aichatWebixAskForm.computeFormHeight(specs),
            };
        },
        renderPill: function (item) {
            var r = item.result || {};
            var pairs = Object.keys(r).map(function (k) {
                return webix.template.escape(k) + ": " + webix.template.escape(String(r[k]));
            });
            return "<span class=\"aichat-webix-widget-pill\">"
                + "<span class=\"label\">submitted:</span>"
                + "<span class=\"value\">" + pairs.join(" · ") + "</span></span>";
        },
    });
})();

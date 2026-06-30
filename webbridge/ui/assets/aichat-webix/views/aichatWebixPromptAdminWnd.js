/* AI Prompt Admin window — edit the agent's flexible system prompt.
   Opens from the AI Assistant toolbar (permission-gated). Targets the workflow
   of the chat that opened it (workflowId arg) / agent1 / systemMessageFlexible.

   ace-editor note: the webix ace-editor component loads ace from the
   cdnjs 1.3.3 CDN and resolves mode/theme files from there at runtime.
   mode "markdown" + theme "textmate" both ship in that build (verified
   200 on the CDN), same mechanism as documentInfoViewerWnd (dracula/
   javascript). getEditor(true) returns a promise to the core ace editor;
   editor.setReadOnly is core ace. */

var AICHAT_PROMPT_ADMIN_WND_ID = "aichatPromptAdminWnd";

function displayAichatPromptAdminWnd(workflowId) {
    workflowId = workflowId || "";
    var existing = $$(AICHAT_PROMPT_ADMIN_WND_ID);
    if (existing) {
        // Same workflow -> just re-show. Different workflow (admin opened from
        // another Search's chat) -> rebuild so the title + loaded prompt match.
        if (existing.$promptState && existing.$promptState.workflowId === workflowId) { existing.show(); return; }
        existing.close();
    }

    var state = { defaultFlex: "", loadedFlex: "", dirty: false, workflowId: workflowId };

    var wnd = webix.ui({
        id: AICHAT_PROMPT_ADMIN_WND_ID,
        view: "window",
        width: 950,
        height: 720,
        move: true,
        resize: true,
        position: "center",
        toFront: true,
        head: {
            view: "toolbar",
            cols: [
                { view: "label", template: function () { return "&nbsp;AI Prompt Admin — " + (workflowId || "(unknown workflow)"); } },
                // NOTE: plain label (no template) — a constant template would
                // override setValue and the badge would never render.
                { view: "label", id: AICHAT_PROMPT_ADMIN_WND_ID + ":badge", width: 220, label: "" },
                // Word-wrap toggle — wraps long lines in BOTH editors so the
                // whole prompt is readable without horizontal scrolling.
                // Default off; icon reflects state (overflow = off, wrap = on).
                {
                    view: "icon",
                    id: AICHAT_PROMPT_ADMIN_WND_ID + ":wrapBtn",
                    icon: "mdi mdi-format-text-wrapping-overflow",
                    tooltip: "Toggle word wrap",
                    click: function () { aichatPromptAdminToggleWrap(); },
                },
                getMaximizeWindowIcon(),
                { view: "icon", icon: "mdi mdi-close", click: function () { aichatPromptAdminClose(); } },
            ],
        },
        body: {
            rows: [
                {
                    view: "ace-editor",
                    id: AICHAT_PROMPT_ADMIN_WND_ID + ":editor",
                    mode: "markdown",
                    theme: "textmate",
                },
                {
                    // No fixed container height: when collapsed the item
                    // shrinks to its header at the bottom of the area (the
                    // flexible editor above reclaims the space) instead of
                    // leaving a dead 180px band.
                    view: "accordion",
                    multi: true,
                    rows: [{
                        header: "Locked rules (read-only — cannot be edited)",
                        collapsed: true,
                        body: {
                            view: "ace-editor",
                            id: AICHAT_PROMPT_ADMIN_WND_ID + ":fixed",
                            mode: "markdown",
                            theme: "textmate",
                            height: 220,
                        },
                    }],
                },
                {
                    // Buttons right-aligned in a plain row — same pattern as
                    // the Claim Search window button row.
                    padding: 8,
                    cols: [
                        { view: "label", id: AICHAT_PROMPT_ADMIN_WND_ID + ":meta", label: "" },
                        {},
                        { view: "button", value: "History", width: 115, click: function () { aichatPromptAdminShowHistory(); } },
                        { width: 10 },
                        // COMMENTED 2026-06-16: "Reset to default" button hidden for now (may roll back).
                        // Restore this button + the spacer below + the aichatPromptAdminReset() fn to revert.
                        // { view: "button", value: "Reset to default", width: 130, click: function () { aichatPromptAdminReset(); } },
                        // { width: 10 },
                        { view: "button", value: "Save", width: 115, type: "form", click: function () { aichatPromptAdminSave(); } },
                    ],
                },
            ],
        },
    });
    wnd.$promptState = state;
    wnd.show();
    aichatPromptAdminLoad();
}

function aichatPromptAdminEditor(idSuffix) { return $$(AICHAT_PROMPT_ADMIN_WND_ID + idSuffix); }

// aichatPromptAdminWorkflowId returns the workflow this admin window targets
// (set from the chat that opened it). All get/save/history calls key off it.
function aichatPromptAdminWorkflowId() {
    var wnd = $$(AICHAT_PROMPT_ADMIN_WND_ID);
    return (wnd && wnd.$promptState && wnd.$promptState.workflowId) || "";
}

// aichatPromptAdminApplyWrap toggles ace soft-wrap on one editor. Uses the
// session's wrap mode so long lines wrap at the editor width (no horizontal
// scroll); independent of setValue/readOnly, so it survives content reloads.
function aichatPromptAdminApplyWrap(idSuffix, on) {
    var v = aichatPromptAdminEditor(idSuffix);
    if (!v || !v.getEditor) return;
    v.getEditor(true).then(function (editor) {
        editor.getSession().setUseWrapMode(!!on);
    }).catch(function () { /* ace not ready — ignore */ });
}

// aichatPromptAdminToggleWrap flips word-wrap for BOTH editors and updates the
// toolbar icon to reflect the current state.
function aichatPromptAdminToggleWrap() {
    var wnd = $$(AICHAT_PROMPT_ADMIN_WND_ID);
    if (!wnd) return;
    var on = !wnd.$promptWrap;
    wnd.$promptWrap = on;
    aichatPromptAdminApplyWrap(":editor", on);
    aichatPromptAdminApplyWrap(":fixed", on);
    var btn = aichatPromptAdminEditor(":wrapBtn");
    if (btn) {
        btn.define("icon", on ? "mdi mdi-format-text-wrapping-wrap" : "mdi mdi-format-text-wrapping-overflow");
        btn.refresh();
    }
}

function aichatPromptAdminSetText(idSuffix, text, readOnly) {
    var v = aichatPromptAdminEditor(idSuffix);
    if (!v) return;
    v.getEditor(true).then(function (editor) {
        // Hide ace's 80-column print-margin ruler (the gray vertical line).
        editor.setShowPrintMargin(false);
        editor.setValue(text || "", -1);
        if (readOnly) editor.setReadOnly(true);
    });
}

function aichatPromptAdminLoad() {
    aichatPromptGet(aichatPromptAdminWorkflowId(), function (ok, data) {
        if (!ok) { webix.message({ type: "error", text: "Load failed: " + data }); return; }
        var wnd = $$(AICHAT_PROMPT_ADMIN_WND_ID);
        if (!wnd) return;
        wnd.$promptState.defaultFlex = data.defaultFlex || "";
        wnd.$promptState.loadedFlex = data.currentFlex || "";
        aichatPromptAdminSetText(":editor", data.currentFlex);
        aichatPromptAdminSetText(":fixed", data.fixed, true);
        aichatPromptAdminBadge(data.source);
        aichatPromptAdminMeta(data.source);
    });
}

// aichatPromptAdminSetLabel writes HTML into a webix label view. Labels
// render config.label (template redefines are ignored), so setValue is the
// reliable path; define("label") + refresh is the fallback.
function aichatPromptAdminSetLabel(idSuffix, html) {
    var b = aichatPromptAdminEditor(idSuffix);
    if (!b) return;
    if (b.setValue) {
        b.setValue(html);
    } else {
        b.define("label", html);
        if (b.refresh) b.refresh();
    }
}

function aichatPromptAdminBadge(source) {
    var html = source === "db"
        ? "<span style='color:#2e7d32;font-weight:bold;'>● DB override</span>"
        : "<span style='color:#777;'>● Default</span>";
    aichatPromptAdminSetLabel(":badge", html);
}

// aichatPromptAdminMeta shows who saved the active override and when
// (newest history entry). Cleared when running on the default.
function aichatPromptAdminMeta(source) {
    if (source !== "db") { aichatPromptAdminSetLabel(":meta", ""); return; }
    aichatPromptHistory(aichatPromptAdminWorkflowId(), function (ok, data) {
        if (!ok || !data || !data.length) return;
        var row = aichatPromptHistoryRow(data[0], 0);
        // Raw userId hidden in the UI (kept in DB for audit) — show time only.
        aichatPromptAdminSetLabel(":meta", "last saved at <b>" + row.createdAt + " UTC</b>");
    });
}

function aichatPromptAdminSave() {
    var v = aichatPromptAdminEditor(":editor");
    v.getEditor(true).then(function (editor) {
        var content = editor.getValue();
        webix.confirm({ title: "Save prompt", text: "Do you confirm you want to apply the prompt?" }).then(function () {
            aichatPromptSave(aichatPromptAdminWorkflowId(), content, function (ok, data) {
                if (!ok) { webix.message({ type: "error", text: "Save rejected: " + data }); return; }
                webix.message({ type: "success", text: "Successful Operation" });
                aichatPromptAdminLoad();
            });
        });
    });
}

// COMMENTED 2026-06-16: "Reset to default" disabled for now (may roll back). Client-only
// helper — loads the JSON default into the editor; the user then presses Save (reuses the
// normal save flow; no server/API reset endpoint is involved). Restore together with the
// "Reset to default" button in the toolbar above.
// function aichatPromptAdminReset() {
//     var wnd = $$(AICHAT_PROMPT_ADMIN_WND_ID);
//     if (!wnd) return;
//     aichatPromptAdminSetText(":editor", wnd.$promptState.defaultFlex);
//     webix.message({ type: "info", text: "Default loaded into the editor — press Save to apply" });
// }

function aichatPromptAdminShowHistory() {
    aichatPromptHistory(aichatPromptAdminWorkflowId(), function (ok, data) {
        if (!ok) { webix.message({ type: "error", text: "History failed: " + data }); return; }
        var rows = (data || []).map(aichatPromptHistoryRow);
        var win = webix.ui({
            view: "window", width: 760, height: 420, move: true, position: "center", toFront: true,
            head: { view: "toolbar", cols: [
                { view: "label", template: function () { return "&nbsp;Prompt versions (newest first)"; } },
                { view: "icon", icon: "mdi mdi-close", click: function () { this.getTopParentView().close(); } },
            ] },
            body: {
                view: "datatable",
                autoheight: false,
                select: "row",
                columns: [
                    // savedBy (raw userId) intentionally NOT shown in the UI;
                    // it stays in the DB rows for audit.
                    { id: "createdAt", header: "Saved at (UTC)", width: 160 },
                    { id: "preview", header: "Content preview", fillspace: true },
                ],
                data: rows,
                on: {
                    onItemDblClick: function (id) {
                        var item = this.getItem(id);
                        aichatPromptAdminSetText(":editor", item.content);
                        webix.message({ type: "info", text: "Version loaded into the editor — press Save to apply" });
                        this.getTopParentView().close();
                    },
                },
            },
        });
        win.show();
    });
}

function aichatPromptAdminClose() {
    var wnd = $$(AICHAT_PROMPT_ADMIN_WND_ID);
    if (!wnd) return;
    var v = aichatPromptAdminEditor(":editor");
    v.getEditor(true).then(function (editor) {
        if (editor.getValue() !== wnd.$promptState.loadedFlex) {
            webix.confirm({ title: "Unsaved changes", text: "Discard your edits?" }).then(function () { wnd.close(); });
        } else {
            wnd.close();
        }
    }).catch(function () {
        // Ace failed to initialize (e.g. CDN unreachable) — never trap the
        // user in an un-closable window; close without the dirty check.
        wnd.close();
    });
}

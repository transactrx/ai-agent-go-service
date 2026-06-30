/* aichat-webix prompt admin service — permission predicate + proxy calls.
   Targets the agent flexible prompt of the workflow the chat was opened for
   (workflowId passed in by the caller); nodeId/field are constant. */

var AICHAT_PROMPT_ADMIN_FN_ID = "powerlineclaimsearchAiPromptAdminFnid";
// Regular/current account-admin permission also grants the prompt editor
// (user decision 2026-06-04 — no new provisioning needed). Mirrors the Go gate.
var AICHAT_PROMPT_ACCOUNT_ADMIN_FN_ID = "powerlineclaimsearchadminFnid";

// nodeId/field are identical across workflows (every workflow's AI agent node
// is "agent1" with the overridable "systemMessageFlexible" field — verified in
// the chatApi workflow JSONs + agent factory OverridableFields). Only the
// workflowId varies per Search, so it is passed in by the caller.
var AICHAT_PROMPT_NODE_ID = "agent1";
var AICHAT_PROMPT_FIELD = "systemMessageFlexible";

function aichatPromptTarget(workflowId) {
    return { workflowId: workflowId || "", nodeId: AICHAT_PROMPT_NODE_ID, field: AICHAT_PROMPT_FIELD };
}

// aichatPromptAdminAllowed returns true when the logged-in user's function
// access grants the regular account-admin OR the dedicated prompt-admin
// permission. Pure given a session object; reads GetUserSessionDetails()
// when none is passed (browser path).
function aichatPromptAdminAllowed(sessionDetails) {
    try {
        var sd = sessionDetails;
        if (!sd && typeof GetUserSessionDetails === "function") sd = GetUserSessionDetails();
        var list = (sd && sd.applicationFunctionsAccess) || [];
        for (var i = 0; i < list.length; i++) {
            if ((list[i].functionId === AICHAT_PROMPT_ADMIN_FN_ID || list[i].functionId === AICHAT_PROMPT_ACCOUNT_ADMIN_FN_ID)
                && list[i].accessGranted === true) return true;
        }
        return false;
    } catch (_) {
        return false;
    }
}

// aichatPromptHistoryRow shapes one API history version for the datatable.
function aichatPromptHistoryRow(v, idx) {
    return {
        id: idx + 1,
        version: v.Version || "",
        savedBy: v.SavedBy || "",
        createdAt: (v.CreatedAt || "").replace("T", " ").slice(0, 19),
        preview: (v.Content || "").slice(0, 120).replace(/\n/g, " "),
        content: v.Content || "",
    };
}

function aichatPromptAdminPost(path, body, cb) {
    webix.ajax().headers({ "Content-Type": "application/json" })
        .post("aichatviewer/admin/prompt/" + path, JSON.stringify(body))
        .then(function (res) { cb(true, res.json()); })
        .fail(function (err) {
            var msg = "request failed";
            try { msg = JSON.parse(err.responseText).code || msg; } catch (_) { /* ignore */ }
            cb(false, msg);
        });
}

function aichatPromptGet(workflowId, cb) { aichatPromptAdminPost("get", aichatPromptTarget(workflowId), cb); }

function aichatPromptSave(workflowId, content, cb) {
    var body = Object.assign({}, aichatPromptTarget(workflowId), { content: content });
    aichatPromptAdminPost("save", body, cb);
}

function aichatPromptHistory(workflowId, cb) {
    var body = Object.assign({}, aichatPromptTarget(workflowId), { limit: 20 });
    aichatPromptAdminPost("history", body, cb);
}

/* node test export guard */
if (typeof module !== "undefined" && module.exports) {
    module.exports = { aichatPromptAdminAllowed: aichatPromptAdminAllowed, aichatPromptHistoryRow: aichatPromptHistoryRow, AICHAT_PROMPT_ADMIN_FN_ID: AICHAT_PROMPT_ADMIN_FN_ID };
}

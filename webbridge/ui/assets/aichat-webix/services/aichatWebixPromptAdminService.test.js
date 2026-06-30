/* Tests for aichatWebixPromptAdminService pure functions.
   Run: node webapp/aichat-webix/services/aichatWebixPromptAdminService.test.js */

const svc = require("./aichatWebixPromptAdminService.js");

let failures = 0;
function check(name, cond, detail) {
    if (cond) console.log("  PASS: " + name);
    else { console.log("  FAIL: " + name + (detail ? " — " + detail : "")); failures++; }
}

// permission predicate
console.log("aichatPromptAdminAllowed — permission predicate");
check("granted", svc.aichatPromptAdminAllowed({ applicationFunctionsAccess: [{ functionId: svc.AICHAT_PROMPT_ADMIN_FN_ID, accessGranted: true }] }) === true);
check("not granted (accessGranted false)", svc.aichatPromptAdminAllowed({ applicationFunctionsAccess: [{ functionId: svc.AICHAT_PROMPT_ADMIN_FN_ID, accessGranted: false }] }) === false);
check("account admin (regular isAdmin) allowed", svc.aichatPromptAdminAllowed({ applicationFunctionsAccess: [{ functionId: "powerlineclaimsearchadminFnid", accessGranted: true }] }) === true);
check("unrelated fn denied", svc.aichatPromptAdminAllowed({ applicationFunctionsAccess: [{ functionId: "powerlineclaimsearchCustomQueryFnid", accessGranted: true }] }) === false);
check("no list (empty object)", svc.aichatPromptAdminAllowed({}) === false);
check("null session (node: no GetUserSessionDetails)", svc.aichatPromptAdminAllowed(null) === false);

// history row shaping
console.log("aichatPromptHistoryRow — history version shaping");
const row = svc.aichatPromptHistoryRow({ Version: "v#1", SavedBy: "alice", CreatedAt: "2026-06-04T19:29:20.5Z", Content: "line1\nline2 long text" }, 0);
check("row id = idx+1 and savedBy correct", row.id === 1 && row.savedBy === "alice", "id=" + row.id + " savedBy=" + row.savedBy);
check("createdAt formatted (T replaced, truncated to 19 chars)", row.createdAt === "2026-06-04 19:29:20", "got: " + row.createdAt);
check("preview has no newlines (single line)", row.preview.indexOf("\n") === -1, "got: " + row.preview);
check("content is intact (newline preserved)", row.content.indexOf("\n") !== -1, "got: " + row.content);

console.log("");
console.log(failures === 0 ? "ALL TESTS PASSED" : (failures + " ASSERTION(S) FAILED"));
process.exit(failures === 0 ? 0 : 1);

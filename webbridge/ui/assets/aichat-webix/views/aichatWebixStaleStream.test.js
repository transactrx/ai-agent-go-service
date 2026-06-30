/* Stale-stream teardown race test — no test framework.
   Run: node webapp/aichat-webix/views/aichatWebixStaleStream.test.js

   Reproduces the "split bubble" race: a previous turn's WebSocket onClose fires
   late, AFTER the next turn has already started streaming. Before the fix it
   nulled the shared $state.assistantRowId mid-stream, orphaning the in-flight
   bubble (the streamed prose stayed a visible assistant bubble instead of
   collapsing into a reasoning row on the next tool_call). The fix tags each
   stream with a token; a stale stream's onClose/onError/onEvent is a no-op.

   Loads the REAL reducer (aichatWebixFrames.js) and the REAL send/WS layer
   (aichatWebixWnd.js) together, stubbing only the transport (openStream) so we
   can fire callbacks with controlled timing. */

const fs = require("fs");
const path = require("path");
const vm = require("vm");

function makeList() {
    const rows = [];
    const idx = (id) => rows.findIndex((r) => r.id === id);
    return {
        add(o, at) { if (typeof at === "number" && at >= 0) rows.splice(at, 0, o); else rows.push(o); return o.id; },
        updateItem(id, patch) { const i = idx(id); if (i >= 0) Object.assign(rows[i], patch); },
        getItem(id) { const i = idx(id); return i >= 0 ? rows[i] : null; },
        remove(id) { const i = idx(id); if (i >= 0) rows.splice(i, 1); },
        count() { return rows.length; },
        getLastId() { return rows.length ? rows[rows.length - 1].id : null; },
        getIndexById(id) { return idx(id); },
        move(id, at) { const i = idx(id); if (i < 0) return; const o = rows.splice(i, 1)[0]; rows.splice(at, 0, o); },
        clearAll() { rows.length = 0; },
        attachEvent() {},
        data: { each(cb) { rows.slice().forEach(cb); } },
        _rows: rows,
    };
}

const registry = {};
let uid = 1000;
const captured = []; // openStream opts, in send order
const sandbox = {
    console: { log: () => {}, warn: () => {}, error: () => {} },
    window: { AICHAT_CHART_DEBUG: false },
    $$: (id) => registry[id],
    webix: { uid: () => ++uid, delay: () => {}, storage: { local: {} } },
    setInterval: () => 0,
    clearInterval: () => {},
    Date: Date,
    aichatWebixWidgets: { has: () => false, destroyWidget: () => {} },
    aichatWebixAttachments: { destroyChart: () => {} },
    aichatWebixThread: { mountPendingSubViews: () => {} },
    aichatWebixComposer: { toggleSending: () => {}, bindChipStripClicks: () => {} },
    aichatWebixBubbles: { upgradeCodeBlocks: () => {} },
    aichatWebixScrollIfSticky: () => {},
    aichatWebixService: { openStream: (opts) => { captured.push(opts); return { cancel() {}, send() {} }; } },
};
vm.createContext(sandbox);
const RUNTIME = path.join(__dirname, "..", "runtime", "aichatWebixFrames.js");
vm.runInContext(fs.readFileSync(RUNTIME, "utf8"), sandbox);
vm.runInContext(fs.readFileSync(path.join(__dirname, "aichatWebixWnd.js"), "utf8"), sandbox);

const REF = "w1";
function freshThread() {
    const list = makeList();
    registry[REF] = {
        $state: {
            assistantRowId: null, requestId: null, sessionId: "s1", workflowId: "powerlineSearch",
            attachmentsByToolUseId: {}, toolCallStartByUseId: {}, workingRowId: null,
            workingTimer: null, showToolSteps: false, stickToBottom: true,
            activeStreamToken: null, handle: null,
        },
    };
    registry[REF + ":thread"] = list;
    return { list, state: registry[REF].$state };
}
const assistantRows = (list) => list._rows.filter((r) => r.kind === "assistant");
const hasWorking = (list) => list._rows.some((r) => r.kind === "working");

let failures = 0;
function check(name, cond, detail) {
    if (cond) console.log("  PASS: " + name);
    else { console.log("  FAIL: " + name + (detail ? " — " + detail : "")); failures++; }
}

console.log("Stale-stream race: previous turn's onClose fires during the next turn's streaming");
(() => {
    const { list, state } = freshThread();

    // ---- Turn 1 ----
    sandbox.aichatWebixSendMessage(REF, "first question", []);
    const opts1 = captured[0];
    opts1.onEvent({ event: "start", data: {} });
    opts1.onEvent({ event: "delta", data: { text: "first answer" } });
    opts1.onEvent({ event: "complete", data: { finalText: "ANSWER ONE" } });
    // (turn 1's WS hasn't closed yet — its onClose will fire late, below)

    // ---- Turn 2 begins ----
    sandbox.aichatWebixSendMessage(REF, "second question", []);
    const opts2 = captured[1];
    opts2.onEvent({ event: "start", data: {} });
    opts2.onEvent({ event: "delta", data: { text: "Quick note on scope before I run it" } }); // preamble bubble
    const preambleRowId = state.assistantRowId;
    check("turn 2 preamble bubble is active", !!preambleRowId, "assistantRowId=" + preambleRowId);

    // ---- The race: turn 1's stale onClose fires NOW, mid turn-2 stream ----
    opts1.onClose();
    check("stale onClose did NOT null the active assistant bubble", state.assistantRowId === preambleRowId,
        "assistantRowId=" + state.assistantRowId + " (expected " + preambleRowId + ")");
    check("stale onClose did NOT remove turn 2's working row", hasWorking(list));

    // ---- Turn 2 continues: tool call should convert the preamble to reasoning ----
    opts2.onEvent({ event: "tool_call", data: { name: "OpenSearchQuery", toolUseId: "t1" } });
    opts2.onEvent({ event: "tool_result", data: { toolUseId: "t1", output: { ok: true } } });
    opts2.onEvent({ event: "delta", data: { text: "Here is your chart " } });
    opts2.onEvent({ event: "delta", data: { text: "![chart](https://b/chart/aaaaaaaa-1111-2222-3333-444444444444.png?s)" } });
    opts2.onEvent({ event: "complete", data: { finalText: "Here is your chart ![chart](https://b/chart/aaaaaaaa-1111-2222-3333-444444444444.png?s)" } });

    const preamble = list.getItem(preambleRowId);
    check("preamble collapsed into a reasoning row (not a stray assistant bubble)", preamble && preamble.kind === "reasoning",
        "kind=" + (preamble && preamble.kind));
    // Count assistant bubbles belonging to turn 2 only (rows after the last user row).
    const rows = list._rows;
    let lastUser = -1;
    for (let i = rows.length - 1; i >= 0; i--) { if (rows[i].kind === "user") { lastUser = i; break; } }
    const turn2Assistants = rows.slice(lastUser + 1).filter((r) => r.kind === "assistant");
    check("turn 2 has exactly one visible assistant bubble (no split)", turn2Assistants.length === 1,
        "got " + turn2Assistants.length);
    check("that bubble holds the final chart answer", turn2Assistants.length === 1 && /chart\/[0-9a-f-]+\.png/.test(turn2Assistants[0].text || ""));
    check("whole thread has one assistant bubble per turn (2 total)", assistantRows(list).length === 2,
        "got " + assistantRows(list).length);
})();

console.log("");
console.log(failures === 0 ? "ALL TESTS PASSED" : (failures + " ASSERTION(S) FAILED"));
process.exit(failures === 0 ? 0 : 1);

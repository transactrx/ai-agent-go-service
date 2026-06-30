/* Standalone reducer test for aichatWebixFrames.applyFrame — no test framework.
   Run: node webapp/aichat-webix/runtime/aichatWebixFrames.test.js

   Loads the reducer in a vm sandbox with stubbed webix/$$/aichat* globals, then
   drives synthetic WS-frame sequences and asserts the thread ends with exactly
   one assistant bubble per turn. Guards the duplicate-assistant-bubble bug:
   when a turn's streamed bubble exists but $state.assistantRowId was cleared
   out-of-band (a second/empty-finalText `complete`), `complete` must reconcile
   with the trailing bubble instead of appending a duplicate. */

const fs = require("fs");
const path = require("path");
const vm = require("vm");

// ---- Fake webix list (ordered row store) -------------------------------------
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
        data: { each(cb) { rows.slice().forEach(cb); } },
        _rows: rows,
    };
}

// ---- Sandbox with all globals the reducer references -------------------------
const registry = {};
const logs = [];
let uid = 1000;
const sandbox = {
    console: { log: (m) => logs.push(String(m)), warn: () => {}, error: () => {} },
    window: {},
    $$: (id) => registry[id],
    webix: { uid: () => ++uid, delay: () => {} },
    setInterval: () => 0,
    clearInterval: () => {},
    Date: Date,
    aichatWebixWidgets: { has: () => false, destroyWidget: () => {} },
    aichatWebixAttachments: { destroyChart: () => {} },
    aichatWebixThread: { mountPendingSubViews: () => {} },
    aichatWebixComposer: { toggleSending: () => {} },
    aichatWebixBubbles: { upgradeCodeBlocks: () => {} },
    aichatWebixScrollIfSticky: () => {},
};
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(path.join(__dirname, "aichatWebixFrames.js"), "utf8"), sandbox);
const applyFrame = (refId, frame) => sandbox.aichatWebixFrames.applyFrame(refId, frame);

// ---- Harness -----------------------------------------------------------------
const REF = "w1";
function freshThread() {
    const list = makeList();
    registry[REF] = {
        $state: {
            assistantRowId: null, requestId: null, sessionId: null, workflowId: "powerlineSearch",
            attachmentsByToolUseId: {}, toolCallStartByUseId: {}, workingRowId: null,
            workingTimer: null, showToolSteps: false,
        },
    };
    registry[REF + ":thread"] = list;
    return { list, state: registry[REF].$state };
}
const run = (frames) => frames.forEach((f) => applyFrame(REF, f));
const assistantRows = (list) => list._rows.filter((r) => r.kind === "assistant");
const assistantCount = (list) => assistantRows(list).length;

let failures = 0;
function check(name, cond, detail) {
    if (cond) { console.log("  PASS: " + name); }
    else { console.log("  FAIL: " + name + (detail ? " — " + detail : "")); failures++; }
}

const CHART = "![chart](https://b.s3.amazonaws.com/chart/aaaaaaaa-1111-2222-3333-444444444444.png?X-Amz-Signature=abc)";

// ---- Scenarios ---------------------------------------------------------------
console.log("Scenario 1: normal streamed answer -> single bubble, finalText authoritative");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    run([
        { event: "start", data: { requestId: "r1", sessionId: "s1" } },
        { event: "delta", data: { text: "Here is the chart " } },
        { event: "delta", data: { text: "![chart](https://b.s3.amazonaws.com/chart/aaaaaaaa-1111-2222-3333-444444444444.png?STALE)" } },
        { event: "complete", data: { finalText: "Here is the chart " + CHART } },
    ]);
    check("exactly one assistant bubble", assistantCount(list) === 1, "got " + assistantCount(list));
    check("bubble holds corrected finalText", assistantRows(list)[0].text === "Here is the chart " + CHART);
    check("stale streamed URL replaced", !assistantRows(list)[0].text.includes("STALE"));
    check("complete cleared _streaming (final chart will render)", assistantRows(list)[0]._streaming === false,
        "_streaming=" + assistantRows(list)[0]._streaming);
})();

console.log("Scenario 2: duplicate complete (both full) -> reconciled, no dup");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    const FULL = "Final answer. " + CHART;
    run([
        { event: "start", data: {} },
        { event: "delta", data: { text: "partial" } },
        { event: "complete", data: { finalText: FULL } },
        { event: "complete", data: { finalText: FULL } },
    ]);
    check("exactly one assistant bubble", assistantCount(list) === 1, "got " + assistantCount(list));
    check("bubble holds full answer", assistantRows(list)[0].text === FULL);
    check("reconcile log emitted", logs.some((l) => l.includes("DUPLICATE PREVENTED")));
})();

console.log("Scenario 3: partial stream + empty-finalText complete then real complete (the observed bug)");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    const FULL = "Complete table and analysis. " + CHART;
    run([
        { event: "start", data: {} },
        { event: "delta", data: { text: "Partial answer cut mid-tab" } }, // partial streamed bubble
        { event: "complete", data: { finalText: "" } },                    // clears assistantRowId, leaves partial row
        { event: "complete", data: { finalText: FULL } },                  // must reconcile, not append
    ]);
    check("exactly one assistant bubble", assistantCount(list) === 1, "got " + assistantCount(list));
    check("partial replaced by full finalText", assistantRows(list)[0].text === FULL);
    check("chart image present", /chart\/[0-9a-f-]+\.png/.test(assistantRows(list)[0].text));
    check("reconcile log emitted", logs.some((l) => l.includes("DUPLICATE PREVENTED")));
})();

console.log("Scenario 4: final answer with no deltas (tool-only turn) -> single bubble added");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    const FULL = "Answer from tool data. " + CHART;
    run([
        { event: "start", data: {} },
        { event: "tool_call", data: { name: "OpenSearchQuery", toolUseId: "t1" } },
        { event: "tool_result", data: { toolUseId: "t1", output: { rows: 3 } } },
        { event: "complete", data: { finalText: FULL } },
    ]);
    check("exactly one assistant bubble", assistantCount(list) === 1, "got " + assistantCount(list));
    check("bubble holds finalText", assistantRows(list)[0].text === FULL);
    check("added (not reconciled) — no false dup-prevent", !logs.some((l) => l.includes("DUPLICATE PREVENTED")));
})();

console.log("Scenario 5: two turns -> two bubbles, no cross-turn merge");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    run([
        { event: "start", data: {} },
        { event: "delta", data: { text: "first" } },
        { event: "complete", data: { finalText: "ANSWER ONE" } },
    ]);
    list.add({ id: ++uid, kind: "user", text: "second question" }); // real flow inserts a user row
    run([
        { event: "start", data: {} },
        { event: "delta", data: { text: "second" } },
        { event: "complete", data: { finalText: "ANSWER TWO" } },
    ]);
    check("exactly two assistant bubbles", assistantCount(list) === 2, "got " + assistantCount(list));
    check("both answers present, distinct", assistantRows(list)[0].text === "ANSWER ONE" && assistantRows(list)[1].text === "ANSWER TWO");
})();

console.log("Scenario 6: duplicate complete in a later turn must not merge into a prior turn's bubble");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    run([
        { event: "start", data: {} },
        { event: "delta", data: { text: "t1" } },
        { event: "complete", data: { finalText: "TURN ONE ANSWER" } },
    ]);
    list.add({ id: ++uid, kind: "user", text: "q2" });
    // Tool-only second turn (no deltas) + a spurious duplicate complete.
    run([
        { event: "start", data: {} },
        { event: "tool_call", data: { name: "OpenSearchQuery", toolUseId: "t2" } },
        { event: "tool_result", data: { toolUseId: "t2", output: { ok: true } } },
        { event: "complete", data: { finalText: "TURN TWO ANSWER" } },
        { event: "complete", data: { finalText: "TURN TWO ANSWER" } },
    ]);
    check("exactly two assistant bubbles", assistantCount(list) === 2, "got " + assistantCount(list));
    check("turn one bubble untouched", assistantRows(list)[0].text === "TURN ONE ANSWER");
    check("turn two bubble correct", assistantRows(list)[1].text === "TURN TWO ANSWER");
})();

console.log("Scenario 7: chart render fails then model retries -> single bubble, errored tool adds no bubble");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    const FINAL = "Here is your chart. " + CHART;
    run([
        { event: "start", data: {} },
        { event: "delta", data: { text: "Let me chart that for you" } },          // preamble -> reasoning on next tool_call
        { event: "tool_call", data: { name: "QuickChart", toolUseId: "c1" } },
        { event: "tool_result", data: { toolUseId: "c1", isError: true, output: { error: "chart render failed (status 400)" } } },
        { event: "tool_call", data: { name: "QuickChart", toolUseId: "c2" } },     // retry
        { event: "tool_result", data: { toolUseId: "c2", output: { imageUrl: "https://b.s3.amazonaws.com/chart/aaaaaaaa-1111-2222-3333-444444444444.png?sig" } } },
        { event: "delta", data: { text: "Here is your chart. " } },
        { event: "delta", data: { text: CHART } },
        { event: "complete", data: { finalText: FINAL } },
    ]);
    check("exactly one assistant bubble", assistantCount(list) === 1, "got " + assistantCount(list));
    check("bubble holds final answer with chart", assistantRows(list)[0].text === FINAL);
    check("errored tool_result did not become an assistant bubble", assistantCount(list) === 1);
    check("no false dup-prevent (normal update)", !logs.some((l) => l.includes("DUPLICATE PREVENTED")));
})();

console.log("Scenario 8: chart render fails, retries, AND a duplicate complete arrives -> still one bubble");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    const FINAL = "Recovered chart. " + CHART;
    run([
        { event: "start", data: {} },
        { event: "tool_call", data: { name: "QuickChart", toolUseId: "c1" } },
        { event: "tool_result", data: { toolUseId: "c1", isError: true, output: { error: "invalid config" } } },
        { event: "tool_call", data: { name: "QuickChart", toolUseId: "c2" } },
        { event: "tool_result", data: { toolUseId: "c2", output: { imageUrl: "https://b/chart/aaaaaaaa-1111-2222-3333-444444444444.png?s" } } },
        { event: "delta", data: { text: "partial" } },
        { event: "complete", data: { finalText: FINAL } },
        { event: "complete", data: { finalText: FINAL } },                          // spurious duplicate complete
    ]);
    check("exactly one assistant bubble", assistantCount(list) === 1, "got " + assistantCount(list));
    check("bubble holds recovered answer", assistantRows(list)[0].text === FINAL);
    check("reconcile prevented the duplicate", logs.some((l) => l.includes("DUPLICATE PREVENTED")));
})();

console.log("Scenario 9: preamble before a tool call becomes an expanded reasoning row");
(() => {
    logs.length = 0;
    const { list } = freshThread();
    run([
        { event: "start", data: {} },
        { event: "delta", data: { text: "Let me check that for you" } },
        { event: "tool_call", data: { name: "OpenSearchQuery", toolUseId: "t1" } },
        { event: "tool_result", data: { toolUseId: "t1", output: { ok: true } } },
        { event: "complete", data: { finalText: "Done." } },
    ]);
    const reasoning = list._rows.filter((r) => r.kind === "reasoning");
    check("exactly one reasoning row", reasoning.length === 1, "got " + reasoning.length);
    check("reasoning row is expanded by default", reasoning[0] && reasoning[0]._expanded === true,
        "_expanded=" + (reasoning[0] && reasoning[0]._expanded));
})();

console.log("");
console.log(failures === 0 ? "ALL TESTS PASSED" : (failures + " ASSERTION(S) FAILED"));
process.exit(failures === 0 ? 0 : 1);

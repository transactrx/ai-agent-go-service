/* Standalone test for aichatWebixTools.toolStepRowVisible — no test framework.
   Run: node webapp/aichat-webix/views/aichatWebixToolStepFilter.test.js

   Loads aichatWebixTools.js in a vm sandbox (its IIFE references webix/marked
   only inside functions we don't call, so an empty-ish sandbox is fine) and
   asserts the pure visibility predicate that drives the "Show tool steps" toggle. */

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const sandbox = { console: console, window: {}, webix: {}, marked: {} };
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(path.join(__dirname, "aichatWebixTools.js"), "utf8"), sandbox);
const visible = sandbox.aichatWebixTools.toolStepRowVisible;

let failures = 0;
function check(name, cond) {
    if (cond) console.log("  PASS: " + name);
    else { console.log("  FAIL: " + name); failures++; }
}

console.log("toolStepRowVisible — steps hidden by default (showToolSteps=false)");
check("tool_call hidden", visible({ kind: "tool_call" }, false) === false);
check("tool_result hidden", visible({ kind: "tool_result" }, false) === false);
check("reasoning shown", visible({ kind: "reasoning" }, false) === true);
check("assistant shown", visible({ kind: "assistant" }, false) === true);
check("attachment shown", visible({ kind: "attachment" }, false) === true);
check("widget shown", visible({ kind: "widget" }, false) === true);
check("working shown", visible({ kind: "working" }, false) === true);
check("null item shown (defensive)", visible(null, false) === true);

console.log("toolStepRowVisible — steps shown (showToolSteps=true)");
check("tool_call shown", visible({ kind: "tool_call" }, true) === true);
check("tool_result shown", visible({ kind: "tool_result" }, true) === true);

console.log("");
console.log(failures === 0 ? "ALL TESTS PASSED" : (failures + " ASSERTION(S) FAILED"));
process.exit(failures === 0 ? 0 : 1);

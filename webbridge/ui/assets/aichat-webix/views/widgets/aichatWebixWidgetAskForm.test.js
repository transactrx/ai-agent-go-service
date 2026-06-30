/* Standalone test for aichatWebixAskForm pure height math — no test framework.
   Run: node webapp/aichat-webix/views/widgets/aichatWebixWidgetAskForm.test.js

   Loads the AskForm widget file in a vm sandbox with a stub
   aichatWebixWidgets (so the registration IIFE does not bail), then asserts
   the pure line-count / height functions. DOM/canvas/webix are only touched
   inside buildConfig, which this test never calls. */

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const sandbox = {
    console: console,
    window: {},
    document: {},
    webix: {},
    aichatWebixWidgets: { register: function () {} },
};
vm.createContext(sandbox);
vm.runInContext(
    fs.readFileSync(path.join(__dirname, "aichatWebixWidgetAskForm.js"), "utf8"),
    sandbox
);
const A = sandbox.aichatWebixAskForm;

let failures = 0;
function check(name, cond) {
    if (cond) console.log("  PASS: " + name);
    else { console.log("  FAIL: " + name); failures++; }
}

console.log("aichatWebixAskForm.coerceFields");
check("array passes through", A.coerceFields([{ name: "a" }]).length === 1);
check("JSON-string array is parsed", A.coerceFields('[{"name":"a"},{"name":"b"}]').length === 2);
check("malformed JSON string -> []", A.coerceFields("{not json").length === 0);
check("non-array object -> []", A.coerceFields({ name: "a" }).length === 0);
check("undefined -> []", A.coerceFields(undefined).length === 0);
check("null -> []", A.coerceFields(null).length === 0);
check("number -> []", A.coerceFields(5).length === 0);
check("drops non-object entries", A.coerceFields([{ name: "a" }, "x", null, 3]).length === 1);

console.log("aichatWebixAskForm.labelLineCount");
check("0 width -> 1 line", A.labelLineCount(0, 700) === 1);
check("exactly fits -> 1 line", A.labelLineCount(700, 700) === 1);
check("just over -> 2 lines", A.labelLineCount(701, 700) === 2);
check("two full lines -> 2", A.labelLineCount(1400, 700) === 2);
check("over two -> 3 lines", A.labelLineCount(1401, 700) === 3);
check("availWidth<=0 -> 1 (defensive)", A.labelLineCount(5000, 0) === 1);

console.log("aichatWebixAskForm.labelHeight");
check("1 line = 18+4 = 22", A.labelHeight(1) === 22);
check("2 lines = 36+4 = 40", A.labelHeight(2) === 40);
check("undefined -> 1 line = 22 (defensive)", A.labelHeight() === 22);

// controlHeight is the active skin's input height, passed in by the DOM layer
// (30 on the app's 'compact' skin). The pure math never invents a size.
console.log("aichatWebixAskForm.fieldHeight (controlHeight=30)");
check("1-line text field = 22+30 = 52", A.fieldHeight({ lines: 1, checkbox: false, controlHeight: 30 }) === 52);
check("2-line text field = 40+30 = 70", A.fieldHeight({ lines: 2, checkbox: false, controlHeight: 30 }) === 70);
check("1-line checkbox = max(30,22) = 30", A.fieldHeight({ lines: 1, checkbox: true, controlHeight: 30 }) === 30);
check("3-line checkbox = max(30,58) = 58", A.fieldHeight({ lines: 3, checkbox: true, controlHeight: 30 }) === 58);

// total adds 2*FORM_PADDING (16) for the 8px inset on top+bottom.
console.log("aichatWebixAskForm.computeFormHeight (controlHeight=30)");
check("empty = pad+button+outer = 16+34+10 = 60", A.computeFormHeight([]) === 60);
check(
    "two 1-line text fields = 52+52 + 12 gap + 16 pad + 44 = 176",
    A.computeFormHeight([
        { lines: 1, checkbox: false, controlHeight: 30 },
        { lines: 1, checkbox: false, controlHeight: 30 },
    ]) === 176
);
check(
    "mixed text(1)+text(2)+checkbox(1) = 52+70+30 + 24 gaps + 16 pad + 44 = 236",
    A.computeFormHeight([
        { lines: 1, checkbox: false, controlHeight: 30 },
        { lines: 2, checkbox: false, controlHeight: 30 },
        { lines: 1, checkbox: true, controlHeight: 30 },
    ]) === 236
);

console.log("");
console.log(failures === 0 ? "ALL TESTS PASSED" : (failures + " ASSERTION(S) FAILED"));
process.exit(failures === 0 ? 0 : 1);

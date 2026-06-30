/* Standalone test for aichatWebixWidgets.helpers.coerceArray — no framework.
   Run: node webapp/aichat-webix/views/aichatWebixWidgetsCoerce.test.js

   Loads aichatWebixWidgets.js in a vm sandbox (it builds the registry object at
   load time with no webix calls) and asserts the defensive array coercion that
   guards every widget against a model-supplied options/fields/columns/rows that
   arrives as a JSON string or non-array (the "options.map is not a function" /
   "fields.forEach is not a function" class of render failures). */

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const sandbox = { console: console, window: {}, webix: {}, document: {} };
vm.createContext(sandbox);
vm.runInContext(
    fs.readFileSync(path.join(__dirname, "aichatWebixWidgets.js"), "utf8"),
    sandbox
);
const coerce = sandbox.aichatWebixWidgets.helpers.coerceArray;

let failures = 0;
function check(name, cond) {
    if (cond) console.log("  PASS: " + name);
    else { console.log("  FAIL: " + name); failures++; }
}

console.log("aichatWebixWidgets.helpers.coerceArray");
check("array of objects passes through", coerce([{ value: "a" }, { value: "b" }]).length === 2);
check("JSON-string array is parsed", coerce('[{"value":"a"},{"value":"b"}]').length === 2);
check("parsed values intact", coerce('[{"value":"x","label":"X"}]')[0].label === "X");
check("malformed JSON string -> []", coerce("{not json").length === 0);
check("non-array object -> []", coerce({ value: "a" }).length === 0);
check("undefined -> []", coerce(undefined).length === 0);
check("null -> []", coerce(null).length === 0);
check("number -> []", coerce(42).length === 0);
check("drops non-object entries", coerce([{ value: "a" }, "x", null, 3]).length === 1);

console.log("");
console.log(failures === 0 ? "ALL TESTS PASSED" : (failures + " ASSERTION(S) FAILED"));
process.exit(failures === 0 ? 0 : 1);

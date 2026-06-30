/* aichat-webix client test runner. Runs every *.test.js in this tree as a
   separate Node process and prints one summary. Exit code is non-zero if any
   suite fails. Run: node webapp/aichat-webix/run-tests.js */

const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");

function findTests(dir) {
    let out = [];
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, entry.name);
        if (entry.isDirectory()) out = out.concat(findTests(p));
        else if (entry.name.endsWith(".test.js")) out.push(p);
    }
    return out;
}

const root = __dirname;
const tests = findTests(root).sort();
let failed = 0;

console.log("Running " + tests.length + " aichat-webix client test suite(s):\n");
for (const t of tests) {
    const rel = path.relative(root, t);
    try {
        const out = execFileSync("node", [t], { encoding: "utf8" });
        const passes = (out.match(/PASS:/g) || []).length;
        console.log("  ✓ " + rel + "  (" + passes + " assertions)");
    } catch (e) {
        failed++;
        console.log("  ✗ " + rel + "  FAILED");
        console.log((e.stdout || "").split("\n").filter((l) => l.includes("FAIL")).map((l) => "      " + l).join("\n"));
    }
}

console.log("");
if (failed === 0) console.log("ALL SUITES PASSED (" + tests.length + ")");
else console.log(failed + " / " + tests.length + " SUITE(S) FAILED");
process.exit(failed === 0 ? 0 : 1);

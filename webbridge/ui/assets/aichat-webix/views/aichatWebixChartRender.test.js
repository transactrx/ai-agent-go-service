/* Chart-image rendering test — no test framework.
   Run: node webapp/aichat-webix/views/aichatWebixChartRender.test.js

   Guards the "broken chart image in a reasoning row" bug: the model often writes
   a ![chart](...) link in its intermediate reasoning prose with a placeholder /
   fabricated URL (only the FINAL answer's chart URL is server-validated). Such a
   link rendered as a broken <img>. The reasoning template must neutralize chart
   images to plain text; the final-answer (assistant) template must STILL render
   them. Loads the real templates with marked/webix stubbed to passthrough so we
   can inspect exactly what each template emits. */

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const sandbox = {
    console: { log: () => {}, warn: () => {}, error: () => {} },
    // passthrough stubs so we can see exactly what text reaches the renderer
    marked: { parse: (t) => (t == null ? "" : String(t)) },
    webix: { template: { escape: (t) => (t == null ? "" : String(t)) }, event: () => {} },
    document: { getElementById: () => null },
};
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(path.join(__dirname, "aichatWebixBubbles.js"), "utf8"), sandbox);
vm.runInContext(fs.readFileSync(path.join(__dirname, "aichatWebixTools.js"), "utf8"), sandbox);

const tools = sandbox.aichatWebixTools;
const bubbles = sandbox.aichatWebixBubbles;

const CHART = "![chart](https://opensearchaichatapi-assistant-files-development.s3.us-east-1.amazonaws.com/chart/c999a1bf-d457-4c80-876d-246121a58e05.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc)";
const CHART_KEY = "c999a1bf";

let failures = 0;
function check(name, cond, detail) {
    if (cond) console.log("  PASS: " + name);
    else { console.log("  FAIL: " + name + (detail ? " — " + detail : "")); failures++; }
}

console.log("neutralizeChartImages() — pure helper");
(() => {
    const n = tools.neutralizeChartImages;
    check("single chart image -> placeholder", n("look " + CHART + " end") === "look *(chart)* end", n("look " + CHART + " end"));
    check("chart key removed", !n("a " + CHART + " b").includes(CHART_KEY));
    check("no image markup remains", n("a " + CHART + " b").indexOf("![") === -1);
    const two = "p " + CHART + " q " + CHART.replace(CHART_KEY, "deadbeef") + " r";
    check("multiple chart images all neutralized", n(two).indexOf("chart/") === -1, n(two));
    check("plain text unchanged", n("just some analysis, no chart") === "just some analysis, no chart");
    check("empty/null safe", n("") === "" && n(null) == null && n(undefined) == undefined);
    // Regression: the model also writes raw quickchart.io render URLs (no .png,
    // different path) in reasoning — a chart-URL-pattern regex missed these and
    // let a broken <img> through. Neutralize ALL markdown images by syntax.
    const qc = "building it ![chart](https://quickchart.io/chart/render/zf-c80e5024-fe4e-43f6-9a73-39c6a2b9cf81) now";
    check("quickchart.io render URL neutralized", n(qc).indexOf("quickchart.io") === -1 && n(qc).indexOf("![") === -1, n(qc));
    const other = "see ![logo](https://x/img/logo.png) here";
    check("any markdown image neutralized (alt preserved)", n(other) === "see *(logo)* here", n(other));
    check("image with empty alt -> (chart)", n("x ![](https://h/p.png) y") === "x *(chart)* y", n("x ![](https://h/p.png) y"));
})();

console.log("reasoning template — chart images neutralized (the screenshot bug)");
(() => {
    const html = tools.template({ kind: "reasoning", id: 1, _expanded: true,
        text: "Let me build a line chart. Computing totals...\n" + CHART });
    check("reasoning output has NO chart url", html.indexOf("chart/" + CHART_KEY) === -1, html.slice(0, 200));
    check("reasoning output has NO image markdown", html.indexOf("![chart]") === -1);
    check("reasoning output keeps the surrounding prose", html.indexOf("Computing totals") !== -1);
    check("reasoning output has the (chart) placeholder", html.indexOf("(chart)") !== -1);
})();

console.log("final-answer (assistant) template — chart STILL renders when finalized");
(() => {
    // Finalized bubble (_streaming falsy) -> chart renders.
    const finalHtml = bubbles.template({ kind: "assistant", id: 2, _streaming: false,
        text: "Here is the chart you asked for:\n" + CHART });
    check("finalized assistant PRESERVES the chart url", finalHtml.indexOf("chart/" + CHART_KEY) !== -1);
    check("finalized assistant PRESERVES image markdown", finalHtml.indexOf("![chart]") !== -1);

    // A bubble with no _streaming flag at all (e.g. complete's add branch) renders too.
    const noFlag = bubbles.template({ kind: "assistant", id: 3, text: CHART });
    check("assistant with no _streaming flag renders chart", noFlag.indexOf("chart/" + CHART_KEY) !== -1);
})();

console.log("streaming assistant bubble — chart suppressed + 'loading chart' placeholder (no transient broken <img>)");
(() => {
    const streamingHtml = bubbles.template({ kind: "assistant", id: 4, _streaming: true,
        text: "Let me chart that:\n" + CHART });
    check("streaming assistant has NO chart url", streamingHtml.indexOf("chart/" + CHART_KEY) === -1, streamingHtml.slice(0, 160));
    check("streaming assistant has NO image markdown", streamingHtml.indexOf("![chart]") === -1);
    check("streaming assistant keeps surrounding prose", streamingHtml.indexOf("Let me chart that") !== -1);
    check("streaming assistant shows the loading-chart placeholder text", streamingHtml.indexOf("loading chart") !== -1, streamingHtml.slice(0, 200));
    check("streaming assistant shows an inline spinner element", streamingHtml.indexOf("aichat-webix-chart-spinner-sm") !== -1);
})();

console.log("loadingChartPlaceholders() — pure helper");
(() => {
    const f = bubbles.loadingChartPlaceholders;
    check("chart image -> loading placeholder (url + markdown gone)", (() => {
        const out = f("see " + CHART + " end");
        return out.indexOf(CHART_KEY) === -1 && out.indexOf("![") === -1 && out.indexOf("loading chart") !== -1;
    })());
    check("plain text unchanged", f("just analysis, no chart") === "just analysis, no chart");
    check("empty/null safe", f("") === "" && f(null) == null && f(undefined) == undefined);
})();

console.log("");
console.log(failures === 0 ? "ALL TESTS PASSED" : (failures + " ASSERTION(S) FAILED"));
process.exit(failures === 0 ? 0 : 1);

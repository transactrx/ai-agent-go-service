# Deferred Hardening — v1.5.1 (design)

Date: 2026-08-07 · Branch: `feature/deferred-hardening` · Status: APPROVED

Resolves all nine items deferred from the machine-caller (v1.4.0) and workflow-derivation
(v1.5.0) reviews. Patch release: fixes of intended behavior, added tests, no new API
surface or config fields. The only caller-visible change is A2 (timeout status code),
called out in the CHANGELOG.

## A. Behavior fixes

### A1. Pad empty tool results
**Problem:** a tool returning empty output is forwarded as an empty tool_result content
block; Bedrock rejects the whole request (`ValidationException: user messages must have
non-empty content`) and the caller gets a 500. Observed live 2026-08-07.
**Fix:** in the agent loop, when a tool's output serializes to the empty string, send the
literal `(tool returned no output)` as the tool_result content instead. Unconditional, no
config. The model then answers "no data" normally.
**Test:** unit test — tool returns empty → request content non-empty, turn completes.

### A2. Mid-stream timeout maps to timeout/504
**Problem:** when `requestTimeoutSeconds` expires while the LLM call is in flight, the
Bedrock SDK error (wrapping `context.DeadlineExceeded`) surfaces through the agent loop as
`llm-error`/500. The trigger's own timeout branch (`errcode.Timeout`/504) never fires
because the error event wins the race. Deferred at v1.4.0 review.
**Fix:** where natschat maps a surfaced error to code/status, classify first: if the error
wraps `context.DeadlineExceeded` → `errcode.Timeout`/504; if it wraps `context.Canceled`
(client cancel) → `errcode.Cancelled` (existing mapping). Everything else stays
`llm-error`/500. Reuses the classification already present in `engine/retry/classify.go`
semantics; no agent-loop restructuring.
**Test:** unit test per mapping (deadline → 504, cancel → Cancelled).

### A3. Tool-description wording (library half)
**Problem:** the opensearch tool's built-in `queryBody` description says the server
"injects security/date constraints into must". Only security scope clauses are injected
(`buildScopeClauses` — origin/ncpdp/npi terms; no date clause exists). The model repeats
the false date claim to users.
**Fix:** change the wording to "injects security constraints into must" in
`pkg/workflow/builtin/opensearch/factory.go`. ✓ DONE
**Consumer follow-up (chatApi, separate branch after release):** same edit in the workflow
JSONs — `systemMessageFixed` ("Security scope and the date range are enforced
server-side…" → security scope only) and `toolDescription` in the base workflows; the
Single* overlays own their prompts and get the same edit.

## B. Derivation loader hardening

Current pass-1 index in `engine.LoadAll` is keyed by source id (from `Source.List`, i.e.
file naming); `extends` resolves against it. Items B1–B3 replace that resolution with a
document-id index built in pass 1:

### B1. Resolve `extends` by workflow id
Index every loadable document by its JSON `"id"` field; `extends` looks up the base there.
File names stop mattering. Log lines keep the same shape.

### B2. Duplicate workflow id guard
While building the id index, a second document declaring an already-seen `"id"` fails
registration (clear log, counted in "loaded N of M"); the first wins deterministically
(alphabetical source order). Isolation preserved — nothing else is affected.

### B3. Wrong-type `extends` is an error
`ExtendsTarget` (or its call site) distinguishes "no extends key" from "extends present
but not a non-empty string". The latter fails that workflow with a clear log instead of
silently loading it as non-derived.

### B4. Merge cleanliness
`deepMerge` returns freshly-built maps (no aliasing of base/overlay sub-maps into the
result). `$remove` keys never appear in the merged output (today `"$remove": false`
leaks). Both verified inert at v1.5.0 review; fixed for future editors.

### B5. Passthrough regression test
Golden test: a base document with representative field shapes (nested objects, arrays,
numbers, nulls-as-values) merged with an overlay touching only some fields → every
untouched field is semantically identical (deep-equal after JSON round-trip) in the merged
result.

## C. Tests, version, rollout

- **C1.** Two natschat unit tests from the v1.4.0 review: terminator-vs-emitErr ordering,
  and client cancellation maps to `Cancelled`, never 504.
- **C2.** Version **v1.5.1**: merge Development → Production release PR *without* a
  version label (pipeline default = patch). CHANGELOG entry with the A2 status-code
  callout.
- **C3.** Rollout: lib release first; then chatApi branch = bump v1.5.1 + A3 JSON wording
  edits (local ecosystem validation per house rules); webApp = optional alignment bump
  only. No other consumer change.

## Out of scope
- Any date-window enforcement (none exists; none added).
- OpenSearch retention / S3 archive access.
- New config fields or workflow schema changes.

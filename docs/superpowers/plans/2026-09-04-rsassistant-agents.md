# RSAssistant Agents (library v1.7.0) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let any eligible `trigger/nats-chat` workflow loaded by this library appear to RSAssistant as its own `nats-agent` agent, with identity verified before the workflow is called, without changing any existing subject, header, or behavior.

**Architecture:** New package `pkg/rsassistant` reads the loaded engine after `natservice.Start()`, decides eligibility from the real node objects (`natschat.ChatEndpoint`, `ClientOnly()`), builds one nats-agent v0.2.0 agent per eligible workflow, and bridges each consult into the workflow's existing NATS subject via `natsstream.DoStreamingRequest`, mapping engine stream events onto nats-agent stream events. Identity headers sent to the engine come only from the nats-agent verified identity. Everything is gated by `RSASSISTANT_ENABLED` (default off).

**Tech Stack:** Go 1.25, github.com/transactrx/nats-agent v0.2.0, github.com/transactrx/nats-service v1.4.46 (already required), nats.go, embedded nats-server for tests (already in go.mod).

**Spec:** `docs/superpowers/specs/2026-09-04-rsassistant-agents-design.md`

## Global Constraints

- Branch `feature/rsassistant-agents` from `Development`. Do not commit unless the user asked; the plan's commit steps are executed only when the user has authorized commits for the session.
- Additive only. Existing exported signatures are not changed. New fields are `omitempty`. Flag off = zero behavior change.
- All builds and tests run with the vendor tree: `go build -mod=vendor ./...`, `go test -mod=vendor ./...`. After any go.mod change: `go mod tidy && go mod vendor`.
- `GOPRIVATE=github.com/transactrx` must be set for `go get` of transactrx modules.
- Naming: JSON key `rsassistant`, package `pkg/rsassistant`, env prefix `RSASSISTANT_`, log prefix `RSASSISTANT event=...`.
- Identity pair comes from `APP_ID` and `APP_FUNCTION_ID` only. Never from the manifest.
- Headers to the engine come only from `turn.Identity`, never from `turn.UserID`.
- Default consult timeout 400s. nats-agent error codes: forbidden `4031`, upstream `5001`.
- Release target v1.7.0; tagging happens only after the user approves the PR.

---

### Task 1: Loader and engine carry the `rsassistant` manifest

**Files:**
- Modify: `pkg/workflow/engine/loader/types.go:6-14`
- Modify: `pkg/workflow/engine/loader/loader.go:15-24` and `:161-170`
- Modify: `pkg/workflow/engine/executor/workflow.go:12-21`
- Modify: `pkg/workflow/engine/engine.go:212-220`
- Test: `pkg/workflow/engine/loader/rsassistant_field_test.go`
- Test: `pkg/workflow/engine/rsassistant_field_test.go`

**Interfaces:**
- Produces: `loader.LoadResult.RSAssistant json.RawMessage`, `executor.Workflow.RSAssistant json.RawMessage` (nil when the file has no block). `engine.Workflow` is an alias of `executor.Workflow`, so later tasks read `wf.RSAssistant`.

- [ ] **Step 1: Write the failing loader test**

```go
// pkg/workflow/engine/loader/rsassistant_field_test.go
package loader

import (
	"encoding/json"
	"testing"
)

func TestRawWorkflowKeepsRSAssistantBlock(t *testing.T) {
	raw := []byte(`{"id":"wf","version":1,"trigger":"t","nodes":[],"connections":[],
	  "rsassistant":{"name":"claimSearch","tags":["a"]}}`)
	var w rawWorkflow
	if err := json.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	if len(w.RSAssistant) == 0 {
		t.Fatal("rsassistant block was dropped by rawWorkflow decode")
	}
	var m map[string]any
	if err := json.Unmarshal(w.RSAssistant, &m); err != nil {
		t.Fatal(err)
	}
	if m["name"] != "claimSearch" {
		t.Fatalf("name = %v, want claimSearch", m["name"])
	}
}

func TestRawWorkflowWithoutBlockHasNilManifest(t *testing.T) {
	var w rawWorkflow
	if err := json.Unmarshal([]byte(`{"id":"wf","version":1,"trigger":"t","nodes":[],"connections":[]}`), &w); err != nil {
		t.Fatal(err)
	}
	if w.RSAssistant != nil {
		t.Fatalf("expected nil manifest, got %s", w.RSAssistant)
	}
}

func TestMergeDerivedInheritsRSAssistantBlock(t *testing.T) {
	base := []byte(`{"id":"p","version":1,"trigger":"n1",
	  "rsassistant":{"name":"claimSearch","description":"base desc"},
	  "nodes":[{"id":"n1","type":"t","config":{}}],"connections":[]}`)
	overlay := []byte(`{"id":"c","extends":"p","rsassistant":{"description":"child desc"}}`)
	merged, err := MergeDerived(base, overlay)
	if err != nil {
		t.Fatal(err)
	}
	var w rawWorkflow
	if err := json.Unmarshal(merged, &w); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(w.RSAssistant, &m)
	if m["name"] != "claimSearch" || m["description"] != "child desc" {
		t.Fatalf("merged manifest = %v, want name from base and description from overlay", m)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service && go test -mod=vendor ./pkg/workflow/engine/loader/ -run 'RSAssistant' -v`
Expected: compile error `w.RSAssistant undefined`.

- [ ] **Step 3: Add the field to the three structs and copy it through**

`pkg/workflow/engine/loader/types.go` — add to `rawWorkflow` after `Description`:

```go
	// RSAssistant is the optional manifest that publishes this workflow as an
	// RSAssistant (nats-agent) agent. Opaque to the loader; parsed by pkg/rsassistant.
	RSAssistant json.RawMessage `json:"rsassistant,omitempty"`
```

`pkg/workflow/engine/loader/loader.go` — add to `LoadResult` after `Description`:

```go
	RSAssistant json.RawMessage // raw "rsassistant" block, nil when absent
```

and in the `return &LoadResult{...}` at the end of `LoadOne` add `RSAssistant: w.RSAssistant,` after `Description: w.Description,`.

`pkg/workflow/engine/executor/workflow.go` — add to `Workflow` after `Description`:

```go
	RSAssistant json.RawMessage // raw "rsassistant" manifest from the workflow file, nil when absent
```

`pkg/workflow/engine/engine.go` — in `registerOne`'s `wf := &Workflow{...}` add `RSAssistant: res.RSAssistant,` after `Description: res.Description,`.

- [ ] **Step 4: Run the loader tests**

Run: `go test -mod=vendor ./pkg/workflow/engine/loader/ -v`
Expected: all PASS, including the three new tests.

- [ ] **Step 5: Write the failing engine propagation test**

```go
// pkg/workflow/engine/rsassistant_field_test.go
package engine_test

import (
	"context"
	"encoding/json"
	"testing"
)

func TestEngineWorkflowCarriesRSAssistantManifest(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "wf1.json", `{
	  "id": "wf1", "version": 1, "trigger": "trig",
	  "rsassistant": {"name": "claimSearch", "displayName": "Claim Search"},
	  "nodes": [
	    {"id": "trig",  "type": "test/trigger", "config": {}},
	    {"id": "agent", "type": "test/agent",   "config": {}}
	  ],
	  "connections": [
	    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}
	  ]
	}`)
	writeFile(t, dir, "wf2.json", `{
	  "id": "wf2", "version": 1, "trigger": "trig",
	  "nodes": [
	    {"id": "trig",  "type": "test/trigger", "config": {}},
	    {"id": "agent", "type": "test/agent",   "config": {}}
	  ],
	  "connections": [
	    {"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}
	  ]
	}`)
	eng := newTestEngine(t, dir)
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	wf1, ok := eng.Workflow("wf1")
	if !ok {
		t.Fatal("wf1 not registered")
	}
	var m map[string]any
	if err := json.Unmarshal(wf1.RSAssistant, &m); err != nil {
		t.Fatalf("wf1.RSAssistant not JSON: %v (%s)", err, wf1.RSAssistant)
	}
	if m["displayName"] != "Claim Search" {
		t.Fatalf("displayName = %v", m["displayName"])
	}
	wf2, _ := eng.Workflow("wf2")
	if wf2.RSAssistant != nil {
		t.Fatalf("wf2 should have nil manifest, got %s", wf2.RSAssistant)
	}
}
```

`writeFile` and `newTestEngine` already exist in `pkg/workflow/engine/engine_test.go` (same test package).

- [ ] **Step 6: Run engine tests**

Run: `go test -mod=vendor ./pkg/workflow/engine/... -v -run 'RSAssistant|TestEngineLoads'`
Expected: PASS.

- [ ] **Step 7: Full build and test, then commit (only if commits are authorized)**

Run: `go build -mod=vendor ./... && go test -mod=vendor ./...`
Expected: PASS everywhere.

```bash
git add pkg/workflow/engine/loader/types.go pkg/workflow/engine/loader/loader.go pkg/workflow/engine/executor/workflow.go pkg/workflow/engine/engine.go pkg/workflow/engine/loader/rsassistant_field_test.go pkg/workflow/engine/rsassistant_field_test.go
git commit -m "feat(loader): carry optional top-level rsassistant manifest through LoadResult and Workflow"
```

---

### Task 2: `natschat.ChatEndpoint` read-only accessors

**Files:**
- Modify: `pkg/workflow/builtin/natschat/factory.go` (append after the `natsChatTrigger` struct, line 104)
- Test: `pkg/workflow/builtin/natschat/chat_endpoint_test.go`

**Interfaces:**
- Produces:
  ```go
  type ChatEndpoint interface {
      ChatSubject() string
      ResponseMode() string
      AllowsResponseModeOverride() bool
      RequestTimeout() time.Duration
  }
  ```
  implemented by `*natsChatTrigger`. `ChatSubject()` is `"<basePath>.<subject>"` once `Init` has run, `""` before.

- [ ] **Step 1: Write the failing test**

```go
// pkg/workflow/builtin/natschat/chat_endpoint_test.go
package natschat

import (
	"testing"
	"time"
)

func TestChatEndpointAccessors(t *testing.T) {
	tr := newTestTrigger(t, `{"responseMode":"single","allowResponseModeOverride":true,"requestTimeoutSeconds":45}`)
	var ep ChatEndpoint = tr // compile-time: trigger implements the interface

	if ep.ChatSubject() != "" {
		t.Fatalf("before Init, ChatSubject should be empty, got %q", ep.ChatSubject())
	}
	tr.basePath = "trx.test"
	tr.subject = "SingleSearch"
	if got := ep.ChatSubject(); got != "trx.test.SingleSearch" {
		t.Fatalf("ChatSubject = %q", got)
	}
	if ep.ResponseMode() != "single" {
		t.Fatalf("ResponseMode = %q", ep.ResponseMode())
	}
	if !ep.AllowsResponseModeOverride() {
		t.Fatal("AllowsResponseModeOverride should be true")
	}
	if ep.RequestTimeout() != 45*time.Second {
		t.Fatalf("RequestTimeout = %s", ep.RequestTimeout())
	}
}

func TestChatEndpointDefaults(t *testing.T) {
	tr := newTestTrigger(t, `{"responseMode":"streaming"}`)
	if tr.AllowsResponseModeOverride() {
		t.Fatal("override must default to false")
	}
	if tr.RequestTimeout() != 600*time.Second {
		t.Fatalf("default RequestTimeout = %s, want 600s", tr.RequestTimeout())
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -mod=vendor ./pkg/workflow/builtin/natschat/ -run ChatEndpoint -v`
Expected: `undefined: ChatEndpoint`.

- [ ] **Step 3: Implement the interface and methods**

Append to `pkg/workflow/builtin/natschat/factory.go`:

```go
// ChatEndpoint is the read-only view of a trigger/nats-chat node used by
// out-of-band publishers (pkg/rsassistant) to reach a workflow over NATS
// without touching how the trigger itself serves requests.
type ChatEndpoint interface {
	// ChatSubject is "<basePath>.<subject>". Empty before Init has run.
	ChatSubject() string
	// ResponseMode is the configured default: "streaming" or "single".
	ResponseMode() string
	// AllowsResponseModeOverride reports whether a request body may set responseMode.
	AllowsResponseModeOverride() bool
	// RequestTimeout is the configured per-request timeout.
	RequestTimeout() time.Duration
}

var _ ChatEndpoint = (*natsChatTrigger)(nil)

func (t *natsChatTrigger) ChatSubject() string {
	if t.basePath == "" || t.subject == "" {
		return ""
	}
	return t.basePath + "." + t.subject
}

func (t *natsChatTrigger) ResponseMode() string { return t.cfg.ResponseMode }

func (t *natsChatTrigger) AllowsResponseModeOverride() bool { return t.cfg.AllowResponseModeOverride }

func (t *natsChatTrigger) RequestTimeout() time.Duration {
	return time.Duration(t.cfg.RequestTimeoutSeconds) * time.Second
}
```

- [ ] **Step 4: Run tests**

Run: `go test -mod=vendor ./pkg/workflow/builtin/natschat/ -v`
Expected: PASS (existing tests unchanged).

- [ ] **Step 5: Commit (only if authorized)**

```bash
git add pkg/workflow/builtin/natschat/factory.go pkg/workflow/builtin/natschat/chat_endpoint_test.go
git commit -m "feat(natschat): ChatEndpoint read-only accessors (subject, mode, override, timeout)"
```

---

### Task 3: `pkg/rsassistant` config and manifest

**Files:**
- Create: `pkg/rsassistant/config.go`
- Create: `pkg/rsassistant/manifest.go`
- Test: `pkg/rsassistant/config_test.go`
- Test: `pkg/rsassistant/manifest_test.go`

**Interfaces:**
- Produces:
  ```go
  type Config struct {
      Enabled         bool
      ObserveOnly     bool
      AppID           string
      FunctionID      string
      IdentitySubject string
      ConsultTimeout  time.Duration
      DefaultVersion  string
  }
  func EnabledFromEnv(lookup func(string) (string, bool)) bool
  func ConfigFromEnv(lookup func(string) (string, bool)) (Config, error)

  type Skill struct { Name, Description string; Examples []string }
  type Manifest struct { Enabled *bool; Name, DisplayName, Description, Version string; Tags []string; Skills []Skill }
  type CardSpec struct { Name, DisplayName, Description, Version string; Tags []string; Skills []Skill }
  var ErrDisabled = errors.New("rsassistant: disabled by manifest")
  func ParseManifest(raw json.RawMessage) (Manifest, error)
  func BuildCardSpec(workflowID, workflowDescription string, raw json.RawMessage, defaultVersion string) (CardSpec, error)
  ```

- [ ] **Step 1: Write the failing config test**

```go
// pkg/rsassistant/config_test.go
package rsassistant

import (
	"testing"
	"time"
)

func lookupFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestEnabledFromEnv(t *testing.T) {
	if EnabledFromEnv(lookupFrom(map[string]string{})) {
		t.Fatal("unset must be disabled")
	}
	if EnabledFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "false"})) {
		t.Fatal("false must be disabled")
	}
	if !EnabledFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "TRUE"})) {
		t.Fatal("TRUE (any case) must enable")
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	cfg, err := ConfigFromEnv(lookupFrom(map[string]string{
		"RSASSISTANT_ENABLED": "true",
		"APP_ID":              "OPENSEARCHAICHATAPIAPPID",
		"APP_FUNCTION_ID":     "OPENSEARCHAICHATAPIFUNCTIONID",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ObserveOnly {
		t.Fatal("default must be strict (ObserveOnly=false)")
	}
	if cfg.IdentitySubject != "trx.identityservice.validateInternalToken" {
		t.Fatalf("IdentitySubject = %q", cfg.IdentitySubject)
	}
	if cfg.ConsultTimeout != 400*time.Second {
		t.Fatalf("ConsultTimeout = %s", cfg.ConsultTimeout)
	}
	if cfg.DefaultVersion != "1.0.0" {
		t.Fatalf("DefaultVersion = %q", cfg.DefaultVersion)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	cfg, err := ConfigFromEnv(lookupFrom(map[string]string{
		"RSASSISTANT_ENABLED":                 "true",
		"RSASSISTANT_IDT_OBSERVE_ONLY":        "true",
		"RSASSISTANT_CONSULT_TIMEOUT_SECONDS": "90",
		"RSASSISTANT_AGENT_VERSION":           "2.1.0",
		"APP_ID":                              "app",
		"APP_FUNCTION_ID":                     "fn",
		"NATS_IDENTITY_BASE_PATH":             "test.identity",
		"NATS_IDENTITY_VALIDATE_SUBJECT":      "validate",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ObserveOnly || cfg.ConsultTimeout != 90*time.Second || cfg.DefaultVersion != "2.1.0" || cfg.IdentitySubject != "test.identity.validate" {
		t.Fatalf("unexpected cfg %+v", cfg)
	}
}

func TestConfigFromEnvRequiresIdentityPair(t *testing.T) {
	if _, err := ConfigFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "true", "APP_ID": "app"})); err == nil {
		t.Fatal("missing APP_FUNCTION_ID must error")
	}
	if _, err := ConfigFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "true", "APP_FUNCTION_ID": "fn"})); err == nil {
		t.Fatal("missing APP_ID must error")
	}
}

func TestConfigFromEnvBadTimeoutFallsBack(t *testing.T) {
	cfg, err := ConfigFromEnv(lookupFrom(map[string]string{
		"RSASSISTANT_ENABLED": "true", "APP_ID": "a", "APP_FUNCTION_ID": "f",
		"RSASSISTANT_CONSULT_TIMEOUT_SECONDS": "not-a-number",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsultTimeout != 400*time.Second {
		t.Fatalf("bad timeout must fall back to 400s, got %s", cfg.ConsultTimeout)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -mod=vendor ./pkg/rsassistant/ -v`
Expected: `no Go files` / undefined symbols.

- [ ] **Step 3: Implement config.go**

```go
// Package rsassistant publishes eligible trigger/nats-chat workflows as
// RSAssistant-discoverable agents on the NATS agent mesh (nats-agent protocol).
// Everything here is additive and disabled unless RSASSISTANT_ENABLED=true.
// See docs/superpowers/specs/2026-09-04-rsassistant-agents-design.md.
package rsassistant

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	envEnabled        = "RSASSISTANT_ENABLED"
	envObserveOnly    = "RSASSISTANT_IDT_OBSERVE_ONLY"
	envConsultTimeout = "RSASSISTANT_CONSULT_TIMEOUT_SECONDS"
	envAgentVersion   = "RSASSISTANT_AGENT_VERSION"
	envAppID          = "APP_ID"
	envFunctionID     = "APP_FUNCTION_ID"
	envIdentityBase   = "NATS_IDENTITY_BASE_PATH"
	envIdentitySubj   = "NATS_IDENTITY_VALIDATE_SUBJECT"

	defaultIdentityBase   = "trx.identityservice"
	defaultIdentitySubj   = "validateInternalToken"
	defaultConsultTimeout = 400 * time.Second
	defaultAgentVersion   = "1.0.0"
)

// Config is the process-wide configuration for the published agents.
type Config struct {
	Enabled         bool
	ObserveOnly     bool          // IDT observe phase: log, never block at the nats-agent layer
	AppID           string        // card access.appId (identity application)
	FunctionID      string        // card access.functionId (identity function)
	IdentitySubject string        // NATS subject of identity validateInternalToken
	ConsultTimeout  time.Duration // per-consult timeout for the engine self-call
	DefaultVersion  string        // card version when the manifest sets none
}

func isTrue(v string) bool { return strings.EqualFold(strings.TrimSpace(v), "true") }

// EnabledFromEnv reports RSASSISTANT_ENABLED=true (case-insensitive).
func EnabledFromEnv(lookup func(string) (string, bool)) bool {
	v, _ := lookup(envEnabled)
	return isTrue(v)
}

// ConfigFromEnv reads the env contract. It errors only for a fatal
// misconfiguration (missing identity pair); bad optional values fall back to
// defaults so a typo cannot silently disable identity checks.
func ConfigFromEnv(lookup func(string) (string, bool)) (Config, error) {
	get := func(k, def string) string {
		if v, ok := lookup(k); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	cfg := Config{
		Enabled:         EnabledFromEnv(lookup),
		ObserveOnly:     isTrue(get(envObserveOnly, "false")),
		AppID:           get(envAppID, ""),
		FunctionID:      get(envFunctionID, ""),
		IdentitySubject: get(envIdentityBase, defaultIdentityBase) + "." + get(envIdentitySubj, defaultIdentitySubj),
		ConsultTimeout:  defaultConsultTimeout,
		DefaultVersion:  get(envAgentVersion, defaultAgentVersion),
	}
	if raw := get(envConsultTimeout, ""); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
			cfg.ConsultTimeout = time.Duration(secs) * time.Second
		}
	}
	if cfg.AppID == "" || cfg.FunctionID == "" {
		return cfg, errors.New("rsassistant: APP_ID and APP_FUNCTION_ID are required when RSASSISTANT_ENABLED=true")
	}
	return cfg, nil
}
```

- [ ] **Step 4: Run config tests**

Run: `go test -mod=vendor ./pkg/rsassistant/ -run Config -v`
Expected: PASS.

- [ ] **Step 5: Write the failing manifest test**

```go
// pkg/rsassistant/manifest_test.go
package rsassistant

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestBuildCardSpecDefaultsFromWorkflow(t *testing.T) {
	spec, err := BuildCardSpec("SinglePowerlineSearch", "Pharmacy claim search agent.", nil, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Name != "SinglePowerlineSearch" || spec.DisplayName != "SinglePowerlineSearch" ||
		spec.Description != "Pharmacy claim search agent." || spec.Version != "1.0.0" {
		t.Fatalf("unexpected defaults %+v", spec)
	}
	if len(spec.Tags) != 0 || len(spec.Skills) != 0 {
		t.Fatalf("tags/skills must default empty: %+v", spec)
	}
}

func TestBuildCardSpecEmptyDescriptionFallsBack(t *testing.T) {
	spec, err := BuildCardSpec("wf", "", nil, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Description != "Workflow wf" {
		t.Fatalf("Description = %q", spec.Description)
	}
}

func TestBuildCardSpecManifestOverrides(t *testing.T) {
	raw := json.RawMessage(`{"name":"powerlineClaimSearch","displayName":"PowerLine Claim Search",
	  "description":"Claims Q&A","version":"3.0.0","tags":["powerline"],
	  "skills":[{"name":"claim-search","description":"search claims","examples":["how many today?"]}]}`)
	spec, err := BuildCardSpec("SinglePowerlineSearch", "ignored", raw, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Name != "powerlineClaimSearch" || spec.DisplayName != "PowerLine Claim Search" ||
		spec.Description != "Claims Q&A" || spec.Version != "3.0.0" {
		t.Fatalf("overrides not applied: %+v", spec)
	}
	if len(spec.Tags) != 1 || len(spec.Skills) != 1 || spec.Skills[0].Examples[0] != "how many today?" {
		t.Fatalf("tags/skills not applied: %+v", spec)
	}
}

func TestBuildCardSpecDisabled(t *testing.T) {
	_, err := BuildCardSpec("wf", "d", json.RawMessage(`{"enabled":false}`), "1.0.0")
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("want ErrDisabled, got %v", err)
	}
}

func TestBuildCardSpecRejectsBadNames(t *testing.T) {
	for _, bad := range []string{"has space", "dots.not.ok", "discover", "announce", "slash/x"} {
		raw := json.RawMessage(`{"name":"` + bad + `"}`)
		if _, err := BuildCardSpec("wf", "d", raw, "1.0.0"); err == nil {
			t.Fatalf("name %q must be rejected", bad)
		}
	}
	if _, err := BuildCardSpec("bad id with spaces", "d", nil, "1.0.0"); err == nil {
		t.Fatal("workflow id used as name must also be validated")
	}
}

func TestParseManifestRejectsMalformed(t *testing.T) {
	if _, err := ParseManifest(json.RawMessage(`{"name": 5}`)); err == nil {
		t.Fatal("non-string name must error")
	}
	m, err := ParseManifest(nil)
	if err != nil || m.Name != "" || m.Enabled != nil {
		t.Fatalf("nil raw must yield zero manifest, got %+v err=%v", m, err)
	}
}
```

- [ ] **Step 6: Run to verify it fails**

Run: `go test -mod=vendor ./pkg/rsassistant/ -run 'Manifest|CardSpec' -v`
Expected: undefined `BuildCardSpec`, `ParseManifest`, `ErrDisabled`.

- [ ] **Step 7: Implement manifest.go**

```go
package rsassistant

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrDisabled is returned by BuildCardSpec when the manifest sets enabled=false.
var ErrDisabled = errors.New("rsassistant: disabled by manifest")

// nameRe mirrors nats-agent's agent-name rule (pkg/agent/agent.go nameRe).
var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// reservedNames mirrors nats-agent's reserved agent names.
var reservedNames = map[string]bool{"discover": true, "announce": true}

// Skill is one advertised capability on the agent card.
type Skill struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Examples    []string `json:"examples,omitempty"`
}

// Manifest is the optional top-level "rsassistant" block of a workflow file.
// Every field is optional; see BuildCardSpec for defaults.
type Manifest struct {
	Enabled     *bool    `json:"enabled,omitempty"`
	Name        string   `json:"name,omitempty"`
	DisplayName string   `json:"displayName,omitempty"`
	Description string   `json:"description,omitempty"`
	Version     string   `json:"version,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Skills      []Skill  `json:"skills,omitempty"`
}

// CardSpec is the resolved, validated description of one published agent.
type CardSpec struct {
	Name        string
	DisplayName string
	Description string
	Version     string
	Tags        []string
	Skills      []Skill
}

// ParseManifest decodes the raw block. nil or empty yields the zero Manifest.
func ParseManifest(raw json.RawMessage) (Manifest, error) {
	var m Manifest
	if len(raw) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("rsassistant manifest: %w", err)
	}
	return m, nil
}

// BuildCardSpec resolves manifest values over workflow defaults and validates
// the agent name. Returns ErrDisabled when the manifest opts out.
func BuildCardSpec(workflowID, workflowDescription string, raw json.RawMessage, defaultVersion string) (CardSpec, error) {
	m, err := ParseManifest(raw)
	if err != nil {
		return CardSpec{}, err
	}
	if m.Enabled != nil && !*m.Enabled {
		return CardSpec{}, ErrDisabled
	}
	spec := CardSpec{
		Name:        strings.TrimSpace(m.Name),
		DisplayName: strings.TrimSpace(m.DisplayName),
		Description: strings.TrimSpace(m.Description),
		Version:     strings.TrimSpace(m.Version),
		Tags:        m.Tags,
		Skills:      m.Skills,
	}
	if spec.Name == "" {
		spec.Name = workflowID
	}
	if !nameRe.MatchString(spec.Name) {
		return CardSpec{}, fmt.Errorf("rsassistant: agent name %q must match %s", spec.Name, nameRe)
	}
	if reservedNames[spec.Name] {
		return CardSpec{}, fmt.Errorf("rsassistant: agent name %q is reserved", spec.Name)
	}
	if spec.DisplayName == "" {
		spec.DisplayName = spec.Name
	}
	if spec.Description == "" {
		spec.Description = strings.TrimSpace(workflowDescription)
	}
	if spec.Description == "" {
		spec.Description = "Workflow " + workflowID
	}
	if spec.Version == "" {
		spec.Version = defaultVersion
	}
	if spec.Tags == nil {
		spec.Tags = []string{}
	}
	if spec.Skills == nil {
		spec.Skills = []Skill{}
	}
	return spec, nil
}
```

- [ ] **Step 8: Run all package tests**

Run: `go test -mod=vendor ./pkg/rsassistant/ -v`
Expected: PASS.

- [ ] **Step 9: Commit (only if authorized)**

```bash
git add pkg/rsassistant/config.go pkg/rsassistant/manifest.go pkg/rsassistant/config_test.go pkg/rsassistant/manifest_test.go
git commit -m "feat(rsassistant): env config and workflow manifest → card spec"
```

---

### Task 4: Eligibility from loaded engine objects

**Files:**
- Create: `pkg/rsassistant/eligible.go`
- Test: `pkg/rsassistant/eligible_test.go`

**Interfaces:**
- Consumes: `natschat.ChatEndpoint` (Task 2), `engine.Workflow{Trigger node.Trigger; Nodes map[string]node.Node}`.
- Produces: `func Eligibility(wf *engine.Workflow) (ep natschat.ChatEndpoint, reason string)`; `reason == ""` means eligible. Constants `ModeStreaming = "streaming"`.

- [ ] **Step 1: Write the failing test**

```go
// pkg/rsassistant/eligible_test.go
package rsassistant

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeChatTrigger implements node.Trigger and natschat.ChatEndpoint.
type fakeChatTrigger struct {
	mode     string
	override bool
}

func (fakeChatTrigger) Spec() node.NodeSpec                               { return node.NodeSpec{Type: "trigger/nats-chat", Role: node.RoleTrigger} }
func (fakeChatTrigger) Init(context.Context, node.NodeEnv) error          { return nil }
func (fakeChatTrigger) Close(context.Context) error                       { return nil }
func (fakeChatTrigger) Subscribe(context.Context, node.TriggerSink) error { return nil }
func (fakeChatTrigger) ChatSubject() string                               { return "trx.test.wf" }
func (f fakeChatTrigger) ResponseMode() string                            { return f.mode }
func (f fakeChatTrigger) AllowsResponseModeOverride() bool                { return f.override }
func (fakeChatTrigger) RequestTimeout() time.Duration                     { return time.Minute }

// otherTrigger is a node.Trigger that is not a chat endpoint.
type otherTrigger struct{}

func (otherTrigger) Spec() node.NodeSpec                               { return node.NodeSpec{Type: "trigger/other", Role: node.RoleTrigger} }
func (otherTrigger) Init(context.Context, node.NodeEnv) error          { return nil }
func (otherTrigger) Close(context.Context) error                       { return nil }
func (otherTrigger) Subscribe(context.Context, node.TriggerSink) error { return nil }

// plainNode is any non-UI node.
type plainNode struct{}

func (plainNode) Spec() node.NodeSpec                      { return node.NodeSpec{Type: "tool/opensearch", Role: node.RoleTool} }
func (plainNode) Init(context.Context, node.NodeEnv) error { return nil }
func (plainNode) Close(context.Context) error              { return nil }

// uiNode carries the ClientOnly marker exactly like node.ClientUITool.
type uiNode struct {
	plainNode
	clientOnly bool
}

func (u uiNode) ClientOnly() bool { return u.clientOnly }

func wfWith(trig node.Trigger, nodes map[string]node.Node) *engine.Workflow {
	if nodes == nil {
		nodes = map[string]node.Node{}
	}
	nodes["trigger1"] = trig
	return &engine.Workflow{ID: "wf", Trigger: trig, Nodes: nodes}
}

func TestEligibilityStreamingWithoutUITools(t *testing.T) {
	ep, reason := Eligibility(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"os": plainNode{}}))
	if reason != "" || ep == nil {
		t.Fatalf("expected eligible, got reason=%q ep=%v", reason, ep)
	}
}

func TestEligibilitySingleWithOverride(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "single", override: true}, nil))
	if reason != "" {
		t.Fatalf("single+override must be eligible, got %q", reason)
	}
}

func TestEligibilitySingleWithoutOverride(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "single"}, nil))
	if !strings.Contains(reason, "cannot stream") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestEligibilityRejectsClientOnlyUITool(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{
		"os":       plainNode{},
		"confirm1": uiNode{clientOnly: true},
	}))
	if !strings.Contains(reason, "confirm1") {
		t.Fatalf("reason must name the UI node, got %q", reason)
	}
}

func TestEligibilityAllowsClientOnlyFalse(t *testing.T) {
	_, reason := Eligibility(wfWith(fakeChatTrigger{mode: "streaming"}, map[string]node.Node{"x": uiNode{clientOnly: false}}))
	if reason != "" {
		t.Fatalf("ClientOnly()==false must not block, got %q", reason)
	}
}

func TestEligibilityRejectsNonChatTrigger(t *testing.T) {
	_, reason := Eligibility(wfWith(otherTrigger{}, nil))
	if !strings.Contains(reason, "trigger/nats-chat") {
		t.Fatalf("reason = %q", reason)
	}
}

func TestEligibilityNilWorkflow(t *testing.T) {
	if _, reason := Eligibility(nil); reason == "" {
		t.Fatal("nil workflow must be ineligible")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -mod=vendor ./pkg/rsassistant/ -run Eligibility -v`
Expected: `undefined: Eligibility`.

- [ ] **Step 3: Implement eligible.go**

```go
package rsassistant

import (
	"fmt"
	"sort"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/natschat"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
)

// ModeStreaming is the trigger/nats-chat responseMode the bridge requires.
const ModeStreaming = "streaming"

// clientOnlyMarker is the method set node.ClientUITool adds over node.Tool.
// Checking the marker structurally keeps this package independent of the
// full Tool method set while matching the agent loop's own test
// (builtin/agent/loop.go: `tool.(node.ClientUITool); ok && cu.ClientOnly()`).
type clientOnlyMarker interface{ ClientOnly() bool }

// Eligibility decides whether a loaded workflow can be published to RSAssistant.
// It returns the chat endpoint and an empty reason when eligible, or a
// human-readable reason otherwise. Rules (spec §3.2):
//  1. trigger implements natschat.ChatEndpoint
//  2. responseMode is streaming, or the body may override it to streaming
//  3. no node is a client-only UI tool (RSAssistant cannot answer them)
func Eligibility(wf *engine.Workflow) (natschat.ChatEndpoint, string) {
	if wf == nil || wf.Trigger == nil {
		return nil, "workflow has no trigger"
	}
	ep, ok := wf.Trigger.(natschat.ChatEndpoint)
	if !ok {
		return nil, "trigger is not trigger/nats-chat"
	}
	if ep.ResponseMode() != ModeStreaming && !ep.AllowsResponseModeOverride() {
		return nil, fmt.Sprintf("cannot stream: responseMode=%q and allowResponseModeOverride=false", ep.ResponseMode())
	}
	ids := make([]string, 0, len(wf.Nodes))
	for id := range wf.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic reason
	for _, id := range ids {
		if cu, ok := wf.Nodes[id].(clientOnlyMarker); ok && cu.ClientOnly() {
			return nil, fmt.Sprintf("has client-only UI tool node %q (%s)", id, wf.Nodes[id].Spec().Type)
		}
	}
	return ep, ""
}
```

- [ ] **Step 4: Run tests**

Run: `go test -mod=vendor ./pkg/rsassistant/ -v`
Expected: PASS.

- [ ] **Step 5: Commit (only if authorized)**

```bash
git add pkg/rsassistant/eligible.go pkg/rsassistant/eligible_test.go
git commit -m "feat(rsassistant): eligibility from loaded trigger and node objects"
```

---

### Task 5: Event relay and identity gate (adds nats-agent dependency)

**Files:**
- Create: `pkg/rsassistant/relay.go`
- Test: `pkg/rsassistant/relay_test.go`
- Modify: `go.mod`, `go.sum`, `vendor/` (via `go get` + `go mod vendor`)

**Interfaces:**
- Consumes: `natsstream.StreamEvent{Type, Sequence, StreamID, Data, Header}` and constants `natsstream.StreamStart|StreamDelta|StreamThought|StreamToolCall|StreamToolResult|StreamComplete|StreamError|StreamAttachment`; nats-agent `agent.Identity{UserID, AccountID, Verified, IDT}`, `wire.Message`, `wire.Usage`, `wire.StopEndTurn`, `wire.StopMaxTokens`, `wire.CodeForbidden (4031)`, `wire.CodeUpstream (5001)`.
- Produces:
  ```go
  type emitter interface {
      Text(delta string)
      ToolUse(toolUseID, toolName string, input any)
      ToolResult(toolUseID, toolName, result, toolErr string)
      Data(kind string, payload any)
      Status(text string)
      Done(stopReason string, usage *wire.Usage)
      Error(message string, code int)
  }
  type relay struct{ ... }            // per-consult state
  func newRelay() *relay
  func (r *relay) apply(ev natsstream.StreamEvent, out emitter)
  func (r *relay) finish(streamErr error, out emitter)   // called when the channel closes
  func (r *relay) terminated() bool
  func (r *relay) count() int
  func gate(id agent.Identity, strict bool) error
  func messageText(m wire.Message) string
  func mapStop(messageStop string) string
  ```
  `*agent.Stream` satisfies `emitter` (method set verified in nats-agent `pkg/agent/stream.go:53-98`).

- [ ] **Step 1: Add the dependency**

Run:
```bash
cd /Users/yceleiro/Documents/TransactRx/GitHub/ai-agent-go-service
export GOPRIVATE=github.com/transactrx
cat > pkg/rsassistant/deps_placeholder.go <<'EOF'
package rsassistant

import (
	_ "github.com/transactrx/nats-agent/pkg/agent"
	_ "github.com/transactrx/nats-agent/pkg/agentclient"
	_ "github.com/transactrx/nats-agent/pkg/wire"
)
EOF
go get github.com/transactrx/nats-agent@v0.2.0
go mod tidy
go mod vendor
go build -mod=vendor ./... && go test -mod=vendor ./...
grep -n 'nats-agent\|nats-service' go.mod
```
Expected: `github.com/transactrx/nats-agent v0.2.0` in `go.mod`; `nats-service` stays `v1.4.46`; build and tests PASS. Delete `deps_placeholder.go` after Step 3 lands real imports (it is only there so tidy/vendor keep the module).

- [ ] **Step 2: Write the failing relay test**

```go
// pkg/rsassistant/relay_test.go
package rsassistant

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

type recorded struct {
	kind string
	a, b string
	c    string
	code int
	any  any
}

type recorder struct{ evs []recorded }

func (r *recorder) Text(d string)                          { r.evs = append(r.evs, recorded{kind: "text", a: d}) }
func (r *recorder) ToolUse(id, name string, in any)        { r.evs = append(r.evs, recorded{kind: "toolUse", a: id, b: name, any: in}) }
func (r *recorder) ToolResult(id, name, res, e string)     { r.evs = append(r.evs, recorded{kind: "toolResult", a: id, b: name, c: res + "|" + e}) }
func (r *recorder) Data(kind string, p any)                { r.evs = append(r.evs, recorded{kind: "data", a: kind, any: p}) }
func (r *recorder) Status(t string)                        { r.evs = append(r.evs, recorded{kind: "status", a: t}) }
func (r *recorder) Done(stop string, _ *wire.Usage)        { r.evs = append(r.evs, recorded{kind: "done", a: stop}) }
func (r *recorder) Error(msg string, code int)             { r.evs = append(r.evs, recorded{kind: "error", a: msg, code: code}) }

func ev(t natsstream.StreamEventType, seq int, data string) natsstream.StreamEvent {
	return natsstream.StreamEvent{Type: t, Sequence: seq, Data: json.RawMessage(data)}
}

func TestRelayMapsFullStream(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamStart, 0, `{"sessionId":"s1","workflowId":"wf","requestId":"r1"}`), out)
	r.apply(ev(natsstream.StreamDelta, 1, `{"text":"Today "}`), out)
	r.apply(ev(natsstream.StreamToolCall, 2, `{"toolUseId":"t1","name":"OpenSearchQuery","input":{"indexPath":"x"}}`), out)
	r.apply(ev(natsstream.StreamToolResult, 3, `{"toolUseId":"t1","output":{"hits":3},"isError":false}`), out)
	r.apply(ev(natsstream.StreamAttachment, 4, `{"toolUseId":"t2","kind":"image","payload":{"imageUrl":"https://s3/chart.png"}}`), out)
	r.apply(ev(natsstream.StreamDelta, 5, `{"text":"1,204 claims."}`), out)
	r.apply(ev(natsstream.StreamComplete, 6, `{"finalText":"Today 1,204 claims.","messageStop":"end_turn"}`), out)

	kinds := []string{}
	for _, e := range out.evs {
		kinds = append(kinds, e.kind)
	}
	want := "text,toolUse,toolResult,data,text,text,done"
	if got := strings.Join(kinds, ","); got != want {
		t.Fatalf("event kinds = %s, want %s", got, want)
	}
	if out.evs[1].b != "OpenSearchQuery" || out.evs[2].b != "OpenSearchQuery" {
		t.Fatalf("tool name must be carried from tool_call to tool_result: %+v", out.evs[1:3])
	}
	if !strings.Contains(out.evs[2].c, `{"hits":3}`) {
		t.Fatalf("tool result text = %q", out.evs[2].c)
	}
	if !strings.Contains(out.evs[4].a, "https://s3/chart.png") {
		t.Fatalf("attachment url must be emitted as text, got %q", out.evs[4].a)
	}
	if out.evs[6].a != wire.StopEndTurn {
		t.Fatalf("stop = %q", out.evs[6].a)
	}
	if !r.terminated() || r.count() != 7 {
		t.Fatalf("terminated=%v count=%d", r.terminated(), r.count())
	}
}

func TestRelayCompleteWithoutDeltasEmitsFinalText(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamStart, 0, `{}`), out)
	r.apply(ev(natsstream.StreamComplete, 1, `{"finalText":"only final","messageStop":"max_tokens"}`), out)
	if len(out.evs) != 2 || out.evs[0].kind != "text" || out.evs[0].a != "only final" || out.evs[1].a != wire.StopMaxTokens {
		t.Fatalf("events = %+v", out.evs)
	}
}

func TestRelayToolResultErrorAndErrorEvent(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamToolResult, 1, `{"toolUseId":"t9","output":"boom","isError":true}`), out)
	if out.evs[0].c != "boom|boom" {
		t.Fatalf("isError must fill toolErr, got %q", out.evs[0].c)
	}
	r.apply(ev(natsstream.StreamError, 2, `{"code":"executor_failed","message":"upstream down"}`), out)
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" || last.code != wire.CodeUpstream || !strings.Contains(last.a, "executor_failed") || !strings.Contains(last.a, "upstream down") {
		t.Fatalf("error mapping = %+v", last)
	}
	if !r.terminated() {
		t.Fatal("error must terminate the relay")
	}
}

func TestRelayFinishWithoutTerminatorEmitsError(t *testing.T) {
	out := &recorder{}
	r := newRelay()
	r.apply(ev(natsstream.StreamDelta, 1, `{"text":"partial"}`), out)
	r.finish(errors.New("natsstream: timed out after 1s"), out)
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" || last.code != wire.CodeUpstream || !strings.Contains(last.a, "timed out") {
		t.Fatalf("finish = %+v", last)
	}
	// finish after a proper terminator must be a no-op
	out2 := &recorder{}
	r2 := newRelay()
	r2.apply(ev(natsstream.StreamComplete, 0, `{"finalText":"x","messageStop":"end_turn"}`), out2)
	r2.finish(nil, out2)
	if len(out2.evs) != 2 { // text(x) + done
		t.Fatalf("finish after complete must not emit, got %+v", out2.evs)
	}
}

func TestGate(t *testing.T) {
	verified := agent.Identity{UserID: "u1", AccountID: "a1", Verified: true}
	observed := agent.Identity{UserID: "u1", AccountID: "a1", Verified: false}
	empty := agent.Identity{IDT: "tok"}

	if err := gate(verified, true); err != nil {
		t.Fatalf("verified must pass strict: %v", err)
	}
	if err := gate(observed, true); err == nil {
		t.Fatal("unverified must fail strict")
	}
	if err := gate(observed, false); err != nil {
		t.Fatalf("unverified with account+user passes observe: %v", err)
	}
	if err := gate(empty, false); err == nil {
		t.Fatal("empty account/user must fail even in observe")
	}
	if err := gate(agent.Identity{UserID: "u", Verified: true}, true); err == nil {
		t.Fatal("missing account must fail")
	}
}

func TestMessageTextJoinsTextBlocks(t *testing.T) {
	m := wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "a"}, {Text: ""}, {Text: "b"}}}
	if got := messageText(m); got != "a\nb" {
		t.Fatalf("messageText = %q", got)
	}
}

func TestMapStop(t *testing.T) {
	if mapStop("end_turn") != wire.StopEndTurn || mapStop("") != wire.StopEndTurn || mapStop("max_tokens") != wire.StopMaxTokens || mapStop("tool_use") != wire.StopEndTurn {
		t.Fatal("mapStop table wrong")
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `go test -mod=vendor ./pkg/rsassistant/ -run 'Relay|Gate|MessageText|MapStop' -v`
Expected: undefined `newRelay`, `gate`, `messageText`, `mapStop`.

- [ ] **Step 4: Implement relay.go and remove the placeholder file**

```go
// pkg/rsassistant/relay.go
package rsassistant

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

// emitter is the subset of *agent.Stream the relay needs. Kept as an
// interface so the mapping is unit-testable with a recorder.
type emitter interface {
	Text(delta string)
	ToolUse(toolUseID, toolName string, input any)
	ToolResult(toolUseID, toolName, result, toolErr string)
	Data(kind string, payload any)
	Status(text string)
	Done(stopReason string, usage *wire.Usage)
	Error(message string, code int)
}

// Engine stream payloads (builtin/agent/loop.go, natsstream/protocol.go).
type deltaPayload struct {
	Text string `json:"text"`
}
type toolCallPayload struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
}
type toolResultPayload struct {
	ToolUseID string          `json:"toolUseId"`
	Output    json.RawMessage `json:"output"`
	IsError   bool            `json:"isError"`
}
type attachmentPayload struct {
	ToolUseID string          `json:"toolUseId"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
}

// relay converts one engine stream into nats-agent stream calls.
type relay struct {
	toolNames map[string]string // toolUseId -> tool name (tool_result carries no name)
	sawDelta  bool
	done      bool
	n         int
}

func newRelay() *relay { return &relay{toolNames: map[string]string{}} }

func (r *relay) terminated() bool { return r.done }
func (r *relay) count() int       { return r.n }

// apply maps one engine event onto the emitter (spec §3.6 table).
func (r *relay) apply(ev natsstream.StreamEvent, out emitter) {
	if r.done {
		return
	}
	r.n++
	switch ev.Type {
	case natsstream.StreamStart:
		// nothing to emit; nats-agent already sent its own start event
	case natsstream.StreamDelta:
		var p deltaPayload
		_ = json.Unmarshal(ev.Data, &p)
		if p.Text != "" {
			r.sawDelta = true
			out.Text(p.Text)
		}
	case natsstream.StreamThought:
		var p deltaPayload
		_ = json.Unmarshal(ev.Data, &p)
		if p.Text != "" {
			out.Status(p.Text)
		}
	case natsstream.StreamToolCall:
		var p toolCallPayload
		_ = json.Unmarshal(ev.Data, &p)
		r.toolNames[p.ToolUseID] = p.Name
		out.ToolUse(p.ToolUseID, p.Name, p.Input)
	case natsstream.StreamToolResult:
		var p toolResultPayload
		_ = json.Unmarshal(ev.Data, &p)
		res := rawToText(p.Output)
		errText := ""
		if p.IsError {
			errText = res
		}
		out.ToolResult(p.ToolUseID, r.toolNames[p.ToolUseID], res, errText)
	case natsstream.StreamAttachment:
		var p attachmentPayload
		_ = json.Unmarshal(ev.Data, &p)
		out.Data("attachment", p.Payload)
		if u := attachmentURL(p.Payload); u != "" {
			out.Text("\n" + u + "\n")
		}
	case natsstream.StreamComplete:
		var p natsstream.CompletePayload
		_ = json.Unmarshal(ev.Data, &p)
		if !r.sawDelta && p.FinalText != "" {
			out.Text(p.FinalText)
		}
		out.Done(mapStop(p.MessageStop), nil)
		r.done = true
	case natsstream.StreamError:
		var p natsstream.ErrorPayload
		_ = json.Unmarshal(ev.Data, &p)
		msg := strings.TrimSpace(p.Code + ": " + p.Message)
		if msg == ":" {
			msg = "workflow error"
		}
		out.Error(msg, wire.CodeUpstream)
		r.done = true
	}
}

// finish is called once the engine channel closes. If no terminator was
// relayed, the consult ends with an upstream error (timeout, closed conn).
func (r *relay) finish(streamErr error, out emitter) {
	if r.done {
		return
	}
	msg := "workflow stream ended without completion"
	if streamErr != nil {
		msg += ": " + streamErr.Error()
	}
	out.Error(msg, wire.CodeUpstream)
	r.done = true
}

// rawToText renders a tool output for the nats-agent toolResult string.
func rawToText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// attachmentURL extracts a url/imageUrl string from an attachment payload.
func attachmentURL(raw json.RawMessage) string {
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return ""
	}
	for _, k := range []string{"url", "imageUrl"} {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// mapStop converts the engine's Bedrock-style messageStop to nats-agent's.
func mapStop(messageStop string) string {
	if messageStop == "max_tokens" {
		return wire.StopMaxTokens
	}
	return wire.StopEndTurn
}

// gate is the bridge's own identity check (spec §3.4 gate 3). It is
// independent of env: the engine is never called without an identity-derived
// account and user; in strict mode the identity must also be Verified.
func gate(id agent.Identity, strict bool) error {
	if strict && !id.Verified {
		return errors.New("identity not verified")
	}
	if strings.TrimSpace(id.AccountID) == "" || strings.TrimSpace(id.UserID) == "" {
		return errors.New("identity did not resolve account and user")
	}
	return nil
}

// messageText joins the text blocks of a nats-agent message.
func messageText(m wire.Message) string {
	parts := make([]string, 0, len(m.Content))
	for _, b := range m.Content {
		if b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}
```

Then: `rm pkg/rsassistant/deps_placeholder.go` (the `agentclient` import returns in Task 6's test; run `go mod vendor` again at that point if `go build -mod=vendor` complains about an unvendored package).

- [ ] **Step 5: Run tests**

Run: `go mod vendor && go build -mod=vendor ./... && go test -mod=vendor ./pkg/rsassistant/ -v`
Expected: PASS.

- [ ] **Step 6: Commit (only if authorized)**

```bash
git add go.mod go.sum vendor pkg/rsassistant/relay.go pkg/rsassistant/relay_test.go
git rm -q --cached pkg/rsassistant/deps_placeholder.go 2>/dev/null || true
git commit -m "feat(rsassistant): engine→nats-agent event relay, identity gate; add nats-agent v0.2.0"
```

---

### Task 6: Consult bridge, agent runtime, integration test

**Files:**
- Create: `pkg/rsassistant/consult.go`
- Create: `pkg/rsassistant/runtime.go`
- Test: `pkg/rsassistant/consult_test.go` (embedded NATS, fake engine responder, no nats-agent)
- Test: `pkg/rsassistant/runtime_integration_test.go` (embedded NATS, real nats-agent, fake identity)

**Interfaces:**
- Consumes: `natsstream.DoStreamingRequest(nc, subject, headers, body, timeout, logger) (*natsstream.StreamSubscription, error)`; `sub.Events()`, `sub.Cancel()`, `sub.Close() error`; nats-agent `agent.New(agent.Config)`, `a.OnChat`, `a.Start()`, `a.Shutdown()`, `a.Conn()`, `agent.Turn{ChatRequest, RunID, Logger, Identity}`, `agent.IDTValidation`, `wire.AgentAccess`, `wire.Skill`.
- Produces:
  ```go
  type consultRequest struct {
      Subject   string
      AccountID string
      UserID    string
      IDT       string
      SessionID string
      Text      string
      Timeout   time.Duration
  }
  func consult(ctx context.Context, nc *nats.Conn, req consultRequest, logger *log.Logger, out emitter) (events int, err error)

  type Deps struct {
      Engine        *engine.Engine
      Logger        *log.Logger
      RepositoryURL string
      LookupEnv     func(string) (string, bool)
  }
  type Runtime struct{ ... }
  func Start(ctx context.Context, deps Deps) (*Runtime, error)
  func (r *Runtime) Names() []string
  func (r *Runtime) Shutdown()
  ```

- [ ] **Step 1: Write the failing consult test (fake engine over embedded NATS)**

```go
// pkg/rsassistant/consult_test.go
package rsassistant

import (
	"context"
	"io"
	"log"
	"strconv"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

func runEmbedded(t *testing.T) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return srv, nc
}

func silent() *log.Logger { return log.New(io.Discard, "", 0) }

// fakeEngine answers one streaming request on subject with a scripted stream
// and records the identity headers and body it received.
type fakeEngine struct {
	seen chan *nats.Msg
}

func startFakeEngine(t *testing.T, nc *nats.Conn, subject string, script []natsstream.StreamEvent) *fakeEngine {
	t.Helper()
	fe := &fakeEngine{seen: make(chan *nats.Msg, 4)}
	_, err := nc.Subscribe(subject, func(m *nats.Msg) {
		fe.seen <- m
		for i, ev := range script {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(ev.Type))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(i))
			out.Header.Set(natsstream.HdrStreamId, "stream-1")
			if i == 0 {
				out.Header.Set(natsstream.HdrStreamCancelSubject, "test.cancel.stream-1")
			}
			out.Data = ev.Data
			_ = nc.PublishMsg(out)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return fe
}

func TestConsultSendsIdentityHeadersAndRelaysEvents(t *testing.T) {
	_, nc := runEmbedded(t)
	fe := startFakeEngine(t, nc, "test.base.SingleSearch", []natsstream.StreamEvent{
		ev(natsstream.StreamStart, 0, `{"sessionId":"s1"}`),
		ev(natsstream.StreamDelta, 1, `{"text":"hello "}`),
		ev(natsstream.StreamDelta, 2, `{"text":"world"}`),
		ev(natsstream.StreamComplete, 3, `{"finalText":"hello world","messageStop":"end_turn"}`),
	})
	out := &recorder{}
	n, err := consult(context.Background(), nc, consultRequest{
		Subject: "test.base.SingleSearch", AccountID: "acct-9", UserID: "u-1", IDT: "tok",
		SessionID: "sess-1", Text: "how many?", Timeout: 5 * time.Second,
	}, silent(), out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("relayed %d events, want 4", n)
	}
	m := <-fe.seen
	if m.Header.Get("X-Account-Id") != "acct-9" || m.Header.Get("X-User-Id") != "u-1" || m.Header.Get("X-TRX-IDT") != "tok" {
		t.Fatalf("headers = %v", m.Header)
	}
	body := string(m.Data)
	for _, want := range []string{`"message":"how many?"`, `"sessionId":"sess-1"`, `"responseMode":"streaming"`} {
		if !contains(body, want) {
			t.Fatalf("body %s missing %s", body, want)
		}
	}
	text := ""
	for _, e := range out.evs {
		if e.kind == "text" {
			text += e.a
		}
	}
	if text != "hello world" || out.evs[len(out.evs)-1].kind != "done" {
		t.Fatalf("events = %+v", out.evs)
	}
}

func TestConsultTimeoutEndsWithUpstreamError(t *testing.T) {
	_, nc := runEmbedded(t)
	startFakeEngine(t, nc, "test.base.Slow", []natsstream.StreamEvent{
		ev(natsstream.StreamStart, 0, `{}`),
		ev(natsstream.StreamDelta, 1, `{"text":"partial"}`),
		// no terminator
	})
	out := &recorder{}
	_, _ = consult(context.Background(), nc, consultRequest{
		Subject: "test.base.Slow", AccountID: "a", UserID: "u", Text: "x", Timeout: 300 * time.Millisecond,
	}, silent(), out)
	last := out.evs[len(out.evs)-1]
	if last.kind != "error" || !contains(last.a, "timed out") {
		t.Fatalf("expected upstream timeout error, got %+v", out.evs)
	}
}

func TestConsultCancelPublishesToCancelSubject(t *testing.T) {
	_, nc := runEmbedded(t)
	cancelled := make(chan struct{}, 1)
	_, _ = nc.Subscribe("test.cancel.stream-1", func(*nats.Msg) { cancelled <- struct{}{} })
	startFakeEngine(t, nc, "test.base.Hang", []natsstream.StreamEvent{
		ev(natsstream.StreamStart, 0, `{}`),
	})
	ctx, cancel := context.WithCancel(context.Background())
	out := &recorder{}
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	_, _ = consult(ctx, nc, consultRequest{Subject: "test.base.Hang", AccountID: "a", UserID: "u", Text: "x", Timeout: 5 * time.Second}, silent(), out)
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel was not propagated to the engine cancel subject")
	}
}

func contains(s, sub string) bool { return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0) }
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -mod=vendor ./pkg/rsassistant/ -run Consult -v`
Expected: undefined `consult`, `consultRequest`.

- [ ] **Step 3: Implement consult.go**

```go
// pkg/rsassistant/consult.go
package rsassistant

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

// consultRequest is one bridged call into a workflow's chat subject.
type consultRequest struct {
	Subject   string
	AccountID string // from verified identity only
	UserID    string // from verified identity only
	IDT       string // forwarded when non-empty
	SessionID string
	Text      string
	Timeout   time.Duration
}

type engineChatBody struct {
	Message      string `json:"message"`
	SessionID    string `json:"sessionId,omitempty"`
	ResponseMode string `json:"responseMode"`
}

// consult performs the engine self-call and relays its stream onto out.
// It returns the number of engine events relayed. An error is returned only
// when the request could not be sent; stream-level failures are reported to
// out as an upstream error and return nil.
func consult(ctx context.Context, nc *nats.Conn, req consultRequest, logger *log.Logger, out emitter) (int, error) {
	headers := map[string]string{
		"X-Account-Id": req.AccountID,
		"X-User-Id":    req.UserID,
	}
	if req.IDT != "" {
		headers["X-TRX-IDT"] = req.IDT
	}
	body, _ := json.Marshal(engineChatBody{Message: req.Text, SessionID: req.SessionID, ResponseMode: ModeStreaming})

	sub, err := natsstream.DoStreamingRequest(nc, req.Subject, headers, body, req.Timeout, logger)
	if err != nil {
		return 0, err
	}
	r := newRelay()
	events := sub.Events()
	for {
		select {
		case <-ctx.Done():
			// RSAssistant cancelled (or agent shutting down): stop the workflow.
			if cerr := sub.Cancel(); cerr != nil {
				logger.Printf("RSASSISTANT event=consult.cancel_failed subject=%s err=%v", req.Subject, cerr)
			}
			_ = sub.Close()
			return r.count(), nil // nats-agent emits done/cancelled itself
		case ev, ok := <-events:
			if !ok {
				r.finish(sub.Close(), out)
				return r.count(), nil
			}
			r.apply(ev, out)
			if r.terminated() {
				_ = sub.Close()
				return r.count(), nil
			}
		}
	}
}
```

- [ ] **Step 4: Run consult tests**

Run: `go test -mod=vendor ./pkg/rsassistant/ -run Consult -v`
Expected: PASS (three tests).

- [ ] **Step 5: Write the failing runtime integration test**

```go
// pkg/rsassistant/runtime_integration_test.go
package rsassistant_test

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/transactrx/nats-agent/pkg/agentclient"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/rsassistant"
	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// chatTrigger: node.Trigger + natschat.ChatEndpoint (subject fixed for the test).
type chatTrigger struct {
	subject string
}

func (chatTrigger) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/chat-trigger", Role: node.RoleTrigger,
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (chatTrigger) Init(context.Context, node.NodeEnv) error          { return nil }
func (chatTrigger) Close(context.Context) error                       { return nil }
func (chatTrigger) Subscribe(context.Context, node.TriggerSink) error { return nil }
func (c chatTrigger) ChatSubject() string                             { return c.subject }
func (chatTrigger) ResponseMode() string                              { return "single" }
func (chatTrigger) AllowsResponseModeOverride() bool                  { return true }
func (chatTrigger) RequestTimeout() time.Duration                     { return time.Minute }

// sinkAgent: minimal node.Agent so the workflow validates (copied shape from engine_test.fakeAgent).
type sinkAgent struct{}

func (sinkAgent) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "test/agent", Role: node.RoleAgent,
		InputPorts:  []node.PortSpec{{Name: "main", Direction: node.PortIn, Cardinality: node.CardOne, Required: true}},
		OutputPorts: []node.PortSpec{{Name: "main", Direction: node.PortOut, Cardinality: node.CardOne}}}
}
func (sinkAgent) Init(context.Context, node.NodeEnv) error { return nil }
func (sinkAgent) Close(context.Context) error              { return nil }
func (sinkAgent) Process(_ context.Context, _ node.AgentInput, sink node.StreamSink) error {
	return sink.Close(context.Background(), node.StreamEvent{Type: node.StreamComplete, Data: []byte(`{"finalText":"ok"}`)})
}

// uiTool: client-only marker so one workflow is ineligible.
type uiTool struct{ sinkAgent }

func (uiTool) Spec() node.NodeSpec {
	return node.NodeSpec{Type: "tool/ui-confirm", Role: node.RoleTool,
		InputPorts: []node.PortSpec{{Name: "main", Direction: node.PortIn, Cardinality: node.CardOne, Required: true}}}
}
func (uiTool) ClientOnly() bool { return true }

const wfJSON = `{
  "id": "%s", "version": 1, "trigger": "trig",
  %s
  "nodes": [
    {"id": "trig",  "type": "test/chat-trigger", "config": {}},
    {"id": "agent", "type": "%s", "config": {}}
  ],
  "connections": [{"from": {"node": "trig", "port": "main"}, "to": {"node": "agent", "port": "main"}}]
}`

func TestRuntimePublishesEligibleWorkflowsAndBridgesVerifiedIdentity(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	t.Setenv("NATS_URL", srv.ClientURL()) // nats-agent opens its own connection from env
	t.Setenv("NATS_JWT", "")
	t.Setenv("NATS_KEY", "")

	// Fake identity: token "good" → granted user u-1 / account acct-9; anything else invalid.
	_, _ = nc.Subscribe("test.identity.validateInternalToken", func(m *nats.Msg) {
		var req struct {
			IDT        string `json:"idt"`
			AgentID    string `json:"agentId"`
			FunctionID string `json:"functionId"`
		}
		_ = json.Unmarshal(m.Data, &req)
		if req.IDT == "good" && req.AgentID == "APPX" && req.FunctionID == "FNX" {
			_ = m.Respond([]byte(`{"valid":true,"functionGranted":true,"userId":"u-1","accountId":"acct-9"}`))
			return
		}
		_ = m.Respond([]byte(`{"valid":false,"reason":"INVALID_IDT"}`))
	})

	// Fake engine on the eligible workflow's chat subject.
	var mu sync.Mutex
	var seen []*nats.Msg
	_, _ = nc.Subscribe("test.base.SingleSearch", func(m *nats.Msg) {
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		script := []struct {
			typ  natsstream.StreamEventType
			data string
		}{
			{natsstream.StreamStart, `{"sessionId":"s"}`},
			{natsstream.StreamDelta, `{"text":"42 claims"}`},
			{natsstream.StreamComplete, `{"finalText":"42 claims","messageStop":"end_turn"}`},
		}
		for i, ev := range script {
			out := nats.NewMsg(m.Reply)
			out.Header.Set(natsstream.HdrStreamEvent, string(ev.typ))
			out.Header.Set(natsstream.HdrStreamSequence, strconv.Itoa(i))
			out.Header.Set(natsstream.HdrStreamId, "st")
			out.Data = []byte(ev.data)
			_ = nc.PublishMsg(out)
		}
	})

	// Engine with two workflows: eligible (manifest name) and ineligible (UI tool).
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("single.json", sprintf(wfJSON, "SingleSearch", `"rsassistant": {"name": "claimSearch", "description": "Claims Q&A"},`, "test/agent"))
	write("ui.json", sprintf(wfJSON, "UiSearch", "", "test/ui"))
	reg := node.NewRegistry()
	_ = reg.Register("test/chat-trigger", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return chatTrigger{subject: "test.base.SingleSearch"}, nil }))
	_ = reg.Register("test/agent", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return sinkAgent{}, nil }))
	_ = reg.Register("test/ui", node.FactoryFunc(func(json.RawMessage) (node.Node, error) { return uiTool{}, nil }))
	eng, err := engine.New(engine.Config{Source: engine.NewFilesystemSource(dir), Registry: reg, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.LoadAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	env := map[string]string{
		"RSASSISTANT_ENABLED":            "true",
		"APP_ID":                         "APPX",
		"APP_FUNCTION_ID":                "FNX",
		"NATS_IDENTITY_BASE_PATH":        "test.identity",
		"NATS_IDENTITY_VALIDATE_SUBJECT": "validateInternalToken",
		"RSASSISTANT_CONSULT_TIMEOUT_SECONDS": "5",
	}
	rt, err := rsassistant.Start(context.Background(), rsassistant.Deps{
		Engine: eng, Logger: log.New(io.Discard, "", 0), RepositoryURL: "https://example/repo",
		LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown()
	if names := rt.Names(); len(names) != 1 || names[0] != "claimSearch" {
		t.Fatalf("published = %v, want [claimSearch]", names)
	}

	cli := agentclient.NewFromConn(nc)

	// Discovery: exactly one card, carrying the shared access pair.
	cards, err := cli.Discover(context.Background(), wire.DiscoverFilter{}, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(cards) != 1 || cards[0].Name != "claimSearch" || cards[0].Access == nil ||
		cards[0].Access.AppID != "APPX" || cards[0].Access.FunctionID != "FNX" || cards[0].Description != "Claims Q&A" {
		t.Fatalf("cards = %+v", cards)
	}

	// Consult with a good token: identity headers must be the verified ones, not the spoofed userId.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := cli.Chat(agentclient.WithIDT(ctx, "good"), "claimSearch", wire.ChatRequest{
		UserID:  "spoofed-user",
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "how many?"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text, stop := "", ""
	for ev := range run.Events {
		switch ev.Type {
		case wire.EventText:
			text += ev.TextDelta
		case wire.EventDone:
			stop = ev.StopReason
		case wire.EventError:
			t.Fatalf("unexpected error event: %s", ev.Error)
		}
	}
	if text != "42 claims" || stop != wire.StopEndTurn {
		t.Fatalf("text=%q stop=%q", text, stop)
	}
	mu.Lock()
	if len(seen) != 1 || seen[0].Header.Get("X-User-Id") != "u-1" || seen[0].Header.Get("X-Account-Id") != "acct-9" || seen[0].Header.Get("X-TRX-IDT") != "good" {
		t.Fatalf("engine saw headers %v", headersOf(seen))
	}
	mu.Unlock()

	// Consult with a bad token: rejected before the engine is ever called.
	_, err = cli.Chat(agentclient.WithIDT(ctx, "bad"), "claimSearch", wire.ChatRequest{
		Message: wire.Message{Role: "user", Content: []wire.ContentBlock{{Text: "how many?"}}},
	})
	if err == nil {
		t.Fatal("bad token must be refused")
	}
	mu.Lock()
	if len(seen) != 1 {
		t.Fatalf("engine must not be called for a denied token; calls=%d", len(seen))
	}
	mu.Unlock()

	rt.Shutdown()
	srv.Shutdown()
}

func headersOf(msgs []*nats.Msg) []nats.Header {
	out := []nats.Header{}
	for _, m := range msgs {
		out = append(out, m.Header)
	}
	return out
}

func sprintf(format string, args ...any) string { return fmtSprintf(format, args...) }
```

Add at top of the file `"fmt"` to imports and define `var fmtSprintf = fmt.Sprintf` (kept as a variable so the helper name does not collide with `fmt` in the test package). Simpler alternative that is also acceptable: replace `sprintf(` with `fmt.Sprintf(` and drop the two helpers.

- [ ] **Step 6: Run to verify it fails**

Run: `go test -mod=vendor ./pkg/rsassistant/ -run Runtime -v`
Expected: undefined `rsassistant.Start`, `rsassistant.Deps`.

- [ ] **Step 7: Implement runtime.go**

```go
// pkg/rsassistant/runtime.go
package rsassistant

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/transactrx/nats-agent/pkg/agent"
	"github.com/transactrx/nats-agent/pkg/wire"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/natschat"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
)

// Deps is what Start needs from the hosting service.
type Deps struct {
	Engine        *engine.Engine
	Logger        *log.Logger
	RepositoryURL string
	LookupEnv     func(string) (string, bool) // defaults to os.LookupEnv
}

// Runtime owns the published agents for one process.
type Runtime struct {
	logger *log.Logger
	agents []*published
}

type published struct {
	name       string
	workflowID string
	endpoint   natschat.ChatEndpoint
	agent      *agent.Agent
	cfg        Config
	logger     *log.Logger
}

// Start publishes every eligible workflow as its own nats-agent agent.
// Per-workflow problems are logged and skipped; only a fatal config error
// (missing identity pair) returns an error. Start never blocks.
func Start(ctx context.Context, deps Deps) (*Runtime, error) {
	if deps.Engine == nil {
		return nil, errors.New("rsassistant: engine is nil")
	}
	if deps.Logger == nil {
		deps.Logger = log.Default()
	}
	if deps.LookupEnv == nil {
		deps.LookupEnv = os.LookupEnv
	}
	cfg, err := ConfigFromEnv(deps.LookupEnv)
	if err != nil {
		return nil, err
	}
	rt := &Runtime{logger: deps.Logger}
	ids := deps.Engine.WorkflowIDs()
	sort.Strings(ids)
	seenNames := map[string]string{} // agent name -> workflow id

	for _, id := range ids {
		wf, ok := deps.Engine.Workflow(id)
		if !ok {
			continue
		}
		ep, reason := Eligibility(wf)
		if reason != "" {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, reason)
			continue
		}
		spec, err := BuildCardSpec(wf.ID, wf.Description, wf.RSAssistant, cfg.DefaultVersion)
		if errors.Is(err, ErrDisabled) {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, "rsassistant.enabled=false")
			continue
		}
		if err != nil {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, err.Error())
			continue
		}
		if prev, dup := seenNames[spec.Name]; dup {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id,
				fmt.Sprintf("agent name %q already published by workflow %s", spec.Name, prev))
			continue
		}
		if ep.ChatSubject() == "" {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, "trigger has no chat subject (not initialized)")
			continue
		}

		p := &published{name: spec.Name, workflowID: id, endpoint: ep, cfg: cfg, logger: deps.Logger}
		a, err := agent.New(agent.Config{
			Name:          spec.Name,
			DisplayName:   spec.DisplayName,
			Description:   spec.Description,
			Version:       spec.Version,
			RepositoryURL: deps.RepositoryURL,
			Tags:          spec.Tags,
			Skills:        toWireSkills(spec.Skills),
			Metadata:      map[string]any{"workflowId": id, "chatSubject": ep.ChatSubject(), "bridge": "ai-agent-go-service/rsassistant"},
			Access:        &wire.AgentAccess{AppID: cfg.AppID, FunctionID: cfg.FunctionID},
			IDTValidation: &agent.IDTValidation{
				Enabled:     true,
				ObserveOnly: cfg.ObserveOnly,
				FailOpen:    false,
				Subject:     cfg.IdentitySubject,
				Timeout:     5 * time.Second,
				CacheTTL:    300 * time.Second,
			},
		})
		if err != nil {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, "agent.New: "+err.Error())
			continue
		}
		p.agent = a
		a.OnChat(p.handleChat)
		if err := a.Start(); err != nil {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, "agent.Start: "+err.Error())
			continue
		}
		seenNames[spec.Name] = id
		rt.agents = append(rt.agents, p)
		mode := "streaming"
		if ep.ResponseMode() != ModeStreaming {
			mode = "override"
		}
		deps.Logger.Printf("RSASSISTANT event=agent.published workflow=%s name=%s subject=%s mode=%s observe_only=%t",
			id, spec.Name, ep.ChatSubject(), mode, cfg.ObserveOnly)
	}
	return rt, nil
}

// Names lists the published agent names in publish order.
func (r *Runtime) Names() []string {
	out := make([]string, 0, len(r.agents))
	for _, p := range r.agents {
		out = append(out, p.name)
	}
	return out
}

// Shutdown drains every agent's subscriptions. In-flight consults finish on
// their own contexts.
func (r *Runtime) Shutdown() {
	for _, p := range r.agents {
		if err := p.agent.Shutdown(); err != nil {
			r.logger.Printf("RSASSISTANT event=agent.shutdown_error name=%s err=%v", p.name, err)
		}
	}
}

// handleChat is the nats-agent OnChat handler: gate, then bridge.
func (p *published) handleChat(ctx context.Context, turn *agent.Turn, stream *agent.Stream) error {
	started := time.Now()
	strict := !p.cfg.ObserveOnly
	p.logger.Printf("RSASSISTANT event=consult.start name=%s run=%s user=%s account=%s verified=%t observe_only=%t",
		p.name, turn.RunID, turn.Identity.UserID, turn.Identity.AccountID, turn.Identity.Verified, p.cfg.ObserveOnly)
	if err := gate(turn.Identity, strict); err != nil {
		p.logger.Printf("RSASSISTANT event=consult.refused name=%s run=%s reason=%q", p.name, turn.RunID, err.Error())
		stream.Error("forbidden: "+err.Error(), wire.CodeForbidden)
		return nil
	}
	n, err := consult(ctx, p.agent.Conn(), consultRequest{
		Subject:   p.endpoint.ChatSubject(),
		AccountID: turn.Identity.AccountID,
		UserID:    turn.Identity.UserID,
		IDT:       turn.Identity.IDT,
		SessionID: turn.SessionID,
		Text:      messageText(turn.Message),
		Timeout:   p.cfg.ConsultTimeout,
	}, p.logger, stream)
	if err != nil {
		p.logger.Printf("RSASSISTANT event=consult.error name=%s run=%s err=%q", p.name, turn.RunID, err.Error())
		stream.Error("workflow unreachable: "+err.Error(), wire.CodeUpstream)
		return nil
	}
	p.logger.Printf("RSASSISTANT event=consult.done name=%s run=%s events=%d ms=%d", p.name, turn.RunID, n, time.Since(started).Milliseconds())
	return nil
}

func toWireSkills(in []Skill) []wire.Skill {
	out := make([]wire.Skill, 0, len(in))
	for _, s := range in {
		out = append(out, wire.Skill{Name: s.Name, Description: s.Description, Examples: s.Examples})
	}
	return out
}
```

- [ ] **Step 8: Vendor, build, run the whole package**

Run: `go mod vendor && go build -mod=vendor ./... && go test -mod=vendor ./pkg/rsassistant/ -v -count=1`
Expected: PASS. If `go vet` complains about the `sprintf` helper in the integration test, use `fmt.Sprintf` directly as noted in Step 5.

- [ ] **Step 9: Commit (only if authorized)**

```bash
git add vendor pkg/rsassistant/consult.go pkg/rsassistant/runtime.go pkg/rsassistant/consult_test.go pkg/rsassistant/runtime_integration_test.go
git commit -m "feat(rsassistant): consult bridge and per-workflow nats-agent runtime with embedded-NATS tests"
```

---

### Task 7: Wire into `Run`, document, changelog

**Files:**
- Modify: `agent/service.go:138-152`
- Create: `docs/RSASSISTANT.md`
- Modify: `CHANGELOG.md` (prepend v1.7.0 entry)
- Modify: `docs/EXTENDING.md` (one paragraph linking to the new doc)

**Interfaces:**
- Consumes: `rsassistant.EnabledFromEnv`, `rsassistant.Start`, `rsassistant.Deps`, `(*rsassistant.Runtime).Shutdown`.

- [ ] **Step 1: Wire startup and shutdown**

In `agent/service.go`, add the import `"github.com/transactrx/ai-agent-go-service/pkg/rsassistant"` and replace lines 138-152 with:

```go
	if err := natservice.Start(); err != nil {
		return fmt.Errorf("nats start: %w", err)
	}
	logger.Print("service started")

	// RSAssistant agents (additive, RSASSISTANT_ENABLED=true only): publish
	// every eligible workflow as its own nats-agent agent. Failures disable the
	// feature with a log line; the engine keeps serving its existing subjects.
	var rsa *rsassistant.Runtime
	if rsassistant.EnabledFromEnv(os.LookupEnv) {
		r, rerr := rsassistant.Start(ctx, rsassistant.Deps{
			Engine:        eng,
			Logger:        logger,
			RepositoryURL: s.repositoryURL,
			LookupEnv:     os.LookupEnv,
		})
		if rerr != nil {
			logger.Printf("boot: rsassistant agents disabled: %v", rerr)
		} else {
			rsa = r
			logger.Printf("boot: rsassistant agents published: %v", r.Names())
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Print("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if rsa != nil {
		rsa.Shutdown()
	}
	_ = eng.Shutdown(shutdownCtx)
	_ = natservice.Shutdown()
	return nil
```

- [ ] **Step 2: Build and run every test**

Run: `go build -mod=vendor ./... && go vet -mod=vendor ./... && go test -mod=vendor ./... -count=1`
Expected: PASS, no vet findings.

- [ ] **Step 3: Write docs/RSASSISTANT.md**

```markdown
# Publishing workflows to RSAssistant

When `RSASSISTANT_ENABLED=true`, the service publishes every **eligible** workflow
as its own agent on the NATS agent mesh (`nats-agent` protocol v1.1). RSAssistant
discovers each one on `trx.agent.discover`, shows it to users who hold the
service's identity function, and consults it on `trx.agent.<name>.chat`.

## Eligibility (decided at startup from the loaded workflow)

1. Trigger is `trigger/nats-chat`.
2. `responseMode` is `streaming`, or `allowResponseModeOverride` is `true`.
3. No client-only UI tool nodes (`tool/ui-*`). Machine callers cannot answer them.
4. `rsassistant.enabled` is not `false`.

Skipped workflows are logged: `RSASSISTANT event=workflow.skipped workflow=<id> reason="..."`.

## Optional manifest (top-level `rsassistant` in the workflow JSON)

```json
"rsassistant": {
  "enabled": true,
  "name": "powerlineClaimSearch",
  "displayName": "PowerLine Claim Search",
  "description": "Answers questions about PowerLine pharmacy claims.",
  "version": "1.0.0",
  "tags": ["powerline", "claims"],
  "skills": [{"name": "claim-search", "description": "Search and aggregate claims", "examples": ["How many claims were rejected today?"]}]
}
```

All fields optional. Defaults: `name` = workflow id, `description` = workflow
description, `displayName` = name, `version` = `RSASSISTANT_AGENT_VERSION` or
`1.0.0`. `name` must match `^[a-zA-Z0-9_-]+$`. A block on a base workflow is
inherited by every workflow that `extends` it; the derived file may override
fields or set `"enabled": false`. Duplicate names: first workflow id (sorted)
wins, others are skipped.

## Identity

Every card declares `access{appId: APP_ID, functionId: APP_FUNCTION_ID}`. The
agent validates `X-TRX-IDT` with identity before acknowledging (403 otherwise).
The bridge then calls the workflow with `X-Account-Id` / `X-User-Id` taken **only**
from the verified identity and forwards `X-TRX-IDT`. The workflow's own scope
policy runs unchanged. `RSASSISTANT_IDT_OBSERVE_ONLY=true` makes the nats-agent
layer log instead of block, but the bridge still refuses any consult whose
identity did not resolve to an account and a user.

## Environment

| Var | Default | Meaning |
|---|---|---|
| `RSASSISTANT_ENABLED` | `false` | master switch |
| `RSASSISTANT_IDT_OBSERVE_ONLY` | `false` | observe phase for agent IDT validation |
| `RSASSISTANT_CONSULT_TIMEOUT_SECONDS` | `400` | per-consult timeout (below RSAssistant's 420s) |
| `RSASSISTANT_AGENT_VERSION` | `1.0.0` | default card version |
| `APP_ID`, `APP_FUNCTION_ID` | required | card access pair |
| `NATS_IDENTITY_BASE_PATH`, `NATS_IDENTITY_VALIDATE_SUBJECT` | `trx.identityservice`, `validateInternalToken` | identity subject |
| `NATS_URL`, `NATS_JWT`, `NATS_KEY` | required | agent connections (same NATS user as the engine) |

The NATS user must be allowed to publish and subscribe on `trx.agent.>`,
`_INBOX.>`, and `trx.identityservice.>`.

## Logs

- `RSASSISTANT event=agent.published workflow= name= subject= mode= observe_only=`
- `RSASSISTANT event=workflow.skipped workflow= reason=`
- `RSASSISTANT event=consult.start|refused|done|error name= run= ...`
- nats-agent: `IDT_METRIC event=validate.allow|deny|observe ...`
```

- [ ] **Step 4: Changelog and EXTENDING pointer**

Prepend to `CHANGELOG.md` under `# Changelog`:

```markdown
## v1.7.0

- feat(rsassistant): opt-in publication of eligible `trigger/nats-chat` workflows as
  RSAssistant-discoverable agents (nats-agent v0.2.0). One agent per eligible
  workflow (streaming or override-capable, no client-only UI tools), single
  `access{APP_ID, APP_FUNCTION_ID}` pair, identity verified before the workflow is
  called, engine headers derived only from the verified identity. Disabled unless
  `RSASSISTANT_ENABLED=true`. New optional top-level workflow key `rsassistant`
  (inherited through `extends`). See `docs/RSASSISTANT.md`.
- feat(natschat): `ChatEndpoint` read-only accessors on `trigger/nats-chat`.
- feat(loader): `LoadResult.RSAssistant` / `Workflow.RSAssistant` carry the raw
  manifest (nil when absent). No behavior change for existing files.
- build: add `github.com/transactrx/nats-agent v0.2.0`.
```

Append to `docs/EXTENDING.md`:

```markdown
## Publishing workflows to RSAssistant

Set `RSASSISTANT_ENABLED=true` and optionally add a top-level `rsassistant`
block to a workflow. See `docs/RSASSISTANT.md` for eligibility, identity, and
environment details.
```

- [ ] **Step 5: Final verification**

Run:
```bash
go build -mod=vendor ./... && go vet -mod=vendor ./... && go test -mod=vendor ./... -count=1
git status --short
```
Expected: PASS; only intended files changed. Confirm `grep -rn 'RSASSISTANT_ENABLED' agent/service.go pkg/rsassistant/config.go` shows the flag read in exactly those two places.

- [ ] **Step 6: Commit (only if authorized)**

```bash
git add agent/service.go docs/RSASSISTANT.md docs/EXTENDING.md CHANGELOG.md
git commit -m "feat(agent): start RSAssistant agents after service start when RSASSISTANT_ENABLED; docs + changelog v1.7.0"
```

Release (after user approval of the PR into Development): `git tag v1.7.0 && git push origin v1.7.0`.

---

## Self-review

- Spec §3.1 (one agent per eligible workflow, after natservice.Start): Task 6 runtime + Task 7 wiring.
- §3.2 eligibility rules 1-3: Task 4; rule 4 (`enabled:false`): Task 3 `ErrDisabled`, handled in Task 6 `Start`.
- §3.3 manifest, defaults, name validation, inheritance, duplicates: Tasks 1, 3, 6.
- §3.4 gates: gate 2 explicit `IDTValidation`/`Access` in Task 6; gate 3 `gate()` in Task 5, applied in Task 6 `handleChat`; headers only from identity in `consult`.
- §3.5 accessors: Task 2.
- §3.6 mapping table, cancel, timeout, session id: Tasks 5 and 6.
- §3.7 wiring and shutdown order: Task 7.
- §3.8 env contract: Task 3.
- §3.9 log lines: Tasks 6 and 7.
- §4 backward compatibility: all new fields `omitempty`; flag off means `Start` never runs.
- Type consistency: `Eligibility` returns `(natschat.ChatEndpoint, string)` in Tasks 4 and 6; `BuildCardSpec(workflowID, workflowDescription, raw, defaultVersion)` identical in Tasks 3 and 6; `consult(ctx, nc, consultRequest, logger, emitter) (int, error)` identical in Task 6 tests and `handleChat`; `emitter` method set matches `*agent.Stream` (`stream.go:53-98`).

## Revisions (2026-09-23, during implementation)

- **Go 1.26.5 minimum.** nats-agent v0.2.0 declares `go 1.26.5`; `go mod tidy` raises this
  module's `go` directive from 1.25.2 (kept at that minimum). CI moved to the latest Go family
  (user decision): `go.yml`/`release.yaml` go-version `1.27.x`, `docker-compose.yml` test image
  `golang:1.27-alpine` (golang images set `GOTOOLCHAIN=local`).
  Test-only nats-server v2.14.0 → v2.14.3.
- **Dedicated stream client** `pkg/rsassistant/stream.go` replaces `natsstream.DoStreamingRequest`
  in `consult`: the shared client drops non-stream error replies (nats-service `status` ≠ 200,
  NATS 503 no-responders), which would hang a consult until the 400s timeout, and can send on a
  closed channel when a late event races timeout/cancel. Existing code untouched. Extra tests:
  service-error fail-fast, no-responders fail-fast, late-events-after-close under `-race`.
- `runtime.Start`: `a.Shutdown()` when `a.Start()` fails (releases the agent's connection).
- **Release**: tags are minted by `release.yaml` (release-on-push) when Development merges into
  Production — patch bump by default, `release:minor` label → minor. Decided: **v1.7.0** via `release:minor`. The manual
  `git tag v1.7.0` step above is wrong; do not tag by hand.

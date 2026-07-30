// Auto-update: resolves the latest Claude model of the same family — primary
// source is the org inferenceGateway (NATS resolveModel, see gateway.go),
// fallback is a Bedrock control-plane scan — validates candidates with a real
// test invocation (production payload shape, 3 attempts), and hot-swaps the
// node's model in memory. Specs:
// docs/superpowers/specs/2026-06-05-bedrock-model-autoupdate-design.md
// docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md
package bedrock

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsbedrock "github.com/aws/aws-sdk-go-v2/service/bedrock"
	cptypes "github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/safego"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// parsedModel is the comparable decomposition of a Bedrock Anthropic model /
// inference-profile ID, e.g. "us.anthropic.claude-opus-4-7" or
// "anthropic.claude-opus-4-1-20250805-v1:0".
type parsedModel struct {
	prefix string // geo prefix incl. trailing dot ("us.", "eu.", "global.") or ""
	family string // "claude-opus", "claude-sonnet", ...
	major  int
	minor  int
}

const anthropicProvider = "anthropic."

var (
	vSuffixRe    = regexp.MustCompile(`-v\d+(?::\d+)?$`)                            // "-v1:0"
	dateSuffixRe = regexp.MustCompile(`-20\d{6}$`)                                  // "-20250805"
	versionRe    = regexp.MustCompile(`^(claude-[a-z]+)-(\d{1,2})(?:-(\d{1,2}))?$`) // "claude-opus-4-7" / "claude-opus-4"
)

// parseModelID decomposes id into prefix/family/version. IDs that don't match
// the modern "claude-<family>-<major>[-<minor>]" naming (e.g. legacy
// claude-3-5-sonnet) return an error and are not auto-updatable.
func parseModelID(id string) (parsedModel, error) {
	fail := func() (parsedModel, error) {
		return parsedModel{}, fmt.Errorf("ai/bedrock: model id %q not parseable for auto-update", id)
	}
	prefix, rest := "", id
	switch i := strings.Index(id, anthropicProvider); {
	case i == 0:
		rest = id[len(anthropicProvider):]
	case i > 0 && strings.HasSuffix(id[:i], ".") && strings.Count(id[:i], ".") == 1:
		prefix, rest = id[:i], id[i+len(anthropicProvider):]
	default:
		return fail()
	}
	rest = vSuffixRe.ReplaceAllString(rest, "")
	rest = dateSuffixRe.ReplaceAllString(rest, "")
	m := versionRe.FindStringSubmatch(rest)
	if m == nil {
		return fail()
	}
	major, _ := strconv.Atoi(m[2])
	minor := 0
	if m[3] != "" {
		minor, _ = strconv.Atoi(m[3])
	}
	return parsedModel{prefix: prefix, family: m[1], major: major, minor: minor}, nil
}

// newerThan reports strict (major, minor) ordering.
func (p parsedModel) newerThan(o parsedModel) bool {
	if p.major != o.major {
		return p.major > o.major
	}
	return p.minor > o.minor
}

// latestCandidate returns the highest-version ID strictly newer than current,
// considering only IDs with the same prefix and family. Unparseable IDs are
// skipped. Second return is false when no upgrade exists.
func latestCandidate(ids []string, current parsedModel) (string, bool) {
	bestID, best := "", current
	for _, id := range ids {
		p, err := parseModelID(id)
		if err != nil || p.prefix != current.prefix || p.family != current.family {
			continue
		}
		if p.newerThan(best) {
			bestID, best = id, p
		}
	}
	return bestID, bestID != ""
}

// gatewayCandidateOK rejects gateway answers the updater could not manage
// afterwards: IDs outside the modern parseable naming (a swap would strand
// the updater at skipped-unparseable-model until restart) and cross-family
// answers (following the gateway covers upgrades, rollbacks, and prefix
// changes — never a silent family switch).
func gatewayCandidateOK(id string, current parsedModel) error {
	p, err := parseModelID(id)
	if err != nil {
		return err
	}
	if p.family != current.family {
		return fmt.Errorf("ai/bedrock: gateway candidate %q is family %q, current family is %q", id, p.family, current.family)
	}
	return nil
}

// checkHour is the daily check time: 02:00 server-local — after-hours, and
// leaves room for other midnight batch jobs (spec decision 2).
const checkHour = 2

// nextRunAt returns the next strictly-future occurrence of 02:00 local.
func nextRunAt(now time.Time) time.Time {
	next := time.Date(now.Year(), now.Month(), now.Day(), checkHour, 0, 0, 0, now.Location())
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// validationAttempts: initial try + 2 retries before declining (spec).
const validationAttempts = 3

// validationBackoffs separate attempts 1→2 and 2→3.
var validationBackoffs = []time.Duration{5 * time.Second, 15 * time.Second}

// noteEvent is the NATS notification payload, published on both outcomes.
type noteEvent struct {
	Event      string `json:"event"` // "upgraded" | "declined"
	WorkflowID string `json:"workflowId"`
	NodeID     string `json:"nodeId"`
	From       string `json:"from"`
	To         string `json:"to"`
	Attempts   int    `json:"attempts"`
	Error      string `json:"error,omitempty"`
	// Resolver says which source produced the candidate: "gateway" (org
	// inferenceGateway) or "fallback" (Bedrock catalog scan).
	Resolver  string `json:"resolver,omitempty"`
	Timestamp string `json:"timestamp"`
}

// autoUpdater runs the daily check. All effects are injected func fields so
// runOnce is unit-testable without AWS or NATS; startAutoUpdate wires the
// real implementations.
type autoUpdater struct {
	wfID, nodeID string
	logger       *log.Logger

	current  func() string                                   // effective model getter
	swap     func(string)                                    // effective model setter
	resolve  func(ctx context.Context) (string, error)       // gateway resolveModel; nil → catalog scan only
	list     func(ctx context.Context) ([]string, error)     // ACTIVE system inference-profile IDs (fallback)
	validate func(ctx context.Context, modelID string) error // test invocation
	publish  func(subject string, data []byte) error         // nil → log-only
	subject  string
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration)
}

// runOnce executes one full check: discover → select → validate (with
// retries) → swap+notify, or decline+notify. Never returns an error: every
// failure mode is "stay on current model and try again next cycle".
//
// Guaranteed summary log: regardless of outcome (upgrade, keep, any error),
// runOnce always emits one final line with the outcome and the model in use,
// so the effective model is always traceable from logs.
func (u *autoUpdater) runOnce(ctx context.Context) {
	outcome := "already-latest"
	defer func() {
		u.logf("autoupdate: run finished outcome=%s modelInUse=%s", outcome, u.current())
	}()

	cur := u.current()
	parsed, err := parseModelID(cur)
	if err != nil {
		outcome = "skipped-unparseable-model"
		u.logf("autoupdate: skipped: %v", err)
		return
	}

	// Stage 1 — org inferenceGateway, the source of truth. Followed wherever
	// it points within the family (upgrade, org rollback, geo-prefix change);
	// every swap is still gated by validation. Any failure — transport error,
	// rejected candidate, or a candidate that fails validation — falls
	// through to stage 2 so a bad gateway answer can never mask an upgrade
	// the catalog scan would find.
	declined := ""
	if u.resolve != nil {
		switch id, rerr := u.resolve(ctx); {
		case rerr != nil:
			u.logf("autoupdate: gateway resolve failed (using catalog-scan fallback): %v", rerr)
		case id == cur:
			u.logf("autoupdate: gateway confirms current model %s is latest", cur)
			return // outcome stays "already-latest"
		default:
			if gerr := gatewayCandidateOK(id, parsed); gerr != nil {
				u.logf("autoupdate: gateway candidate rejected (using catalog-scan fallback): %v", gerr)
			} else if u.tryUpgrade(ctx, cur, id, "gateway", &outcome) {
				return
			} else if outcome == "cancelled" {
				return
			} else {
				declined = id
			}
		}
	}
	if ctx.Err() != nil {
		outcome = "cancelled"
		return
	}

	// Stage 2 — Bedrock catalog scan, strictly newer within the same
	// prefix+family (pre-gateway behavior, unchanged).
	ids, lerr := u.list(ctx)
	if lerr != nil {
		outcome = "list-failed"
		u.logf("autoupdate: list inference profiles failed (retry next cycle): %v", lerr)
		return
	}
	cand, ok := latestCandidate(ids, parsed)
	if !ok || cand == declined {
		return // nothing new, or the scan agrees with the already-declined candidate
	}
	u.tryUpgrade(ctx, cur, cand, "fallback", &outcome)
}

// tryUpgrade validates cand (tool-use probe invocation, 3 attempts) and hot-swaps on
// success. Returns true only after a validated swap; on decline it emits the
// "declined" event and returns false so the caller can try another source.
// Sets *outcome to upgraded/declined/cancelled.
func (u *autoUpdater) tryUpgrade(ctx context.Context, cur, cand, resolver string, outcome *string) bool {
	u.logf("autoupdate: found candidate %s (current %s, resolver %s), validating", cand, cur, resolver)
	var lastErr error
	for attempt := 1; attempt <= validationAttempts; attempt++ {
		if ctx.Err() != nil {
			*outcome = "cancelled"
			return false
		}
		if lastErr = u.validate(ctx, cand); lastErr == nil {
			u.swap(cand)
			*outcome = "upgraded"
			u.logf("autoupdate: upgraded %s -> %s (attempt %d/%d, resolver %s)", cur, cand, attempt, validationAttempts, resolver)
			u.notify(noteEvent{Event: "upgraded", From: cur, To: cand, Attempts: attempt, Resolver: resolver})
			return true
		}
		u.logf("autoupdate: validation %d/%d of %s failed: %v", attempt, validationAttempts, cand, lastErr)
		if attempt < validationAttempts {
			u.sleep(ctx, validationBackoffs[attempt-1])
		}
	}
	*outcome = "declined"
	u.notify(noteEvent{Event: "declined", From: cur, To: cand, Attempts: validationAttempts, Error: lastErr.Error(), Resolver: resolver})
	return false
}

// notify stamps identity+timestamp and publishes; falls back to log-only when
// no NATS publisher is wired. Publish failures never affect the swap decision.
func (u *autoUpdater) notify(ev noteEvent) {
	ev.WorkflowID, ev.NodeID = u.wfID, u.nodeID
	ev.Timestamp = u.now().UTC().Format(time.RFC3339)
	data, err := json.Marshal(ev)
	if err != nil {
		u.logf("autoupdate: marshal notify event: %v", err)
		return
	}
	if u.publish == nil {
		u.logf("autoupdate: notify (log-only): %s", data)
		return
	}
	if err := u.publish(u.subject, data); err != nil {
		u.logf("autoupdate: publish %s failed: %v", u.subject, err)
	}
}

func (u *autoUpdater) logf(format string, args ...any) {
	if u.logger != nil {
		u.logger.Printf("ai/bedrock wf=%s node=%s "+format, append([]any{u.wfID, u.nodeID}, args...)...)
	}
}

// notifySubjectEnv overrides the NATS notification subject; default is
// "<nats_basePath>.modelAutoUpdate" (mirrors .promptChanged).
const (
	notifySubjectEnv    = "MODEL_AUTOUPDATE_NOTIFY_SUBJECT"
	notifySubjectSuffix = ".modelAutoUpdate"
)

// run executes the startup check, then one check per day at 02:00 local,
// until ctx is cancelled.
func (u *autoUpdater) run(ctx context.Context) {
	u.runOnce(ctx)
	for {
		next := nextRunAt(u.now())
		timer := time.NewTimer(next.Sub(u.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			u.runOnce(ctx)
		}
	}
}

// startAutoUpdate wires real AWS + NATS dependencies into an autoUpdater and
// launches its goroutine. Called from Init when cfg.autoUpdateEnabled().
func (b *bedrockLLM) startAutoUpdate(env node.NodeEnv, awsCfg aws.Config) {
	cp := awsbedrock.NewFromConfig(awsCfg)
	subject, publish, disabledReason := resolveNotify(env)
	if publish == nil {
		b.logger.Printf("ai/bedrock wf=%s node=%s autoupdate: NATS notifications disabled (log-only): %s", b.wfID, b.nodeID, disabledReason)
	}

	// Gateway resolution: primary source for the daily check. Missing NATS
	// conn → nil resolve → catalog scan only (pre-gateway behavior). The
	// lab/family query is derived from the CURRENT model on every call, so it
	// stays correct across gateway-issued prefix changes.
	var resolve func(ctx context.Context) (string, error)
	gwSubject := gatewaySubject()
	if nc := natsConn(env); nc == nil {
		b.logger.Printf("ai/bedrock wf=%s node=%s autoupdate: gateway resolution off (no NATS connection) — catalog scan only", b.wfID, b.nodeID)
	} else {
		resolve = newGatewayResolve(nc, gwSubject, b.currentModel)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.cancelUpdater = cancel

	u := &autoUpdater{
		wfID:    b.wfID,
		nodeID:  b.nodeID,
		logger:  b.logger,
		current: b.currentModel,
		swap:    b.setModel,
		resolve: resolve,
		list: func(ctx context.Context) ([]string, error) {
			return listActiveProfileIDs(ctx, cp)
		},
		validate: b.validateModel,
		publish:  publish,
		subject:  subject,
		now:      time.Now,
		sleep:    sleepCtx,
	}
	go func() {
		_ = safego.Run("bedrock-autoupdate", func() error {
			u.run(ctx)
			return nil
		})
	}()
	b.logger.Printf("ai/bedrock wf=%s node=%s autoupdate: enabled (model=%s gateway=%s subject=%s)", b.wfID, b.nodeID, b.currentModel(), gwSubject, subject)
}

// resolveNotify resolves the notification subject and publisher from env vars
// and the hosts map. Either can be missing → log-only mode (nil publisher);
// disabledReason then says why, so operators know notifications are off.
func resolveNotify(env node.NodeEnv) (subject string, publish func(string, []byte) error, disabledReason string) {
	subject = os.Getenv(notifySubjectEnv)
	if subject == "" {
		if bp, ok := env.Host("nats_basePath"); ok {
			if s, _ := bp.(string); s != "" {
				subject = s + notifySubjectSuffix
			}
		}
	}
	if subject == "" {
		return "", nil, "no subject (" + notifySubjectEnv + " unset and nats_basePath host missing)"
	}
	host, ok := env.Host("nats")
	if !ok {
		return subject, nil, "nats host not registered"
	}
	ns, ok := host.(*nats_service.NatService)
	if !ok || ns.GetNatsService() == nil {
		return subject, nil, "nats host wrong type or no connection"
	}
	return subject, ns.GetNatsService().Publish, ""
}

// hostLookup is the slice of node.NodeEnv that natsConn needs; narrowed so
// tests can fake it without implementing the full interface.
type hostLookup interface {
	Host(kind string) (any, bool)
}

// natsConn returns the shared NATS connection from the hosts map, or nil
// when the host is missing, mistyped, or not connected.
func natsConn(env hostLookup) *nats.Conn {
	host, ok := env.Host("nats")
	if !ok {
		return nil
	}
	ns, ok := host.(*nats_service.NatService)
	if !ok {
		return nil
	}
	return ns.GetNatsService()
}

// newGatewayResolve builds the stage-1 resolver: derives lab/family from the
// CURRENT model on every call, then asks the gateway.
func newGatewayResolve(nc *nats.Conn, subject string, current func() string) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		lab, family, err := deriveGatewayQuery(current())
		if err != nil {
			return "", err
		}
		return resolveViaGateway(ctx, nc, subject, lab, family)
	}
}

// listActiveProfileIDs pages through SYSTEM_DEFINED inference profiles and
// returns the ACTIVE profile IDs (e.g. "us.anthropic.claude-opus-4-8").
func listActiveProfileIDs(ctx context.Context, client *awsbedrock.Client) ([]string, error) {
	var ids []string
	var token *string
	for {
		out, err := client.ListInferenceProfiles(ctx, &awsbedrock.ListInferenceProfilesInput{
			TypeEquals: cptypes.InferenceProfileTypeSystemDefined,
			NextToken:  token,
		})
		if err != nil {
			return nil, err
		}
		for _, p := range out.InferenceProfileSummaries {
			if p.Status == cptypes.InferenceProfileStatusActive && p.InferenceProfileId != nil {
				ids = append(ids, *p.InferenceProfileId)
			}
		}
		if out.NextToken == nil {
			return ids, nil
		}
		token = out.NextToken
	}
}

// probeToolName is the tool the validation probe forces the candidate to call.
const probeToolName = "echo"

// validateModel performs a production-shaped invocation against the
// candidate: the same payload envelope and stream decoder the agent uses,
// plus a forced tool call — production workflows are tool-heavy, so a model
// that streams text but cannot produce a well-formed tool_use block must be
// declined. Success = stream completes AND a completed tool_use block named
// probeToolName with valid JSON input was decoded.
//
// Temperature is deliberately NOT set: production requests never carry it
// (the agent leaves LLMRequest.Temperature nil), and newer Claude models
// (Opus 4.7+) reject the parameter with a 400 ValidationException — sending
// it here would decline models that production would run fine on.
func (b *bedrockLLM) validateModel(ctx context.Context, modelID string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req := node.LLMRequest{
		System:         "Connectivity check. Call the echo tool with value \"pong\".",
		MaxTokens:      128,
		ToolChoiceName: probeToolName,
		Tools: []node.ToolSpec{{
			Name:        probeToolName,
			Description: "Echoes back the provided value.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}`),
		}},
		Messages: []node.Message{{
			Role:    node.UserMsg,
			Content: []node.ContentBlock{{Type: node.BlockText, Text: "ping"}},
		}},
	}
	payload, err := buildAnthropicPayload(req, b.cfg)
	if err != nil {
		return err
	}
	resp, err := b.client.InvokeModelWithResponseStream(ctx, &bedrockruntime.InvokeModelWithResponseStreamInput{
		ModelId:     aws.String(modelID),
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
		Body:        payload,
	})
	if err != nil {
		return err
	}
	stream := resp.GetStream()
	defer stream.Close()

	events, err := collectProbeEvents(ctx, stream)
	if err != nil {
		return err
	}
	return verifyToolProbe(events)
}

// collectProbeEvents drains a probe stream through handleAnthropicChunk —
// the same decoder production streaming uses — and returns the events.
// Non-chunk stream members are decode failures, exactly as in Stream.
func collectProbeEvents(ctx context.Context, stream *bedrockruntime.InvokeModelWithResponseStreamEventStream) ([]node.LLMEvent, error) {
	out := make(chan node.LLMEvent, 64)
	var events []node.LLMEvent
	done := make(chan struct{})
	go func() {
		defer close(done)
		for evt := range out {
			events = append(events, evt)
		}
	}()

	accum := map[int]*node.LLMToolUse{}
	var decodeErr error
	for evt := range stream.Events() {
		if ctx.Err() != nil {
			decodeErr = ctx.Err()
			break
		}
		chunk, ok := evt.(*types.ResponseStreamMemberChunk)
		if !ok {
			decodeErr = fmt.Errorf("ai/bedrock: probe stream: unexpected event %T", evt)
			break
		}
		if err := handleAnthropicChunk(ctx, chunk.Value.Bytes, accum, out); err != nil {
			decodeErr = err
			break
		}
	}
	close(out)
	<-done
	if decodeErr != nil {
		return nil, decodeErr
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// verifyToolProbe requires a completed tool_use block for probeToolName with
// valid, non-empty JSON input among the probe's decoded events.
func verifyToolProbe(events []node.LLMEvent) error {
	for _, ev := range events {
		if ev.Kind != node.LLMToolUseStop || ev.ToolUse == nil {
			continue
		}
		if ev.ToolUse.Name != probeToolName {
			return fmt.Errorf("ai/bedrock: probe expected tool %q, model called %q", probeToolName, ev.ToolUse.Name)
		}
		if len(ev.ToolUse.InputJSON) == 0 || !json.Valid(ev.ToolUse.InputJSON) {
			return fmt.Errorf("ai/bedrock: probe tool_use input is not valid JSON: %q", string(ev.ToolUse.InputJSON))
		}
		return nil
	}
	return fmt.Errorf("ai/bedrock: probe stream completed without a tool_use block (model did not honor tool_choice)")
}

// sleepCtx sleeps for d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

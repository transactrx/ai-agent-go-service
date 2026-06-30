// Auto-update: discovers newer Claude models of the same family via the
// Bedrock control plane, validates candidates with a real test invocation
// (production payload shape, 3 attempts), and hot-swaps the node's model in
// memory. Spec: docs/superpowers/specs/2026-06-05-bedrock-model-autoupdate-design.md
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
	Timestamp  string `json:"timestamp"`
}

// autoUpdater runs the daily check. All effects are injected func fields so
// runOnce is unit-testable without AWS or NATS; startAutoUpdate wires the
// real implementations.
type autoUpdater struct {
	wfID, nodeID string
	logger       *log.Logger

	current  func() string                                   // effective model getter
	swap     func(string)                                    // effective model setter
	list     func(ctx context.Context) ([]string, error)     // ACTIVE system inference-profile IDs
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
	ids, err := u.list(ctx)
	if err != nil {
		outcome = "list-failed"
		u.logf("autoupdate: list inference profiles failed (retry next cycle): %v", err)
		return
	}
	cand, ok := latestCandidate(ids, parsed)
	if !ok {
		return // outcome stays "already-latest"
	}
	u.logf("autoupdate: found candidate %s (current %s), validating", cand, cur)

	var lastErr error
	for attempt := 1; attempt <= validationAttempts; attempt++ {
		if ctx.Err() != nil {
			outcome = "cancelled"
			return
		}
		if lastErr = u.validate(ctx, cand); lastErr == nil {
			u.swap(cand)
			outcome = "upgraded"
			u.logf("autoupdate: upgraded %s -> %s (attempt %d/%d)", cur, cand, attempt, validationAttempts)
			u.notify(noteEvent{Event: "upgraded", From: cur, To: cand, Attempts: attempt})
			return
		}
		u.logf("autoupdate: validation %d/%d of %s failed: %v", attempt, validationAttempts, cand, lastErr)
		if attempt < validationAttempts {
			u.sleep(ctx, validationBackoffs[attempt-1])
		}
	}
	outcome = "declined"
	u.notify(noteEvent{Event: "declined", From: cur, To: cand, Attempts: validationAttempts, Error: lastErr.Error()})
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
	ctx, cancel := context.WithCancel(context.Background())
	b.cancelUpdater = cancel

	u := &autoUpdater{
		wfID:    b.wfID,
		nodeID:  b.nodeID,
		logger:  b.logger,
		current: b.currentModel,
		swap:    b.setModel,
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
	b.logger.Printf("ai/bedrock wf=%s node=%s autoupdate: enabled (model=%s subject=%s)", b.wfID, b.nodeID, b.currentModel(), subject)
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

// validateModel performs a tiny production-shaped invocation against the
// candidate: same anthropic_version as the node, "ping" prompt, 16 output
// tokens. Success = stream completes without error.
//
// Temperature is deliberately NOT set: production requests never carry it
// (the agent leaves LLMRequest.Temperature nil), and newer Claude models
// (Opus 4.7+) reject the parameter with a 400 ValidationException — sending
// it here would decline models that production would run fine on.
func (b *bedrockLLM) validateModel(ctx context.Context, modelID string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req := node.LLMRequest{
		System:    "Connectivity check. Reply with the single word: pong",
		MaxTokens: 16,
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
	for range stream.Events() {
		// drain — content is irrelevant, completion is the signal
	}
	return stream.Err()
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

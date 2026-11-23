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
	ui         *uiBridge // client-UI tools answered by asking the user; nil = none
	sessions   sessionLocks
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
		access, skip := agentAccess(cfg, wf.IndexAccess)
		if skip != "" {
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, skip)
			continue
		}

		p := &published{name: spec.Name, workflowID: id, endpoint: ep, cfg: cfg, logger: deps.Logger,
			ui: newUIBridge(ClientUITools(wf))}
		// A workflow with client-UI tools asks the user through RSAssistant
		// (protocol 1.7). RSAssistant offers user input only to agents that
		// also advertise progress and renew, so the three go together. These
		// workflows were not published before, so no existing agent changes.
		withUI := p.ui != nil
		var askWait time.Duration
		if withUI {
			askWait = bridgeAskWait(engineWait(wf), deps.LookupEnv)
		}
		a, err := agent.New(agent.Config{
			Name:          spec.Name,
			DisplayName:   spec.DisplayName,
			Description:   spec.Description,
			Category:      spec.Category,
			Version:       spec.Version,
			RepositoryURL: deps.RepositoryURL,
			Tags:          spec.Tags,
			Skills:        toWireSkills(spec.Skills),
			Metadata:      map[string]any{"workflowId": id, "chatSubject": ep.ChatSubject(), "bridge": "ai-agent-go-service/rsassistant"},
			Access:        &access,
			UserContext:   true, // workflows render userName/userTimeZone/nowLocal
			UserInput:     withUI,
			Progress:      withUI,
			Renew:         withUI,
			AskWait:       askWait,
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
			_ = a.Shutdown() // release the connection agent.New opened
			deps.Logger.Printf("RSASSISTANT event=workflow.skipped workflow=%s reason=%q", id, "agent.Start: "+err.Error())
			continue
		}
		seenNames[spec.Name] = id
		rt.agents = append(rt.agents, p)
		mode := "streaming"
		if ep.ResponseMode() != ModeStreaming {
			mode = "override"
		}
		deps.Logger.Printf("RSASSISTANT event=agent.published workflow=%s name=%s subject=%s mode=%s observe_only=%t function=%s user_input=%t",
			id, spec.Name, ep.ChatSubject(), mode, cfg.ObserveOnly, access.FunctionID, withUI)
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
	req := consultRequest{
		Subject:   p.endpoint.ChatSubject(),
		AccountID: turn.Identity.AccountID,
		UserID:    turn.Identity.UserID,
		IDT:       turn.Identity.IDT,
		SessionID: turn.SessionID,
		Text:      withInputs(messageText(turn.Message), turn.ValidInputs),
		Timeout:   p.cfg.ConsultTimeout,
		TimeZone:  userTimeZone(turn.Context),
		UserName:  userName(turn.Context),
	}
	if p.ui != nil {
		req.UI, req.Asker = p.ui, stream
		// One consult per session: a resume waits for a parked turn's
		// workflow drain to finish (bounded by the consult timeout).
		unlock, lerr := p.sessions.acquire(ctx, turn.SessionID, p.cfg.ConsultTimeout)
		if lerr != nil {
			if ctx.Err() != nil {
				return nil // cancelled while waiting; the runtime ends the run
			}
			p.logger.Printf("RSASSISTANT event=consult.session_wait name=%s run=%s session=%s err=%q", p.name, turn.RunID, turn.SessionID, lerr.Error())
		}
		defer unlock()
	}
	n, err := consult(ctx, p.agent.Conn(), req, p.logger, stream)
	if err != nil {
		p.logger.Printf("RSASSISTANT event=consult.error name=%s run=%s err=%q", p.name, turn.RunID, err.Error())
		stream.Error("workflow unreachable: "+err.Error(), wire.CodeUpstream)
		return nil
	}
	p.logger.Printf("RSASSISTANT event=consult.done name=%s run=%s events=%d ms=%d", p.name, turn.RunID, n, time.Since(started).Milliseconds())
	return nil
}

// engineWait is the shortest client-UI result wait among the workflow's
// agent nodes (default 5m).
func engineWait(wf *engine.Workflow) time.Duration {
	wait := defaultEngineWait
	for _, n := range wf.Nodes {
		if h, ok := n.(interface{ HumanResponseTimeout() time.Duration }); ok {
			if d := h.HumanResponseTimeout(); d > 0 && d < wait {
				wait = d
			}
		}
	}
	return wait
}

func toWireSkills(in []Skill) []wire.Skill {
	out := make([]wire.Skill, 0, len(in))
	for _, s := range in {
		out = append(out, wire.Skill{Name: s.Name, Description: s.Description, Examples: s.Examples})
	}
	return out
}

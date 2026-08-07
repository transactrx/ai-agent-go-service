package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/executor"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/loader"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/retry"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Engine is the top-level handle: registry + hosts + workflows + lifecycle.
type Engine struct {
	cfg      Config
	loader   *loader.Loader
	renderer *render.Renderer

	mu        sync.RWMutex
	workflows map[string]*Workflow
	executors map[string]*executor.Executor

	shutdown sync.Once
}

// Config wires Engine dependencies.
type Config struct {
	Source    WorkflowSource
	Registry  *node.Registry
	Hosts     map[string]any
	Logger    *log.Logger
	LookupEnv func(string) (string, bool) // defaults to os.LookupEnv if nil
	Metrics   MetricsRecorder              // defaults to NoOpMetrics if nil
	Tracer    EngineTracer                 // defaults to NoOpTracer if nil
}

// New returns a configured Engine.
func New(cfg Config) (*Engine, error) {
	if cfg.Source == nil {
		return nil, fmt.Errorf("engine: Source required")
	}
	if cfg.Registry == nil {
		return nil, fmt.Errorf("engine: Registry required")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stdout, "[engine] ", log.LstdFlags|log.Lshortfile)
	}
	if cfg.LookupEnv == nil {
		cfg.LookupEnv = os.LookupEnv
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NoOpMetrics{}
	}
	if cfg.Tracer == nil {
		cfg.Tracer = NoOpTracer{}
	}
	r := render.NewRenderer()
	e := &Engine{
		cfg:       cfg,
		renderer:  r,
		workflows: map[string]*Workflow{},
		executors: map[string]*executor.Executor{},
	}
	envBuilder := func(workflowID, nodeID string, policy *retry.Policy) (node.NodeEnv, func(func(string) ([]node.Node, error))) {
		ne := newNodeEnv(nodeEnvConfig{
			logger:     cfg.Logger,
			nodeID:     nodeID,
			workflowID: workflowID,
			hosts:      cfg.Hosts,
			renderer:   r,
			lookupEnv:  cfg.LookupEnv,
			policy:     policy,
		})
		return ne, ne.setPeerResolver
	}
	var prompts loader.PromptOverrides
	if cfg.Hosts != nil {
		if h, ok := cfg.Hosts["promptstore"]; ok {
			if p, ok := h.(loader.PromptOverrides); ok {
				prompts = p
			}
		}
	}
	e.loader = loader.NewLoader(cfg.Registry, cfg.Hosts, r, cfg.Logger, cfg.LookupEnv, envBuilder, prompts)
	return e, nil
}

// LoadAll loads every workflow from the configured source. Per-workflow
// failures are logged; successful workflows are registered. Returns an error
// only if the source itself errors. A summary line is emitted at the end so
// operators can spot at a glance which workflows came up and which didn't.
func (e *Engine) LoadAll(ctx context.Context) error {
	ids, err := e.cfg.Source.List(ctx)
	if err != nil {
		return fmt.Errorf("source.List: %w", err)
	}
	// WorkflowSource does not contractually guarantee List order (only
	// FilesystemSource happens to via lexical directory walk). Sort so
	// "first definition wins" for duplicate workflow ids is deterministic
	// across any source implementation.
	sort.Strings(ids)

	// Pass 1: read every document so derived files can find their base, and
	// index each by its own JSON "id" (not its Source id) so extends can
	// resolve against the workflow id regardless of what the file is named.
	raws := make(map[string][]byte, len(ids))
	byWorkflowID := make(map[string][]byte, len(ids))
	dupes := make(map[string]string) // source id -> JSON workflow id that collided
	var loadable []string
	var failed []string
	for _, id := range ids {
		raw, err := e.cfg.Source.Load(ctx, id)
		if err != nil {
			e.cfg.Logger.Printf("workflow %s: load failed: %v", id, err)
			failed = append(failed, id)
			continue
		}
		raws[id] = raw
		loadable = append(loadable, id)

		var docPeek struct {
			ID string `json:"id"`
		}
		// Best-effort peek; malformed JSON just yields "" here — the real
		// parse error surfaces downstream in registerOne.
		_ = json.Unmarshal(raw, &docPeek)
		if docPeek.ID != "" {
			if _, seen := byWorkflowID[docPeek.ID]; seen {
				dupes[id] = docPeek.ID
			} else {
				byWorkflowID[docPeek.ID] = raw
			}
		}
	}
	// Pass 2: resolve extends (if any) and register. Non-derived files use
	// their original bytes — identical code path to before derivation existed.
	var loaded []string
	for _, id := range loadable {
		raw := raws[id]
		if wfID, isDup := dupes[id]; isDup {
			e.cfg.Logger.Printf("workflow %s: register failed: duplicate workflow id %q (first definition wins)", id, wfID)
			failed = append(failed, id)
			continue
		}
		baseID, isDerived, extErr := loader.ExtendsTarget(raw)
		if extErr != nil {
			e.cfg.Logger.Printf("workflow %s: register failed: %v", id, extErr)
			failed = append(failed, id)
			continue
		}
		if isDerived {
			baseRaw, ok := byWorkflowID[baseID]
			if !ok {
				e.cfg.Logger.Printf("workflow %s: register failed: extends %q: base workflow not found", id, baseID)
				failed = append(failed, id)
				continue
			}
			_, baseDerived, baseExtErr := loader.ExtendsTarget(baseRaw)
			if baseExtErr != nil {
				e.cfg.Logger.Printf("workflow %s: register failed: base %q: %v", id, baseID, baseExtErr)
				failed = append(failed, id)
				continue
			}
			if baseDerived {
				e.cfg.Logger.Printf("workflow %s: register failed: extends %q: base is itself derived (chained extends is not supported)", id, baseID)
				failed = append(failed, id)
				continue
			}
			merged, err := loader.MergeDerived(baseRaw, raw)
			if err != nil {
				e.cfg.Logger.Printf("workflow %s: register failed: %v", id, err)
				failed = append(failed, id)
				continue
			}
			if v, _ := e.cfg.LookupEnv("WORKFLOW_DERIVE_DEBUG"); v == "true" {
				e.cfg.Logger.Printf("workflow %s: derived from %s, merged config: %s", id, baseID, merged)
			}
			raw = merged
		}
		if err := e.registerOne(ctx, raw); err != nil {
			e.cfg.Logger.Printf("workflow %s: register failed: %v", id, err)
			failed = append(failed, id)
			continue
		}
		loaded = append(loaded, id)
	}
	if len(failed) > 0 {
		e.cfg.Logger.Printf("engine: loaded %d of %d workflows (succeeded: %v; failed: %v)", len(loaded), len(ids), loaded, failed)
	} else {
		e.cfg.Logger.Printf("engine: loaded %d of %d workflows: %v", len(loaded), len(ids), loaded)
	}
	return nil
}

func (e *Engine) registerOne(ctx context.Context, raw []byte) error {
	res, err := e.loader.LoadOne(ctx, raw)
	if err != nil {
		return err
	}
	conns := map[ConnKey][]ConnRef{}
	for _, c := range res.Connections {
		k := ConnKey{DestNode: c.To.Node, DestPort: c.To.Port}
		conns[k] = append(conns[k], ConnRef{SrcNode: c.From.Node, SrcPort: c.From.Port})
	}
	wf := &Workflow{
		ID:          res.ID,
		Description: res.Description,
		Trigger:     res.Trigger,
		Nodes:       res.Nodes,
		TopoOrder:   res.TopoOrder,
		Connections: conns,
		RawConfigs:  res.RawConfigs,
	}
	exec := executor.New(wf, e.cfg.Logger, e.uploader())

	// Subscribe the trigger; sink emits via the executor.
	sink := triggerSinkFn(func(ctx context.Context, evt node.TriggerEvent) error {
		return exec.Run(ctx, evt)
	})
	if err := wf.Trigger.Subscribe(ctx, sink); err != nil {
		return fmt.Errorf("trigger.Subscribe: %w", err)
	}

	e.mu.Lock()
	e.workflows[wf.ID] = wf
	e.executors[wf.ID] = exec
	e.mu.Unlock()
	e.cfg.Logger.Printf("workflow %s registered", wf.ID)
	return nil
}

// Workflow returns the loaded workflow with id, if present.
func (e *Engine) Workflow(id string) (*Workflow, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	wf, ok := e.workflows[id]
	return wf, ok
}

// WorkflowIDs returns the IDs of every successfully loaded workflow.
func (e *Engine) WorkflowIDs() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.workflows))
	for id := range e.workflows {
		out = append(out, id)
	}
	return out
}

// Shutdown closes every node in every workflow and waits up to deadline.
func (e *Engine) Shutdown(ctx context.Context) error {
	var firstErr error
	e.shutdown.Do(func() {
		closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		e.mu.RLock()
		defer e.mu.RUnlock()
		for _, wf := range e.workflows {
			for _, n := range wf.Nodes {
				if err := n.Close(closeCtx); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
	})
	return firstErr
}

// ApplyPromptUpdate hot-applies a new value for one node's overridable field.
// Value must arrive env-substituted. Nodes that don't implement
// Reconfigurable return an error (restart picks the change up from the store).
func (e *Engine) ApplyPromptUpdate(workflowID, nodeID, field, value string) error {
	wf, ok := e.Workflow(workflowID)
	if !ok {
		return fmt.Errorf("engine: unknown workflow %q", workflowID)
	}
	n, ok := wf.Nodes[nodeID]
	if !ok {
		return fmt.Errorf("engine: workflow %s has no node %q", workflowID, nodeID)
	}
	rc, ok := n.(node.Reconfigurable)
	if !ok {
		return fmt.Errorf("engine: node %s/%s does not support hot reconfigure", workflowID, nodeID)
	}
	return rc.Reconfigure(field, value)
}

// triggerSinkFn adapts a function to node.TriggerSink.
type triggerSinkFn func(ctx context.Context, evt node.TriggerEvent) error

func (f triggerSinkFn) Emit(ctx context.Context, evt node.TriggerEvent) error { return f(ctx, evt) }

// uploader returns the s3files uploader registered via cfg.Hosts["s3files"],
// or nil if none was supplied. Nil is valid — tests and dev runs without an
// S3 bucket skip the upload step.
func (e *Engine) uploader() executor.Uploader {
	if e.cfg.Hosts == nil {
		return nil
	}
	h, ok := e.cfg.Hosts["s3files"]
	if !ok {
		return nil
	}
	up, ok := h.(executor.Uploader)
	if !ok {
		return nil
	}
	return up
}

package promptadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/promptstore"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/loader"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/render"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// promptChangedSubject is appended to the NATS base path; published WITHOUT a
// queue group so EVERY instance receives prompt-change notifications.
const promptChangedSubject = ".promptChanged"

// Deps wires the package; all fields required unless noted.
type Deps struct {
	Engine    *engine.Engine
	Store     *promptstore.Store
	NatsHost  *nats_service.NatService
	NatsConn  *nats.Conn // raw conn for broadcast pub/sub
	BasePath  string
	Logger    *log.Logger
	LookupEnv func(string) (string, bool)
}

type service struct {
	deps     Deps
	renderer *render.Renderer
}

type promptKey struct {
	WorkflowID string `json:"workflowId"`
	NodeID     string `json:"nodeId"`
	Field      string `json:"field"`
}

type saveBody struct {
	promptKey
	Content string `json:"content"`
}

// Register adds the PromptGet/PromptSave/PromptHistory endpoints and
// subscribes the broadcast listener.
func Register(deps Deps) error {
	s := &service{deps: deps, renderer: render.NewRenderer()}
	regs := []nats_service.EndpointRegistration{
		{Path: "PromptGet", Description: "Get fixed + default + current flexible prompt for a node field", Handler: s.handleGet},
		{Path: "PromptSave", Description: "Save a new flexible prompt version (X-User-Id required)", Handler: s.handleSave,
			Headers: []nats_service.HeaderDoc{{Name: "X-User-Id", Description: "Saver identity for audit", Required: true}}},
		{Path: "PromptHistory", Description: "List saved versions for a node field", Handler: s.handleHistory},
	}
	if err := deps.NatsHost.AddEndpointWithDocs(regs); err != nil {
		return err
	}
	return s.subscribeBroadcast()
}

// nodeFieldOverridable returns the node + whether field is declared overridable.
func (s *service) nodeFieldOverridable(workflowID, nodeID, field string) (node.Node, error) {
	wf, ok := s.deps.Engine.Workflow(workflowID)
	if !ok {
		return nil, fmt.Errorf("unknown workflow %q", workflowID)
	}
	n, ok := wf.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node %q", nodeID)
	}
	for _, f := range n.Spec().OverridableFields {
		if f == field {
			return n, nil
		}
	}
	return nil, fmt.Errorf("field %q is not overridable on %s/%s", field, workflowID, nodeID)
}

// rawDefault reads the workflow-JSON default value (pre-override) of a field.
func (s *service) rawDefault(workflowID, nodeID, field string) string {
	wf, ok := s.deps.Engine.Workflow(workflowID)
	if !ok || wf.RawConfigs == nil {
		return ""
	}
	var cfg map[string]any
	if err := json.Unmarshal(wf.RawConfigs[nodeID], &cfg); err != nil {
		return ""
	}
	v, _ := cfg[field].(string)
	return v
}

func (s *service) handleGet(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	var key promptKey
	if err := json.Unmarshal(msg.Body, &key); err != nil {
		return badReq("bad body: " + err.Error())
	}
	if _, err := s.nodeFieldOverridable(key.WorkflowID, key.NodeID, key.Field); err != nil {
		return badReq(err.Error())
	}
	current, found, err := s.deps.Store.Latest(context.Background(), key.WorkflowID, key.NodeID, key.Field)
	if err != nil {
		return srvErr(err.Error())
	}
	source := "default"
	defaultFlex := s.rawDefault(key.WorkflowID, key.NodeID, key.Field)
	if found {
		source = "db"
	} else {
		current = defaultFlex
	}
	fixed := s.rawDefault(key.WorkflowID, key.NodeID, "systemMessageFixed")
	if fixed == "" {
		fixed = s.rawDefault(key.WorkflowID, key.NodeID, "systemMessage") // legacy alias
	}
	resp := map[string]any{
		"fixed":       fixed,
		"defaultFlex": defaultFlex,
		"currentFlex": current,
		"source":      source,
	}
	msg.ResponseBody, _ = json.Marshal(resp)
	return nil
}

func (s *service) handleSave(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	userID := msg.Header.Get("X-User-Id")
	if userID == "" {
		return badReq("X-User-Id header required")
	}
	var body saveBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return badReq("bad body: " + err.Error())
	}
	if _, err := s.nodeFieldOverridable(body.WorkflowID, body.NodeID, body.Field); err != nil {
		return badReq(err.Error())
	}
	if err := ValidateContent(body.Content, s.renderer, s.deps.LookupEnv); err != nil {
		return badReq(err.Error())
	}
	version, err := s.deps.Store.Save(context.Background(), body.WorkflowID, body.NodeID, body.Field, body.Content, userID)
	if err != nil {
		return srvErr(err.Error())
	}
	s.deps.Logger.Printf("promptadmin: saved %s/%s/%s version=%s by=%s", body.WorkflowID, body.NodeID, body.Field, version, userID)
	// Broadcast AFTER the durable write; every instance (incl. this one)
	// re-resolves via the subscription handler.
	note, _ := json.Marshal(body.promptKey)
	if err := s.deps.NatsConn.Publish(s.deps.BasePath+promptChangedSubject, note); err != nil {
		s.deps.Logger.Printf("promptadmin: broadcast failed (instances stale until restart): %v", err)
	}
	msg.ResponseBody, _ = json.Marshal(map[string]string{"version": version})
	return nil
}

func (s *service) handleHistory(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	var key struct {
		promptKey
		Limit int `json:"limit,omitempty"`
	}
	if err := json.Unmarshal(msg.Body, &key); err != nil {
		return badReq("bad body: " + err.Error())
	}
	if _, err := s.nodeFieldOverridable(key.WorkflowID, key.NodeID, key.Field); err != nil {
		return badReq(err.Error())
	}
	versions, err := s.deps.Store.History(context.Background(), key.WorkflowID, key.NodeID, key.Field, key.Limit)
	if err != nil {
		return srvErr(err.Error())
	}
	msg.ResponseBody, _ = json.Marshal(versions)
	return nil
}

// subscribeBroadcast listens (NO queue group) and hot-applies changes.
func (s *service) subscribeBroadcast() error {
	subj := s.deps.BasePath + promptChangedSubject
	_, err := s.deps.NatsConn.Subscribe(subj, func(m *nats.Msg) {
		var key promptKey
		if err := json.Unmarshal(m.Data, &key); err != nil {
			s.deps.Logger.Printf("promptadmin: bad broadcast payload: %v", err)
			return
		}
		content, found, err := s.deps.Store.Latest(context.Background(), key.WorkflowID, key.NodeID, key.Field)
		if err != nil || !found {
			s.deps.Logger.Printf("promptadmin: broadcast re-resolve failed (found=%v err=%v)", found, err)
			return
		}
		resolved, rerrs := loader.Resolve(content, s.deps.LookupEnv)
		if len(rerrs) > 0 {
			s.deps.Logger.Printf("promptadmin: broadcast envsubst failed: %v", rerrs)
			return
		}
		resolvedStr, _ := resolved.(string)
		if err := s.deps.Engine.ApplyPromptUpdate(key.WorkflowID, key.NodeID, key.Field, resolvedStr); err != nil {
			s.deps.Logger.Printf("promptadmin: hot apply failed (will apply on restart): %v", err)
			return
		}
		s.deps.Logger.Printf("promptadmin: hot-applied %s/%s/%s", key.WorkflowID, key.NodeID, key.Field)
	})
	return err
}

func badReq(message string) *nats_service.NatsServiceError {
	e := nats_service.NewValidationError(message, 400, fmt.Errorf("prompt-admin"))
	return &e
}

func srvErr(message string) *nats_service.NatsServiceError {
	e := nats_service.NewServerError(message, 500, fmt.Errorf("prompt-admin"))
	return &e
}

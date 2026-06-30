package workflowlist

import (
	"encoding/json"
	"log"
	"sort"

	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
)

// WorkflowInfo is one entry in the ListWorkflows response.
type WorkflowInfo struct {
	ID          string `json:"id"`
	Description string `json:"description"`
}

type service struct {
	eng *engine.Engine
}

// Deps is the registration context, mirroring pkg/promptadmin.Deps.
type Deps struct {
	Engine   *engine.Engine
	NatsHost *nats_service.NatService
	Logger   *log.Logger
}

// buildListBody marshals the current workflow list. Extracted so it is unit
// testable without a live NATS host.
func (s *service) buildListBody() ([]byte, error) {
	return json.Marshal(listWorkflows(s.eng))
}

func (s *service) handleList(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	body, err := s.buildListBody()
	if err != nil {
		e := nats_service.NewServerError("workflow-list-marshal-error", 500, err)
		return &e
	}
	msg.ResponseBody = body
	return nil
}

// Register adds the ListWorkflows endpoint (subject ${NATS_BASE_PATH}.ListWorkflows).
func Register(deps Deps) error {
	s := &service{eng: deps.Engine}
	regs := []nats_service.EndpointRegistration{
		{Path: "ListWorkflows", Description: "List currently-loaded chat workflows as [{id,description}]", Handler: s.handleList},
	}
	return deps.NatsHost.AddEndpointWithDocs(regs)
}

// listWorkflows reads every loaded workflow's id + description, sorted by id
// for deterministic output.
func listWorkflows(eng *engine.Engine) []WorkflowInfo {
	ids := eng.WorkflowIDs()
	out := make([]WorkflowInfo, 0, len(ids))
	for _, id := range ids {
		wf, ok := eng.Workflow(id)
		if !ok {
			continue
		}
		out = append(out, WorkflowInfo{ID: wf.ID, Description: wf.Description})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

package workflowlist

import (
	"encoding/json"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/executor"
)

func TestListWorkflowsReturnsIdAndDescriptionSorted(t *testing.T) {
	eng := engine.NewForTest(map[string]*executor.Workflow{
		"powerlineSearch": {ID: "powerlineSearch", Description: "Pharmacy claim search agent."},
		"alphaFlow":       {ID: "alphaFlow", Description: "Alpha."},
	})

	got := listWorkflows(eng)

	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].ID != "alphaFlow" || got[0].Description != "Alpha." {
		t.Fatalf("got[0] = %+v, want {alphaFlow, Alpha.}", got[0])
	}
	if got[1].ID != "powerlineSearch" || got[1].Description != "Pharmacy claim search agent." {
		t.Fatalf("got[1] = %+v", got[1])
	}
}

func TestHandleListSetsResponseBody(t *testing.T) {
	eng := engine.NewForTest(map[string]*executor.Workflow{
		"powerlineSearch": {ID: "powerlineSearch", Description: "Pharmacy claim search agent."},
	})
	s := &service{eng: eng}

	body, err := s.buildListBody()
	if err != nil {
		t.Fatalf("buildListBody err: %v", err)
	}
	var got []WorkflowInfo
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 1 || got[0].ID != "powerlineSearch" {
		t.Fatalf("got %+v", got)
	}
}

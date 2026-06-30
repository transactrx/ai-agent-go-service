package node

import (
	"context"
	"encoding/json"
	"testing"
)

type fakeServerTool struct{}

func (fakeServerTool) Spec() NodeSpec                                                      { return NodeSpec{} }
func (fakeServerTool) Init(_ context.Context, _ NodeEnv) error                            { return nil }
func (fakeServerTool) Close(_ context.Context) error                                      { return nil }
func (fakeServerTool) ToolSpec() ToolSpec                                                 { return ToolSpec{} }
func (fakeServerTool) FailurePolicy() FailurePolicy                                       { return FailurePolicy("") }
func (fakeServerTool) RetryPolicy() RetryPolicy                                           { return nil }
func (fakeServerTool) Invoke(_ context.Context, _ json.RawMessage) (json.RawMessage, error) { return nil, nil }

type fakeClientTool struct{ fakeServerTool }

func (fakeClientTool) ClientOnly() bool { return true }

func TestIsClientUITool(t *testing.T) {
	var server Tool = fakeServerTool{}
	if _, ok := server.(ClientUITool); ok {
		t.Fatalf("plain Tool must not satisfy ClientUITool")
	}
	var client Tool = fakeClientTool{}
	cu, ok := client.(ClientUITool)
	if !ok {
		t.Fatalf("fakeClientTool must satisfy ClientUITool")
	}
	if !cu.ClientOnly() {
		t.Fatalf("ClientOnly() must return true")
	}
}

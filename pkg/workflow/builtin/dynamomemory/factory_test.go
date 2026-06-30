package dynamomemory

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFactory_rejectsMissingTableName(t *testing.T) {
	_, err := Factory.New(json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "tableName is required") {
		t.Fatalf("expected tableName required error, got %v", err)
	}
}

func TestFactory_rejectsInvalidTableName(t *testing.T) {
	_, err := Factory.New(json.RawMessage(`{"tableName":"bad name!"}`))
	if err == nil || !strings.Contains(err.Error(), "tableName") {
		t.Fatalf("expected tableName invalid error, got %v", err)
	}
}

func TestFactory_appliesDefaults(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"tableName":"foo-bar"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := n.(*dynamoMemory)
	if m.cfg.Region != "us-east-1" {
		t.Errorf("Region default = %q, want us-east-1", m.cfg.Region)
	}
	if m.cfg.MaxTurns != 30 {
		t.Errorf("MaxTurns default = %d, want 30", m.cfg.MaxTurns)
	}
	if m.cfg.TTLDays != 90 {
		t.Errorf("TTLDays default = %d, want 90", m.cfg.TTLDays)
	}
}

func TestFactory_honorsOverrides(t *testing.T) {
	raw := json.RawMessage(`{"tableName":"foo","region":"eu-west-1","maxTurns":10,"ttlDays":7}`)
	n, err := Factory.New(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m := n.(*dynamoMemory)
	if m.cfg.Region != "eu-west-1" || m.cfg.MaxTurns != 10 || m.cfg.TTLDays != 7 {
		t.Errorf("overrides not honored: %+v", m.cfg)
	}
}

func TestFactory_rejectsNegativeTTL(t *testing.T) {
	_, err := Factory.New(json.RawMessage(`{"tableName":"foo","ttlDays":-1}`))
	if err == nil || !strings.Contains(err.Error(), "ttlDays") {
		t.Fatalf("expected ttlDays invalid error, got %v", err)
	}
}

func TestSpec_announcesMemoryRole(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"tableName":"foo"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	spec := n.Spec()
	if spec.Type != "memory/dynamodb" {
		t.Errorf("Spec.Type = %q, want memory/dynamodb", spec.Type)
	}
	if len(spec.OutputPorts) != 1 || spec.OutputPorts[0].Name != "ai_memory" {
		t.Errorf("expected one ai_memory output port, got %+v", spec.OutputPorts)
	}
}

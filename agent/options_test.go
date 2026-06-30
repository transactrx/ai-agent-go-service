package agent

import "testing"

func TestNewService_DefaultAppName(t *testing.T) {
	s := NewService()
	if s.appName != "ai-agent-service" {
		t.Errorf("default appName = %q, want ai-agent-service", s.appName)
	}
}

func TestWithAppName_Overrides(t *testing.T) {
	s := NewService(WithAppName("opensearchAiChatApi"))
	if s.appName != "opensearchAiChatApi" {
		t.Errorf("appName = %q, want opensearchAiChatApi", s.appName)
	}
}

func TestWithAppName_EmptyKeepsDefault(t *testing.T) {
	s := NewService(WithAppName(""))
	if s.appName != "ai-agent-service" {
		t.Errorf("empty WithAppName changed default to %q", s.appName)
	}
}

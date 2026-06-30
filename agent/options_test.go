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

func TestNewService_DefaultRepositoryURL(t *testing.T) {
	s := NewService()
	if s.repositoryURL != "https://github.com/transactrx/ai-agent-go-service" {
		t.Errorf("default repositoryURL = %q", s.repositoryURL)
	}
}

func TestWithRepositoryURL_Overrides(t *testing.T) {
	want := "https://github.com/transactrx/opensearchAiChatApi"
	s := NewService(WithRepositoryURL(want))
	if s.repositoryURL != want {
		t.Errorf("repositoryURL = %q, want %q", s.repositoryURL, want)
	}
}

func TestWithRepositoryURL_EmptyKeepsDefault(t *testing.T) {
	s := NewService(WithRepositoryURL(""))
	if s.repositoryURL != "https://github.com/transactrx/ai-agent-go-service" {
		t.Errorf("empty WithRepositoryURL changed default to %q", s.repositoryURL)
	}
}

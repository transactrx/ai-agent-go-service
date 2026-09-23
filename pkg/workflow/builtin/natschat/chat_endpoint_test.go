// pkg/workflow/builtin/natschat/chat_endpoint_test.go
package natschat

import (
	"testing"
	"time"
)

func TestChatEndpointAccessors(t *testing.T) {
	tr := newTestTrigger(t, `{"responseMode":"single","allowResponseModeOverride":true,"requestTimeoutSeconds":45}`)
	var ep ChatEndpoint = tr // compile-time: trigger implements the interface

	if ep.ChatSubject() != "" {
		t.Fatalf("before Init, ChatSubject should be empty, got %q", ep.ChatSubject())
	}
	tr.basePath = "trx.test"
	tr.subject = "SingleSearch"
	if got := ep.ChatSubject(); got != "trx.test.SingleSearch" {
		t.Fatalf("ChatSubject = %q", got)
	}
	if ep.ResponseMode() != "single" {
		t.Fatalf("ResponseMode = %q", ep.ResponseMode())
	}
	if !ep.AllowsResponseModeOverride() {
		t.Fatal("AllowsResponseModeOverride should be true")
	}
	if ep.RequestTimeout() != 45*time.Second {
		t.Fatalf("RequestTimeout = %s", ep.RequestTimeout())
	}
}

func TestChatEndpointDefaults(t *testing.T) {
	tr := newTestTrigger(t, `{"responseMode":"streaming"}`)
	if tr.AllowsResponseModeOverride() {
		t.Fatal("override must default to false")
	}
	if tr.RequestTimeout() != 600*time.Second {
		t.Fatalf("default RequestTimeout = %s, want 600s", tr.RequestTimeout())
	}
}

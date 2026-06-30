package safego_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/safego"
)

func TestRunReturnsFnError(t *testing.T) {
	want := errors.New("boom")
	got := safego.Run("test", func() error { return want })
	if !errors.Is(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestRunReturnsNilOnSuccess(t *testing.T) {
	got := safego.Run("test", func() error { return nil })
	if got != nil {
		t.Fatalf("got %v", got)
	}
}

func TestRunRecoversPanic(t *testing.T) {
	got := safego.Run("test", func() error { panic("kaboom") })
	if got == nil {
		t.Fatal("expected error from recovered panic")
	}
	if !strings.Contains(got.Error(), "panic in test") {
		t.Fatalf("error missing context: %v", got)
	}
	if !strings.Contains(got.Error(), "kaboom") {
		t.Fatalf("error missing panic value: %v", got)
	}
}

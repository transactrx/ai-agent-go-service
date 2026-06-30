package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestClassify_NilError_Empty(t *testing.T) {
	if c := Classify(nil); c != "" {
		t.Errorf("got %q want empty", c)
	}
}

func TestClassify_ToolErrorRetryableTrue(t *testing.T) {
	e := &node.ToolError{Code: "upstream", Cause: errors.New("blank"), Retryable: true}
	if c := Classify(e); c != ClassUpstream {
		t.Errorf("got %q want %q", c, ClassUpstream)
	}
}

func TestClassify_ToolErrorRetryableFalse_NotRetryable(t *testing.T) {
	e := &node.ToolError{Code: "transient", Cause: errors.New("x"), Retryable: false}
	// Even with Code that maps to a retryable class, Retryable:false wins.
	if c := Classify(e); c != ClassValidation {
		t.Errorf("got %q want %q (non-retryable override)", c, ClassValidation)
	}
}

func TestClassify_ContextDeadline_IsTimeout(t *testing.T) {
	if c := Classify(context.DeadlineExceeded); c != ClassTimeout {
		t.Errorf("got %q want %q", c, ClassTimeout)
	}
}

func TestClassify_ContextCanceled_IsValidation(t *testing.T) {
	if c := Classify(context.Canceled); c != ClassValidation {
		t.Errorf("got %q want %q", c, ClassValidation)
	}
}

func TestClassify_PolicyDeniedError_IsPermission(t *testing.T) {
	pde := &node.PolicyDeniedError{Reason: "nope", AllowedIndices: []string{"prod.*"}}
	if c := Classify(pde); c != ClassPermission {
		t.Errorf("got %q want %q", c, ClassPermission)
	}
}

func TestClassify_HTTPPatterns_AreTransient(t *testing.T) {
	for _, msg := range []string{
		"server: 500: internal error",
		"upstream returned 502",
		"503 Service Unavailable",
		"received 504 from gateway",
		"too many requests: 429",
		"408 Request Timeout",
		"read tcp: connection reset by peer",
		"unexpected EOF",
	} {
		if c := Classify(errors.New(msg)); c != ClassTransient {
			t.Errorf("%q: got %q want %q", msg, c, ClassTransient)
		}
	}
}

func TestClassify_PlainError_IsValidation(t *testing.T) {
	if c := Classify(errors.New("bad input: missing field")); c != ClassValidation {
		t.Errorf("got %q want %q", c, ClassValidation)
	}
}

func TestClassify_WrappedToolError(t *testing.T) {
	inner := &node.ToolError{Code: "transient", Cause: errors.New("x"), Retryable: true}
	wrapped := fmt.Errorf("call failed: %w", inner)
	if c := Classify(wrapped); c != ClassTransient {
		t.Errorf("got %q want %q (errors.As should unwrap)", c, ClassTransient)
	}
}

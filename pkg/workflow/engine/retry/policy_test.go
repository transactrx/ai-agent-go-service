package retry

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParsePolicy_Defaults_EmptyObject(t *testing.T) {
	p, err := ParsePolicy(json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.MaxAttempts != 3 {
		t.Errorf("MaxAttempts: got %d want 3", p.MaxAttempts)
	}
	if p.Backoff != BackoffExponential {
		t.Errorf("Backoff: got %q want %q", p.Backoff, BackoffExponential)
	}
	if p.InitialDelay != 500*time.Millisecond {
		t.Errorf("InitialDelay: got %v want 500ms", p.InitialDelay)
	}
	if p.MaxDelay != 5*time.Second {
		t.Errorf("MaxDelay: got %v want 5s", p.MaxDelay)
	}
	if !p.Jitter {
		t.Errorf("Jitter: got false want true")
	}
	if len(p.RetryOn) != 2 || p.RetryOn[0] != ClassTransient || p.RetryOn[1] != ClassTimeout {
		t.Errorf("RetryOn: got %v want [transient timeout]", p.RetryOn)
	}
	if p.PerAttemptTimeout != 0 {
		t.Errorf("PerAttemptTimeout: got %v want 0", p.PerAttemptTimeout)
	}
	if p.TotalBudget != 0 {
		t.Errorf("TotalBudget: got %v want 0", p.TotalBudget)
	}
}

func TestParsePolicy_NilInput_ReturnsNilPolicyNoError(t *testing.T) {
	p, err := ParsePolicy(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p != nil {
		t.Errorf("expected nil policy for nil input, got %+v", p)
	}
}

func TestParsePolicy_AllFields(t *testing.T) {
	raw := json.RawMessage(`{
		"maxAttempts": 5,
		"backoff": "fixed",
		"initialDelayMs": 1000,
		"maxDelayMs": 10000,
		"jitter": false,
		"retryOn": ["transient", "upstream"],
		"perAttemptTimeoutSeconds": 15,
		"totalBudgetSeconds": 60
	}`)
	p, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.MaxAttempts != 5 {
		t.Errorf("MaxAttempts: got %d want 5", p.MaxAttempts)
	}
	if p.Backoff != BackoffFixed {
		t.Errorf("Backoff: got %q want fixed", p.Backoff)
	}
	if p.InitialDelay != time.Second {
		t.Errorf("InitialDelay: got %v want 1s", p.InitialDelay)
	}
	if p.MaxDelay != 10*time.Second {
		t.Errorf("MaxDelay: got %v want 10s", p.MaxDelay)
	}
	if p.Jitter {
		t.Errorf("Jitter: got true want false")
	}
	if len(p.RetryOn) != 2 || p.RetryOn[0] != ClassTransient || p.RetryOn[1] != ClassUpstream {
		t.Errorf("RetryOn: got %v", p.RetryOn)
	}
	if p.PerAttemptTimeout != 15*time.Second {
		t.Errorf("PerAttemptTimeout: got %v want 15s", p.PerAttemptTimeout)
	}
	if p.TotalBudget != 60*time.Second {
		t.Errorf("TotalBudget: got %v want 60s", p.TotalBudget)
	}
}

func TestParsePolicy_AllExpansion(t *testing.T) {
	raw := json.RawMessage(`{"retryOn": ["all"]}`)
	p, err := ParsePolicy(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(p.RetryOn) != len(AllClasses) {
		t.Fatalf("RetryOn length: got %d want %d", len(p.RetryOn), len(AllClasses))
	}
	for i, c := range AllClasses {
		if p.RetryOn[i] != c {
			t.Errorf("RetryOn[%d]: got %q want %q", i, p.RetryOn[i], c)
		}
	}
}

func TestParsePolicy_RejectsInvalidBackoff(t *testing.T) {
	_, err := ParsePolicy(json.RawMessage(`{"backoff": "linear"}`))
	if err == nil {
		t.Fatal("expected error for backoff=linear")
	}
}

func TestParsePolicy_RejectsMaxAttemptsOutOfRange(t *testing.T) {
	for _, raw := range []string{`{"maxAttempts": 0}`, `{"maxAttempts": 11}`, `{"maxAttempts": -1}`} {
		_, err := ParsePolicy(json.RawMessage(raw))
		if err == nil {
			t.Errorf("expected error for %s", raw)
		}
	}
}

func TestParsePolicy_RejectsUnknownRetryOn(t *testing.T) {
	_, err := ParsePolicy(json.RawMessage(`{"retryOn": ["bogus"]}`))
	if err == nil {
		t.Fatal("expected error for unknown retryOn class")
	}
}

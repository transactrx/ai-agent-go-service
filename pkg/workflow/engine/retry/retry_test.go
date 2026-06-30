package retry

import (
	"bytes"
	"context"
	"errors"
	stdlog "log"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock records sleep durations and signals each timer immediately.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(0, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) clockTimer {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	c.now = c.now.Add(d)
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- c.Now()
	return fakeTimer{ch: ch}
}

func (c *fakeClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.sleeps))
	copy(out, c.sleeps)
	return out
}

type fakeTimer struct{ ch chan time.Time }

func (t fakeTimer) C() <-chan time.Time { return t.ch }
func (t fakeTimer) Stop() bool          { return true }

func TestDo_NilPolicy_RunsOpOnceSuccess(t *testing.T) {
	calls := 0
	r := Do(context.Background(), nil, func(_ context.Context, attempt int) (int, error) {
		calls++
		if attempt != 1 {
			t.Errorf("attempt: got %d want 1", attempt)
		}
		return 42, nil
	})
	if calls != 1 {
		t.Errorf("calls: got %d want 1", calls)
	}
	if r.Value != 42 || r.Err != nil {
		t.Errorf("Result: %+v", r)
	}
	if r.Attempts != 1 || r.Decision != DecisionSucceeded {
		t.Errorf("Result: %+v", r)
	}
}

func TestDo_NilPolicy_RunsOpOnceError(t *testing.T) {
	wantErr := errors.New("boom")
	r := Do(context.Background(), nil, func(_ context.Context, _ int) (int, error) {
		return 0, wantErr
	})
	if r.Err != wantErr {
		t.Errorf("Err: got %v want %v", r.Err, wantErr)
	}
	if r.Decision != DecisionNotRetryable {
		t.Errorf("Decision: got %v want NotRetryable", r.Decision)
	}
	if r.Attempts != 1 {
		t.Errorf("Attempts: got %d want 1", r.Attempts)
	}
}

func TestDo_MaxAttemptsOne_BehavesLikeNilPolicy(t *testing.T) {
	p := &Policy{MaxAttempts: 1, RetryOn: []Class{ClassTransient}}
	calls := 0
	r := Do(context.Background(), p, func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, errors.New("500: server error")
	})
	if calls != 1 {
		t.Errorf("calls: got %d want 1", calls)
	}
	if r.Attempts != 1 {
		t.Errorf("Attempts: got %d want 1", r.Attempts)
	}
}

func TestDoWithClock_ExhaustedRetries(t *testing.T) {
	p := &Policy{
		MaxAttempts:  3,
		Backoff:      BackoffExponential,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		RetryOn:      []Class{ClassTransient},
	}
	clk := newFakeClock()
	wantErr := errors.New("503: still down")
	calls := 0
	r := doWithClockAndLogger(context.Background(), p, clk, nil, "", func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, wantErr
	})
	if calls != 3 {
		t.Errorf("calls: got %d want 3", calls)
	}
	if r.Decision != DecisionExhaustedRetries {
		t.Errorf("Decision: got %v want ExhaustedRetries", r.Decision)
	}
	if r.Err != wantErr {
		t.Errorf("Err: got %v want %v", r.Err, wantErr)
	}
	if r.Attempts != 3 {
		t.Errorf("Attempts: got %d want 3", r.Attempts)
	}
	if r.LastClass != ClassTransient {
		t.Errorf("LastClass: got %q want %q", r.LastClass, ClassTransient)
	}
}

func TestDoWithClock_NonRetryableShortCircuits(t *testing.T) {
	p := &Policy{
		MaxAttempts: 5,
		Backoff:     BackoffExponential,
		RetryOn:     []Class{ClassTransient}, // does NOT include validation
	}
	clk := newFakeClock()
	calls := 0
	wantErr := errors.New("invalid arg")
	r := doWithClockAndLogger(context.Background(), p, clk, nil, "", func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, wantErr
	})
	if calls != 1 {
		t.Errorf("calls: got %d want 1", calls)
	}
	if r.Decision != DecisionNotRetryable {
		t.Errorf("Decision: got %v want NotRetryable", r.Decision)
	}
}

func TestDoWithClock_MaxDelayCapsExponential(t *testing.T) {
	p := &Policy{
		MaxAttempts:  5,
		Backoff:      BackoffExponential,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     250 * time.Millisecond,
		Jitter:       false,
		RetryOn:      []Class{ClassTransient},
	}
	clk := newFakeClock()
	doWithClockAndLogger(context.Background(), p, clk, nil, "", func(_ context.Context, _ int) (int, error) {
		return 0, errors.New("500: err")
	})
	got := clk.Sleeps()
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond, 250 * time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("sleeps len: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sleep[%d]: got %v want %v", i, got[i], want[i])
		}
	}
}

func TestDoWithClock_FixedBackoff(t *testing.T) {
	p := &Policy{
		MaxAttempts:  4,
		Backoff:      BackoffFixed,
		InitialDelay: 50 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Jitter:       false,
		RetryOn:      []Class{ClassTransient},
	}
	clk := newFakeClock()
	doWithClockAndLogger(context.Background(), p, clk, nil, "", func(_ context.Context, _ int) (int, error) {
		return 0, errors.New("500: err")
	})
	got := clk.Sleeps()
	want := []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("sleeps len: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sleep[%d]: got %v want %v", i, got[i], want[i])
		}
	}
}

func TestDoWithClock_RetriesTransientErrors_ExponentialBackoff(t *testing.T) {
	p := &Policy{
		MaxAttempts:  4,
		Backoff:      BackoffExponential,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Jitter:       false,
		RetryOn:      []Class{ClassTransient},
	}
	clk := newFakeClock()
	calls := 0
	r := doWithClockAndLogger(context.Background(), p, clk, nil, "", func(_ context.Context, attempt int) (string, error) {
		calls++
		if attempt < 3 {
			return "", errors.New("502: bad gateway")
		}
		return "ok", nil
	})
	if calls != 3 {
		t.Errorf("calls: got %d want 3", calls)
	}
	if r.Value != "ok" || r.Err != nil {
		t.Errorf("Result: %+v", r)
	}
	if r.Attempts != 3 || r.Decision != DecisionSucceeded {
		t.Errorf("Result: %+v", r)
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	got := clk.Sleeps()
	if len(got) != len(want) {
		t.Fatalf("sleeps len: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sleep[%d]: got %v want %v", i, got[i], want[i])
		}
	}
}

func TestDoWithClock_PerAttemptTimeoutCancelsAttemptCtx(t *testing.T) {
	p := &Policy{
		MaxAttempts:       3,
		Backoff:           BackoffFixed,
		InitialDelay:      0,
		Jitter:            false,
		RetryOn:           []Class{ClassTimeout},
		PerAttemptTimeout: 5 * time.Millisecond,
	}
	clk := newFakeClock()
	calls := 0
	r := doWithClockAndLogger(context.Background(), p, clk, nil, "", func(ctx context.Context, _ int) (int, error) {
		calls++
		// op blocks until ctx fires; with PerAttemptTimeout=5ms this should
		// terminate via ctx.Err()=DeadlineExceeded.
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if calls != 3 {
		t.Errorf("calls: got %d want 3", calls)
	}
	if r.Decision != DecisionExhaustedRetries {
		t.Errorf("Decision: got %v want ExhaustedRetries", r.Decision)
	}
	if r.LastClass != ClassTimeout {
		t.Errorf("LastClass: got %q want %q", r.LastClass, ClassTimeout)
	}
}

func TestDoWithClock_TotalBudgetExpires(t *testing.T) {
	p := &Policy{
		MaxAttempts:  10,
		Backoff:      BackoffFixed,
		InitialDelay: 100 * time.Millisecond,
		Jitter:       false,
		RetryOn:      []Class{ClassTransient},
		TotalBudget:  250 * time.Millisecond,
	}
	clk := newFakeClock()
	calls := 0
	r := doWithClockAndLogger(context.Background(), p, clk, nil, "", func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, errors.New("500: err")
	})
	if r.Decision != DecisionBudgetExpired {
		t.Errorf("Decision: got %v want BudgetExpired", r.Decision)
	}
	// With InitialDelay=100ms and TotalBudget=250ms, fakeClock advances by the
	// scheduled sleep on each NewTimer call:
	//   attempt 1 (clock 0ms) → plan sleep 100ms → after sleep clock=100ms
	//   attempt 2 (clock 100ms) → plan sleep 100ms → after sleep clock=200ms
	//   attempt 3 (clock 200ms) → plan sleep 100ms → projected clock 300ms > 250ms budget → BudgetExpired
	if calls < 2 {
		t.Errorf("calls: got %d expected at least 2", calls)
	}
}

func TestDoWithClock_CtxCancelledBetweenAttempts(t *testing.T) {
	p := &Policy{
		MaxAttempts:  5,
		Backoff:      BackoffFixed,
		InitialDelay: 50 * time.Millisecond,
		Jitter:       false,
		RetryOn:      []Class{ClassTransient},
	}
	// Real clock + real-but-short sleep so we can cancel during it.
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()
	r := Do(ctx, p, func(_ context.Context, _ int) (int, error) {
		calls++
		return 0, errors.New("500: err")
	})
	if r.Decision != DecisionCancelled {
		t.Errorf("Decision: got %v want Cancelled", r.Decision)
	}
	if !errors.Is(r.Err, context.Canceled) {
		t.Errorf("Err: got %v want context.Canceled", r.Err)
	}
	if calls > 2 {
		t.Errorf("calls: got %d expected <=2", calls)
	}
}

func TestDoWithClock_LogsAttemptOutcomes(t *testing.T) {
	p := &Policy{
		MaxAttempts:  3,
		Backoff:      BackoffFixed,
		InitialDelay: 0,
		Jitter:       false,
		RetryOn:      []Class{ClassTransient},
	}
	t.Run("retrying-then-succeeded", func(t *testing.T) {
		var buf bytes.Buffer
		logger := stdlog.New(&buf, "", 0)
		clk := newFakeClock()
		failsLeft := 1
		doWithClockAndLogger(context.Background(), p, clk, logger, "wf=W tool=T", func(_ context.Context, _ int) (int, error) {
			if failsLeft > 0 {
				failsLeft--
				return 0, errors.New("503: down")
			}
			return 1, nil
		})
		out := buf.String()
		if !strings.Contains(out, `retrying wf=W tool=T attempt=1/3 class="transient"`) {
			t.Errorf("missing retrying line: %q", out)
		}
		if !strings.Contains(out, `succeeded wf=W tool=T attempts=2/3`) {
			t.Errorf("missing succeeded-after-retry line: %q", out)
		}
	})
	t.Run("not-retryable", func(t *testing.T) {
		var buf bytes.Buffer
		logger := stdlog.New(&buf, "", 0)
		clk := newFakeClock()
		doWithClockAndLogger(context.Background(), p, clk, logger, "wf=W tool=T", func(_ context.Context, _ int) (int, error) {
			return 0, errors.New("invalid arg")
		})
		out := buf.String()
		if !strings.Contains(out, `not-retryable wf=W tool=T attempt=1/3 class="validation"`) {
			t.Errorf("missing not-retryable line: %q", out)
		}
	})
	t.Run("exhausted", func(t *testing.T) {
		var buf bytes.Buffer
		logger := stdlog.New(&buf, "", 0)
		clk := newFakeClock()
		doWithClockAndLogger(context.Background(), p, clk, logger, "wf=W tool=T", func(_ context.Context, _ int) (int, error) {
			return 0, errors.New("503: down")
		})
		out := buf.String()
		if !strings.Contains(out, `exhausted wf=W tool=T attempts=3/3 class="transient"`) {
			t.Errorf("missing exhausted line: %q", out)
		}
	})
	t.Run("budget-expired", func(t *testing.T) {
		var buf bytes.Buffer
		logger := stdlog.New(&buf, "", 0)
		pb := &Policy{
			MaxAttempts:  10,
			Backoff:      BackoffFixed,
			InitialDelay: 100 * time.Millisecond,
			Jitter:       false,
			RetryOn:      []Class{ClassTransient},
			TotalBudget:  250 * time.Millisecond,
		}
		clk := newFakeClock()
		doWithClockAndLogger(context.Background(), pb, clk, logger, "wf=W tool=T", func(_ context.Context, _ int) (int, error) {
			return 0, errors.New("503: down")
		})
		out := buf.String()
		if !strings.Contains(out, `budget-expired wf=W tool=T`) {
			t.Errorf("missing budget-expired line: %q", out)
		}
	})
}

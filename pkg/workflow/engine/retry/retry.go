package retry

import (
	"context"
	"log"
	"math/rand"
	"time"
)

// Op is the op signature Do drives. attempt is 1-indexed.
type Op[T any] func(ctx context.Context, attempt int) (T, error)

// DoWithLogger is like Do but also emits log lines for every retry decision:
// retrying, succeeded-after-retry, not-retryable, exhausted, budget-expired,
// cancelled. Pass nil to disable logging. The label is appended verbatim to
// every line so operators can correlate retries with the originating
// workflow/node/tool. Pass "" if you do not care.
func DoWithLogger[T any](ctx context.Context, p *Policy, logger *log.Logger, label string, op Op[T]) Result[T] {
	return doWithClockAndLogger(ctx, p, systemClock{}, logger, label, op)
}

// Do executes op once if policy is nil or MaxAttempts<=1; otherwise retries
// per policy. See Result.Decision for outcome categories. For logging, use
// DoWithLogger.
func Do[T any](ctx context.Context, p *Policy, op Op[T]) Result[T] {
	return doWithClockAndLogger(ctx, p, systemClock{}, nil, "", op)
}

func doWithClockAndLogger[T any](ctx context.Context, p *Policy, clk clock, logger *log.Logger, label string, op Op[T]) Result[T] {
	start := clk.Now()
	if p == nil || p.MaxAttempts <= 1 {
		v, err := op(ctx, 1)
		out := Result[T]{Value: v, Err: err, Attempts: 1, Elapsed: clk.Now().Sub(start)}
		if err == nil {
			out.Decision = DecisionSucceeded
		} else {
			out.LastClass = Classify(err)
			out.Decision = DecisionNotRetryable
		}
		return out
	}
	rng := rand.New(rand.NewSource(clk.Now().UnixNano()))
	var lastV T
	var lastErr error
	var lastClass Class

	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		attemptCtx := ctx
		var cancel context.CancelFunc
		if p.PerAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, p.PerAttemptTimeout)
		}
		v, err := op(attemptCtx, attempt)
		if cancel != nil {
			cancel()
		}
		lastV, lastErr = v, err
		if err == nil {
			if logger != nil && attempt > 1 {
				logger.Printf("retry: succeeded %s attempts=%d/%d elapsedMs=%d", label, attempt, p.MaxAttempts, clk.Now().Sub(start).Milliseconds())
			}
			return Result[T]{
				Value:    v,
				Attempts: attempt,
				Decision: DecisionSucceeded,
				Elapsed:  clk.Now().Sub(start),
			}
		}
		lastClass = Classify(err)
		if !p.InRetryOn(lastClass) {
			if logger != nil {
				logger.Printf("retry: not-retryable %s attempt=%d/%d class=%q err=%v", label, attempt, p.MaxAttempts, lastClass, err)
			}
			return Result[T]{
				Value:     lastV,
				Err:       lastErr,
				Attempts:  attempt,
				LastClass: lastClass,
				Decision:  DecisionNotRetryable,
				Elapsed:   clk.Now().Sub(start),
			}
		}
		if attempt == p.MaxAttempts {
			break
		}
		delay := nextDelay(p, attempt, rng)
		if p.TotalBudget > 0 && clk.Now().Add(delay).Sub(start) > p.TotalBudget {
			if logger != nil {
				logger.Printf("retry: budget-expired %s attempts=%d/%d class=%q budgetMs=%d elapsedMs=%d err=%v",
					label, attempt, p.MaxAttempts, lastClass, p.TotalBudget.Milliseconds(), clk.Now().Sub(start).Milliseconds(), lastErr)
			}
			return Result[T]{
				Value:     lastV,
				Err:       lastErr,
				Attempts:  attempt,
				LastClass: lastClass,
				Decision:  DecisionBudgetExpired,
				Elapsed:   clk.Now().Sub(start),
			}
		}
		if logger != nil {
			logger.Printf("retry: retrying %s attempt=%d/%d class=%q delayMs=%d err=%v", label, attempt, p.MaxAttempts, lastClass, delay.Milliseconds(), err)
		}
		timer := clk.NewTimer(delay)
		select {
		case <-timer.C():
		case <-ctx.Done():
			timer.Stop()
			if logger != nil {
				logger.Printf("retry: cancelled %s attempts=%d/%d class=%q err=%v", label, attempt, p.MaxAttempts, lastClass, ctx.Err())
			}
			return Result[T]{
				Value:     lastV,
				Err:       ctx.Err(),
				Attempts:  attempt,
				LastClass: lastClass,
				Decision:  DecisionCancelled,
				Elapsed:   clk.Now().Sub(start),
			}
		}
	}

	if logger != nil {
		logger.Printf("retry: exhausted %s attempts=%d/%d class=%q elapsedMs=%d err=%v",
			label, p.MaxAttempts, p.MaxAttempts, lastClass, clk.Now().Sub(start).Milliseconds(), lastErr)
	}
	return Result[T]{
		Value:     lastV,
		Err:       lastErr,
		Attempts:  p.MaxAttempts,
		LastClass: lastClass,
		Decision:  DecisionExhaustedRetries,
		Elapsed:   clk.Now().Sub(start),
	}
}

func nextDelay(p *Policy, attempt int, rng *rand.Rand) time.Duration {
	var d time.Duration
	switch p.Backoff {
	case BackoffFixed:
		d = p.InitialDelay
	default: // exponential
		d = p.InitialDelay * (1 << (attempt - 1))
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	if p.Jitter && d > 0 && rng != nil {
		d = time.Duration(float64(d) * (0.5 + rng.Float64()*0.5))
	}
	return d
}

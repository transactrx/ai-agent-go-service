// Package retry provides a generic, bounded retry helper used by the
// agent loop to wrap fallible operations (tool.Invoke, LLM stream).
//
// The helper is driven by a Policy parsed from per-node JSON. It is
// off by default — callers pass *Policy nil for single-attempt behavior.
package retry

import "time"

// Backoff selects between fixed and exponential delay growth.
type Backoff string

const (
	BackoffFixed       Backoff = "fixed"
	BackoffExponential Backoff = "exponential"
)

// Class is the bucket the classifier assigns to an error.
// retryOn matches against these class values.
type Class string

const (
	ClassTransient        Class = "transient"
	ClassTimeout          Class = "timeout"
	ClassUpstream         Class = "upstream"
	ClassValidation       Class = "validation"
	ClassPermission       Class = "permission"
	ClassStreamEmitted    Class = "stream-already-emitted"
)

// AllClasses is the set retryOn=["all"] resolves to. Includes every class
// literally, per resolved decision Q3 in the spec.
var AllClasses = []Class{
	ClassTransient,
	ClassTimeout,
	ClassUpstream,
	ClassValidation,
	ClassPermission,
}

// Policy is the parsed per-node retry config.
type Policy struct {
	MaxAttempts       int
	Backoff           Backoff
	InitialDelay      time.Duration
	MaxDelay          time.Duration
	Jitter            bool
	RetryOn           []Class
	PerAttemptTimeout time.Duration
	TotalBudget       time.Duration
}

// Decision categorizes how Do returned.
type Decision int

const (
	DecisionSucceeded Decision = iota
	DecisionExhaustedRetries
	DecisionNotRetryable
	DecisionBudgetExpired
	DecisionCancelled
)

// Result captures the outcome of Do and is the only return shape callers
// see. Value carries op's last return when present.
type Result[T any] struct {
	Value     T
	Err       error
	Attempts  int
	LastClass Class
	Decision  Decision
	Elapsed   time.Duration
}

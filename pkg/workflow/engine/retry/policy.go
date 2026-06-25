package retry

import (
	"encoding/json"
	"fmt"
	"time"
)

// rawPolicy mirrors the JSON shape from the workflow file.
type rawPolicy struct {
	MaxAttempts              *int     `json:"maxAttempts,omitempty"`
	Backoff                  string   `json:"backoff,omitempty"`
	InitialDelayMs           *int     `json:"initialDelayMs,omitempty"`
	MaxDelayMs               *int     `json:"maxDelayMs,omitempty"`
	Jitter                   *bool    `json:"jitter,omitempty"`
	RetryOn                  []string `json:"retryOn,omitempty"`
	PerAttemptTimeoutSeconds *int     `json:"perAttemptTimeoutSeconds,omitempty"`
	TotalBudgetSeconds       *int     `json:"totalBudgetSeconds,omitempty"`
}

// ParsePolicy decodes a retry JSON block into a Policy and applies defaults.
// Returns (nil, nil) when raw is nil or empty — meaning "no retry block,
// run once."
func ParsePolicy(raw json.RawMessage) (*Policy, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var r rawPolicy
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("retry: parse: %w", err)
	}
	p := &Policy{
		MaxAttempts:  3,
		Backoff:      BackoffExponential,
		InitialDelay: 500 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		Jitter:       true,
		RetryOn:      []Class{ClassTransient, ClassTimeout},
	}
	if r.MaxAttempts != nil {
		if *r.MaxAttempts < 1 || *r.MaxAttempts > 10 {
			return nil, fmt.Errorf("retry: maxAttempts %d out of range [1,10]", *r.MaxAttempts)
		}
		p.MaxAttempts = *r.MaxAttempts
	}
	if r.Backoff != "" {
		switch Backoff(r.Backoff) {
		case BackoffFixed, BackoffExponential:
			p.Backoff = Backoff(r.Backoff)
		default:
			return nil, fmt.Errorf("retry: backoff %q must be fixed|exponential", r.Backoff)
		}
	}
	if r.InitialDelayMs != nil {
		if *r.InitialDelayMs < 0 {
			return nil, fmt.Errorf("retry: initialDelayMs %d must be >= 0", *r.InitialDelayMs)
		}
		p.InitialDelay = time.Duration(*r.InitialDelayMs) * time.Millisecond
	}
	if r.MaxDelayMs != nil {
		if *r.MaxDelayMs < 0 {
			return nil, fmt.Errorf("retry: maxDelayMs %d must be >= 0", *r.MaxDelayMs)
		}
		p.MaxDelay = time.Duration(*r.MaxDelayMs) * time.Millisecond
	}
	if r.Jitter != nil {
		p.Jitter = *r.Jitter
	}
	if r.RetryOn != nil {
		classes, err := parseClasses(r.RetryOn)
		if err != nil {
			return nil, err
		}
		p.RetryOn = classes
	}
	if r.PerAttemptTimeoutSeconds != nil {
		if *r.PerAttemptTimeoutSeconds < 0 {
			return nil, fmt.Errorf("retry: perAttemptTimeoutSeconds %d must be >= 0", *r.PerAttemptTimeoutSeconds)
		}
		p.PerAttemptTimeout = time.Duration(*r.PerAttemptTimeoutSeconds) * time.Second
	}
	if r.TotalBudgetSeconds != nil {
		if *r.TotalBudgetSeconds < 0 {
			return nil, fmt.Errorf("retry: totalBudgetSeconds %d must be >= 0", *r.TotalBudgetSeconds)
		}
		p.TotalBudget = time.Duration(*r.TotalBudgetSeconds) * time.Second
	}
	return p, nil
}

func parseClasses(in []string) ([]Class, error) {
	for _, s := range in {
		if s == "all" {
			out := make([]Class, len(AllClasses))
			copy(out, AllClasses)
			return out, nil
		}
	}
	out := make([]Class, 0, len(in))
	for _, s := range in {
		c := Class(s)
		switch c {
		case ClassTransient, ClassTimeout, ClassUpstream, ClassValidation, ClassPermission, ClassStreamEmitted:
			out = append(out, c)
		default:
			return nil, fmt.Errorf("retry: unknown retryOn class %q", s)
		}
	}
	return out, nil
}

// InRetryOn reports whether class is configured to retry.
func (p *Policy) InRetryOn(c Class) bool {
	if p == nil {
		return false
	}
	for _, x := range p.RetryOn {
		if x == c {
			return true
		}
	}
	return false
}

// IsRetryEnabled satisfies node.RetryPolicy. A non-nil *Policy with
// MaxAttempts>1 enables retry.
func (p *Policy) IsRetryEnabled() bool {
	return p != nil && p.MaxAttempts > 1
}

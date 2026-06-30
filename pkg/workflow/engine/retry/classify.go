package retry

import (
	"context"
	"errors"
	"regexp"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// httpTransientPattern matches error messages that strongly indicate a
// retryable transport / upstream condition. Tools should prefer
// node.ToolError for precision; this is a fallback for plain errors.
var httpTransientPattern = regexp.MustCompile(`\b(408|429|5\d{2})\b|connection reset|unexpected EOF`)

// Classify maps an error into a retry Class.
//
// Precedence:
//  1. *node.ToolError with Retryable:true → e.Code (mapped to Class).
//  2. *node.ToolError with Retryable:false → ClassValidation (asserted non-retryable).
//  3. context.DeadlineExceeded → ClassTimeout.
//  4. context.Canceled → ClassValidation (caller asked to stop; do not retry).
//  5. *node.PolicyDeniedError → ClassPermission.
//  6. HTTP status / network patterns → ClassTransient.
//  7. Otherwise → ClassValidation.
func Classify(err error) Class {
	if err == nil {
		return ""
	}
	var te *node.ToolError
	if errors.As(err, &te) {
		if te.Retryable {
			c := Class(te.Code)
			switch c {
			case ClassTransient, ClassTimeout, ClassUpstream, ClassValidation, ClassPermission, ClassStreamEmitted:
				return c
			default:
				return ClassUpstream
			}
		}
		return ClassValidation
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ClassTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ClassValidation
	}
	var pde *node.PolicyDeniedError
	if errors.As(err, &pde) {
		return ClassPermission
	}
	if httpTransientPattern.MatchString(err.Error()) {
		return ClassTransient
	}
	return ClassValidation
}

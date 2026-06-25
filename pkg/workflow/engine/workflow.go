package engine

import "github.com/transactrx/ai-agent-go-service/pkg/workflow/engine/executor"

// Workflow is one loaded workflow ready to receive trigger events.
// It is an alias for executor.Workflow so the engine and executor packages
// share the same type without a circular import.
type Workflow = executor.Workflow

// ConnKey identifies a connection's destination.
type ConnKey = executor.ConnKey

// ConnRef identifies a connection's source.
type ConnRef = executor.ConnRef

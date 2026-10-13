package node

import "context"

// IndexAccessStatus is the outcome of resolving which identity function
// guards a workflow's index.
type IndexAccessStatus string

const (
	// IndexAccessNone: the workflow has no index function to apply; callers keep
	// the service-wide function (APP_FUNCTION_ID). The zero value means the same.
	IndexAccessNone IndexAccessStatus = "none"
	// IndexAccessBound: exactly one function guards the workflow's index.
	IndexAccessBound IndexAccessStatus = "bound"
	// IndexAccessUnbound: the index has no usable function. Callers fail closed.
	IndexAccessUnbound IndexAccessStatus = "unbound"
)

// IndexAccess is resolved once per workflow when it loads.
type IndexAccess struct {
	Status     IndexAccessStatus
	Index      string // index pattern(s), comma-joined when a workflow has several
	FunctionID string // set when Status is IndexAccessBound
	Cause      string // set when Status is IndexAccessUnbound
}

// Kind returns the status, reading the zero value as IndexAccessNone.
func (a IndexAccess) Kind() IndexAccessStatus {
	if a.Status == "" {
		return IndexAccessNone
	}
	return a.Status
}

// IndexScoped is implemented by a node that reads one index pattern
// (tool/opensearch). The engine discovers it by interface assertion.
type IndexScoped interface {
	AllowedIndexPattern() string
}

// IndexFunctionProvider is implemented by a policy node that can say which
// identity function guards an index pattern. Any error means "no usable
// function". A policy node that does not implement it keeps today's behavior.
type IndexFunctionProvider interface {
	FunctionForIndex(ctx context.Context, indexPattern string) (functionID string, err error)
}

// IndexAccessAware is implemented by a trigger that needs the resolved access
// before it starts listening. The engine calls SetIndexAccess once, before
// Subscribe.
type IndexAccessAware interface {
	SetIndexAccess(IndexAccess)
}

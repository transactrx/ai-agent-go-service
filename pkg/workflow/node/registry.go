package node

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Factory creates a Node instance from a config block.
type Factory interface {
	New(rawConfig json.RawMessage) (Node, error)
}

// FactoryFunc adapts a function value to Factory.
type FactoryFunc func(rawConfig json.RawMessage) (Node, error)

// New satisfies Factory.
func (f FactoryFunc) New(c json.RawMessage) (Node, error) { return f(c) }

// Registry maps "type" strings to factories. Workflow loader queries it.
// Concurrent-safe via RWMutex.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{factories: map[string]Factory{}}
}

// Register associates typeKey with f. Returns an error if typeKey is already
// registered.
func (r *Registry) Register(typeKey string, f Factory) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.factories[typeKey]; dup {
		return fmt.Errorf("node type %q already registered", typeKey)
	}
	r.factories[typeKey] = f
	return nil
}

// Get returns the factory for typeKey if one is registered.
func (r *Registry) Get(typeKey string) (Factory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[typeKey]
	return f, ok
}

// Types returns the list of registered type keys.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.factories))
	for k := range r.factories {
		out = append(out, k)
	}
	return out
}

// Replace sets typeKey to f, overwriting any existing factory. Unlike Register it
// never errors on a duplicate — used by the agent facade so consumer-supplied factories
// can override library defaults.
func (r *Registry) Replace(typeKey string, f Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[typeKey] = f
}

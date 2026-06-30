// Package safego runs functions with panic recovery so a node panic cannot
// crash the process. Used to wrap every long-running goroutine that hosts
// node code.
package safego

import (
	"fmt"
	"runtime/debug"
)

// Run executes fn and recovers from panics, converting them to errors that
// embed the panic value plus a stack trace.
func Run(name string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in %s: %v\n%s", name, r, debug.Stack())
		}
	}()
	return fn()
}

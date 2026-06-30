// Package identity carries layered request identity through context.Context so
// any node downstream of the trigger can read it without a parameter shim.
//
// Two layers (see spec §13.2):
//  1. NATS user (connection-level, _User_Id header from nats-service)
//  2. App user + Account (business-level, X-User-Id / X-Account-Id headers)
package identity

import "context"

// Identity carries both layers. Body never carries identity values.
type Identity struct {
	NatsUserID string // _User_Id header — service identity (transport layer)
	UserID     string // X-User-Id header — human user (load-bearing)
	AccountID  string // X-Account-Id header — security scope (load-bearing)
	UserName   string // X-User-Name header — human display name (optional)
	TimeZone   string // X-Time-Zone header — IANA timezone of the asker (optional)
}

type ctxKey struct{}

// WithIdentity returns a derived context carrying id.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// FromContext returns the identity stored in ctx, if any.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

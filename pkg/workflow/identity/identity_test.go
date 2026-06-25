package identity_test

import (
	"context"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
)

func TestRoundTrip(t *testing.T) {
	id := identity.Identity{NatsUserID: "svc-a", UserID: "user@x", AccountID: "AM-1"}
	ctx := identity.WithIdentity(context.Background(), id)
	got, ok := identity.FromContext(ctx)
	if !ok {
		t.Fatal("identity not present")
	}
	if got != id {
		t.Fatalf("got %#v want %#v", got, id)
	}
}

func TestMissing(t *testing.T) {
	if _, ok := identity.FromContext(context.Background()); ok {
		t.Fatal("FromContext on bare ctx should report missing")
	}
}

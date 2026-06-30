package node

import (
	"context"
	"testing"
)

// compile-time + behavioral check that the interface exists with the agreed
// signature and that RenderCtx carries the new fields.
type fakeMP struct{ s string }

func (f fakeMP) IndexMapping(_ context.Context) (string, error) { return f.s, nil }

func TestMappingProviderAndRenderCtxFields(t *testing.T) {
	var mp MappingProvider = fakeMP{s: "ok"}
	got, err := mp.IndexMapping(context.Background())
	if err != nil || got != "ok" {
		t.Fatalf("got %q err %v", got, err)
	}
	rc := RenderCtx{UserName: "Ada", TimeZone: "America/New_York", IndexMapping: "{}"}
	if rc.UserName != "Ada" || rc.TimeZone != "America/New_York" || rc.IndexMapping != "{}" {
		t.Fatalf("RenderCtx fields not wired: %+v", rc)
	}
}

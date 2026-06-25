package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
)

func TestFilesystemSourceListAndLoad(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.json"), []byte(`{"id":"a"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.json"), []byte(`{"id":"b"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	src := engine.NewFilesystemSource(dir)
	ids, err := src.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("got %v", ids)
	}
	body, err := src.Load(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"id":"a"}` {
		t.Fatalf("body: %s", body)
	}
	if _, ok := src.Watch(context.Background()); ok {
		t.Fatal("Watch should report unsupported")
	}
}

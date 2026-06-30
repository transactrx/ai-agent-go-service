// pkg/aichatviewer/storage_local_janitor_test.go
package webbridge

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocalJanitor_RemovesOldFilesKeepsFresh(t *testing.T) {
	dir := t.TempDir()

	old := filepath.Join(dir, "acct1", "u-old", "old.png")
	fresh := filepath.Join(dir, "acct1", "u-fresh", "fresh.png")
	for _, p := range []string{old, fresh} {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-30 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}

	j := NewLocalJanitor(dir, 24*time.Hour, time.Hour, log.New(io.Discard, "", 0))
	j.Sweep()

	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Errorf("old file should have been removed: %v", err)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh file should survive: %v", err)
	}
}

func TestLocalJanitor_PrunesEmptyDirs(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "acct1", "u-old", "old.png")
	if err := os.MkdirAll(filepath.Dir(old), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(old, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-30 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}

	j := NewLocalJanitor(dir, 24*time.Hour, time.Hour, log.New(io.Discard, "", 0))
	j.Sweep()

	if _, err := os.Stat(filepath.Join(dir, "acct1", "u-old")); !os.IsNotExist(err) {
		t.Errorf("upload dir should be pruned after file removal: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "acct1")); !os.IsNotExist(err) {
		t.Errorf("account dir should be pruned when empty: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("root dir must survive: %v", err)
	}
}

func TestLocalJanitor_DefaultTTL(t *testing.T) {
	j := NewLocalJanitor(t.TempDir(), 0, 0, log.New(io.Discard, "", 0))
	if j.ttl != DefaultLocalTTL {
		t.Errorf("ttl=%s, want %s", j.ttl, DefaultLocalTTL)
	}
	if j.interval != time.Hour {
		t.Errorf("interval=%s, want 1h", j.interval)
	}
}

func TestLocalJanitor_StartStopsOnContextCancel(t *testing.T) {
	dir := t.TempDir()
	j := NewLocalJanitor(dir, time.Minute, 50*time.Millisecond, log.New(io.Discard, "", 0))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		j.Start(ctx)
		close(done)
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("janitor did not stop after cancel")
	}
}

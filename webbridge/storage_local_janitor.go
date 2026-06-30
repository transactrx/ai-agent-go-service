// pkg/aichatviewer/storage_local_janitor.go
package webbridge

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"
)

// DefaultLocalTTL is how long a locally-stored upload survives before the
// janitor removes it. The chat backend uploads the bytes to S3 the moment the
// agent processes the message, so the local file only needs to live long
// enough for in-flight WS reconnects / retries on the same browser session.
const DefaultLocalTTL = 24 * time.Hour

// LocalJanitor periodically deletes files under LocalStorage.root that are
// older than TTL. It walks the tree once per Interval and removes individual
// files, then prunes any directory that ends up empty. Errors are logged and
// the sweep continues; one bad file does not stall the rest of the job.
type LocalJanitor struct {
	root     string
	ttl      time.Duration
	interval time.Duration
	logger   *log.Logger
	now      func() time.Time // injectable for tests; defaults to time.Now
}

// NewLocalJanitor returns a janitor for the given storage root. ttl == 0 falls
// back to DefaultLocalTTL. interval defaults to one hour.
func NewLocalJanitor(root string, ttl, interval time.Duration, logger *log.Logger) *LocalJanitor {
	if ttl <= 0 {
		ttl = DefaultLocalTTL
	}
	if interval <= 0 {
		interval = time.Hour
	}
	if logger == nil {
		logger = log.Default()
	}
	return &LocalJanitor{root: root, ttl: ttl, interval: interval, logger: logger, now: time.Now}
}

// Start runs Sweep once immediately, then on a timer until ctx is cancelled.
// Intended to be called as `go janitor.Start(ctx)` from boot code.
func (j *LocalJanitor) Start(ctx context.Context) {
	j.logger.Printf("aichatviewer: local janitor started (root=%s ttl=%s interval=%s)", j.root, j.ttl, j.interval)
	j.Sweep()
	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			j.logger.Print("aichatviewer: local janitor stopped")
			return
		case <-t.C:
			j.Sweep()
		}
	}
}

// Sweep walks the storage tree once and removes files older than TTL plus any
// empty directories left behind. Exported so callers can trigger an immediate
// cleanup (tests, manual ops) without waiting for the next tick.
func (j *LocalJanitor) Sweep() {
	cutoff := j.now().Add(-j.ttl)
	removedFiles := 0
	removedDirs := 0

	_ = filepath.WalkDir(j.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			j.logger.Printf("aichatviewer: janitor walk %s: %v", path, err)
			return nil
		}
		if d.IsDir() || path == j.root {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			j.logger.Printf("aichatviewer: janitor stat %s: %v", path, err)
			return nil
		}
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(path); err != nil {
				j.logger.Printf("aichatviewer: janitor remove %s: %v", path, err)
				return nil
			}
			removedFiles++
		}
		return nil
	})

	// Second pass: collect every dir under root, then prune empties from the
	// deepest path up. filepath.WalkDir is top-down; we need bottom-up so a
	// parent becomes removable once its child is gone.
	var dirs []string
	_ = filepath.WalkDir(j.root, func(path string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || path == j.root {
			return nil
		}
		dirs = append(dirs, path)
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		entries, err := os.ReadDir(dirs[i])
		if err != nil || len(entries) > 0 {
			continue
		}
		if err := os.Remove(dirs[i]); err == nil {
			removedDirs++
		}
	}

	if removedFiles > 0 || removedDirs > 0 {
		j.logger.Printf("aichatviewer: janitor swept %d file(s), %d dir(s) older than %s", removedFiles, removedDirs, j.ttl)
	}
}

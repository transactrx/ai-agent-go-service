package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WorkflowSource abstracts where workflow JSON lives. Cycle 1 = filesystem.
// Future cycles drop in DB/S3 backed sources without changing the loader.
type WorkflowSource interface {
	List(ctx context.Context) ([]string, error)
	Load(ctx context.Context, id string) ([]byte, error)
	Watch(ctx context.Context) (<-chan string, bool)
}

// FilesystemSource is a directory of *.json workflow files. Filename minus
// the .json extension is the workflow id used by List/Load.
type FilesystemSource struct {
	Dir string
}

// NewFilesystemSource returns a source rooted at dir.
func NewFilesystemSource(dir string) *FilesystemSource { return &FilesystemSource{Dir: dir} }

// List enumerates *.json files under Dir.
func (s *FilesystemSource) List(_ context.Context) ([]string, error) {
	var ids []string
	err := filepath.WalkDir(s.Dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		id := strings.TrimSuffix(d.Name(), ".json")
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("filesystem source: walk %s: %w", s.Dir, err)
	}
	return ids, nil
}

// Load reads the JSON bytes for id.
func (s *FilesystemSource) Load(_ context.Context, id string) ([]byte, error) {
	path := filepath.Join(s.Dir, id+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("filesystem source: read %s: %w", path, err)
	}
	return b, nil
}

// Watch returns nil, false — cycle 1 has no hot-reload.
func (s *FilesystemSource) Watch(_ context.Context) (<-chan string, bool) { return nil, false }

package webbridge_test

import (
	"io/fs"
	"testing"

	"github.com/transactrx/ai-agent-go-service/webbridge"
)

func TestDefaultWebixUI_NotNil(t *testing.T) {
	if webbridge.DefaultWebixUI == nil {
		t.Fatal("DefaultWebixUI is nil")
	}
}

func TestDefaultWebixUI_ContainsKnownFile(t *testing.T) {
	const known = "views/aichatWebixWnd.js"
	_, err := fs.Stat(webbridge.DefaultWebixUI, known)
	if err != nil {
		t.Fatalf("fs.Stat(%q): %v", known, err)
	}
}

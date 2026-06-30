package agent

import (
	"log"
	"os"
	"testing"
)

func TestResolveFilesBucket_ExplicitWins(t *testing.T) {
	t.Setenv("S3_FILES_BUCKET", "explicit-bucket")
	t.Setenv("APP_NAME", "ignored")
	t.Setenv("ENVIRONMENT", "ignored")
	if got := resolveFilesBucket(log.New(os.Stderr, "", 0)); got != "explicit-bucket" {
		t.Errorf("got %q, want explicit-bucket", got)
	}
}

func TestResolveFilesBucket_DefaultFromAppEnv(t *testing.T) {
	t.Setenv("S3_FILES_BUCKET", "")
	t.Setenv("APP_NAME", "MyApp")
	t.Setenv("ENVIRONMENT", "Prod")
	if got := resolveFilesBucket(log.New(os.Stderr, "", 0)); got != "myapp-assistant-files-prod" {
		t.Errorf("got %q, want myapp-assistant-files-prod", got)
	}
}

func TestResolveFilesBucket_EmptyWhenUnset(t *testing.T) {
	t.Setenv("S3_FILES_BUCKET", "")
	t.Setenv("APP_NAME", "")
	t.Setenv("ENVIRONMENT", "")
	if got := resolveFilesBucket(log.New(os.Stderr, "", 0)); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

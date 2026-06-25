package secret_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/secret"
)

func TestStringRedactsViaStringer(t *testing.T) {
	s := secret.New("super-secret-value")
	if got := fmt.Sprint(s); got != "<redacted>" {
		t.Fatalf("Sprint = %q, want <redacted>", got)
	}
}

func TestStringRedactsViaPrintfV(t *testing.T) {
	s := secret.New("abc")
	if got := fmt.Sprintf("%v", s); got != "<redacted>" {
		t.Fatalf("%%v = %q", got)
	}
	if got := fmt.Sprintf("%s", s); got != "<redacted>" {
		t.Fatalf("%%s = %q", got)
	}
}

func TestStringRedactsViaJSON(t *testing.T) {
	s := secret.New("hunter2")
	b, err := json.Marshal(struct {
		Pwd secret.String `json:"pwd"`
	}{Pwd: s})
	if err != nil {
		t.Fatal(err)
	}
	jsonStr := string(b)
	if !strings.Contains(jsonStr, `"pwd":`) || !strings.Contains(jsonStr, `redacted`) {
		t.Fatalf("json = %s", jsonStr)
	}
	if strings.Contains(jsonStr, "hunter2") {
		t.Fatalf("raw value leaked: %s", jsonStr)
	}
}

func TestStringRedactsViaText(t *testing.T) {
	s := secret.New("hunter2")
	b, err := s.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "<redacted>" {
		t.Fatalf("MarshalText = %q", b)
	}
}

func TestRevealReturnsRaw(t *testing.T) {
	s := secret.New("plain")
	if s.Reveal() != "plain" {
		t.Fatalf("Reveal lost value")
	}
}

func TestIsSet(t *testing.T) {
	var zero secret.String
	if zero.IsSet() {
		t.Fatal("zero value should not be set")
	}
	if !secret.New("x").IsSet() {
		t.Fatal("New value should be set")
	}
}

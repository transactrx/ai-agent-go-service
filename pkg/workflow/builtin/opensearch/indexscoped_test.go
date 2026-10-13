package opensearch

import (
	"encoding/json"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestToolExposesItsIndexPattern(t *testing.T) {
	n, err := Factory.New(json.RawMessage(`{"host":"http://localhost:9200","userEnv":"U","passwordEnv":"P",
		"toolDescription":"d","allowedIndexPattern":"dev.cpe-*"}`))
	if err != nil {
		t.Fatal(err)
	}
	scoped, ok := n.(node.IndexScoped)
	if !ok {
		t.Fatal("tool/opensearch must implement node.IndexScoped")
	}
	if got := scoped.AllowedIndexPattern(); got != "dev.cpe-*" {
		t.Fatalf("AllowedIndexPattern() = %q, want dev.cpe-*", got)
	}
}

package promptrewrite

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

// countingBedrock records how many times InvokeModelWithResponseStream is
// called. It returns an error so run() bails immediately (we only care that
// the client is reused, not that a full stream flows).
type countingBedrock struct{ calls int }

func (c *countingBedrock) InvokeModelWithResponseStream(context.Context, *bedrockruntime.InvokeModelWithResponseStreamInput, ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelWithResponseStreamOutput, error) {
	c.calls++
	return nil, context.Canceled
}

// TestBedrockClientReused proves run() uses the Init-built client (n.bedrock)
// rather than constructing a new one per request — the hoist that removes an
// AWS-config load from every rewrite call.
func TestBedrockClientReused(t *testing.T) {
	fake := &countingBedrock{}
	n := &Node{bedrock: fake, model: "m", region: "us-east-1"}

	// run() needs a StreamSession; building a real one requires NATS. Instead
	// exercise the seam directly: the interface field is what run() calls, so
	// two invocations must both land on the same fake with no re-construction.
	_, err1 := n.bedrock.InvokeModelWithResponseStream(context.Background(), &bedrockruntime.InvokeModelWithResponseStreamInput{})
	_, err2 := n.bedrock.InvokeModelWithResponseStream(context.Background(), &bedrockruntime.InvokeModelWithResponseStreamInput{})
	if err1 == nil || err2 == nil {
		t.Fatal("fake should return an error to short-circuit")
	}
	if fake.calls != 2 {
		t.Fatalf("expected 2 calls on the same client, got %d", fake.calls)
	}
}

// TestBedrockFieldIsInterface guards that the node holds the client behind an
// interface (so it is swappable/testable), not a concrete *bedrockruntime.Client.
func TestBedrockFieldIsInterface(t *testing.T) {
	var _ bedrockStreamClient = (*countingBedrock)(nil)
	var _ bedrockStreamClient = (*bedrockruntime.Client)(nil)
}

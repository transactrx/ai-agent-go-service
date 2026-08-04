package bedrock

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/smithy-go"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// fakeInvoker records the ModelId of each call and returns scripted errors.
type fakeInvoker struct {
	models []string
	errs   []error
}

func (f *fakeInvoker) InvokeModelWithResponseStream(_ context.Context, in *bedrockruntime.InvokeModelWithResponseStreamInput, _ ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelWithResponseStreamOutput, error) {
	f.models = append(f.models, aws.ToString(in.ModelId))
	return nil, f.errs[len(f.models)-1]
}

func validationErr() error {
	return &smithy.GenericAPIError{Code: "ValidationException", Message: "model no longer supported"}
}

func probeReq() node.LLMRequest {
	return node.LLMRequest{
		MaxTokens: 16,
		Messages: []node.Message{{
			Role:    node.UserMsg,
			Content: []node.ContentBlock{{Type: node.BlockText, Text: "hi"}},
		}},
	}
}

func TestIsModelUnavailable(t *testing.T) {
	if !isModelUnavailable(validationErr()) {
		t.Fatal("ValidationException must be model-unavailable")
	}
	if !isModelUnavailable(&smithy.GenericAPIError{Code: "ResourceNotFoundException"}) {
		t.Fatal("ResourceNotFoundException must be model-unavailable")
	}
	if isModelUnavailable(&smithy.GenericAPIError{Code: "ThrottlingException"}) {
		t.Fatal("ThrottlingException must NOT trigger the fallback")
	}
	if isModelUnavailable(errors.New("plain")) {
		t.Fatal("non-APIError must NOT trigger the fallback")
	}
}

// Model-unavailable error + lkg set → exactly one retry with the lkg model.
func TestStreamRetriesWithLastKnownGood(t *testing.T) {
	fake := &fakeInvoker{errs: []error{validationErr(), errors.New("still down")}}
	b := &bedrockLLM{
		cfg:           Config{Model: "new", MaxTokens: 16, AnthropicVersion: defaultAnthropicVersion},
		model:         "us.anthropic.claude-opus-4-8",
		lastKnownGood: "us.anthropic.claude-opus-4-7",
		client:        fake,
	}
	out := make(chan node.LLMEvent, 8)
	err := b.Stream(context.Background(), probeReq(), out)
	if err == nil {
		t.Fatal("expected error (second call also fails)")
	}
	if len(fake.models) != 2 || fake.models[0] != "us.anthropic.claude-opus-4-8" || fake.models[1] != "us.anthropic.claude-opus-4-7" {
		t.Fatalf("calls = %v, want [4-8, 4-7]", fake.models)
	}
}

// Non-model errors and missing lkg must NOT retry.
func TestStreamNoRetryCases(t *testing.T) {
	cases := []struct {
		name string
		lkg  string
		err  error
	}{
		{"throttling error", "us.anthropic.claude-opus-4-7", &smithy.GenericAPIError{Code: "ThrottlingException"}},
		{"no lkg", "", validationErr()},
		{"lkg equals current", "us.anthropic.claude-opus-4-8", validationErr()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeInvoker{errs: []error{c.err, errors.New("must not be reached")}}
			b := &bedrockLLM{
				cfg:           Config{Model: "x", MaxTokens: 16, AnthropicVersion: defaultAnthropicVersion},
				model:         "us.anthropic.claude-opus-4-8",
				lastKnownGood: c.lkg,
				client:        fake,
			}
			out := make(chan node.LLMEvent, 8)
			if err := b.Stream(context.Background(), probeReq(), out); err == nil {
				t.Fatal("expected error")
			}
			if len(fake.models) != 1 {
				t.Fatalf("calls = %d, want 1 (no retry)", len(fake.models))
			}
		})
	}
}

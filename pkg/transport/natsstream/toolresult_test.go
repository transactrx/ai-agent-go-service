package natsstream_test

import (
	"context"
	"testing"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
)

func TestWaitForToolResultReturnsPublishedPayload(t *testing.T) {
	_, nc := runEmbedded(t)
	logger := newSilentLogger()

	streamID := "stream-1"
	toolCallID := "tc-1"
	basePath := "test.base"

	host := natsstream.NewToolResultHost(nc, basePath, streamID, logger)
	subj := host.SubjectFor(toolCallID)

	type result struct {
		out []byte
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		out, err := host.WaitForToolResult(context.Background(), toolCallID, 2*time.Second)
		resultCh <- result{out, err}
	}()

	// Give the subscription a moment to land before publishing.
	time.Sleep(50 * time.Millisecond)
	if err := nc.Publish(subj, []byte(`{"approved":true}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("WaitForToolResult: %v", r.err)
		}
		if string(r.out) != `{"approved":true}` {
			t.Fatalf("payload mismatch: %s", r.out)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestWaitForToolResultTimesOut(t *testing.T) {
	_, nc := runEmbedded(t)
	logger := newSilentLogger()

	host := natsstream.NewToolResultHost(nc, "test.base", "stream-2", logger)
	_, err := host.WaitForToolResult(context.Background(), "tc-2", 200*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestToolResultSubjectIsStableForStream(t *testing.T) {
	host := natsstream.NewToolResultHost(nil, "test.base", "stream-3", nil)
	got := host.SubjectFor("tc-x")
	want := "test.base_.tool_result.stream-3.tc-x"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

package agent

import (
	"strings"
	"testing"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func TestAttachmentBlockImage(t *testing.T) {
	a := node.Attachment{
		MediaType: "image/png",
		Filename:  "claim.png",
		Data:      []byte{0x89, 0x50, 0x4E, 0x47},
	}
	blk, ok := attachmentBlock(a)
	if !ok {
		t.Fatal("expected ok=true for image/png")
	}
	if blk.Type != node.BlockImage {
		t.Errorf("type = %v, want BlockImage", blk.Type)
	}
	if blk.MediaType != "image/png" || blk.Filename != "claim.png" {
		t.Errorf("blk = %+v", blk)
	}
}

func TestAttachmentBlockPdf(t *testing.T) {
	a := node.Attachment{
		MediaType: "application/pdf",
		Filename:  "report.pdf",
		Data:      []byte("%PDF-1.4"),
	}
	blk, ok := attachmentBlock(a)
	if !ok || blk.Type != node.BlockDocument {
		t.Errorf("got (%+v, %v), want BlockDocument", blk, ok)
	}
}

func TestAttachmentBlockUnsupportedDropped(t *testing.T) {
	for _, mt := range []string{"text/csv", "text/plain", "application/json", "", "video/mp4"} {
		_, ok := attachmentBlock(node.Attachment{MediaType: mt, Data: []byte("x")})
		if ok {
			t.Errorf("media type %q should be dropped", mt)
		}
	}
}

func TestAttachmentBlockEmptyDataDropped(t *testing.T) {
	_, ok := attachmentBlock(node.Attachment{MediaType: "image/png", Data: nil})
	if ok {
		t.Error("empty data should be dropped")
	}
}

// TestMessageForMemoryStripsBytes verifies image/document blocks are replaced
// by a short descriptor line in the persisted message so memory doesn't carry
// the raw bytes across turns.
func TestMessageForMemoryStripsBytes(t *testing.T) {
	in := node.Message{
		Role: node.UserMsg,
		Content: []node.ContentBlock{
			{Type: node.BlockImage, MediaType: "image/jpeg", Filename: "x.jpg", Data: make([]byte, 1000)},
			{Type: node.BlockText, Text: "describe please"},
		},
	}
	out := messageForMemory(in)
	if len(out.Content) != 2 {
		t.Fatalf("len(content) = %d, want 2", len(out.Content))
	}
	if out.Content[0].Type != node.BlockText {
		t.Fatalf("content[0].Type = %v, want BlockText", out.Content[0].Type)
	}
	if !strings.Contains(out.Content[0].Text, "x.jpg") || !strings.Contains(out.Content[0].Text, "image/jpeg") {
		t.Errorf("descriptor missing filename/mediaType: %q", out.Content[0].Text)
	}
	if out.Content[1].Text != "describe please" {
		t.Errorf("original text dropped")
	}
	// Confirm bytes are not retained anywhere.
	for _, b := range out.Content {
		if len(b.Data) > 0 {
			t.Errorf("memory message still carries %d bytes on block type %v", len(b.Data), b.Type)
		}
	}
}

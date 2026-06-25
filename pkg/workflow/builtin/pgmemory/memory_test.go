package pgmemory

import (
	"strings"
	"testing"
)

// TestLoadQueryOrdersUserBeforeAssistant pins the per-turn ordering. Both
// user/assistant rows of a turn share turn_index; the secondary sort must put
// 'user' BEFORE 'assistant'. With "role DESC" alphabetics do that ('u' > 'a').
// The previous "role ASC" inverted history and made the LLM read each turn
// assistant-then-user.
func TestLoadQueryOrdersUserBeforeAssistant(t *testing.T) {
	q := buildLoadQuery("chat_messages")
	if !strings.Contains(q, "ORDER BY turn_index ASC, role DESC") {
		t.Fatalf("buildLoadQuery missing 'ORDER BY turn_index ASC, role DESC'\n--- got:\n%s", q)
	}
}

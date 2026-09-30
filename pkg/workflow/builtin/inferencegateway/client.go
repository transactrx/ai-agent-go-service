package inferencegateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	basePathEnv        = "INFERENCE_GATEWAY_BASE_PATH"
	defaultBasePath    = "example.inferenceGateway"
	invokeStreamSuffix = ".invokeStream"
	// requestTimeout bounds the ack round trip only; the stream has its own
	// timeouts (Config).
	requestTimeout = 10 * time.Second
)

// invokeSubject resolves <base>.invokeStream: node config override, else the
// env var, else the repo-convention placeholder (cf. bedrock.gatewaySubject).
func invokeSubject(override string) string {
	base := strings.TrimSpace(override)
	if base == "" {
		base = strings.TrimSpace(os.Getenv(basePathEnv))
	}
	if base == "" {
		base = defaultBasePath
	}
	return base + invokeStreamSuffix
}

// streamAck is the invokeStream reply (gateway models.InvokeStreamAck) plus
// the NatsServiceError field a failure carries. AppliedParams and
// ParamWarnings are present only when the alias carries a paramPolicy.
type streamAck struct {
	Accepted      bool           `json:"accepted"`
	ModelID       string         `json:"modelId"`
	InvokeID      string         `json:"invokeId"`
	ErrorMessage  string         `json:"errorMessage"`
	AppliedParams map[string]any `json:"appliedParams,omitempty"`
	ParamWarnings []string       `json:"paramWarnings,omitempty"`
}

// parseAck applies the nats-service STATUS rule: empty or strict numeric
// 200-299 is success; anything else is an error carrying the gateway's
// errorMessage (or the truncated raw body).
func parseAck(status string, body []byte) (streamAck, error) {
	var a streamAck
	jsonErr := json.Unmarshal(body, &a)
	if status != "" {
		if n, err := strconv.Atoi(status); err != nil || n < 200 || n > 299 {
			return streamAck{}, fmt.Errorf("gateway status %s: %s", status, errText(a.ErrorMessage, body))
		}
	}
	if jsonErr != nil {
		return streamAck{}, fmt.Errorf("gateway ack not JSON: %w", jsonErr)
	}
	if !a.Accepted {
		return streamAck{}, errors.New("gateway did not accept the stream request: " + errText(a.ErrorMessage, body))
	}
	return a, nil
}

func errText(msg string, body []byte) string {
	if msg != "" {
		return msg
	}
	const max = 200
	if len(body) > max {
		return string(body[:max]) + "..."
	}
	return string(body)
}

// policyLine renders the gateway-policy log line, or ok=false when the ack
// carried no policy result (nothing to report; older gateways never do).
// encoding/json sorts map keys, so applied= is stable for grep and tests.
func policyLine(wf, nodeID, alias, model string, ack streamAck) (string, bool) {
	if len(ack.AppliedParams) == 0 && len(ack.ParamWarnings) == 0 {
		return "", false
	}
	applied, err := json.Marshal(ack.AppliedParams)
	if err != nil {
		applied = []byte(`{}`)
	}
	return fmt.Sprintf("gateway-policy wf=%s node=%s alias=%s model=%s applied=%s warnings=%s",
		wf, nodeID, alias, model, applied, strings.Join(ack.ParamWarnings, "; ")), true
}

// streamUsage mirrors the gateway's models.Usage.
type streamUsage struct {
	InputTokens  int32 `json:"inputTokens"`
	OutputTokens int32 `json:"outputTokens"`
}

// streamEvent mirrors the gateway's models.StreamEvent published on the
// stream subject.
type streamEvent struct {
	Seq  int    `json:"seq"`
	Type string `json:"type"` // messageStart, delta, toolUseStart, contentBlockStop, messageStop, metadata, done, error

	ContentIndex int    `json:"contentIndex,omitempty"`
	Text         string `json:"text,omitempty"`

	ToolUseID      string `json:"toolUseId,omitempty"`
	ToolName       string `json:"toolName,omitempty"`
	ToolInputDelta string `json:"toolInputDelta,omitempty"`

	StopReason string       `json:"stopReason,omitempty"`
	Usage      *streamUsage `json:"usage,omitempty"`
	Error      string       `json:"error,omitempty"`
}

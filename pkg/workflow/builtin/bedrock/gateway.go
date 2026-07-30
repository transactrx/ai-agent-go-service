// Gateway model resolution: asks the org inferenceGateway NATS service
// (resolveModel endpoint) for the latest release of the node's model family.
// Primary source for the auto-update check in autoupdate.go; the Bedrock
// catalog scan there remains the fallback. Spec:
// docs/superpowers/specs/2026-07-30-bedrock-gateway-model-resolution-design.md
package bedrock

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
)

// INFERENCE_GATEWAY_BASE_PATH points at the deployment's gateway (org value
// "trx.inferenceGateway"). The default is an obvious placeholder (repo
// convention, cf. pkg/idt): with no responder there, resolution fails fast
// and the auto-updater falls back to the catalog scan.
const (
	gatewayBasePathEnv     = "INFERENCE_GATEWAY_BASE_PATH"
	defaultGatewayBasePath = "example.inferenceGateway"
	resolveModelSuffix     = ".resolveModel"
	gatewayRequestTimeout  = 10 * time.Second
	anthropicLab           = "anthropic"
)

// gatewaySubject returns the resolveModel request subject.
func gatewaySubject() string {
	base := strings.TrimSpace(os.Getenv(gatewayBasePathEnv))
	if base == "" {
		base = defaultGatewayBasePath
	}
	return base + resolveModelSuffix
}

// deriveGatewayQuery maps a concrete model/profile ID to the gateway request
// pair. parseModelID only accepts anthropic IDs, so the lab is fixed; the
// dashed family ("claude-opus") matches the gateway's "claude opus" — its
// tokenizer splits dashes.
func deriveGatewayQuery(modelID string) (lab, family string, err error) {
	p, err := parseModelID(modelID)
	if err != nil {
		return "", "", err
	}
	return anthropicLab, p.family, nil
}

// resolveReply is the subset of the gateway's ModelInfo (success) and
// NatsServiceError (failure) bodies this client consumes.
type resolveReply struct {
	InvokeID     string `json:"invokeId"`
	ErrorMessage string `json:"errorMessage"`
}

// parseResolveReply extracts the invokeId from a resolveModel reply. status
// is the nats-service STATUS header value; empty is treated as success, else
// it must parse as a strict 2xx numeric code (200-299) — anything else
// (non-numeric, or numeric but outside 2xx) is an error, even if it happens
// to start with the digit "2" (e.g. "2" or "20000").
func parseResolveReply(status string, body []byte) (string, error) {
	var r resolveReply
	jsonErr := json.Unmarshal(body, &r)
	if status != "" {
		if n, err := strconv.Atoi(status); err != nil || n < 200 || n > 299 {
			return "", fmt.Errorf("gateway status %s: %s", status, errText(r.ErrorMessage, body))
		}
	}
	if jsonErr != nil {
		return "", fmt.Errorf("gateway reply not JSON: %w", jsonErr)
	}
	invokeID := strings.TrimSpace(r.InvokeID)
	if invokeID == "" {
		return "", fmt.Errorf("gateway reply has no invokeId: %s", errText("", body))
	}
	return invokeID, nil
}

// errText prefers the structured errorMessage, else the (truncated) raw body.
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

// resolveViaGateway performs one request/reply against the gateway. Kept
// thin (like listActiveProfileIDs): decisions live in the pure helpers above.
func resolveViaGateway(ctx context.Context, nc *nats.Conn, subject, lab, family string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gatewayRequestTimeout)
	defer cancel()
	body, err := json.Marshal(map[string]string{"lab": lab, "family": family})
	if err != nil {
		return "", err
	}
	msg, err := nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		return "", fmt.Errorf("gateway request %s: %w", subject, err)
	}
	return parseResolveReply(msg.Header.Get(nats_service_common.STATUS), msg.Data)
}

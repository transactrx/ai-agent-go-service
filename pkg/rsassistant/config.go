// Package rsassistant publishes eligible trigger/nats-chat workflows as
// RSAssistant-discoverable agents on the NATS agent mesh (nats-agent protocol).
// Everything here is additive and disabled unless RSASSISTANT_ENABLED=true.
// See docs/superpowers/specs/2026-09-04-rsassistant-agents-design.md.
package rsassistant

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	envEnabled        = "RSASSISTANT_ENABLED"
	envObserveOnly    = "RSASSISTANT_IDT_OBSERVE_ONLY"
	envConsultTimeout = "RSASSISTANT_CONSULT_TIMEOUT_SECONDS"
	envAgentVersion   = "RSASSISTANT_AGENT_VERSION"
	envAppID          = "APP_ID"
	envFunctionID     = "APP_FUNCTION_ID"
	envIdentityBase   = "NATS_IDENTITY_BASE_PATH"
	envIdentitySubj   = "NATS_IDENTITY_VALIDATE_SUBJECT"

	defaultIdentityBase   = "trx.identityservice"
	defaultIdentitySubj   = "validateInternalToken"
	defaultConsultTimeout = 400 * time.Second
	defaultAgentVersion   = "1.0.0"
)

// Config is the process-wide configuration for the published agents.
type Config struct {
	Enabled         bool
	ObserveOnly     bool          // IDT observe phase: log, never block at the nats-agent layer
	AppID           string        // card access.appId (identity application)
	FunctionID      string        // card access.functionId (identity function)
	IdentitySubject string        // NATS subject of identity validateInternalToken
	ConsultTimeout  time.Duration // per-consult timeout for the engine self-call
	DefaultVersion  string        // card version when the manifest sets none
}

func isTrue(v string) bool { return strings.EqualFold(strings.TrimSpace(v), "true") }

// EnabledFromEnv reports RSASSISTANT_ENABLED=true (case-insensitive).
func EnabledFromEnv(lookup func(string) (string, bool)) bool {
	v, _ := lookup(envEnabled)
	return isTrue(v)
}

// ConfigFromEnv reads the env contract. It errors only for a fatal
// misconfiguration (missing identity pair); bad optional values fall back to
// defaults so a typo cannot silently disable identity checks.
func ConfigFromEnv(lookup func(string) (string, bool)) (Config, error) {
	get := func(k, def string) string {
		if v, ok := lookup(k); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	cfg := Config{
		Enabled:         EnabledFromEnv(lookup),
		ObserveOnly:     isTrue(get(envObserveOnly, "false")),
		AppID:           get(envAppID, ""),
		FunctionID:      get(envFunctionID, ""),
		IdentitySubject: get(envIdentityBase, defaultIdentityBase) + "." + get(envIdentitySubj, defaultIdentitySubj),
		ConsultTimeout:  defaultConsultTimeout,
		DefaultVersion:  get(envAgentVersion, defaultAgentVersion),
	}
	if raw := get(envConsultTimeout, ""); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
			cfg.ConsultTimeout = time.Duration(secs) * time.Second
		}
	}
	if cfg.AppID == "" || cfg.FunctionID == "" {
		return cfg, errors.New("rsassistant: APP_ID and APP_FUNCTION_ID are required when RSASSISTANT_ENABLED=true")
	}
	return cfg, nil
}

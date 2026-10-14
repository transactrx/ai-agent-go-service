package rsassistant

import (
	"testing"
	"time"
)

func lookupFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestEnabledFromEnv(t *testing.T) {
	if EnabledFromEnv(lookupFrom(map[string]string{})) {
		t.Fatal("unset must be disabled")
	}
	if EnabledFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "false"})) {
		t.Fatal("false must be disabled")
	}
	if !EnabledFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "TRUE"})) {
		t.Fatal("TRUE (any case) must enable")
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	cfg, err := ConfigFromEnv(lookupFrom(map[string]string{
		"RSASSISTANT_ENABLED": "true",
		"APP_ID":              "OPENSEARCHAICHATAPIAPPID",
		"APP_FUNCTION_ID":     "OPENSEARCHAICHATAPIFUNCTIONID",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ObserveOnly {
		t.Fatal("default must be strict (ObserveOnly=false)")
	}
	if cfg.IdentitySubject != "trx.identityservice.validateInternalToken" {
		t.Fatalf("IdentitySubject = %q", cfg.IdentitySubject)
	}
	if cfg.ConsultTimeout != 400*time.Second {
		t.Fatalf("ConsultTimeout = %s", cfg.ConsultTimeout)
	}
	if cfg.DefaultVersion != "1.0.0" {
		t.Fatalf("DefaultVersion = %q", cfg.DefaultVersion)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	cfg, err := ConfigFromEnv(lookupFrom(map[string]string{
		"RSASSISTANT_ENABLED":                 "true",
		"IDT_OBSERVE_ONLY":                    "true",
		"RSASSISTANT_CONSULT_TIMEOUT_SECONDS": "90",
		"RSASSISTANT_AGENT_VERSION":           "2.1.0",
		"APP_ID":                              "app",
		"APP_FUNCTION_ID":                     "fn",
		"NATS_IDENTITY_BASE_PATH":             "test.identity",
		"NATS_IDENTITY_VALIDATE_SUBJECT":      "validate",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ObserveOnly || cfg.ConsultTimeout != 90*time.Second || cfg.DefaultVersion != "2.1.0" || cfg.IdentitySubject != "test.identity.validate" {
		t.Fatalf("unexpected cfg %+v", cfg)
	}
}

// TestConfigFromEnvObserveOnlySharedWithTrigger pins the single-switch contract:
// the agent layer follows IDT_OBSERVE_ONLY (the same flag the trigger reads) and the
// retired RSASSISTANT_IDT_OBSERVE_ONLY is ignored.
func TestConfigFromEnvObserveOnlySharedWithTrigger(t *testing.T) {
	base := map[string]string{"RSASSISTANT_ENABLED": "true", "APP_ID": "app", "APP_FUNCTION_ID": "fn"}
	withLegacy := map[string]string{"RSASSISTANT_IDT_OBSERVE_ONLY": "true"}
	for k, v := range base {
		withLegacy[k] = v
	}
	cfg, err := ConfigFromEnv(lookupFrom(withLegacy))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ObserveOnly {
		t.Fatal("RSASSISTANT_IDT_OBSERVE_ONLY must be ignored; only IDT_OBSERVE_ONLY controls the observe phase")
	}
	withShared := map[string]string{"IDT_OBSERVE_ONLY": "TRUE"}
	for k, v := range base {
		withShared[k] = v
	}
	cfg, err = ConfigFromEnv(lookupFrom(withShared))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ObserveOnly {
		t.Fatal("IDT_OBSERVE_ONLY=true must put the agent layer in observe mode")
	}
}

func TestConfigFromEnvRequiresIdentityPair(t *testing.T) {
	if _, err := ConfigFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "true", "APP_ID": "app"})); err == nil {
		t.Fatal("missing APP_FUNCTION_ID must error")
	}
	if _, err := ConfigFromEnv(lookupFrom(map[string]string{"RSASSISTANT_ENABLED": "true", "APP_FUNCTION_ID": "fn"})); err == nil {
		t.Fatal("missing APP_ID must error")
	}
}

func TestConfigFromEnvBadTimeoutFallsBack(t *testing.T) {
	cfg, err := ConfigFromEnv(lookupFrom(map[string]string{
		"RSASSISTANT_ENABLED": "true", "APP_ID": "a", "APP_FUNCTION_ID": "f",
		"RSASSISTANT_CONSULT_TIMEOUT_SECONDS": "not-a-number",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsultTimeout != 400*time.Second {
		t.Fatalf("bad timeout must fall back to 400s, got %s", cfg.ConsultTimeout)
	}
}

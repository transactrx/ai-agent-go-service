// Package idt provides feature-flagged IDT (Internal Delegation Token) validation
// for incoming NATS requests. Disabled by default; opt in via IDT_VALIDATION.
package idt

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

const HeaderIDT = "X-TRX-IDT"

// defaultFunctionId is this project's RBAC functionId, used when APP_FUNCTION_ID
// is unset. Kept overridable via env so it can change without a code release.
const defaultFunctionId = "OPENSEARCHAICHATAPIFUNCTIONID"

type ValidateResult struct {
	Valid           bool    `json:"valid"`
	UserID          string  `json:"userId"`
	AccountID       string  `json:"accountId"`
	FunctionGranted bool    `json:"functionGranted"`
	Reason          *string `json:"reason"`
}

type Validator struct {
	enabled        bool
	failOpen       bool
	observeOnly    bool
	subject        string
	agentId        string
	functionId     string
	nc             *nats.Conn
	requestTimeout time.Duration
}

func NewFromEnv(nc *nats.Conn) *Validator {
	enabled := strings.EqualFold(strings.TrimSpace(os.Getenv("IDT_VALIDATION")), "true")
	failOpen := strings.EqualFold(strings.TrimSpace(os.Getenv("IDT_FAIL_OPEN")), "true")
	// observeOnly: TEMPORARY rollout gate. When true, validation runs and logs the
	// would-be decision but does NOT block/override (enforcement preserved, just not
	// applied). Default false = enforce (secure). Flip to false once Identity perms
	// are confirmed via the observe logs.
	observeOnly := strings.EqualFold(strings.TrimSpace(os.Getenv("IDT_OBSERVE_ONLY")), "true")
	if !enabled {
		log.Printf("idt: IDT_VALIDATION is not true — IDT validation disabled (pass-through)")
		return &Validator{enabled: false, failOpen: failOpen}
	}
	if nc == nil {
		log.Printf("WARNING: IDT_VALIDATION=true but no NATS connection provided — disabling")
		return &Validator{enabled: false, failOpen: failOpen}
	}
	base := strings.TrimSpace(os.Getenv("NATS_IDENTITY_BASE_PATH"))
	if base == "" {
		base = "trx.identityservice"
	}
	suffix := strings.TrimSpace(os.Getenv("NATS_IDENTITY_VALIDATE_SUBJECT"))
	if suffix == "" {
		suffix = "validateInternalToken"
	}
	// agentId identifies THIS agent to Identity. Dedicated APP_ID env (kept separate
	// from APP_NAME since the identity-side app id may differ from the project name).
	agentId := strings.TrimSpace(os.Getenv("APP_ID"))
	// functionId is this project's RBAC function — roles grant one/many functionIds and
	// Identity checks the token's user against it. Set via APP_FUNCTION_ID (env so it can
	// change without a code release); defaults to this project's known functionId.
	functionId := strings.TrimSpace(os.Getenv("APP_FUNCTION_ID"))
	if functionId == "" {
		functionId = defaultFunctionId
	}
	timeout := 5 * time.Second
	if raw := strings.TrimSpace(os.Getenv("IDT_VALIDATE_TIMEOUT_SECONDS")); raw != "" {
		var secs int
		if _, err := fmt.Sscanf(raw, "%d", &secs); err == nil && secs > 0 {
			timeout = time.Duration(secs) * time.Second
		}
	}
	log.Printf("idt: validation enabled (subject=%s.%s, agentId=%s, functionId=%q, observeOnly=%v, failOpen=%v, timeout=%s)",
		base, suffix, agentId, functionId, observeOnly, failOpen, timeout)
	return &Validator{
		enabled:        true,
		failOpen:       failOpen,
		observeOnly:    observeOnly,
		subject:        base + "." + suffix,
		agentId:        agentId,
		functionId:     functionId,
		nc:             nc,
		requestTimeout: timeout,
	}
}

func (v *Validator) Enabled() bool { return v.enabled }

// ObserveOnly reports the temporary rollout mode: true = log the would-be decision
// without blocking/overriding; false = enforce. Only meaningful when Enabled().
func (v *Validator) ObserveOnly() bool { return v.observeOnly }

// IdPrefix returns the "IDT-{uuid}" prefix of a wire token "IDT-{uuid}.{cipher}"
// for safe log breadcrumbs — never logs the secret cipher. Empty in → empty out.
func IdPrefix(wire string) string {
	if i := strings.IndexByte(wire, '.'); i >= 0 {
		return wire[:i]
	}
	return wire
}

func (v *Validator) ValidateIfEnabled(idt, functionId string) (allow bool, denyReason, userId, accountId string) {
	if !v.enabled {
		return true, "", "", ""
	}
	// Configured per-project functionId (RBAC) wins over the caller's fallback.
	if v.functionId != "" {
		functionId = v.functionId
	}
	if idt == "" {
		if v.failOpen {
			log.Printf("IDT_METRIC event=validate.fail_open reason=missing_idt agent=%s function=%s", v.agentId, functionId)
			return true, "", "", ""
		}
		log.Printf("IDT_METRIC event=validate.deny reason=MISSING_IDT agent=%s function=%s", v.agentId, functionId)
		return false, "MISSING_IDT", "", ""
	}
	body, err := json.Marshal(map[string]string{
		"idt":        idt,
		"agentId":    v.agentId,
		"functionId": functionId,
	})
	if err != nil {
		log.Printf("IDT_METRIC event=validate.encode_error agent=%s function=%s err=%q", v.agentId, functionId, err.Error())
		return v.failOpen, fmt.Sprintf("ENCODE_ERROR:%v", err), "", ""
	}
	resp, err := v.nc.Request(v.subject, body, v.requestTimeout)
	if err != nil {
		if v.failOpen {
			log.Printf("IDT_METRIC event=validate.fail_open reason=nats_error agent=%s function=%s err=%q", v.agentId, functionId, err.Error())
			return true, "", "", ""
		}
		log.Printf("IDT_METRIC event=validate.deny reason=VALIDATE_ERROR agent=%s function=%s err=%q", v.agentId, functionId, err.Error())
		return false, fmt.Sprintf("VALIDATE_ERROR:%v", err), "", ""
	}
	var result ValidateResult
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		if v.failOpen {
			log.Printf("IDT_METRIC event=validate.fail_open reason=decode_error agent=%s function=%s err=%q", v.agentId, functionId, err.Error())
			return true, "", "", ""
		}
		log.Printf("IDT_METRIC event=validate.deny reason=DECODE_ERROR agent=%s function=%s err=%q", v.agentId, functionId, err.Error())
		return false, fmt.Sprintf("DECODE_ERROR:%v", err), "", ""
	}
	if !result.Valid {
		reason := ""
		if result.Reason != nil {
			reason = *result.Reason
		}
		log.Printf("IDT_METRIC event=validate.deny reason=%s agent=%s function=%s", reason, v.agentId, functionId)
		return false, reason, "", ""
	}
	if !result.FunctionGranted {
		log.Printf("IDT_METRIC event=validate.deny reason=DENIED_FN agent=%s function=%s user=%s account=%s", v.agentId, functionId, result.UserID, result.AccountID)
		return false, "DENIED_FN", "", ""
	}
	log.Printf("IDT_METRIC event=validate.allow agent=%s function=%s user=%s account=%s", v.agentId, functionId, result.UserID, result.AccountID)
	return true, "", result.UserID, result.AccountID
}

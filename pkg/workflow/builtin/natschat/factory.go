// Package natschat implements trigger/nats-chat: registers a NATS endpoint at
// Init time via existing AddEndpointWithDocs, decodes chat requests, drives
// the executor via TriggerSink, and streams responses via natsstream.
package natschat

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/idt"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// IdentitySource configures which header/body fields supply identity.
type IdentitySource struct {
	NatsUserHeader string `json:"natsUserHeader,omitempty"`
	UserHeader     string `json:"userHeader,omitempty"`
	AccountHeader  string `json:"accountHeader,omitempty"`
	UserNameHeader string `json:"userNameHeader,omitempty"`
	TimeZoneHeader string `json:"timeZoneHeader,omitempty"`
	RequireAccount *bool  `json:"requireAccount,omitempty"`
	RequireUser    *bool  `json:"requireUser,omitempty"`
}

// Config validated by Factory.
type Config struct {
	Subject               string         `json:"subject,omitempty"`
	ResponseMode          string         `json:"responseMode"`
	IdentitySource        IdentitySource `json:"identitySource"`
	RequestTimeoutSeconds int            `json:"requestTimeoutSeconds,omitempty"`
	// AllowResponseModeOverride lets a request's body field "responseMode"
	// override ResponseMode for that one request. Default false: existing
	// workflows ignore the body field entirely, so their behavior cannot
	// change until they opt in via config.
	AllowResponseModeOverride bool `json:"allowResponseModeOverride,omitempty"`
}

const (
	responseModeStreaming = "streaming"
	responseModeSingle    = "single"
	defaultRequestTimeout = 600
	defaultNatsUserHeader = "_User_Id"
	defaultUserHeader     = "X-User-Id"
	defaultAccountHeader  = "X-Account-Id"
	defaultUserNameHeader = "X-User-Name"
	defaultTimeZoneHeader = "X-Time-Zone"
)

// Factory builds a natschat trigger.
var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("trigger/nats-chat: parse config: %w", err)
	}
	if cfg.ResponseMode != responseModeStreaming && cfg.ResponseMode != responseModeSingle {
		return nil, fmt.Errorf("trigger/nats-chat: responseMode must be %q or %q", responseModeStreaming, responseModeSingle)
	}
	if cfg.RequestTimeoutSeconds == 0 {
		cfg.RequestTimeoutSeconds = defaultRequestTimeout
	}
	if cfg.IdentitySource.NatsUserHeader == "" {
		cfg.IdentitySource.NatsUserHeader = defaultNatsUserHeader
	}
	if cfg.IdentitySource.UserHeader == "" {
		cfg.IdentitySource.UserHeader = defaultUserHeader
	}
	if cfg.IdentitySource.AccountHeader == "" {
		cfg.IdentitySource.AccountHeader = defaultAccountHeader
	}
	if cfg.IdentitySource.UserNameHeader == "" {
		cfg.IdentitySource.UserNameHeader = defaultUserNameHeader
	}
	if cfg.IdentitySource.TimeZoneHeader == "" {
		cfg.IdentitySource.TimeZoneHeader = defaultTimeZoneHeader
	}
	if cfg.IdentitySource.RequireAccount == nil {
		v := true
		cfg.IdentitySource.RequireAccount = &v
	}
	if cfg.IdentitySource.RequireUser == nil {
		v := true
		cfg.IdentitySource.RequireUser = &v
	}
	return &natsChatTrigger{cfg: cfg}, nil
})

type natsChatTrigger struct {
	cfg            Config
	logger         *log.Logger
	workflowID     string
	subject        string
	requestTimeout time.Duration
	natsHost       *nats_service.NatService
	basePath       string
	idtValidator   *idt.Validator

	mu   sync.Mutex
	sink node.TriggerSink
}

// Spec returns the node's metadata.
func (t *natsChatTrigger) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "trigger/nats-chat",
		Role: node.RoleTrigger,
		OutputPorts: []node.PortSpec{
			{Name: node.PortMain, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

// chatRequestBody is the per-request payload.
type chatRequestBody struct {
	Message      string `json:"message"`
	SessionID    string `json:"sessionId,omitempty"`
	ResponseMode string `json:"responseMode,omitempty"`
}

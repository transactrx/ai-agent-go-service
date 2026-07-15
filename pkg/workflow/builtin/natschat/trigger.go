package natschat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/idt"
	"github.com/transactrx/ai-agent-go-service/pkg/transport/natsstream"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/errcode"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/identity"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

// Init grabs the NATS host and registers the endpoint.
func (t *natsChatTrigger) Init(_ context.Context, env node.NodeEnv) error {
	t.logger = env.Logger()
	t.workflowID = env.WorkflowID()

	host, ok := env.Host("nats")
	if !ok {
		return errors.New("trigger/nats-chat: nats host not registered")
	}
	natsHost, ok := host.(*nats_service.NatService)
	if !ok {
		return errors.New("trigger/nats-chat: nats host has wrong type")
	}
	t.natsHost = natsHost

	// nats_basePath is plumbed through the hosts map by main.go from
	// cfg.NatsBasePath (the same value passed to nats_service.NewLowLevel).
	// We pull it from there rather than the lib so this project stays
	// additive-only against nats-service.
	if bp, ok := env.Host("nats_basePath"); ok {
		if s, _ := bp.(string); s != "" {
			t.basePath = s
		}
	}
	if t.basePath == "" {
		return errors.New("trigger/nats-chat: nats_basePath host entry missing or empty")
	}

	t.subject = t.cfg.Subject
	if t.subject == "" {
		t.subject = t.workflowID
	}
	t.requestTimeout = time.Duration(t.cfg.RequestTimeoutSeconds) * time.Second

	// IDT validator: env-driven. Disabled by default; pass-through when off.
	// When IDT_VALIDATION=true it requires APP_ID + APP_FUNCTION_ID — a missing
	// one is a fatal misconfiguration, surfaced here at engine startup.
	validator, err := idt.NewFromEnv(t.natsHost.GetNatsService())
	if err != nil {
		return fmt.Errorf("trigger/nats-chat: %w", err)
	}
	t.idtValidator = validator

	headerDocs := []nats_service.HeaderDoc{
		{Name: t.cfg.IdentitySource.AccountHeader, Description: "Account id — security scope for all data the agent can query", Required: *t.cfg.IdentitySource.RequireAccount, Example: "5480"},
		{Name: t.cfg.IdentitySource.UserHeader, Description: "Human user id — keys chat session memory and audit logging", Required: *t.cfg.IdentitySource.RequireUser, Example: "jdoe"},
		{Name: t.cfg.IdentitySource.UserNameHeader, Description: "Human display name, given to the agent for personalized answers", Required: false, Example: "John Doe"},
		{Name: t.cfg.IdentitySource.TimeZoneHeader, Description: "IANA timezone of the asker; used to resolve date questions like 'today'", Required: false, Example: "America/New_York"},
		{Name: t.cfg.IdentitySource.NatsUserHeader, Description: "Calling service identity; set automatically by the nats-service client", Required: false, Example: "powerlineWebApp"},
	}
	reg := nats_service.EndpointRegistration{
		Path: t.subject,
		Description: fmt.Sprintf(
			"AI chat endpoint for workflow '%s'. Streams the agent's answer as NATS events on the reply inbox. "+
				"Request body: {message, sessionId} — message is required; sessionId is optional, the server generates one and returns it in the start event.",
			t.workflowID),
		Headers: headerDocs,
		Response: &nats_service.ResponseDoc{
			Description: "Stream of NATS events on the reply inbox: start → (delta | thought | tool_call | tool_result | attachment)* → complete | error. " +
				"Event type is in the _Stream_Event header, ordering in _Stream_Sequence. " +
				"The start event carries _Stream_Cancel_Subject — publish any message on that subject to cancel. " +
				"The example below shows each event type's payload.",
			ContentType: "application/json",
			Example:     `{"start": {"sessionId": "e1f0c9a2-4b7d-4f7e-9c1a-8f2d3e4a5b6c"}, "delta": {"text": "Today you have 1,204 paid claims..."}, "tool_call": {"toolUseId": "toolu_01", "name": "opensearch_query", "input": {"query": "..."}}, "tool_result": {"toolUseId": "toolu_01", "output": {"hits": "..."}, "isError": false}, "complete": {"finalText": "Today you have 1,204 paid claims...", "messageStop": "end_turn"}, "error": {"code": "cancelled", "message": "stream cancelled by client"}}`,
		},
		Handler: t.handle,
	}
	return t.natsHost.AddEndpointWithDocs([]nats_service.EndpointRegistration{reg})
}

// Subscribe wires the executor sink. Engine calls this once after Init returns.
func (t *natsChatTrigger) Subscribe(_ context.Context, sink node.TriggerSink) error {
	t.mu.Lock()
	t.sink = sink
	t.mu.Unlock()
	return nil
}

func (t *natsChatTrigger) Close(_ context.Context) error { return nil }

// handle is registered with nats-service. It decodes the body, validates
// identity, allocates a streaming session, hands a TriggerEvent to the sink,
// and returns Status:302 so nats-service does not auto-respond.
func (t *natsChatTrigger) handle(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	hdrAccount := msg.Header.Get(t.cfg.IdentitySource.AccountHeader)
	if *t.cfg.IdentitySource.RequireAccount && hdrAccount == "" {
		return validationErr(errcode.AccountIDMissing, "account id header missing", 400)
	}

	hdrUser := msg.Header.Get(t.cfg.IdentitySource.UserHeader)
	if *t.cfg.IdentitySource.RequireUser && hdrUser == "" {
		return validationErr(errcode.UserIDMissing, "user id header missing", 400)
	}

	// IDT handling. Gated by IDT_VALIDATION. When on, IDT_OBSERVE_ONLY picks:
	//   - observe-only (TEMPORARY rollout): run validation ASYNC and LOG the would-be
	//     decision, but never block or override identity — zero added latency, current
	//     flow unchanged. Used until Identity's NATS perms are confirmed via these logs.
	//   - enforce (default, secure): synchronous — Identity is source of truth; deny on
	//     fail (403), override inbound user/account on allow.
	// The IDT_TRACE logs are temporary rollout instrumentation (remove once enforcing).
	if t.idtValidator != nil && t.idtValidator.Enabled() {
		idtToken := msg.Header.Get(idt.HeaderIDT)
		t.logger.Printf("IDT_TRACE event=inbound workflow=%s idt_present=%t idtid=%s inbound_user=%q inbound_account=%q observe_only=%t",
			t.workflowID, idtToken != "", idt.IdPrefix(idtToken), hdrUser, hdrAccount, t.idtValidator.ObserveOnly())
		if t.idtValidator.ObserveOnly() {
			go func(tok, fn, inUser, inAcct string) {
				allow, reason, vUser, vAcct := t.idtValidator.ValidateIfEnabled(tok, fn)
				t.logger.Printf("IDT_TRACE event=validate.observed workflow=%s would_allow=%t reason=%q inbound_user=%q inbound_account=%q validated_user=%q validated_account=%q note=OBSERVE-ONLY-not-enforced",
					fn, allow, reason, inUser, inAcct, vUser, vAcct)
			}(idtToken, t.workflowID, hdrUser, hdrAccount)
		} else {
			allow, reason, validatedUser, validatedAccount := t.idtValidator.ValidateIfEnabled(idtToken, t.workflowID)
			if !allow {
				t.logger.Printf("IDT_TRACE event=validate.denied workflow=%s reason=%q", t.workflowID, reason)
				return validationErr(errcode.BadRequest, "IDT validation failed: "+reason, 403)
			}
			if validatedUser != "" {
				hdrUser = validatedUser
			}
			if validatedAccount != "" {
				hdrAccount = validatedAccount
			}
		}
	}

	var body chatRequestBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return validationErr(errcode.BadBody, err.Error(), 400)
	}
	if body.Message == "" {
		return validationErr(errcode.BadRequest, "message required", 400)
	}
	if body.SessionID == "" {
		body.SessionID = uuid.NewString()
	}

	id := identity.Identity{
		NatsUserID: msg.Header.Get(t.cfg.IdentitySource.NatsUserHeader),
		UserID:     hdrUser,
		AccountID:  hdrAccount,
		UserName:   msg.Header.Get(t.cfg.IdentitySource.UserNameHeader),
		TimeZone:   msg.Header.Get(t.cfg.IdentitySource.TimeZoneHeader),
	}

	t.mu.Lock()
	sink := t.sink
	t.mu.Unlock()
	if sink == nil {
		return serverErr(errcode.ExecutorFailed, "trigger sink not yet wired", 500)
	}

	if t.cfg.ResponseMode == responseModeStreaming {
		return t.handleStreaming(msg, body, id, sink)
	}
	return t.handleSingle(msg, body, id, sink)
}

func (t *natsChatTrigger) handleStreaming(msg *nats_service.NatsMessage, body chatRequestBody, id identity.Identity, sink node.TriggerSink) *nats_service.NatsServiceError {
	sess, err := natsstream.NewStreamSession(t.natsHost.GetNatsService(), msg, t.basePath, t.logger)
	if err != nil {
		return serverErr(errcode.StreamInitFailed, err.Error(), 500)
	}
	if err := sess.Start(context.Background(), natsstream.StartPayload{
		SessionID: body.SessionID, WorkflowID: t.workflowID, RequestID: msg.MessageId,
	}); err != nil {
		return serverErr(errcode.StreamInitFailed, err.Error(), 500)
	}

	evt := node.TriggerEvent{
		RequestID:  msg.MessageId,
		Identity:   id,
		SessionID:  body.SessionID,
		Body:       msg.Body,
		Headers:    natsHeaderMap(msg.Header),
		StreamSink: streamSinkAdapter{sess: sess},
		Done:       sess.Context().Done(),
	}

	go func() {
		ctx, cancel := context.WithTimeout(sess.Context(), t.requestTimeout)
		defer cancel()
		ctx = engine.WithToolResultHost(ctx, sess.ToolResultHost())
		if emitErr := sink.Emit(ctx, evt); emitErr != nil {
			_ = sess.Fail(context.Background(), natsstream.ErrorPayload{
				Code: errcode.ExecutorFailed, Message: emitErr.Error(),
			})
		}
	}()

	return &nats_service.NatsServiceError{Status: 302}
}

func (t *natsChatTrigger) handleSingle(msg *nats_service.NatsMessage, body chatRequestBody, id identity.Identity, sink node.TriggerSink) *nats_service.NatsServiceError {
	resp := make(chan struct {
		body    []byte
		headers map[string]string
		err     error
	}, 1)
	reply := node.ReplyFunc(func(_ context.Context, b []byte, h map[string]string) error {
		resp <- struct {
			body    []byte
			headers map[string]string
			err     error
		}{body: b, headers: h}
		return nil
	})
	evt := node.TriggerEvent{
		RequestID: msg.MessageId,
		Identity:  id,
		SessionID: body.SessionID,
		Body:      msg.Body,
		Headers:   natsHeaderMap(msg.Header),
		Reply:     reply,
		Done:      make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.requestTimeout)
	defer cancel()
	if err := sink.Emit(ctx, evt); err != nil {
		return serverErr(errcode.ExecutorFailed, err.Error(), 500)
	}
	select {
	case r := <-resp:
		// NatsMessage uses ResponseBody / ResponseHeader (not RespBody / RespHeader)
		msg.ResponseBody = r.body
		for k, v := range r.headers {
			msg.ResponseHeader.Set(k, v)
		}
		return nil
	case <-ctx.Done():
		return serverErr(errcode.Timeout, "request timeout", 504)
	}
}

// validationErr wraps NewValidationError. nats-service signature:
// NewValidationError(errorMessage string, apiStatusCode int, err error) NatsServiceError
// — always returns Status:400 regardless of the apiStatusCode arg.
func validationErr(code, message string, apiStatusCode int) *nats_service.NatsServiceError {
	e := nats_service.NewValidationError(message, apiStatusCode, fmt.Errorf("%s", code))
	return &e
}

// serverErr wraps NewServerError. nats-service signature:
// NewServerError(errorMessage string, apiStatusCode int, err error) NatsServiceError
// — always returns Status:500 regardless of the apiStatusCode arg.
func serverErr(code, message string, apiStatusCode int) *nats_service.NatsServiceError {
	e := nats_service.NewServerError(message, apiStatusCode, fmt.Errorf("%s", code))
	return &e
}

func natsHeaderMap(h nats.Header) map[string]string {
	out := map[string]string{}
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

// streamSinkAdapter bridges node.StreamSink → natsstream.StreamSession.
type streamSinkAdapter struct{ sess *natsstream.StreamSession }

func (a streamSinkAdapter) Send(ctx context.Context, evt node.StreamEvent) error {
	return a.sess.Send(ctx, natsstream.StreamEvent{
		Type:   natsstream.StreamEventType(evt.Type),
		Data:   evt.Data,
		Header: evt.Header,
	})
}
func (a streamSinkAdapter) Close(ctx context.Context, term node.StreamEvent) error {
	if term.Type == node.StreamComplete {
		var p natsstream.CompletePayload
		_ = json.Unmarshal(term.Data, &p)
		return a.sess.Complete(ctx, p)
	}
	var p natsstream.ErrorPayload
	_ = json.Unmarshal(term.Data, &p)
	return a.sess.Fail(ctx, p)
}

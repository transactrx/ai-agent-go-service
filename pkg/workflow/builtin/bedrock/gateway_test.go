package bedrock

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
)

// TestGatewaySubject: env override wins (trimmed); default is the generic
// placeholder base path (repo convention: deployments set the org value).
func TestGatewaySubject(t *testing.T) {
	t.Setenv(gatewayBasePathEnv, "")
	if got := gatewaySubject(); got != "example.inferenceGateway.resolveModel" {
		t.Fatalf("default subject = %q", got)
	}
	t.Setenv(gatewayBasePathEnv, "trx.inferenceGateway")
	if got := gatewaySubject(); got != "trx.inferenceGateway.resolveModel" {
		t.Fatalf("override subject = %q", got)
	}
	t.Setenv(gatewayBasePathEnv, "  trx.gw  ")
	if got := gatewaySubject(); got != "trx.gw.resolveModel" {
		t.Fatalf("trimmed subject = %q", got)
	}
}

// TestDeriveGatewayQuery: lab is always "anthropic" (the only lab
// parseModelID accepts); family is the dash form the gateway tokenizer
// matches ("claude-opus" ≡ catalog "claude opus"). Legacy IDs error.
func TestDeriveGatewayQuery(t *testing.T) {
	cases := []struct {
		id, lab, family string
		wantErr         bool
	}{
		{id: "us.anthropic.claude-opus-4-7", lab: "anthropic", family: "claude-opus"},
		{id: "global.anthropic.claude-opus-5-20270101-v1:0", lab: "anthropic", family: "claude-opus"},
		{id: "anthropic.claude-sonnet-4-5", lab: "anthropic", family: "claude-sonnet"},
		{id: "anthropic.claude-3-5-sonnet-20241022-v2:0", wantErr: true},
		{id: "meta.llama3-1-8b-instruct-v1:0", wantErr: true},
	}
	for _, c := range cases {
		lab, family, err := deriveGatewayQuery(c.id)
		if c.wantErr {
			if err == nil {
				t.Errorf("deriveGatewayQuery(%q): expected error", c.id)
			}
			continue
		}
		if err != nil {
			t.Errorf("deriveGatewayQuery(%q): %v", c.id, err)
			continue
		}
		if lab != c.lab || family != c.family {
			t.Errorf("deriveGatewayQuery(%q) = %q,%q want %q,%q", c.id, lab, family, c.lab, c.family)
		}
	}
}

// TestParseResolveReply: success needs 2xx (or absent) status AND a non-empty
// invokeId; error replies surface status + errorMessage; garbage fails.
func TestParseResolveReply(t *testing.T) {
	ok := `{"modelId":"anthropic.claude-opus-4-8","invokeId":"us.anthropic.claude-opus-4-8","family":"claude opus"}`
	cases := []struct {
		name, status, body, want, wantErrPart string
	}{
		{name: "success", status: "200", body: ok, want: "us.anthropic.claude-opus-4-8"},
		{name: "no status header", status: "", body: ok, want: "us.anthropic.claude-opus-4-8"},
		{name: "error status", status: "400", body: `{"status":400,"errorMessage":"no invocable model matches"}`, wantErrPart: "400"},
		{name: "empty invokeId", status: "200", body: `{"modelId":"anthropic.claude-opus-4-8"}`, wantErrPart: "invokeId"},
		{name: "not json", status: "200", body: `Server Error`, wantErrPart: "JSON"},
	}
	for _, c := range cases {
		got, err := parseResolveReply(c.status, []byte(c.body))
		if c.wantErrPart != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErrPart) {
				t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErrPart)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.name, got, err, c.want)
		}
	}
}

// runEmbeddedNATS starts an in-process NATS server for resolveViaGateway
// integration tests (pattern: pkg/transport/natsstream/stream_test.go).
func runEmbeddedNATS(t *testing.T) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return srv, nc
}

// TestResolveViaGatewayIntegration exercises resolveViaGateway end-to-end
// against an in-process NATS server, with a fake resolveModel responder
// standing in for the gateway.
func TestResolveViaGatewayIntegration(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		_, nc := runEmbeddedNATS(t)
		const subject = "trx.test.resolveModel"

		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			var req map[string]string
			if err := json.Unmarshal(m.Data, &req); err != nil {
				t.Errorf("responder: bad request body: %v", err)
				return
			}
			if req["lab"] != "anthropic" || req["family"] != "claude-opus" {
				t.Errorf("responder: request body = %v, want lab=anthropic family=claude-opus", req)
			}
			reply := &nats.Msg{
				Subject: m.Reply,
				Header:  nats.Header{nats_service_common.STATUS: []string{"200"}},
				Data:    []byte(`{"modelId":"anthropic.claude-opus-4-8","invokeId":"us.anthropic.claude-opus-4-8"}`),
			}
			if err := m.RespondMsg(reply); err != nil {
				t.Errorf("responder: RespondMsg: %v", err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		got, err := resolveViaGateway(context.Background(), nc, subject, "anthropic", "claude-opus")
		if err != nil {
			t.Fatalf("resolveViaGateway: %v", err)
		}
		if got != "us.anthropic.claude-opus-4-8" {
			t.Fatalf("resolveViaGateway = %q, want %q", got, "us.anthropic.claude-opus-4-8")
		}
	})

	t.Run("error reply", func(t *testing.T) {
		_, nc := runEmbeddedNATS(t)
		const subject = "trx.test.resolveModel.error"

		sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
			reply := &nats.Msg{
				Subject: m.Reply,
				Header:  nats.Header{nats_service_common.STATUS: []string{"400"}},
				Data:    []byte(`{"status":400,"errorMessage":"no invocable model matches"}`),
			}
			if err := m.RespondMsg(reply); err != nil {
				t.Errorf("responder: RespondMsg: %v", err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		_, err = resolveViaGateway(context.Background(), nc, subject, "anthropic", "claude-opus")
		if err == nil {
			t.Fatal("resolveViaGateway: expected error, got nil")
		}
		if !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "no invocable model matches") {
			t.Fatalf("resolveViaGateway error = %v, want containing %q and %q", err, "400", "no invocable model matches")
		}
	})

	t.Run("no responder", func(t *testing.T) {
		_, nc := runEmbeddedNATS(t)
		const subject = "trx.test.resolveModel.noresponder"

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := resolveViaGateway(ctx, nc, subject, "anthropic", "claude-opus")
		if err == nil {
			t.Fatal("resolveViaGateway: expected error, got nil")
		}
	})
}

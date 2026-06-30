package webbridge

import (
	"log"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// fiberTestApp returns a minimal *fiber.App for route-registration tests.
func fiberTestApp(t *testing.T) *fiber.App {
	t.Helper()
	return fiber.New(fiber.Config{DisableStartupMessage: true})
}

// routePaths returns a set of all registered route paths for an app.
func routePaths(app *fiber.App) map[string]bool {
	out := make(map[string]bool)
	for _, stk := range app.Stack() {
		for _, r := range stk {
			out[r.Path] = true
		}
	}
	return out
}

func TestMount_RequiresAuth(t *testing.T) {
	app := fiberTestApp(t)
	err := Mount(app, Options{NATSChatPath: "trx.x", Auth: nil})
	if err == nil {
		t.Fatal("Mount must error when Auth is nil")
	}
}

func TestMount_RequiresNATSChatPath(t *testing.T) {
	app := fiberTestApp(t)
	err := Mount(app, Options{NATSChatPath: "", Auth: fakeAuth{}})
	if err == nil {
		t.Fatal("Mount must error when NATSChatPath is empty")
	}
}

func TestMount_RegistersCoreRoutes(t *testing.T) {
	app := fiberTestApp(t)
	err := Mount(app, Options{NATSChatPath: "trx.x", Auth: fakeAuth{}, Logger: log.Default()})
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}
	paths := routePaths(app)
	for _, want := range []string{"/aichatviewer/token", "/aichatviewer/workflows", "/aichatviewer/stream"} {
		if !paths[want] {
			t.Errorf("missing route %s", want)
		}
	}
}

func TestMount_DefaultsLogger(t *testing.T) {
	app := fiberTestApp(t)
	// Logger nil — should not panic or error.
	err := Mount(app, Options{NATSChatPath: "trx.x", Auth: fakeAuth{}})
	if err != nil {
		t.Fatalf("Mount with nil Logger: %v", err)
	}
}

func TestMount_DefaultsAuthorizer(t *testing.T) {
	app := fiberTestApp(t)
	// Authorizer nil — should default to AllowAll without error.
	err := Mount(app, Options{NATSChatPath: "trx.x", Auth: fakeAuth{}, Authorizer: nil})
	if err != nil {
		t.Fatalf("Mount with nil Authorizer: %v", err)
	}
}

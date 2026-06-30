package gofiber_session

import (
	"sync"
	"time"
)

// idtRegistry holds the most-recent IDT per gofiber session id. The proxy
// injects a fresh TRX_IDT header on every forwarded request; a webapp captures
// it here so a long-lived WebSocket can read the *current* IDT at publish time
// instead of a token frozen at handshake. In-process is sufficient because the
// ALB pins a browser's traffic and its socket to the same task (sticky sessions).
type idtEntry struct {
	idt     string
	updated time.Time
}

var idtRegistry = struct {
	sync.RWMutex
	m map[string]idtEntry
}{m: make(map[string]idtEntry)}

// idtRegistryMaxAge bounds memory if a session never calls ForgetIDT (e.g. the
// process was killed mid-socket). Entries older than this are swept lazily.
const idtRegistryMaxAge = 2 * time.Hour

// CaptureIDT records idt as the current token for sessionID. No-op if either is
// empty (IDT off / non-IDT request).
func CaptureIDT(sessionID, idt string) {
	if sessionID == "" || idt == "" {
		return
	}
	now := time.Now()
	idtRegistry.Lock()
	idtRegistry.m[sessionID] = idtEntry{idt: idt, updated: now}
	// Opportunistic sweep so the map stays bounded.
	for k, e := range idtRegistry.m {
		if now.Sub(e.updated) > idtRegistryMaxAge {
			delete(idtRegistry.m, k)
		}
	}
	idtRegistry.Unlock()
}

// CurrentIDT returns the latest captured IDT for sessionID, or "" if none.
func CurrentIDT(sessionID string) string {
	if sessionID == "" {
		return ""
	}
	idtRegistry.RLock()
	e, ok := idtRegistry.m[sessionID]
	idtRegistry.RUnlock()
	if !ok {
		return ""
	}
	return e.idt
}

// ForgetIDT drops a session's entry. Call on WebSocket close and logout.
func ForgetIDT(sessionID string) {
	idtRegistry.Lock()
	delete(idtRegistry.m, sessionID)
	idtRegistry.Unlock()
}

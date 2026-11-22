package fleetdaemon

import (
	"net/http"
	"sync"
)

// DrainHandler adds an explicit drain operation to the daemon's existing
// private Unix-socket API. A drain is non-blocking on the server side: it
// persists deletion intents, and the regular worker subsequently observes
// provider-side absence before releasing reservations.
//
// The admission fence and all deletion intents are atomically persisted in the
// journal. The daemon refuses new work after a restart of a draining journal.
func DrainHandler(d *Daemon) http.Handler {
	base := d.Handler()
	guard := &drainGuard{d: d, base: base}
	if c, ok := d.controller.(durableDrainer); ok {
		guard.drainer = c
		guard.draining = c.Draining()
	}
	return http.HandlerFunc(guard.serveHTTP)
}

type durableDrainer interface {
	BeginDrain() (int, error)
	Draining() bool
}

type drainGuard struct {
	mu       sync.Mutex
	d        *Daemon
	base     http.Handler
	drainer  durableDrainer
	draining bool
}

func (g *drainGuard) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && r.URL.Path == "/v1/drain" {
		g.drain(w)
		return
	}
	if r.Method == http.MethodPost && (r.URL.Path == "/v1/submit" || r.URL.Path == "/v1/intent") {
		// Serialize admission/intent calls with drain initiation. A pending
		// submission cannot race past the deletion-intent snapshot.
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.draining {
			fail(w, http.StatusConflict, "fleet is draining; only status, metrics and recovery are available")
			return
		}
	}
	g.base.ServeHTTP(w, r)
}

func (g *drainGuard) drain(w http.ResponseWriter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.drainer == nil {
		fail(w, http.StatusNotImplemented, "controller does not support durable drain")
		return
	}
	g.draining = true // fail closed on journal errors
	requested, err := g.drainer.BeginDrain()
	g.d.signal() // bypass timer-driven retry delay for operator cleanup
	if err != nil {
		respond(w, http.StatusServiceUnavailable, map[string]any{
			"draining": true, "requested": requested, "error": err.Error(),
		})
		return
	}
	respond(w, http.StatusAccepted, map[string]any{
		"draining": true, "requested": requested,
	})
}

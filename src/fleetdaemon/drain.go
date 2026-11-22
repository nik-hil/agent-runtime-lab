package fleetdaemon

import (
	"net/http"
	"sort"
	"sync"

	"agent-runtime-lab/src/admission"
	"agent-runtime-lab/src/lifecycle"
)

// DrainHandler adds an explicit drain operation to the daemon's existing
// private Unix-socket API. A drain is non-blocking on the server side: it
// persists deletion intents, and the regular worker subsequently observes
// provider-side absence before releasing reservations.
//
// The admission fence is process-local. A caller restarting the daemon should
// reissue drain before attempting another operation. The durable delete intents
// continue to reconcile across process restarts.
func DrainHandler(d *Daemon) http.Handler {
	base := d.Handler()
	guard := &drainGuard{d: d, base: base}
	return http.HandlerFunc(guard.serveHTTP)
}

type drainGuard struct {
	mu       sync.Mutex
	d        *Daemon
	base     http.Handler
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
	g.draining = true // reject admission even if one delete intent fails
	state := g.d.controller.Status()
	ids := make([]string, 0, len(state.Jobs))
	for id := range state.Jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	requested := 0
	var failures []string
	for _, id := range ids {
		if state.Jobs[id].Entry.State == admission.Deleted {
			continue
		}
		if err := g.d.controller.SetDesired(id, lifecycle.WantDeleted); err != nil {
			// Returning a failure preserves admission fencing. Reissuing drain
			// is safe, and a partial deletion still converges in background.
			failures = append(failures, id+": "+err.Error())
			continue
		}
		requested++
	}
	g.d.signal() // bypass timer-driven retry delay for operator cleanup
	if len(failures) != 0 {
		respond(w, http.StatusServiceUnavailable, map[string]any{
			"draining": true, "requested": requested, "errors": failures,
		})
		return
	}
	respond(w, http.StatusAccepted, map[string]any{
		"draining": true, "requested": requested,
	})
}

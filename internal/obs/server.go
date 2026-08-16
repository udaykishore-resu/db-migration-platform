package obs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// HealthState is the readiness of one dependency.
type HealthState struct {
	Ready   bool      `json:"ready"`
	Message string    `json:"message,omitempty"`
	Since   time.Time `json:"since"`
}

// Health tracks the readiness of each dependency a process needs. Liveness and
// readiness are kept distinct on purpose: a process that has lost its database
// is not ready to take work, but restarting it will not help, so it must stay
// live or the orchestrator will crash-loop it through the outage.
type Health struct {
	mu    sync.RWMutex
	deps  map[string]HealthState
	live  bool
	start time.Time
}

// NewHealth returns a Health that is live but not yet ready.
func NewHealth() *Health {
	return &Health{deps: make(map[string]HealthState), live: true, start: time.Now()}
}

// Set records the readiness of a dependency.
func (h *Health) Set(name string, ready bool, msg string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	prev, existed := h.deps[name]
	if existed && prev.Ready == ready {
		prev.Message = msg
		h.deps[name] = prev
		return
	}
	h.deps[name] = HealthState{Ready: ready, Message: msg, Since: time.Now()}
}

// SetLive marks the process itself as unrecoverable, which is the only condition
// that should cause an orchestrator to restart it.
func (h *Health) SetLive(live bool) {
	h.mu.Lock()
	h.live = live
	h.mu.Unlock()
}

// Ready reports whether every registered dependency is ready.
func (h *Health) Ready() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.deps) == 0 {
		return false
	}
	for _, s := range h.deps {
		if !s.Ready {
			return false
		}
	}
	return true
}

// Live reports process liveness.
func (h *Health) Live() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.live
}

// Snapshot returns a copy of dependency states for the status endpoint.
func (h *Health) Snapshot() map[string]HealthState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]HealthState, len(h.deps))
	for k, v := range h.deps {
		out[k] = v
	}
	return out
}

// AdminServer exposes health, readiness and metrics on a dedicated port, kept
// separate from any business API so that a saturated data path does not make the
// process look dead to its orchestrator.
type AdminServer struct {
	srv    *http.Server
	log    *slog.Logger
	health *Health
}

// NewAdminServer builds the admin server. Extra routes may be registered by the
// caller through the returned mux parameter of the routes callback.
func NewAdminServer(addr string, reg *Registry, health *Health, log *slog.Logger, routes func(mux *http.ServeMux)) *AdminServer {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if health.Live() {
			writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "dead"})
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		snap := health.Snapshot()
		if health.Ready() {
			writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "dependencies": snap})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "dependencies": snap})
	})

	mux.Handle("GET /metrics", reg.Handler())

	if routes != nil {
		routes(mux)
	}

	return &AdminServer{
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      60 * time.Second,
			IdleTimeout:       120 * time.Second,
		},
		log:    log,
		health: health,
	}
}

// Start serves until the context is cancelled, then drains gracefully.
func (a *AdminServer) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		a.log.InfoContext(ctx, "admin server listening", slog.String("addr", a.srv.Addr))
		if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return a.srv.Shutdown(shutdownCtx)
	}
}

// Addr reports the configured listen address.
func (a *AdminServer) Addr() string { return a.srv.Addr }

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// WriteJSON is the shared JSON response helper for the control-plane API.
func WriteJSON(w http.ResponseWriter, status int, body any) { writeJSON(w, status, body) }

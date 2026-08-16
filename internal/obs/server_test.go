package obs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestHealthReadyRequiresAllDependencies(t *testing.T) {
	h := NewHealth()
	// No dependencies registered means nothing has reported in yet; a process
	// must not claim readiness before it has checked anything.
	if h.Ready() {
		t.Fatal("empty health must not be ready")
	}
	h.Set("target-db", true, "")
	h.Set("kafka", false, "no coordinator")
	if h.Ready() {
		t.Fatal("must not be ready while a dependency is down")
	}
	h.Set("kafka", true, "")
	if !h.Ready() {
		t.Fatal("expected ready once all dependencies are up")
	}
}

// A process that has lost its database is not ready, but restarting it will not
// help, so it must remain live or the orchestrator crash-loops it.
func TestUnreadyProcessStaysLive(t *testing.T) {
	h := NewHealth()
	h.Set("target-db", false, "connection refused")
	if h.Ready() {
		t.Fatal("expected not ready")
	}
	if !h.Live() {
		t.Fatal("losing a dependency must not affect liveness")
	}
}

func TestHealthSinceOnlyMovesOnStateChange(t *testing.T) {
	h := NewHealth()
	h.Set("db", false, "first")
	first := h.Snapshot()["db"].Since
	h.Set("db", false, "still down")
	if got := h.Snapshot()["db"].Since; !got.Equal(first) {
		t.Fatal("Since must not move when state is unchanged")
	}
	if msg := h.Snapshot()["db"].Message; msg != "still down" {
		t.Fatalf("message should update, got %q", msg)
	}
	h.Set("db", true, "")
	if got := h.Snapshot()["db"].Since; got.Equal(first) {
		t.Fatal("Since must move on state change")
	}
}

func TestAdminEndpoints(t *testing.T) {
	reg := NewRegistry()
	reg.Counter("events_total", "events").Add(7)
	h := NewHealth()
	log := NewLogger(os.Stderr, "error", "test", "i")
	a := NewAdminServer(":0", reg, h, log, func(mux *http.ServeMux) {
		mux.HandleFunc("GET /custom", func(w http.ResponseWriter, _ *http.Request) {
			WriteJSON(w, http.StatusOK, map[string]string{"ok": "yes"})
		})
	})

	// Not ready yet.
	rec := httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz: got %d, want 503", rec.Code)
	}

	h.Set("db", true, "")
	rec = httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz: got %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("readyz body not JSON: %v", err)
	}
	if body["status"] != "ready" {
		t.Fatalf("unexpected status %v", body["status"])
	}

	rec = httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: got %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics: got %d", rec.Code)
	}
	if got := rec.Body.String(); got == "" || !contains(got, "events_total 7") {
		t.Fatalf("metrics body unexpected:\n%s", got)
	}

	rec = httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/custom", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("custom route: got %d", rec.Code)
	}
}

func TestSetLiveFlipsHealthz(t *testing.T) {
	reg := NewRegistry()
	h := NewHealth()
	a := NewAdminServer(":0", reg, h, NewLogger(os.Stderr, "error", "t", "i"), nil)
	h.SetLive(false)
	rec := httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", rec.Code)
	}
}

func contains(h, n string) bool {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}

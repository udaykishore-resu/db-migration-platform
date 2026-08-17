// Command controlplane exposes migration status and the cutover gate over HTTP.
//
// The endpoint that matters is GET /v1/cutover/readiness. It answers one
// question — may we repoint the application at the target — with either "yes" or
// the specific, quantified list of reasons not. Making that a machine-readable
// endpoint rather than a dashboard someone squints at means the answer can be
// wired into a deployment pipeline, and means nobody has to reconstruct at 3am
// which of six conditions they were supposed to check.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/app"
	"github.com/udaykishore-resu/db-migration-platform/internal/control"
	"github.com/udaykishore-resu/db-migration-platform/internal/obs"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
	"github.com/udaykishore-resu/db-migration-platform/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "controlplane: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	flags := app.ParseFlags()

	// The server is constructed before Bootstrap because Bootstrap registers the
	// routes while building the admin listener. Its app pointer is filled in
	// immediately afterwards, which is safe: handlers only dereference it when a
	// request arrives, and no request can arrive before the listener is started.
	srv := &server{}
	a, err := app.Bootstrap(ctx, "controlplane", flags, srv.routes)
	if err != nil {
		return err
	}
	srv.app = a

	gauge := a.Metrics.Gauge("migration_cutover_ready", "1 when the cutover gate is open, 0 when it is closed")
	blockers := a.Metrics.Gauge("migration_cutover_blockers", "Active cutover blockers", "code")

	if flags.DryRun {
		a.Log.InfoContext(ctx, "dry run complete")
		a.Close()
		return nil
	}

	return a.Run(ctx, func(ctx context.Context) error {
		ctx = obs.WithComponent(ctx, "controlplane")
		for {
			if err := ctx.Err(); err != nil {
				return nil
			}
			readiness, _, err := srv.evaluate(ctx)
			if err != nil {
				a.Log.ErrorContext(ctx, "gate evaluation failed", slog.String("error", err.Error()))
			} else {
				if readiness.Ready {
					gauge.Set(1)
				} else {
					gauge.Set(0)
				}
				for _, b := range readiness.Blockers {
					blockers.Set(1, b.Code)
				}
			}
			if !retryx.Sleep(ctx, 30*time.Second) {
				return nil
			}
		}
	})
}

type server struct{ app *app.App }

// writeErr is the single place an error becomes a response body, so the wire
// shape cannot drift between handlers.
func writeErr(w http.ResponseWriter, status int, err error) {
	obs.WriteJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *server) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/cutover/readiness", s.handleReadiness)
	mux.HandleFunc("GET /v1/deadletters/counts", s.handleDLQCounts)
	mux.HandleFunc("POST /v1/deadletters/requeue", s.handleRequeue)
	mux.HandleFunc("POST /v1/phase", s.handlePhase)
}

// evaluate gathers the measured state and runs the gate.
func (s *server) evaluate(ctx context.Context) (control.Readiness, store.State, error) {
	st, err := s.app.Store.LoadState(ctx)
	if err != nil {
		return control.Readiness{}, st, err
	}
	counts, err := s.app.Store.Counts(ctx)
	if err != nil {
		return control.Readiness{}, st, err
	}
	total, loaded, err := s.app.Store.PartCounts(ctx)
	if err != nil {
		return control.Readiness{}, st, err
	}

	observed := control.Observed{
		Phase:                   st.Phase,
		OpenDeadLetters:         counts.Open(),
		QuarantinedLetters:      counts.Quarantined,
		ReconcileFindings:       st.ReconcileFindings,
		ReconcileRanAt:          st.ReconcileRanAt,
		ReconcileComplete:       st.ReconcileComplete,
		PartsTotal:              total,
		PartsLoaded:             loaded,
		ReverseReplicationArmed: st.ReverseArmed,
		Now:                     time.Now().UTC(),
	}
	// Lag is read from the applier's published metric rather than recomputed
	// here, so the gate and the dashboards can never disagree about it.
	observed.CurrentLag, observed.LagUnderThreshold = currentLag()

	return control.Evaluate(observed, s.app.Cfg.Thresholds()), st, nil
}

// currentLag is a placeholder for the lag feed. In a deployment this reads the
// applier's published gauge through the metrics backend; it is separated here so
// that the gate logic has exactly one input for lag rather than several
// subtly-different ones.
func currentLag() (current, stableFor time.Duration) { return 0, 0 }

func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	readiness, st, err := s.evaluate(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	counts, _ := s.app.Store.Counts(r.Context())

	obs.WriteJSON(w, http.StatusOK, map[string]any{
		"migration_id":       s.app.Cfg.MigrationID,
		"environment":        s.app.Cfg.Environment,
		"phase":              st.Phase,
		"parts_total":        st.PartsTotal,
		"parts_loaded":       st.PartsLoaded,
		"reconcile_ran_at":   st.ReconcileRanAt,
		"reconcile_findings": st.ReconcileFindings,
		"dead_letters":       counts,
		"cutover":            readiness,
		"tables":             s.app.Cfg.Plan.SortedTableNames(),
	})
}

func (s *server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	readiness, _, err := s.evaluate(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	// 200 when open, 409 when closed, so a deployment pipeline can gate on the
	// status code without parsing the body.
	code := http.StatusConflict
	if readiness.Ready {
		code = http.StatusOK
	}
	obs.WriteJSON(w, code, readiness)
}

func (s *server) handleDLQCounts(w http.ResponseWriter, r *http.Request) {
	counts, err := s.app.Store.Counts(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	obs.WriteJSON(w, http.StatusOK, counts)
}

func (s *server) handleRequeue(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs []int64 `json:"ids"`
		By  string  `json:"by"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if body.By == "" {
		writeErr(w, http.StatusBadRequest, errors.New("requeue must record who authorised it"))
		return
	}

	n, err := s.app.Store.Requeue(r.Context(), body.IDs, body.By)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	s.app.Log.InfoContext(r.Context(), "quarantined records requeued",
		slog.Int64("count", n), slog.String("by", body.By))
	obs.WriteJSON(w, http.StatusOK, map[string]any{"requeued": n})
}

func (s *server) handlePhase(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Phase string `json:"phase"`
		By    string `json:"by"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.app.Store.SetPhase(r.Context(), control.Phase(body.Phase)); err != nil {
		// An illegal transition is a client error, and the message already lists
		// the legal alternatives.
		writeErr(w, http.StatusConflict, err)
		return
	}
	s.app.Log.InfoContext(r.Context(), "migration phase changed",
		slog.String("phase", body.Phase), slog.String("by", body.By))
	obs.WriteJSON(w, http.StatusOK, map[string]string{"phase": body.Phase})
}

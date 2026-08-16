// Command reconciler verifies that the target matches the source.
//
// It runs continuously rather than only at the end. Discovering at cutover that
// four thousand rows are wrong means starting over; discovering it twenty minutes
// after the bug was introduced means fixing one thing. The hierarchical digest
// comparison is what makes that affordable — a table that matches costs two
// queries to confirm, regardless of how many rows it has.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/app"
	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/obs"
	"github.com/udaykishore-resu/db-migration-platform/internal/recon"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "reconciler: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	flags := app.ParseFlags()

	a, err := app.Bootstrap(ctx, "reconciler", flags, nil)
	if err != nil {
		return err
	}
	if a.Source == nil {
		return fmt.Errorf("the reconciler needs a source connection; set SOURCE_DSN")
	}

	sourceDial, err := dialect.For(a.Cfg.Source.Engine)
	if err != nil {
		return err
	}

	reg := a.Metrics
	findingsTotal := reg.Counter("migration_recon_findings_total", "Discrepancies found", "table", "kind")
	runsTotal := reg.Counter("migration_recon_runs_total", "Reconciliation passes completed", "table")
	digestQueries := reg.Counter("migration_recon_digest_queries_total", "Digest queries issued", "table")
	rowReads := reg.Counter("migration_recon_row_reads_total", "Leaf row reads issued", "table")
	runSeconds := reg.Histogram("migration_recon_run_seconds", "Time for one reconciliation pass", nil, "table")
	openFindings := reg.Gauge("migration_recon_open_findings", "Discrepancies from the most recent pass", "table")

	if flags.DryRun {
		a.Log.InfoContext(ctx, "dry run complete: both databases reachable")
		a.Close()
		return nil
	}

	interval := time.Duration(0)
	if !flags.Once {
		interval = durationOr(a.Cfg.Recon.Interval, 5*time.Minute)
	}

	return a.Run(ctx, func(ctx context.Context) error {
		ctx = obs.WithComponent(ctx, "reconciler")
		for {
			if err := ctx.Err(); err != nil {
				return nil
			}

			passCtx := obs.WithTrace(ctx, obs.NewTraceID())
			totalFindings := 0
			complete := true

			for _, spec := range a.Cfg.Plan.Tables {
				pair := &recon.SQLPair{
					Source: a.Source, Target: a.Target,
					SourceDial: sourceDial, TargetDial: a.Dialect,
					Spec: spec,
				}
				r := recon.New(spec.Source, spec.Key(), pair, pair, recon.Options{
					LeafRows:    a.Cfg.Recon.LeafRows,
					MaxDepth:    a.Cfg.Recon.MaxDepth,
					MaxFindings: a.Cfg.Recon.MaxFindings,
				})

				start := time.Now()
				findings, err := r.Reconcile(passCtx, recon.FullRange(spec))
				stats := r.Stats()
				runSeconds.Observe(time.Since(start).Seconds(), spec.Source.String())
				digestQueries.Add(float64(stats.DigestQueries), spec.Source.String())
				rowReads.Add(float64(stats.RowReads), spec.Source.String())

				if err != nil {
					complete = false
					a.Log.ErrorContext(passCtx, "reconciliation failed",
						slog.String("table", spec.Source.String()), slog.String("error", err.Error()))
					continue
				}
				runsTotal.Inc(spec.Source.String())

				summary := recon.Summarise(spec.Source, findings, stats)
				openFindings.Set(float64(summary.Total), spec.Source.String())
				totalFindings += summary.Total
				for kind, n := range summary.ByKind {
					findingsTotal.Add(float64(n), spec.Source.String(), string(kind))
				}

				logReconResult(passCtx, a.Log, spec, summary, findings)
			}

			if err := a.Store.RecordReconcileRun(passCtx, totalFindings, complete); err != nil {
				a.Log.ErrorContext(passCtx, "recording reconciliation run failed", slog.String("error", err.Error()))
			}

			if flags.Once {
				if totalFindings > 0 {
					return fmt.Errorf("reconciliation found %d discrepancies", totalFindings)
				}
				return nil
			}
			if !retryx.Sleep(ctx, interval) {
				return nil
			}
		}
	})
}

// logReconResult reports a pass at a level that matches what it found, and
// includes the cost so that "verification is too expensive to run often" can be
// answered with a number rather than an assumption.
func logReconResult(ctx context.Context, log *slog.Logger, spec model.TableSpec, s recon.Summary, findings []recon.Finding) {
	attrs := []any{
		slog.String("table", spec.Source.String()),
		slog.Int("findings", s.Total),
		slog.Int("digest_queries", s.DigestQueries),
		slog.Int("row_reads", s.RowReads),
		slog.Int("ranges_visited", s.RangesVisited),
		slog.Int("deepest_descent", s.DeepestDescent),
	}
	if s.Total == 0 {
		log.InfoContext(ctx, "table reconciled clean", attrs...)
		return
	}

	log.WarnContext(ctx, "reconciliation found discrepancies", attrs...)
	// Log a bounded sample rather than every finding: a systemic problem produces
	// thousands, and drowning the log makes the summary harder to see, not easier.
	const sample = 10
	for i, f := range findings {
		if i >= sample {
			log.WarnContext(ctx, "further findings suppressed",
				slog.Int("suppressed", len(findings)-sample),
				slog.String("hint", "query migration_ctl.recon_finding for the full list"))
			break
		}
		log.WarnContext(ctx, "discrepancy",
			slog.String("table", spec.Source.String()),
			slog.String("kind", string(f.Kind)),
			slog.String("key", f.KeyHash),
			slog.String("detail", f.Detail))
	}
}

func durationOr(s string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// Command repair-worker drains the dead-letter store.
//
// Its job is unglamorous and load-bearing: every record the applier could not
// write is sitting in a table waiting for someone to try again, and if nobody
// does, the migration can never satisfy the cutover gate. Many workers can run
// concurrently — claiming uses SKIP LOCKED — so the drain scales with the size of
// the backlog rather than serialising behind one slow record.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/app"
	"github.com/udaykishore-resu/db-migration-platform/internal/cdc"
	"github.com/udaykishore-resu/db-migration-platform/internal/dlq"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/obs"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
	"github.com/udaykishore-resu/db-migration-platform/internal/sink"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "repair-worker: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	flags := app.ParseFlags()

	a, err := app.Bootstrap(ctx, "repair-worker", flags, nil)
	if err != nil {
		return err
	}

	reg := a.Metrics
	claimed := reg.Counter("migration_dlq_claimed_total", "Dead letters claimed for retry")
	resolved := reg.Counter("migration_dlq_resolved_total", "Dead letters applied successfully", "table")
	requeued := reg.Counter("migration_dlq_rescheduled_total", "Dead letters rescheduled after a failed retry", "table")
	quarantined := reg.Counter("migration_dlq_quarantined_total", "Dead letters that exhausted their budget", "table")
	openGauge := reg.Gauge("migration_dlq_open", "Dead letters still representing unfinished work", "status")
	oldest := reg.Gauge("migration_dlq_oldest_open_age_seconds", "Age of the oldest unfinished dead letter")

	if flags.DryRun {
		a.Log.InfoContext(ctx, "dry run complete")
		a.Close()
		return nil
	}

	applier := sink.New(a.Target, a.Dialect, a.Cfg.Plan, sink.Options{
		MigrationID:         a.Cfg.MigrationID,
		MaxRowsPerStatement: 1, // one record at a time, so a failure is unambiguous
	})
	decoder := &cdc.DebeziumDecoder{KeyColumns: keyColumns(a.Cfg.Plan)}
	policy := a.Cfg.DLQPolicy()
	worker := a.Cfg.Obs.Instance

	return a.Run(ctx, func(ctx context.Context) error {
		ctx = obs.WithComponent(ctx, "repair")
		for {
			if err := ctx.Err(); err != nil {
				return nil
			}

			// Publishing the backlog even when there is nothing to do is what
			// makes "the drain has stalled" visible. A worker that only reports
			// while it is working looks identical to one that has silently
			// stopped finding due records.
			if counts, err := a.Store.Counts(ctx); err == nil {
				openGauge.Set(float64(counts.Pending), "pending")
				openGauge.Set(float64(counts.Retrying), "retrying")
				openGauge.Set(float64(counts.Quarantined), "quarantined")
				oldest.Set(counts.OldestOpenAge.Seconds())
			}

			entries, err := a.Store.Claim(ctx, worker, 100)
			if err != nil {
				a.Log.ErrorContext(ctx, "claim failed", slog.String("error", err.Error()))
				if !retryx.Sleep(ctx, 10*time.Second) {
					return nil
				}
				continue
			}
			if len(entries) == 0 {
				if flags.Once {
					return nil
				}
				if !retryx.Sleep(ctx, 10*time.Second) {
					return nil
				}
				continue
			}
			claimed.Add(float64(len(entries)))

			for _, e := range entries {
				now := time.Now().UTC()
				table := e.SourceTable.String()

				ev, decodeErr := rebuild(decoder, e)
				var applyErr error
				if decodeErr != nil {
					applyErr = decodeErr
				} else {
					_, applyErr = applier.Apply(ctx, []*model.ChangeEvent{ev}, nil)
				}

				if applyErr == nil {
					d := dlq.EvaluateSuccess(e, now)
					if err := a.Store.Resolve(ctx, e, d); err != nil {
						return err
					}
					resolved.Inc(table)
					a.Log.InfoContext(ctx, "dead letter resolved",
						slog.String("table", table), slog.String("key", e.KeyHash),
						slog.String("outcome", d.Reason))
					continue
				}

				d := dlq.Evaluate(e, applyErr, policy, now)
				if err := a.Store.Reschedule(ctx, e, d, applyErr); err != nil {
					return err
				}
				if d.Status == dlq.StatusQuarantined {
					quarantined.Inc(table)
					// Quarantine is a terminal state that needs a person, so it
					// is logged at warning level with the reason attached rather
					// than left to be discovered by querying the table.
					a.Log.WarnContext(ctx, "dead letter quarantined and now needs a decision",
						slog.String("table", table), slog.String("key", e.KeyHash),
						slog.Int("attempts", d.Attempts), slog.String("reason", d.Reason))
					continue
				}
				requeued.Inc(table)
			}
		}
	})
}

// rebuild reconstructs the change event from the stored payload. Replaying the
// original bytes rather than a reconstruction is what makes the retry equivalent
// to the attempt that failed.
func rebuild(d *cdc.DebeziumDecoder, e dlq.Entry) (*model.ChangeEvent, error) {
	if len(e.Payload) == 0 {
		return nil, fmt.Errorf("dead letter %d has no stored payload and cannot be replayed", e.ID)
	}
	ev, err := d.Decode(e.Topic, e.Partition, e.Offset, nil, e.Payload)
	if err != nil {
		return nil, err
	}
	if ev == nil {
		return nil, fmt.Errorf("dead letter %d decoded to nothing", e.ID)
	}
	// Trust the stored key over a re-derived one: the stored key is what the
	// original attempt used, and re-deriving could differ if the plan changed.
	if e.Key.Len() > 0 {
		ev.Key = e.Key
	}
	return ev, nil
}

func keyColumns(plan model.Plan) map[string][]string {
	out := make(map[string][]string, len(plan.Tables))
	for _, t := range plan.Tables {
		out[t.Source.String()] = t.PrimaryKey
	}
	return out
}

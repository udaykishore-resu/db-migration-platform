// Command cdc-applier consumes the change stream and applies it to the target.
//
// It is the steady-state heart of the migration: once the bulk load is done this
// is the only thing standing between the source and the target, and its lag is
// the number the cutover gate watches.
package main

import (
	"context"
	"errors"
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
	"github.com/udaykishore-resu/db-migration-platform/internal/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "cdc-applier: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	flags := app.ParseFlags()

	a, err := app.Bootstrap(ctx, "cdc-applier", flags, nil)
	if err != nil {
		return err
	}

	m := newMetrics(a.Metrics)

	decoder := &cdc.DebeziumDecoder{
		KeyColumns:  keyColumns(a.Cfg.Plan),
		SignalTable: a.Cfg.Extract.SignalSchema + ".snapshot_signal",
	}

	applier := sink.New(a.Target, a.Dialect, a.Cfg.Plan, sink.Options{
		MigrationID:         a.Cfg.MigrationID,
		MaxRowsPerStatement: a.Cfg.Apply.MaxRowsPerStatement,
	})

	positions, err := a.Store.LoadOffsets(ctx)
	if err != nil {
		return err
	}
	a.Log.InfoContext(ctx, "restored stream position",
		slog.Int("partitions", len(positions)),
		slog.String("note", "the stored offset always wins over the configured start position"))

	assignments, err := stream.DiscoverPartitions(ctx, a.Cfg.Kafka.Brokers, a.Cfg.Kafka.Topics)
	if err != nil {
		return err
	}
	consumer, err := stream.NewConsumer(a.Cfg.Kafka, assignments, positions)
	if err != nil {
		return err
	}
	consumer.SetBatchSize(a.Cfg.Apply.BatchSize)
	defer func() { _ = consumer.Close() }()
	a.Health.Set("kafka", true, "")

	if flags.DryRun {
		a.Log.InfoContext(ctx, "dry run complete: configuration, key material, database and stream all reachable")
		a.Close()
		return nil
	}

	policy := a.Cfg.ApplyPolicy()
	dlqPolicy := a.Cfg.DLQPolicy()
	sampled := obs.NewSampledLogger(a.Log, 100)

	return a.Run(ctx, func(ctx context.Context) error {
		ctx = obs.WithComponent(ctx, "applier")
		for {
			if err := ctx.Err(); err != nil {
				return nil
			}

			messages, err := consumer.Poll(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				a.Health.Set("kafka", false, err.Error())
				m.pollErrors.Inc()
				if !retryx.Sleep(ctx, time.Second) {
					return nil
				}
				continue
			}
			a.Health.Set("kafka", true, "")
			if len(messages) == 0 {
				continue
			}

			batchCtx := obs.WithTrace(ctx, obs.NewTraceID())
			events := make([]*model.ChangeEvent, 0, len(messages))
			for _, msg := range messages {
				ev, err := decoder.Decode(msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				if err != nil {
					// A message that cannot be decoded will never decode. Dead
					// letter it immediately rather than stalling the partition.
					m.decodeFailures.Inc(msg.Topic)
					entry := dlq.NewEntry(a.Cfg.MigrationID, &model.ChangeEvent{
						Table: model.TableRef{Name: "unknown"}, Op: model.OpCreate,
						Raw: msg.Value, Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
					}, err, dlqPolicy, time.Now().UTC())
					if perr := a.Store.PutDeadLetter(batchCtx, entry); perr != nil {
						return perr
					}
					continue
				}
				if ev == nil {
					continue // compaction tombstone
				}
				if _, _, isWatermark := decoder.IsWatermark(ev); isWatermark {
					// Watermarks belong to the snapshot window, not the applier.
					m.watermarks.Inc()
					continue
				}
				events = append(events, ev)
			}

			if len(events) == 0 {
				continue
			}

			start := time.Now()
			res, err := applyWithRetry(batchCtx, applier, events, policy)
			if err != nil {
				a.Health.Set("target-db", false, err.Error())
				m.applyErrors.Inc()
				sampled.Warn(batchCtx, "batch failed, backing off", slog.String("error", err.Error()))
				if !retryx.Sleep(ctx, time.Second) {
					return nil
				}
				continue
			}
			a.Health.Set("target-db", true, "")

			m.applied.Add(float64(res.Applied))
			m.deleted.Add(float64(res.Deleted))
			m.coalesced.Add(float64(res.Skipped))
			m.batchSeconds.Observe(time.Since(start).Seconds())

			if len(res.DeadLettered) > 0 {
				entries := make([]dlq.Entry, 0, len(res.DeadLettered))
				now := time.Now().UTC()
				for _, f := range res.DeadLettered {
					entries = append(entries, dlq.NewEntry(a.Cfg.MigrationID, f.Event, f.Err, dlqPolicy, now))
					m.deadLettered.Inc(f.Event.Table.String())
				}
				if err := a.Store.PutDeadLetters(batchCtx, entries); err != nil {
					return err
				}
				a.Log.WarnContext(batchCtx, "records dead-lettered after isolation",
					slog.Int("count", len(entries)))
			}

			// Lag is measured from source commit to apply, not from broker
			// receipt: the question the cutover gate is asking is how far behind
			// the target is, and broker receipt time hides connector delay.
			if last := events[len(events)-1]; last.Lag() > 0 {
				m.lagSeconds.Set(last.Lag().Seconds())
			}
		}
	})
}

// applyWithRetry retries a batch on transient failures and falls back to
// bisection to isolate a genuinely bad record.
func applyWithRetry(ctx context.Context, a *sink.Applier, events []*model.ChangeEvent, p retryx.Policy) (sink.Result, error) {
	var res sink.Result
	err := retryx.Do(ctx, p, func(ctx context.Context, _ int) error {
		var err error
		res, err = a.ApplyWithIsolation(ctx, events, sink.HighWaterMarks(events))
		return err
	})
	if err != nil && errors.Is(err, context.Canceled) {
		return res, nil
	}
	return res, err
}

func keyColumns(plan model.Plan) map[string][]string {
	out := make(map[string][]string, len(plan.Tables))
	for _, t := range plan.Tables {
		out[t.Source.String()] = t.PrimaryKey
	}
	return out
}

type metrics struct {
	applied        *obs.CounterVec
	deleted        *obs.CounterVec
	coalesced      *obs.CounterVec
	deadLettered   *obs.CounterVec
	decodeFailures *obs.CounterVec
	applyErrors    *obs.CounterVec
	pollErrors     *obs.CounterVec
	watermarks     *obs.CounterVec
	batchSeconds   *obs.HistogramVec
	lagSeconds     *obs.GaugeVec
}

func newMetrics(r *obs.Registry) *metrics {
	return &metrics{
		applied:        r.Counter("migration_rows_applied_total", "Rows written to the target"),
		deleted:        r.Counter("migration_rows_tombstoned_total", "Rows tombstoned on the target"),
		coalesced:      r.Counter("migration_events_coalesced_total", "Events dropped as superseded within a batch"),
		deadLettered:   r.Counter("migration_dead_lettered_total", "Records written to the dead-letter store", "table"),
		decodeFailures: r.Counter("migration_decode_failures_total", "Messages that could not be decoded", "topic"),
		applyErrors:    r.Counter("migration_apply_errors_total", "Batches that failed to apply"),
		pollErrors:     r.Counter("migration_poll_errors_total", "Failures reading from the change stream"),
		watermarks:     r.Counter("migration_watermarks_seen_total", "Snapshot watermarks observed in the stream"),
		batchSeconds:   r.Histogram("migration_batch_apply_seconds", "Time to apply one batch", nil),
		lagSeconds:     r.Gauge("migration_replication_lag_seconds", "Source commit to target apply, in seconds"),
	}
}

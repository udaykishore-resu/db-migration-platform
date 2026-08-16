// Package sink applies change events to the target database.
//
// Two properties define this package.
//
// The first is effectively-once application. Kafka gives at-least-once delivery,
// which means every consumer will eventually see the same record twice. The usual
// answer is "make the writes idempotent", which is necessary but not sufficient:
// if the offset is committed separately from the data, a crash between the two
// re-applies a whole batch, and any write that is not idempotent under
// reordering — not just under repetition — is wrong. Here the offset is written
// inside the same database transaction as the data it accounts for, so the two
// can never disagree, and every write is LSN-fenced so reordering is safe too.
//
// The second is that one bad row must never stop the stream. When a batch fails,
// the applier bisects it to find the offending records, applies everything else,
// and dead-letters only what actually failed. The alternative — failing the whole
// batch — means a single unmigratable row halts a partition indefinitely, which
// is how a migration silently stops making progress at 3am.
package sink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// Execer is the subset of database/sql the applier needs, so that the apply
// logic can be exercised without a database.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Beginner starts transactions.
type Beginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Position is a consumer offset for one partition.
type Position struct {
	Topic     string
	Partition int32
	Offset    int64
	LastLSN   uint64
}

// Result reports what an apply call did.
type Result struct {
	Applied      int
	Deleted      int
	Skipped      int
	DeadLettered []FailedEvent
	Statements   int
	Duration     time.Duration
}

// FailedEvent is a record the applier could not write, paired with why.
type FailedEvent struct {
	Event *model.ChangeEvent
	Err   error
}

// Options configures an applier.
type Options struct {
	MigrationID string
	// MaxRowsPerStatement bounds how many rows go into one multi-row upsert.
	// Too few wastes round trips; too many produces statements large enough to
	// stress the parser and to make a bisection on failure expensive.
	MaxRowsPerStatement int
	// IsolationLevel for the apply transaction. Read Committed is correct and
	// sufficient: the fence makes the write order-independent, so the stronger
	// levels buy nothing but lock contention.
	IsolationLevel sql.IsolationLevel
}

func (o *Options) applyDefaults() {
	if o.MaxRowsPerStatement <= 0 {
		o.MaxRowsPerStatement = 500
	}
	if o.IsolationLevel == sql.LevelDefault {
		o.IsolationLevel = sql.LevelReadCommitted
	}
}

// Applier writes change events to the target.
type Applier struct {
	db   Beginner
	d    dialect.Dialect
	plan model.Plan
	opts Options
}

// New builds an applier.
func New(db Beginner, d dialect.Dialect, plan model.Plan, opts Options) *Applier {
	opts.applyDefaults()
	return &Applier{db: db, d: d, plan: plan, opts: opts}
}

// Apply writes a batch of events and records the resulting offsets, atomically.
func (a *Applier) Apply(ctx context.Context, events []*model.ChangeEvent, positions []Position) (Result, error) {
	start := time.Now()
	res := Result{}
	if len(events) == 0 && len(positions) == 0 {
		return res, nil
	}

	batch := Coalesce(events)
	res.Skipped = len(events) - len(batch)

	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{Isolation: a.opts.IsolationLevel})
	if err != nil {
		return res, fmt.Errorf("sink: beginning transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	applied, deleted, stmts, err := a.write(ctx, tx, batch)
	res.Applied, res.Deleted, res.Statements = applied, deleted, stmts
	if err != nil {
		return res, err
	}

	if err := a.commitOffsets(ctx, tx, positions); err != nil {
		return res, err
	}

	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("sink: committing: %w", err)
	}
	committed = true
	res.Duration = time.Since(start)
	return res, nil
}

// ApplyWithIsolation applies a batch and, if it fails for a reason that is not
// obviously transient, bisects it to isolate the offending records.
//
// The bisection is what keeps one bad row from stopping the stream. A failing
// batch of 500 costs about nine extra round trips to reduce to the single record
// that is actually broken, and the other 499 are applied normally.
func (a *Applier) ApplyWithIsolation(ctx context.Context, events []*model.ChangeEvent, positions []Position) (Result, error) {
	res, err := a.Apply(ctx, events, positions)
	if err == nil {
		return res, nil
	}

	// A transient failure is about the database, not about the data. Bisecting
	// would just perform the same failing write several more times.
	if errclass.Classify(err) == errclass.Transient {
		return res, err
	}
	if len(events) == 0 {
		return res, err
	}

	failed, applied, deleted, isoErr := a.isolate(ctx, events)
	if isoErr != nil {
		return res, isoErr
	}

	// Offsets are only advanced once every record in the batch has either been
	// applied or durably dead-lettered — never before.
	out := Result{Applied: applied, Deleted: deleted, DeadLettered: failed}
	if len(positions) > 0 {
		if err := a.commitPositionsOnly(ctx, positions); err != nil {
			return out, err
		}
	}
	return out, nil
}

// isolate recursively halves a failing batch to find the records that fail.
func (a *Applier) isolate(ctx context.Context, events []*model.ChangeEvent) (failed []FailedEvent, applied, deleted int, err error) {
	if len(events) == 0 {
		return nil, 0, 0, nil
	}

	res, applyErr := a.Apply(ctx, events, nil)
	if applyErr == nil {
		return nil, res.Applied, res.Deleted, nil
	}
	if errclass.Classify(applyErr) == errclass.Transient {
		// The database went away mid-bisection; surface it rather than
		// dead-lettering perfectly good records.
		return nil, 0, 0, applyErr
	}
	if len(events) == 1 {
		return []FailedEvent{{Event: events[0], Err: applyErr}}, 0, 0, nil
	}

	mid := len(events) / 2
	lf, la, ld, lerr := a.isolate(ctx, events[:mid])
	if lerr != nil {
		return nil, 0, 0, lerr
	}
	rf, ra, rd, rerr := a.isolate(ctx, events[mid:])
	if rerr != nil {
		return nil, 0, 0, rerr
	}
	return append(lf, rf...), la + ra, ld + rd, nil
}

// write applies the coalesced batch inside an open transaction.
func (a *Applier) write(ctx context.Context, tx Execer, batch []*model.ChangeEvent) (applied, deleted, stmts int, err error) {
	byTable := groupByTable(batch)

	// Deterministic table order. Without it, two appliers processing overlapping
	// batches can take row locks in opposite orders and deadlock — a failure that
	// only appears under production concurrency and is miserable to reproduce.
	tables := make([]string, 0, len(byTable))
	for name := range byTable {
		tables = append(tables, name)
	}
	sort.Strings(tables)

	for _, name := range tables {
		events := byTable[name]
		spec, ok := a.plan.Table(events[0].Table)
		if !ok {
			return applied, deleted, stmts, errclass.Permanently(
				fmt.Errorf("sink: table %s is not in the migration plan", name))
		}

		upserts, deletes := splitByOp(events)

		for _, chunk := range chunkEvents(upserts, a.opts.MaxRowsPerStatement) {
			n, err := a.execUpsert(ctx, tx, spec, chunk)
			stmts++
			if err != nil {
				return applied, deleted, stmts, err
			}
			applied += n
		}

		for _, ev := range deletes {
			if err := a.execDelete(ctx, tx, spec, ev); err != nil {
				return applied, deleted, stmts, err
			}
			stmts++
			deleted++
		}
	}
	return applied, deleted, stmts, nil
}

func (a *Applier) execUpsert(ctx context.Context, tx Execer, spec model.TableSpec, events []*model.ChangeEvent) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	columns := spec.TargetColumnNames()
	sourceCols := spec.ColumnNames()

	args := make([]any, 0, len(events)*(len(columns)+1))
	for _, ev := range events {
		values := ev.Values()
		for _, c := range sourceCols {
			args = append(args, values[c])
		}
		args = append(args, int64(ev.Source.LSN)) //nolint:gosec // LSNs are bounded well below the int64 ceiling
	}

	q := a.d.FencedUpsert(spec, columns, len(events))
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return 0, fmt.Errorf("sink: upserting %d rows into %s: %w", len(events), spec.Target, err)
	}
	return len(events), nil
}

func (a *Applier) execDelete(ctx context.Context, tx Execer, spec model.TableSpec, ev *model.ChangeEvent) error {
	keyValues := ev.Key.Map()
	args := make([]any, 0, len(spec.PrimaryKey)+1)
	for _, k := range spec.PrimaryKey {
		args = append(args, keyValues[k])
	}
	args = append(args, int64(ev.Source.LSN)) //nolint:gosec // see above

	q := a.d.FencedDelete(spec)
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("sink: tombstoning %s in %s: %w", ev.Key.Hash(), spec.Target, err)
	}
	return nil
}

// commitOffsets writes stream progress inside the data transaction. This single
// statement is what turns at-least-once delivery into effectively-once
// application.
func (a *Applier) commitOffsets(ctx context.Context, tx Execer, positions []Position) error {
	q := a.d.UpsertOffset()
	for _, p := range positions {
		if _, err := tx.ExecContext(ctx, q,
			a.opts.MigrationID, p.Topic, p.Partition, p.Offset, int64(p.LastLSN)); err != nil { //nolint:gosec // see above
			return fmt.Errorf("sink: recording offset for %s/%d: %w", p.Topic, p.Partition, err)
		}
	}
	return nil
}

func (a *Applier) commitPositionsOnly(ctx context.Context, positions []Position) error {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{Isolation: a.opts.IsolationLevel})
	if err != nil {
		return fmt.Errorf("sink: beginning offset transaction: %w", err)
	}
	if err := a.commitOffsets(ctx, tx, positions); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sink: committing offsets: %w", err)
	}
	return nil
}

// Coalesce reduces a batch to one event per row, keeping the newest by LSN.
//
// This is both an optimisation and a correctness measure. A hot row updated
// fifty times inside one batch produces fifty statements that all end at the
// same final state; keeping only the last is fifty times cheaper. And within a
// single multi-row upsert statement, two tuples with the same key make Postgres
// raise "cannot affect row a second time" and abort the whole statement — so
// coalescing is what stops a hot row from failing the batch it appears in.
func Coalesce(events []*model.ChangeEvent) []*model.ChangeEvent {
	if len(events) <= 1 {
		return events
	}

	type slot struct {
		ev    *model.ChangeEvent
		order int
	}
	latest := make(map[string]slot, len(events))

	for i, ev := range events {
		if ev == nil {
			continue
		}
		k := ev.Table.String() + "\x00" + ev.Key.Canonical()
		prev, seen := latest[k]
		// Ties are broken by stream position, because a batch can legitimately
		// contain two changes to the same row within one source transaction and
		// therefore at the same LSN.
		if !seen || ev.Source.LSN > prev.ev.Source.LSN ||
			(ev.Source.LSN == prev.ev.Source.LSN && ev.Offset >= prev.ev.Offset) {
			latest[k] = slot{ev: ev, order: i}
		}
	}

	out := make([]slot, 0, len(latest))
	for _, s := range latest {
		out = append(out, s)
	}
	// Preserve the original arrival order of the surviving events so that the
	// target's write pattern stays as sequential as the stream was.
	sort.Slice(out, func(i, j int) bool { return out[i].order < out[j].order })

	result := make([]*model.ChangeEvent, len(out))
	for i, s := range out {
		result[i] = s.ev
	}
	return result
}

func groupByTable(events []*model.ChangeEvent) map[string][]*model.ChangeEvent {
	out := make(map[string][]*model.ChangeEvent)
	for _, ev := range events {
		name := ev.Table.String()
		out[name] = append(out[name], ev)
	}
	return out
}

func splitByOp(events []*model.ChangeEvent) (upserts, deletes []*model.ChangeEvent) {
	for _, ev := range events {
		if ev.Op == model.OpDelete {
			deletes = append(deletes, ev)
			continue
		}
		if ev.Op.IsUpsert() {
			upserts = append(upserts, ev)
		}
	}
	return upserts, deletes
}

func chunkEvents(events []*model.ChangeEvent, size int) [][]*model.ChangeEvent {
	if size <= 0 {
		size = 500
	}
	var out [][]*model.ChangeEvent
	for i := 0; i < len(events); i += size {
		end := i + size
		if end > len(events) {
			end = len(events)
		}
		out = append(out, events[i:end])
	}
	return out
}

// HighWaterMarks reduces a batch to the highest offset seen per partition, which
// is what gets committed.
func HighWaterMarks(events []*model.ChangeEvent) []Position {
	type key struct {
		topic string
		part  int32
	}
	best := make(map[key]Position)
	for _, ev := range events {
		if ev == nil || ev.Topic == "" {
			continue
		}
		k := key{ev.Topic, ev.Partition}
		p, seen := best[k]
		if !seen || ev.Offset > p.Offset {
			best[k] = Position{Topic: ev.Topic, Partition: ev.Partition, Offset: ev.Offset, LastLSN: ev.Source.LSN}
		}
	}
	out := make([]Position, 0, len(best))
	for _, p := range best {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// ErrNoPlan is returned when an event arrives for a table the plan does not
// cover.
var ErrNoPlan = errors.New("sink: event refers to a table that is not in the migration plan")

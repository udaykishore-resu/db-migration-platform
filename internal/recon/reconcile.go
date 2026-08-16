package recon

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// Side identifies which database a query runs against.
type Side string

// The two sides of a comparison.
const (
	SourceSide Side = "source"
	TargetSide Side = "target"
)

// FindingKind classifies a discrepancy.
type FindingKind string

// Kinds of discrepancy, ordered roughly by how alarming they are.
const (
	// MissingInTarget means the source has a row the target does not. Almost
	// always a dropped event or a part that failed to load.
	MissingInTarget FindingKind = "missing_in_target"
	// MissingInSource means the target has a row the source does not. Either a
	// delete that was not replicated, or — more worryingly — a row invented by a
	// bad transform.
	MissingInSource FindingKind = "missing_in_source"
	// ValueMismatch means both sides have the row but its contents differ. This
	// is the one that counts alone will never catch.
	ValueMismatch FindingKind = "value_mismatch"
	// RangeUnresolved means the digests disagreed but the descent hit its budget
	// before isolating specific rows. It is a directive to investigate, not a
	// conclusion.
	RangeUnresolved FindingKind = "range_unresolved"
)

// Repairable reports whether the repair worker can act on the finding
// automatically by re-reading the row from the source and re-applying it.
func (k FindingKind) Repairable() bool {
	return k == MissingInTarget || k == ValueMismatch || k == MissingInSource
}

// Finding is one discrepancy.
type Finding struct {
	Table model.TableRef `json:"table"`
	Kind  FindingKind    `json:"kind"`
	// KeyHash identifies the row without revealing it. Primary keys are
	// frequently PII, and a findings table is read by more people than the data
	// itself, so the raw key is deliberately not stored here.
	KeyHash string `json:"key_hash,omitempty"`
	// Key carries the actual key values for the repair worker to re-read the row.
	// It is stored encrypted at rest; see the dead-letter store.
	Key model.RowKey `json:"-"`
	// Range is populated for range-level findings.
	Range      *dialect.Range `json:"range,omitempty"`
	SourceRows int64          `json:"source_rows,omitempty"`
	TargetRows int64          `json:"target_rows,omitempty"`
	Detail     string         `json:"detail,omitempty"`
	FoundAt    time.Time      `json:"found_at"`
}

// Digester computes a range digest on one side. Implemented by the SQL layer and
// faked in tests.
type Digester interface {
	Digest(ctx context.Context, side Side, r dialect.Range) (dialect.Digest, error)
}

// RowReader fetches row digests keyed by canonical row key for a narrow range.
// It is only called at the leaves of the descent, where the range is small.
type RowReader interface {
	Rows(ctx context.Context, side Side, r dialect.Range) (map[string]RowDigest, error)
}

// RowDigest is one row's identity and content hash.
type RowDigest struct {
	Key    model.RowKey
	Digest string
}

// Options tunes a reconciliation run.
type Options struct {
	// LeafRows is the range size at which the descent stops bisecting and reads
	// actual rows. Too small and the descent makes many round trips; too large
	// and each leaf read is expensive. A few thousand rows is a good balance:
	// one indexed range scan on each side.
	LeafRows int64
	// MaxDepth bounds the descent. Reaching it produces a RangeUnresolved
	// finding rather than an unbounded recursion, which matters when a key
	// column turns out not to be bisectable in practice.
	MaxDepth int
	// MaxFindings stops the run once this many discrepancies are found. A
	// migration with ten thousand bad rows has a systemic problem; enumerating
	// every one of them wastes hours that should be spent fixing the cause.
	MaxFindings int
	// Now is injectable for deterministic tests.
	Now func() time.Time
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{LeafRows: 5000, MaxDepth: 48, MaxFindings: 1000, Now: time.Now}
}

func (o *Options) applyDefaults() {
	if o.LeafRows <= 0 {
		o.LeafRows = 5000
	}
	if o.MaxDepth <= 0 {
		o.MaxDepth = 48
	}
	if o.MaxFindings <= 0 {
		o.MaxFindings = 1000
	}
	if o.Now == nil {
		o.Now = time.Now
	}
}

// Stats reports the cost of a run, which is what justifies running it
// continuously rather than only at the end.
type Stats struct {
	DigestQueries int
	RowReads      int
	MaxDepth      int
	RangesVisited int
}

// Reconciler compares one table across two databases.
type Reconciler struct {
	table    model.TableRef
	keyCol   string
	digester Digester
	rows     RowReader
	opts     Options

	stats Stats
}

// New builds a reconciler for one table.
func New(table model.TableRef, keyColumn string, d Digester, r RowReader, opts Options) *Reconciler {
	opts.applyDefaults()
	return &Reconciler{table: table, keyCol: keyColumn, digester: d, rows: r, opts: opts}
}

// Stats returns the cost of the last run.
func (r *Reconciler) Stats() Stats { return r.stats }

// Reconcile compares a key range and returns every discrepancy found.
//
// The happy path — a range that matches — costs exactly two queries regardless of
// how many rows it covers. That asymmetry is the whole design: correctness is
// cheap to confirm and only incorrectness costs anything to localise.
func (r *Reconciler) Reconcile(ctx context.Context, rng dialect.Range) ([]Finding, error) {
	r.stats = Stats{}
	if rng.Column == "" {
		rng.Column = r.keyCol
	}
	var findings []Finding
	if err := r.descend(ctx, rng, 0, &findings); err != nil {
		return findings, err
	}
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].KeyHash < findings[j].KeyHash })
	return findings, nil
}

func (r *Reconciler) descend(ctx context.Context, rng dialect.Range, depth int, out *[]Finding) error {
	if len(*out) >= r.opts.MaxFindings {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.stats.RangesVisited++
	if depth > r.stats.MaxDepth {
		r.stats.MaxDepth = depth
	}

	src, err := r.digester.Digest(ctx, SourceSide, rng)
	if err != nil {
		return fmt.Errorf("digesting source range: %w", err)
	}
	tgt, err := r.digester.Digest(ctx, TargetSide, rng)
	if err != nil {
		return fmt.Errorf("digesting target range: %w", err)
	}
	r.stats.DigestQueries += 2

	if src.Equal(tgt) {
		return nil
	}

	// Both sides empty but unequal is impossible; both sides empty and equal
	// returned above. A range where both are empty needs no further work.
	if src.Empty() && tgt.Empty() {
		return nil
	}

	// Narrow enough to look at rows directly.
	maxRows := src.Rows
	if tgt.Rows > maxRows {
		maxRows = tgt.Rows
	}
	if maxRows <= r.opts.LeafRows || depth >= r.opts.MaxDepth {
		return r.compareRows(ctx, rng, src, tgt, depth, out)
	}

	mid, ok := r.bisect(rng)
	if !ok {
		// The key space cannot be split further, so read rows regardless of size.
		return r.compareRows(ctx, rng, src, tgt, depth, out)
	}

	lower := dialect.Range{Column: rng.Column, Low: rng.Low, High: mid}
	upper := dialect.Range{Column: rng.Column, Low: mid, High: rng.High}
	if err := r.descend(ctx, lower, depth+1, out); err != nil {
		return err
	}
	return r.descend(ctx, upper, depth+1, out)
}

// bisect picks a midpoint, choosing the bisector from whichever bound is
// available so that the key's actual runtime type drives the decision.
func (r *Reconciler) bisect(rng dialect.Range) (any, bool) {
	sample := rng.Low
	if sample == nil {
		sample = rng.High
	}
	if sample == nil {
		// Fully unbounded on the first descent: assume an integer key space,
		// which is by far the common case, and let the string bisector take over
		// once a concrete bound exists.
		return IntBisector{}.Bisect(nil, nil)
	}
	return BisectorFor(sample).Bisect(rng.Low, rng.High)
}

// compareRows reads both sides of a narrow range and reports exactly which rows
// differ and how.
func (r *Reconciler) compareRows(ctx context.Context, rng dialect.Range, src, tgt dialect.Digest, depth int, out *[]Finding) error {
	if r.rows == nil {
		*out = append(*out, Finding{
			Table: r.table, Kind: RangeUnresolved, Range: &rng,
			SourceRows: src.Rows, TargetRows: tgt.Rows,
			Detail:  "digests disagree and no row reader is configured",
			FoundAt: r.opts.Now(),
		})
		return nil
	}

	if depth >= r.opts.MaxDepth {
		// Budget exhausted. Report the range so an operator can investigate
		// rather than silently returning "no differences found".
		*out = append(*out, Finding{
			Table: r.table, Kind: RangeUnresolved, Range: &rng,
			SourceRows: src.Rows, TargetRows: tgt.Rows,
			Detail:  fmt.Sprintf("descent reached the depth limit of %d with digests still disagreeing", r.opts.MaxDepth),
			FoundAt: r.opts.Now(),
		})
		return nil
	}

	srcRows, err := r.rows.Rows(ctx, SourceSide, rng)
	if err != nil {
		return fmt.Errorf("reading source rows: %w", err)
	}
	tgtRows, err := r.rows.Rows(ctx, TargetSide, rng)
	if err != nil {
		return fmt.Errorf("reading target rows: %w", err)
	}
	r.stats.RowReads += 2

	now := r.opts.Now()

	// Deterministic ordering so that two runs over the same data produce the same
	// findings in the same order, which makes diffing consecutive runs useful.
	srcKeys := make([]string, 0, len(srcRows))
	for k := range srcRows {
		srcKeys = append(srcKeys, k)
	}
	sort.Strings(srcKeys)

	for _, k := range srcKeys {
		if len(*out) >= r.opts.MaxFindings {
			return nil
		}
		s := srcRows[k]
		t, present := tgtRows[k]
		switch {
		case !present:
			*out = append(*out, Finding{
				Table: r.table, Kind: MissingInTarget, KeyHash: s.Key.Hash(), Key: s.Key,
				Detail: "row exists on the source and not on the target", FoundAt: now,
			})
		case s.Digest != t.Digest:
			*out = append(*out, Finding{
				Table: r.table, Kind: ValueMismatch, KeyHash: s.Key.Hash(), Key: s.Key,
				Detail: "row exists on both sides with differing contents", FoundAt: now,
			})
		}
	}

	tgtKeys := make([]string, 0, len(tgtRows))
	for k := range tgtRows {
		tgtKeys = append(tgtKeys, k)
	}
	sort.Strings(tgtKeys)

	for _, k := range tgtKeys {
		if len(*out) >= r.opts.MaxFindings {
			return nil
		}
		if _, present := srcRows[k]; !present {
			t := tgtRows[k]
			*out = append(*out, Finding{
				Table: r.table, Kind: MissingInSource, KeyHash: t.Key.Hash(), Key: t.Key,
				Detail: "row exists on the target and not on the source", FoundAt: now,
			})
		}
	}
	return nil
}

// Summary aggregates findings for reporting and for the cutover gate.
type Summary struct {
	Table           model.TableRef      `json:"table"`
	Total           int                 `json:"total"`
	ByKind          map[FindingKind]int `json:"by_kind"`
	Repairable      int                 `json:"repairable"`
	FirstSeen       time.Time           `json:"first_seen,omitempty"`
	DigestQueries   int                 `json:"digest_queries"`
	RowReads        int                 `json:"row_reads"`
	RangesVisited   int                 `json:"ranges_visited"`
	DeepestDescent  int                 `json:"deepest_descent"`
	ReconciledClean bool                `json:"reconciled_clean"`
}

// Summarise builds a summary from a run's findings and stats.
func Summarise(table model.TableRef, findings []Finding, s Stats) Summary {
	sum := Summary{
		Table: table, Total: len(findings), ByKind: make(map[FindingKind]int),
		DigestQueries: s.DigestQueries, RowReads: s.RowReads,
		RangesVisited: s.RangesVisited, DeepestDescent: s.MaxDepth,
		ReconciledClean: len(findings) == 0,
	}
	for _, f := range findings {
		sum.ByKind[f.Kind]++
		if f.Kind.Repairable() {
			sum.Repairable++
		}
		if sum.FirstSeen.IsZero() || f.FoundAt.Before(sum.FirstSeen) {
			sum.FirstSeen = f.FoundAt
		}
	}
	return sum
}

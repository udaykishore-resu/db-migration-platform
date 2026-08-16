package recon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// fakeDB is an in-memory table that can compute the same digests the SQL layer
// would. Modelling both sides in memory lets the descent algorithm be tested
// exhaustively, including cases that are extremely hard to stage against real
// databases — a single wrong byte in the middle of a billion-row key space, for
// instance.
type fakeDB struct {
	rows map[int64]string // key -> row content
	err  error
	// calls counts digest queries so the tests can assert on cost, not just
	// correctness. A verifier that is correct but linear in table size would not
	// be usable during a migration.
	digestCalls int
	rowCalls    int
}

func newFakeDB(n int) *fakeDB {
	db := &fakeDB{rows: make(map[int64]string, n)}
	for i := int64(0); i < int64(n); i++ {
		db.rows[i] = "v" + strconv.FormatInt(i, 10)
	}
	return db
}

func (f *fakeDB) inRange(k int64, r dialect.Range) bool {
	if r.Low != nil && k <= toI64(r.Low) {
		return false
	}
	if r.High != nil && k > toI64(r.High) {
		return false
	}
	return true
}

func toI64(v any) int64 {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int64:
		return t
	case float64:
		return int64(t)
	default:
		panic(fmt.Sprintf("unexpected bound type %T", v))
	}
}

func rowHash(k int64, v string) string {
	sum := sha256.Sum256([]byte(strconv.FormatInt(k, 10) + "\x1f" + v))
	return hex.EncodeToString(sum[:])
}

func (f *fakeDB) digest(r dialect.Range) dialect.Digest {
	f.digestCalls++
	var count int64
	// Sum the hashes so the digest is order-independent, exactly as the SQL
	// formulation is.
	var lo, hi uint64
	for k, v := range f.rows {
		if !f.inRange(k, r) {
			continue
		}
		count++
		h := rowHash(k, v)
		l, _ := strconv.ParseUint(h[0:15], 16, 64)
		g, _ := strconv.ParseUint(h[16:31], 16, 64)
		lo += l
		hi += g
	}
	return dialect.Digest{
		Rows:   count,
		SumLow: strconv.FormatUint(lo, 10),
		SumHi:  strconv.FormatUint(hi, 10),
	}
}

type pair struct{ src, tgt *fakeDB }

func (p *pair) Digest(_ context.Context, side Side, r dialect.Range) (dialect.Digest, error) {
	db := p.src
	if side == TargetSide {
		db = p.tgt
	}
	if db.err != nil {
		return dialect.Digest{}, db.err
	}
	return db.digest(r), nil
}

func (p *pair) Rows(_ context.Context, side Side, r dialect.Range) (map[string]RowDigest, error) {
	db := p.src
	if side == TargetSide {
		db = p.tgt
	}
	db.rowCalls++
	out := make(map[string]RowDigest)
	for k, v := range db.rows {
		if !db.inRange(k, r) {
			continue
		}
		key := model.NewRowKey(map[string]any{"id": k})
		out[key.Canonical()] = RowDigest{Key: key, Digest: rowHash(k, v)}
	}
	return out, nil
}

func newReconciler(p *pair, opts Options) *Reconciler {
	return New(model.TableRef{Schema: "app", Name: "accounts"}, "id", p, p, opts)
}

func fullRange() dialect.Range {
	return dialect.Range{Column: "id", Low: int64(-1), High: int64(1_000_000)}
}

// A table that matches must cost exactly one digest query per side, no matter
// how large it is. This is the property that makes continuous verification
// affordable during the migration rather than only at the end.
func TestIdenticalTablesCostTwoQueries(t *testing.T) {
	p := &pair{src: newFakeDB(50_000), tgt: newFakeDB(50_000)}
	r := newReconciler(p, DefaultOptions())

	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected no findings on identical tables, got %d: %+v", len(findings), findings[0])
	}
	if got := r.Stats().DigestQueries; got != 2 {
		t.Fatalf("a matching table should cost exactly 2 digest queries, cost %d", got)
	}
	if got := r.Stats().RowReads; got != 0 {
		t.Fatalf("a matching table should read no rows, read %d", got)
	}
}

// A row present on the source and absent on the target is the classic dropped
// event. Counts alone would catch this one, but the descent must also identify
// which row.
func TestDetectsRowMissingInTarget(t *testing.T) {
	p := &pair{src: newFakeDB(20_000), tgt: newFakeDB(20_000)}
	delete(p.tgt.rows, 13_337)

	r := newReconciler(p, DefaultOptions())
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected exactly 1 finding, got %d", len(findings))
	}
	f := findings[0]
	if f.Kind != MissingInTarget {
		t.Fatalf("wrong kind: %s", f.Kind)
	}
	want := model.NewRowKey(map[string]any{"id": int64(13_337)})
	if !f.Key.Equal(want) {
		t.Fatalf("wrong row identified: %s", f.Key.Canonical())
	}
	if f.KeyHash != want.Hash() {
		t.Fatal("finding must carry the safe key hash for reporting")
	}
}

// The failure mode that row counts can never catch: both sides have the row, the
// contents differ. A migration that only compares counts declares this correct.
func TestDetectsValueMismatchThatCountsWouldMiss(t *testing.T) {
	p := &pair{src: newFakeDB(20_000), tgt: newFakeDB(20_000)}
	p.tgt.rows[9_001] = "corrupted"

	if len(p.src.rows) != len(p.tgt.rows) {
		t.Fatal("test setup should leave the counts identical")
	}

	r := newReconciler(p, DefaultOptions())
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Kind != ValueMismatch {
		t.Fatalf("expected one value mismatch, got %+v", findings)
	}
	if !findings[0].Key.Equal(model.NewRowKey(map[string]any{"id": int64(9_001)})) {
		t.Fatalf("wrong row identified: %s", findings[0].Key.Canonical())
	}
}

func TestDetectsRowMissingInSource(t *testing.T) {
	p := &pair{src: newFakeDB(5_000), tgt: newFakeDB(5_000)}
	p.tgt.rows[999_999] = "phantom"

	r := newReconciler(p, DefaultOptions())
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Kind != MissingInSource {
		t.Fatalf("expected one missing-in-source finding, got %+v", findings)
	}
}

// Localising a single bad row in a large table must be logarithmic, not linear.
// If this regresses, verification stops being affordable and quietly gets turned
// off — which is how migrations ship unverified.
func TestDescentCostIsLogarithmic(t *testing.T) {
	p := &pair{src: newFakeDB(100_000), tgt: newFakeDB(100_000)}
	p.tgt.rows[77_777] = "wrong"

	r := newReconciler(p, Options{LeafRows: 1000, MaxDepth: 48, MaxFindings: 100})
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}

	// A linear scan of 100k rows at 1000 per leaf would be 100 leaves and 200+
	// digest queries. Binary descent should be far below that.
	if q := r.Stats().DigestQueries; q > 80 {
		t.Fatalf("descent cost %d digest queries; expected logarithmic behaviour", q)
	}
	if reads := r.Stats().RowReads; reads > 4 {
		t.Fatalf("descent read rows %d times; expected to reach a single leaf", reads)
	}
	t.Logf("localised 1 bad row in 100000 with %d digest queries, %d row reads, depth %d",
		r.Stats().DigestQueries, r.Stats().RowReads, r.Stats().MaxDepth)
}

func TestFindsMultipleScatteredDiscrepancies(t *testing.T) {
	p := &pair{src: newFakeDB(50_000), tgt: newFakeDB(50_000)}
	delete(p.tgt.rows, 100)
	p.tgt.rows[25_000] = "wrong"
	delete(p.tgt.rows, 49_999)
	p.tgt.rows[888_888] = "phantom"

	r := newReconciler(p, DefaultOptions())
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 4 {
		t.Fatalf("expected 4 findings, got %d: %+v", len(findings), findings)
	}

	sum := Summarise(model.TableRef{Name: "accounts"}, findings, r.Stats())
	if sum.ByKind[MissingInTarget] != 2 || sum.ByKind[ValueMismatch] != 1 || sum.ByKind[MissingInSource] != 1 {
		t.Fatalf("unexpected summary: %+v", sum.ByKind)
	}
	if sum.ReconciledClean {
		t.Fatal("summary must not report clean when findings exist")
	}
	if sum.Repairable != 4 {
		t.Fatalf("all four kinds should be repairable, got %d", sum.Repairable)
	}
}

// A migration with thousands of bad rows has a systemic cause; enumerating every
// one wastes hours that should be spent fixing it.
func TestMaxFindingsStopsTheRun(t *testing.T) {
	p := &pair{src: newFakeDB(5_000), tgt: newFakeDB(5_000)}
	for i := int64(0); i < 500; i++ {
		p.tgt.rows[i] = "wrong"
	}

	r := newReconciler(p, Options{LeafRows: 1000, MaxDepth: 48, MaxFindings: 10})
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) > 10 {
		t.Fatalf("expected the run to stop at 10 findings, got %d", len(findings))
	}
}

// Hitting the depth limit must produce an explicit "unresolved" finding. Silently
// returning "no differences" when the digests disagreed would be the single worst
// possible behaviour for a verification tool.
func TestDepthLimitReportsUnresolvedRatherThanClean(t *testing.T) {
	p := &pair{src: newFakeDB(100_000), tgt: newFakeDB(100_000)}
	p.tgt.rows[50_000] = "wrong"

	r := newReconciler(p, Options{LeafRows: 1, MaxDepth: 2, MaxFindings: 100})
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) == 0 {
		t.Fatal("digests disagreed but the run reported clean")
	}
	var sawUnresolved bool
	for _, f := range findings {
		if f.Kind == RangeUnresolved {
			sawUnresolved = true
			if f.Range == nil {
				t.Error("unresolved finding must carry the range to investigate")
			}
		}
	}
	if !sawUnresolved {
		t.Fatalf("expected an unresolved-range finding, got %+v", findings)
	}
}

func TestEmptyTablesOnBothSidesAreClean(t *testing.T) {
	p := &pair{src: newFakeDB(0), tgt: newFakeDB(0)}
	r := newReconciler(p, DefaultOptions())
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("two empty tables must reconcile clean, got %+v", findings)
	}
}

func TestTargetEntirelyEmptyReportsEveryRow(t *testing.T) {
	p := &pair{src: newFakeDB(50), tgt: newFakeDB(0)}
	r := newReconciler(p, DefaultOptions())
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 50 {
		t.Fatalf("expected 50 missing rows, got %d", len(findings))
	}
	for _, f := range findings {
		if f.Kind != MissingInTarget {
			t.Fatalf("unexpected kind %s", f.Kind)
		}
	}
}

func TestDigestErrorsPropagate(t *testing.T) {
	sentinel := errors.New("connection reset by peer")
	p := &pair{src: newFakeDB(100), tgt: newFakeDB(100)}
	p.tgt.err = sentinel

	r := newReconciler(p, DefaultOptions())
	if _, err := r.Reconcile(context.Background(), fullRange()); !errors.Is(err, sentinel) {
		t.Fatalf("expected the underlying error to surface, got %v", err)
	}
}

func TestCancellationStopsTheDescent(t *testing.T) {
	p := &pair{src: newFakeDB(100_000), tgt: newFakeDB(100_000)}
	for i := int64(0); i < 10_000; i++ {
		p.tgt.rows[i] = "wrong"
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := newReconciler(p, DefaultOptions())
	if _, err := r.Reconcile(ctx, fullRange()); err == nil {
		t.Fatal("expected cancellation to surface as an error")
	}
}

// Two runs over the same data must produce identical output, so that diffing
// consecutive runs is meaningful.
func TestFindingsAreDeterministicallyOrdered(t *testing.T) {
	build := func() []Finding {
		p := &pair{src: newFakeDB(10_000), tgt: newFakeDB(10_000)}
		for _, k := range []int64{7, 4_002, 9_998, 512} {
			delete(p.tgt.rows, k)
		}
		r := newReconciler(p, Options{LeafRows: 500, MaxDepth: 48, MaxFindings: 100, Now: func() time.Time { return time.Unix(0, 0) }})
		f, err := r.Reconcile(context.Background(), fullRange())
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	a, b := build(), build()
	if len(a) != len(b) {
		t.Fatalf("run lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].KeyHash != b[i].KeyHash || a[i].Kind != b[i].Kind {
			t.Fatalf("runs differ at index %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestReconcilerWithoutRowReaderReportsUnresolved(t *testing.T) {
	p := &pair{src: newFakeDB(100), tgt: newFakeDB(99)}
	r := New(model.TableRef{Name: "accounts"}, "id", p, nil, Options{LeafRows: 1000, MaxDepth: 4, MaxFindings: 10})
	findings, err := r.Reconcile(context.Background(), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Kind != RangeUnresolved {
		t.Fatalf("expected a single unresolved finding, got %+v", findings)
	}
}

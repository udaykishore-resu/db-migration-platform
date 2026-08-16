package obs

import (
	"math"
	"strings"
	"sync"
	"testing"
)

func TestCounterAddAndLabels(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("events_applied_total", "Events applied", "table", "op")
	c.Inc("accounts", "c")
	c.Add(4, "accounts", "c")
	c.Inc("accounts", "d")

	if got := c.Value("accounts", "c"); got != 5 {
		t.Fatalf("got %v, want 5", got)
	}
	if got := c.Value("accounts", "d"); got != 1 {
		t.Fatalf("got %v, want 1", got)
	}
	// Series are independent.
	if got := c.Value("disputes", "c"); got != 0 {
		t.Fatalf("unseen series should be zero, got %v", got)
	}
}

// A counter that can go backwards silently corrupts every rate() built on it,
// so negative deltas must be dropped rather than applied.
func TestCounterRejectsNegativeDelta(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("x_total", "x")
	c.Add(5)
	c.Add(-3)
	if got := c.Value(); got != 5 {
		t.Fatalf("counter went backwards: got %v, want 5", got)
	}
}

func TestGaugeUpAndDown(t *testing.T) {
	r := NewRegistry()
	g := r.Gauge("inflight_chunks", "Chunks in flight", "table")
	g.Inc("accounts")
	g.Inc("accounts")
	g.Dec("accounts")
	if got := g.Value("accounts"); got != 1 {
		t.Fatalf("got %v, want 1", got)
	}
	g.Set(42, "accounts")
	if got := g.Value("accounts"); got != 42 {
		t.Fatalf("got %v, want 42", got)
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("apply_seconds", "Apply latency", []float64{0.1, 1, 10}, "table")
	for _, v := range []float64{0.05, 0.5, 5, 50} {
		h.Observe(v, "accounts")
	}
	if got := h.Count("accounts"); got != 4 {
		t.Fatalf("count %d, want 4", got)
	}
	if got := h.Sum("accounts"); math.Abs(got-55.55) > 1e-9 {
		t.Fatalf("sum %v, want 55.55", got)
	}

	var b strings.Builder
	if err := r.Write(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		`apply_seconds_bucket{table="accounts",le="0.1"} 1`,
		`apply_seconds_bucket{table="accounts",le="1"} 2`,
		`apply_seconds_bucket{table="accounts",le="10"} 3`,
		`apply_seconds_bucket{table="accounts",le="+Inf"} 4`,
		`apply_seconds_count{table="accounts"} 4`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q\n---\n%s", want, out)
		}
	}
}

func TestExpositionFormatHeadersAndOrdering(t *testing.T) {
	r := NewRegistry()
	r.Counter("b_total", "B help").Inc()
	r.Counter("a_total", "A help").Inc()

	var b strings.Builder
	if err := r.Write(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if !strings.Contains(out, "# HELP a_total A help") || !strings.Contains(out, "# TYPE a_total counter") {
		t.Fatalf("missing HELP/TYPE headers:\n%s", out)
	}
	if strings.Index(out, "a_total") > strings.Index(out, "b_total") {
		t.Fatalf("families must be emitted in sorted order:\n%s", out)
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("dlq_total", "DLQ", "reason")
	c.Inc(`bad "quoted" \ value`)

	var b strings.Builder
	if err := r.Write(&b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), `reason="bad \"quoted\" \\ value"`) {
		t.Fatalf("label not escaped:\n%s", b.String())
	}
}

func TestUnlabelledMetricEmitsNoBraces(t *testing.T) {
	r := NewRegistry()
	r.Gauge("replication_lag_seconds", "Lag").Set(1.5)
	var b strings.Builder
	if err := r.Write(&b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "replication_lag_seconds 1.5") {
		t.Fatalf("unexpected output:\n%s", b.String())
	}
}

func TestWrongLabelCardinalityPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on label cardinality mismatch")
		}
	}()
	r := NewRegistry()
	r.Counter("x_total", "x", "a", "b").Inc("only-one")
}

func TestConcurrentUpdatesAreRaceFree(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("events_total", "events", "table")
	h := r.Histogram("lat_seconds", "lat", nil, "table")
	g := r.Gauge("inflight", "inflight", "table")

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.Inc("accounts")
				h.Observe(0.01, "accounts")
				g.Inc("accounts")
				g.Dec("accounts")
			}
		}()
	}
	wg.Wait()

	if got := c.Value("accounts"); got != 32*200 {
		t.Fatalf("counter lost updates: got %v, want %d", got, 32*200)
	}
	if got := h.Count("accounts"); got != 32*200 {
		t.Fatalf("histogram lost updates: got %d", got)
	}
	if got := g.Value("accounts"); got != 0 {
		t.Fatalf("gauge should net to zero, got %v", got)
	}
}

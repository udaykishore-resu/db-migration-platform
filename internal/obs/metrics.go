// Package obs provides logging, metrics and health endpoints.
//
// The metrics implementation is deliberately small and dependency-free. A
// migration platform that moves regulated data has a supply chain that gets
// audited; every third-party module in the binary is a line item in that audit.
// Prometheus text exposition format is simple enough to emit directly, so the
// platform does, and scrapers cannot tell the difference.
package obs

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Kind is the Prometheus metric type.
type Kind string

// Supported metric kinds.
const (
	KindCounter   Kind = "counter"
	KindGauge     Kind = "gauge"
	KindHistogram Kind = "histogram"
)

// DefaultBuckets are latency buckets in seconds, spanning the sub-millisecond
// batch apply through to a multi-minute stalled chunk.
var DefaultBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300,
}

// Registry holds every metric family exposed by a process.
type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{families: make(map[string]*family)}
}

type family struct {
	name    string
	help    string
	kind    Kind
	labels  []string
	buckets []float64

	mu     sync.RWMutex
	series map[string]*series
}

type series struct {
	labelValues []string

	mu    sync.Mutex
	value float64

	// Histogram state.
	counts []uint64
	sum    float64
	count  uint64
}

func (r *Registry) family(name, help string, kind Kind, labels []string, buckets []float64) *family {
	r.mu.RLock()
	f, ok := r.families[name]
	r.mu.RUnlock()
	if ok {
		return f
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.families[name]; ok {
		return f
	}
	f = &family{
		name:    name,
		help:    help,
		kind:    kind,
		labels:  append([]string(nil), labels...),
		buckets: append([]float64(nil), buckets...),
		series:  make(map[string]*series),
	}
	r.families[name] = f
	return f
}

// seriesFor resolves (and lazily creates) the series for a set of label values.
// Mismatched cardinality is a programming error and panics loudly in tests
// rather than silently producing a wrong time series in production.
func (f *family) seriesFor(values []string) *series {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metric %s: expected %d label values %v, got %d %v",
			f.name, len(f.labels), f.labels, len(values), values))
	}
	key := strings.Join(values, "\xff")

	f.mu.RLock()
	s, ok := f.series[key]
	f.mu.RUnlock()
	if ok {
		return s
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.series[key]; ok {
		return s
	}
	s = &series{labelValues: append([]string(nil), values...)}
	if f.kind == KindHistogram {
		s.counts = make([]uint64, len(f.buckets))
	}
	f.series[key] = s
	return s
}

// CounterVec is a monotonically increasing counter partitioned by labels.
type CounterVec struct{ f *family }

// Counter registers or returns a counter family.
func (r *Registry) Counter(name, help string, labels ...string) *CounterVec {
	return &CounterVec{f: r.family(name, help, KindCounter, labels, nil)}
}

// Inc adds one to the series identified by the label values.
func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Add increases the series by delta. Negative deltas are ignored, since a
// counter that goes backwards breaks every rate() query built on it.
func (c *CounterVec) Add(delta float64, labelValues ...string) {
	if delta < 0 {
		return
	}
	s := c.f.seriesFor(labelValues)
	s.mu.Lock()
	s.value += delta
	s.mu.Unlock()
}

// Value reads the current value, primarily for tests.
func (c *CounterVec) Value(labelValues ...string) float64 {
	s := c.f.seriesFor(labelValues)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}

// GaugeVec is a value that can go up or down, partitioned by labels.
type GaugeVec struct{ f *family }

// Gauge registers or returns a gauge family.
func (r *Registry) Gauge(name, help string, labels ...string) *GaugeVec {
	return &GaugeVec{f: r.family(name, help, KindGauge, labels, nil)}
}

// Set replaces the value of the series.
func (g *GaugeVec) Set(v float64, labelValues ...string) {
	s := g.f.seriesFor(labelValues)
	s.mu.Lock()
	s.value = v
	s.mu.Unlock()
}

// Add adjusts the series by delta, which may be negative.
func (g *GaugeVec) Add(delta float64, labelValues ...string) {
	s := g.f.seriesFor(labelValues)
	s.mu.Lock()
	s.value += delta
	s.mu.Unlock()
}

// Inc adds one. Dec subtracts one.
func (g *GaugeVec) Inc(labelValues ...string) { g.Add(1, labelValues...) }

// Dec subtracts one from the series.
func (g *GaugeVec) Dec(labelValues ...string) { g.Add(-1, labelValues...) }

// Value reads the current value, primarily for tests.
func (g *GaugeVec) Value(labelValues ...string) float64 {
	s := g.f.seriesFor(labelValues)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}

// HistogramVec observes a distribution, partitioned by labels.
type HistogramVec struct{ f *family }

// Histogram registers or returns a histogram family. Passing nil buckets uses
// DefaultBuckets.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	sorted := append([]float64(nil), buckets...)
	sort.Float64s(sorted)
	return &HistogramVec{f: r.family(name, help, KindHistogram, labels, sorted)}
}

// Observe records a single measurement.
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	s := h.f.seriesFor(labelValues)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sum += v
	s.count++
	for i, ub := range h.f.buckets {
		if v <= ub {
			s.counts[i]++
		}
	}
}

// Count returns the number of observations, primarily for tests.
func (h *HistogramVec) Count(labelValues ...string) uint64 {
	s := h.f.seriesFor(labelValues)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// Sum returns the sum of observations, primarily for tests.
func (h *HistogramVec) Sum(labelValues ...string) float64 {
	s := h.f.seriesFor(labelValues)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sum
}

// Write emits the registry in Prometheus text exposition format.
func (r *Registry) Write(w io.Writer) error {
	r.mu.RLock()
	names := make([]string, 0, len(r.families))
	for name := range r.families {
		names = append(names, name)
	}
	r.mu.RUnlock()
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		r.mu.RLock()
		f := r.families[name]
		r.mu.RUnlock()
		f.write(&b)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func (f *family) write(b *strings.Builder) {
	f.mu.RLock()
	keys := make([]string, 0, len(f.series))
	for k := range f.series {
		keys = append(keys, k)
	}
	f.mu.RUnlock()
	if len(keys) == 0 {
		return
	}
	sort.Strings(keys)

	fmt.Fprintf(b, "# HELP %s %s\n", f.name, escapeHelp(f.help))
	fmt.Fprintf(b, "# TYPE %s %s\n", f.name, f.kind)

	for _, k := range keys {
		f.mu.RLock()
		s := f.series[k]
		f.mu.RUnlock()

		s.mu.Lock()
		switch f.kind {
		case KindHistogram:
			// counts are maintained cumulatively by Observe, which is what the
			// exposition format requires for the le buckets.
			for i, ub := range f.buckets {
				b.WriteString(f.name)
				b.WriteString("_bucket")
				writeLabels(b, f.labels, s.labelValues, "le", formatFloat(ub))
				b.WriteByte(' ')
				b.WriteString(strconv.FormatUint(s.counts[i], 10))
				b.WriteByte('\n')
			}
			b.WriteString(f.name)
			b.WriteString("_bucket")
			writeLabels(b, f.labels, s.labelValues, "le", "+Inf")
			fmt.Fprintf(b, " %d\n", s.count)

			b.WriteString(f.name)
			b.WriteString("_sum")
			writeLabels(b, f.labels, s.labelValues, "", "")
			fmt.Fprintf(b, " %s\n", formatFloat(s.sum))

			b.WriteString(f.name)
			b.WriteString("_count")
			writeLabels(b, f.labels, s.labelValues, "", "")
			fmt.Fprintf(b, " %d\n", s.count)
		default:
			b.WriteString(f.name)
			writeLabels(b, f.labels, s.labelValues, "", "")
			b.WriteByte(' ')
			b.WriteString(formatFloat(s.value))
			b.WriteByte('\n')
		}
		s.mu.Unlock()
	}
}

func writeLabels(b *strings.Builder, names, values []string, extraName, extraValue string) {
	if len(names) == 0 && extraName == "" {
		return
	}
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(values[i]))
		b.WriteByte('"')
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extraName)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(extraValue))
		b.WriteByte('"')
	}
	b.WriteByte('}')
}

func escapeLabel(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}

func escapeHelp(s string) string {
	r := strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	return r.Replace(s)
}

func formatFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	if math.IsInf(f, -1) {
		return "-Inf"
	}
	if math.IsNaN(f) {
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// Handler serves the registry over HTTP in Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := r.Write(w); err != nil {
			http.Error(w, "failed to render metrics", http.StatusInternalServerError)
		}
	})
}

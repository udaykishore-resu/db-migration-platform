package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

type ctxKey int

const (
	ctxKeyTrace ctxKey = iota
	ctxKeyComponent
)

// NewLogger builds the process logger. Output is JSON so that it can be shipped
// to Splunk or CloudWatch and parsed without a grok pattern, and the service
// name is bound once so every line is attributable.
func NewLogger(w io.Writer, level, service, instance string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: ParseLevel(level),
		// Source location is expensive and rarely useful in a data pipeline;
		// the message plus component is enough to locate the code.
		AddSource: false,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			// Normalise the timestamp key so downstream parsers agree.
			if a.Key == slog.TimeKey {
				a.Key = "ts"
			}
			if a.Key == slog.MessageKey {
				a.Key = "msg"
			}
			if a.Key == slog.LevelKey {
				a.Key = "level"
			}
			return a
		},
	})
	return slog.New(&redactHandler{inner: h}).With(
		slog.String("service", service),
		slog.String("instance", instance),
	)
}

// ParseLevel maps a configuration string to a slog level, defaulting to info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// sensitiveKeys are attribute names that must never reach a log sink. In a
// migration that moves regulated data the fastest route to a reportable incident
// is a well-meaning debug line that prints a row image, so the logger refuses to
// emit these keys regardless of what the calling code passes.
var sensitiveKeys = map[string]bool{
	"row":        true,
	"before":     true,
	"after":      true,
	"values":     true,
	"payload":    true,
	"plaintext":  true,
	"pii":        true,
	"key_values": true,
	"password":   true,
	"secret":     true,
	"token":      true,
	"credential": true,
	"dsn":        true,
	"url":        true,
}

// redactHandler drops sensitive attributes before they are serialised.
type redactHandler struct{ inner slog.Handler }

func (h *redactHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *redactHandler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	if tid := TraceID(ctx); tid != "" {
		out.AddAttrs(slog.String("trace_id", tid))
	}
	if c := Component(ctx); c != "" {
		out.AddAttrs(slog.String("component", c))
	}
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redactAttr(a))
		return true
	})
	return h.inner.Handle(ctx, out)
}

func (h *redactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	safe := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		safe = append(safe, redactAttr(a))
	}
	return &redactHandler{inner: h.inner.WithAttrs(safe)}
}

func (h *redactHandler) WithGroup(name string) slog.Handler {
	return &redactHandler{inner: h.inner.WithGroup(name)}
}

func redactAttr(a slog.Attr) slog.Attr {
	if sensitiveKeys[strings.ToLower(a.Key)] {
		return slog.String(a.Key, "[redacted]")
	}
	if a.Value.Kind() == slog.KindGroup {
		grp := a.Value.Group()
		safe := make([]any, 0, len(grp))
		for _, g := range grp {
			safe = append(safe, redactAttr(g))
		}
		return slog.Group(a.Key, safe...)
	}
	return a
}

// NewTraceID returns a random 128-bit trace identifier in W3C hex form.
func NewTraceID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A trace ID is diagnostic; degrade rather than fail the operation.
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// WithTrace binds a trace identifier to the context so that every log line
// emitted downstream is correlatable.
func WithTrace(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		traceID = NewTraceID()
	}
	return context.WithValue(ctx, ctxKeyTrace, traceID)
}

// TraceID reads the trace identifier bound to the context, if any.
func TraceID(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyTrace).(string); ok {
		return v
	}
	return ""
}

// WithComponent labels the context with the subsystem doing the work.
func WithComponent(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, ctxKeyComponent, name)
}

// Component reads the subsystem label bound to the context, if any.
func Component(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyComponent).(string); ok {
		return v
	}
	return ""
}

// Traceparent renders a W3C traceparent header for the context, so that lag
// investigations can be correlated across the connector, the applier and the
// target database's own tracing.
func Traceparent(ctx context.Context) string {
	tid := TraceID(ctx)
	if tid == "" {
		return ""
	}
	var span [8]byte
	if _, err := rand.Read(span[:]); err != nil {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-01", tid, hex.EncodeToString(span[:]))
}

// SampledLogger wraps a logger so that a high-frequency message is emitted at
// most once per N occurrences. The apply path can process tens of thousands of
// events per second; logging each retry would cost more than the retry.
type SampledLogger struct {
	log  *slog.Logger
	rate int

	mu     sync.Mutex
	counts map[string]int
}

// NewSampledLogger returns a logger that emits one line in every rate calls for
// a given message key. A rate below 1 disables sampling.
func NewSampledLogger(log *slog.Logger, rate int) *SampledLogger {
	if rate < 1 {
		rate = 1
	}
	return &SampledLogger{log: log, rate: rate, counts: make(map[string]int)}
}

// Warn emits at most one line per rate occurrences of the same message.
func (s *SampledLogger) Warn(ctx context.Context, msg string, args ...any) {
	if !s.should(msg) {
		return
	}
	s.log.WarnContext(ctx, msg, args...)
}

// Info emits at most one line per rate occurrences of the same message.
func (s *SampledLogger) Info(ctx context.Context, msg string, args ...any) {
	if !s.should(msg) {
		return
	}
	s.log.InfoContext(ctx, msg, args...)
}

func (s *SampledLogger) should(msg string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.counts[msg]
	s.counts[msg] = n + 1
	return n%s.rate == 0
}

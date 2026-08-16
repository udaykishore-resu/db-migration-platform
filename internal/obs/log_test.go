package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not valid JSON: %q (%v)", line, err)
		}
		out = append(out, m)
	}
	return out
}

// The single fastest route to a reportable incident in a migration is a debug
// line that prints a row image. The handler must refuse to emit those keys even
// when the calling code asks it to.
func TestSensitiveAttributesAreRedacted(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "debug", "cdc-applier", "test-1")
	log.Info("applied row",
		slog.Any("after", map[string]any{"ssn": "123-45-6789"}),
		slog.String("password", "hunter2"),
		slog.String("table", "accounts"),
	)

	lines := decodeLines(t, &buf)
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	m := lines[0]
	if m["after"] != "[redacted]" {
		t.Errorf("row image leaked: %v", m["after"])
	}
	if m["password"] != "[redacted]" {
		t.Errorf("password leaked: %v", m["password"])
	}
	if m["table"] != "accounts" {
		t.Errorf("non-sensitive attribute should survive, got %v", m["table"])
	}
	if strings.Contains(buf.String(), "123-45-6789") {
		t.Error("PII value present in raw output")
	}
}

func TestRedactionAppliesToWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "info", "svc", "i").With(slog.String("secret", "shhh"))
	log.Info("hello")
	if strings.Contains(buf.String(), "shhh") {
		t.Fatalf("secret bound via With leaked: %s", buf.String())
	}
}

func TestRedactionRecursesIntoGroups(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "info", "svc", "i")
	log.Info("event", slog.Group("event", slog.String("table", "accounts"), slog.Any("before", "sensitive")))
	if strings.Contains(buf.String(), "sensitive") {
		t.Fatalf("nested sensitive attribute leaked: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "accounts") {
		t.Fatalf("nested safe attribute dropped: %s", buf.String())
	}
}

func TestTraceAndComponentAreAttached(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "info", "svc", "i")
	ctx := WithComponent(WithTrace(context.Background(), "abc123"), "applier")
	log.InfoContext(ctx, "working")

	m := decodeLines(t, &buf)[0]
	if m["trace_id"] != "abc123" {
		t.Errorf("trace_id missing, got %v", m["trace_id"])
	}
	if m["component"] != "applier" {
		t.Errorf("component missing, got %v", m["component"])
	}
	if m["service"] != "svc" {
		t.Errorf("service missing, got %v", m["service"])
	}
}

func TestStandardKeysAreRenamed(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, "info", "svc", "i").Info("x")
	m := decodeLines(t, &buf)[0]
	for _, k := range []string{"ts", "level", "msg"} {
		if _, ok := m[k]; !ok {
			t.Errorf("expected normalised key %q in %v", k, m)
		}
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug, "DEBUG": slog.LevelDebug,
		"warn": slog.LevelWarn, "warning": slog.LevelWarn,
		"error": slog.LevelError, "": slog.LevelInfo, "nonsense": slog.LevelInfo,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "warn", "svc", "i")
	log.Info("should not appear")
	log.Warn("should appear")
	if strings.Contains(buf.String(), "should not appear") {
		t.Fatal("info line emitted at warn level")
	}
	if !strings.Contains(buf.String(), "should appear") {
		t.Fatal("warn line suppressed")
	}
}

func TestSampledLoggerEmitsOneInN(t *testing.T) {
	var buf bytes.Buffer
	s := NewSampledLogger(NewLogger(&buf, "info", "svc", "i"), 10)
	for i := 0; i < 30; i++ {
		s.Warn(context.Background(), "retrying batch")
	}
	if n := len(decodeLines(t, &buf)); n != 3 {
		t.Fatalf("expected 3 sampled lines out of 30, got %d", n)
	}
}

func TestTraceparentFormat(t *testing.T) {
	ctx := WithTrace(context.Background(), NewTraceID())
	tp := Traceparent(ctx)
	parts := strings.Split(tp, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 {
		t.Fatalf("malformed traceparent %q", tp)
	}
	if Traceparent(context.Background()) != "" {
		t.Fatal("traceparent without a trace should be empty")
	}
}

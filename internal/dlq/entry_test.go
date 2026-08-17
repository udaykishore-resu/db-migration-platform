package dlq

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
)

func policy() retryx.Policy {
	return retryx.Policy{Base: time.Second, Max: time.Hour, Multiplier: 2, MaxAttempts: 5, Jitter: false}
}

func event() *model.ChangeEvent {
	return &model.ChangeEvent{
		Table:     model.TableRef{Schema: "app", Name: "accounts"},
		Op:        model.OpUpdate,
		Key:       model.NewRowKey(map[string]any{"account_id": "A-1"}),
		Source:    model.SourceMeta{LSN: 4242},
		Raw:       []byte(`{"op":"u"}`),
		Topic:     "cdc.app.accounts",
		Partition: 3,
		Offset:    99,
	}
}

var now = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

// A permanent error will fail identically forever. Entering a retry loop wastes
// the budget and delays the operator's discovery of a real data problem.
func TestPermanentErrorQuarantinesImmediately(t *testing.T) {
	e := NewEntry("mig-1", event(), errclass.Permanently(errors.New("value too long for column")), policy(), now)
	if e.Status != StatusQuarantined {
		t.Fatalf("expected immediate quarantine, got %s", e.Status)
	}
	if e.Attempts != 1 {
		t.Fatalf("expected 1 attempt recorded, got %d", e.Attempts)
	}
	if !e.NextRetryAt.IsZero() {
		t.Fatal("a quarantined entry must not be scheduled for retry")
	}
}

func TestTransientErrorSchedulesRetry(t *testing.T) {
	e := NewEntry("mig-1", event(), errors.New("deadlock detected"), policy(), now)
	if e.Status != StatusPending {
		t.Fatalf("expected pending, got %s", e.Status)
	}
	if !e.NextRetryAt.Equal(now.Add(time.Second)) {
		t.Fatalf("expected the first retry one base delay out, got %s", e.NextRetryAt)
	}
	if e.ErrorClass != errclass.Transient {
		t.Fatalf("expected transient classification, got %s", e.ErrorClass)
	}
}

// The entry must carry everything needed to replay the record byte-for-byte,
// rather than requiring an operator to reconstruct it from a partial parse.
func TestEntryCapturesReplayContext(t *testing.T) {
	ev := event()
	e := NewEntry("mig-1", ev, errors.New("connection reset"), policy(), now)

	if !bytes.Equal(e.Payload, ev.Raw) {
		t.Error("original payload not retained")
	}
	if e.Topic != ev.Topic || e.Partition != ev.Partition || e.Offset != ev.Offset {
		t.Error("stream position not retained")
	}
	if e.SourceLSN != ev.Source.LSN {
		t.Error("source LSN not retained; the fenced re-apply needs it")
	}
	if !e.Key.Equal(ev.Key) {
		t.Error("row key not retained")
	}
	// Reporting must be possible without exposing the key.
	if e.KeyHash != ev.Key.Hash() || strings.Contains(e.KeyHash, "A-1") {
		t.Error("entry must carry a safe key hash for reporting")
	}
}

func TestBackoffGrowsAcrossAttempts(t *testing.T) {
	p := policy()
	e := Entry{Attempts: 0, FirstSeenAt: now}

	var delays []time.Duration
	for i := 0; i < 4; i++ {
		d := Evaluate(e, errors.New("lock wait timeout"), p, now)
		if d.Status != StatusPending {
			t.Fatalf("attempt %d unexpectedly %s: %s", i, d.Status, d.Reason)
		}
		delays = append(delays, d.NextRetryAt.Sub(now))
		e.Attempts = d.Attempts
	}
	for i := 1; i < len(delays); i++ {
		if delays[i] <= delays[i-1] {
			t.Fatalf("backoff did not grow: %v", delays)
		}
	}
}

// A record that keeps failing is a signal. Retrying it forever turns that signal
// into background noise, so the budget must terminate in quarantine.
func TestBudgetExhaustionQuarantines(t *testing.T) {
	p := policy()
	e := Entry{Attempts: p.MaxAttempts - 1, FirstSeenAt: now}
	d := Evaluate(e, errors.New("connection refused"), p, now)

	if d.Status != StatusQuarantined {
		t.Fatalf("expected quarantine at the budget limit, got %s", d.Status)
	}
	if !strings.Contains(d.Reason, "budget") {
		t.Fatalf("quarantine reason should explain the budget: %q", d.Reason)
	}
	// The entry must explain itself: an operator finding it a week later should
	// not have to correlate logs to learn why it stopped.
	if !strings.Contains(d.Reason, "connection refused") {
		t.Fatalf("quarantine reason should carry the last error: %q", d.Reason)
	}
}

func TestPermanentErrorQuarantinesEvenWithBudgetRemaining(t *testing.T) {
	d := Evaluate(Entry{Attempts: 0}, errclass.Permanently(errors.New("bad row")), policy(), now)
	if d.Status != StatusQuarantined {
		t.Fatalf("expected quarantine, got %s", d.Status)
	}
	if !strings.Contains(d.Reason, "retrying cannot succeed") {
		t.Fatalf("reason should say why retrying is pointless: %q", d.Reason)
	}
}

// An unknown error must still be retried — a novel transient failure should not
// dead-letter good data — but within the same bounded budget.
func TestUnknownErrorIsRetriedWithinBudget(t *testing.T) {
	d := Evaluate(Entry{Attempts: 0}, errors.New("something nobody has seen"), policy(), now)
	if d.Status != StatusPending {
		t.Fatalf("unknown errors should be retried, got %s", d.Status)
	}
	if !strings.Contains(d.Reason, string(errclass.Unknown)) {
		t.Fatalf("reason should record the classification: %q", d.Reason)
	}
}

func TestSuccessResolvesAndReportsDuration(t *testing.T) {
	e := Entry{Attempts: 3, FirstSeenAt: now.Add(-90 * time.Second)}
	d := EvaluateSuccess(e, now)
	if d.Status != StatusResolved {
		t.Fatalf("got %s", d.Status)
	}
	if d.Attempts != 4 {
		t.Fatalf("expected 4 attempts, got %d", d.Attempts)
	}
	if !strings.Contains(d.Reason, "1m30s") {
		t.Fatalf("reason should report how long it took: %q", d.Reason)
	}
}

// Cutting over with unapplied records means knowingly shipping an incomplete
// target, so every unfinished state must count as open.
func TestOpenStatusesBlockCutover(t *testing.T) {
	for _, s := range []Status{StatusPending, StatusRetrying, StatusQuarantined} {
		if !s.Open() {
			t.Errorf("%s must count as open work", s)
		}
	}
	for _, s := range []Status{StatusResolved, StatusDiscarded} {
		if s.Open() {
			t.Errorf("%s must not count as open work", s)
		}
	}
}

func TestCountsCleanliness(t *testing.T) {
	if !(Counts{Resolved: 100, Discarded: 3}).Clean() {
		t.Error("resolved and discarded records must not block cutover")
	}
	if (Counts{Resolved: 100, Quarantined: 1}).Clean() {
		t.Error("a single quarantined record must block cutover")
	}
	if got := (Counts{Pending: 2, Retrying: 3, Quarantined: 1}).Open(); got != 6 {
		t.Errorf("open count = %d, want 6", got)
	}
}

// The store is read by more people than the data itself, so a huge or
// multi-line driver error must be trimmed before it lands there.
func TestErrorSummaryIsTrimmed(t *testing.T) {
	long := strings.Repeat("x", 5000) + "\nsecond line"
	e := NewEntry("m", event(), errors.New(long), policy(), now)
	if len(e.LastError) > 1100 {
		t.Fatalf("error not truncated: %d characters", len(e.LastError))
	}
	if strings.Contains(e.LastError, "\n") {
		t.Fatal("newlines must be stripped so the column stays greppable")
	}
	if !strings.HasSuffix(e.LastError, "(truncated)") {
		t.Fatal("truncation must be signposted rather than silent")
	}
}

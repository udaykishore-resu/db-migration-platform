package retryx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
)

func TestDelayGrowsExponentiallyAndCaps(t *testing.T) {
	p := Policy{Base: time.Second, Max: 10 * time.Second, Multiplier: 2, Jitter: false}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := p.Delay(i + 1); got != w {
			t.Errorf("attempt %d: got %s, want %s", i+1, got, w)
		}
	}
}

// Full jitter is the property that prevents a synchronised retry storm from
// re-killing a database that is trying to recover. Verify the delays actually
// spread across the window rather than clustering at the top.
func TestFullJitterSpreadsAcrossWindow(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: true}
	const attempt = 5 // window is 16s
	window := 16 * time.Second

	var below, above int
	for i := 0; i < 500; i++ {
		d := p.Delay(attempt)
		if d < 0 || d > window {
			t.Fatalf("delay %s outside [0,%s]", d, window)
		}
		if d < window/2 {
			below++
		} else {
			above++
		}
	}
	// With full jitter roughly half should land in each half of the window.
	if below < 150 || above < 150 {
		t.Fatalf("jitter not spread: %d below midpoint, %d above (want both > 150)", below, above)
	}
}

func TestDelayNeverExceedsMaxEvenOnOverflow(t *testing.T) {
	p := Policy{Base: time.Second, Max: 30 * time.Second, Multiplier: 10, Jitter: false}
	if got := p.Delay(500); got != 30*time.Second {
		t.Fatalf("expected clamp to max on overflow, got %s", got)
	}
}

func TestDoStopsImmediatelyOnPermanentError(t *testing.T) {
	p := Policy{Base: time.Millisecond, Max: time.Millisecond, Multiplier: 1, MaxAttempts: 10, Jitter: false}
	var calls int
	err := Do(context.Background(), p, func(context.Context, int) error {
		calls++
		return errclass.Permanently(errors.New("bad row"))
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("permanent error must not be retried, got %d calls", calls)
	}
}

func TestDoRetriesTransientUntilSuccess(t *testing.T) {
	p := Policy{Base: time.Millisecond, Max: time.Millisecond, Multiplier: 1, MaxAttempts: 10, Jitter: false}
	var calls int
	err := Do(context.Background(), p, func(_ context.Context, attempt int) error {
		calls++
		if attempt < 4 {
			return errclass.Transiently(errors.New("deadlock detected"))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls, got %d", calls)
	}
}

func TestDoRespectsAttemptBudget(t *testing.T) {
	p := Policy{Base: time.Millisecond, Max: time.Millisecond, Multiplier: 1, MaxAttempts: 3, Jitter: false}
	var calls int
	err := Do(context.Background(), p, func(context.Context, int) error {
		calls++
		return errors.New("connection reset by peer")
	})
	if err == nil {
		t.Fatal("expected error after budget exhaustion")
	}
	if calls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", calls)
	}
}

func TestDoReturnsLastErrorNotContextError(t *testing.T) {
	// The caller needs the real failure to classify and dead-letter it; losing it
	// to a context error would make triage impossible.
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{Base: 50 * time.Millisecond, Max: time.Second, Multiplier: 2, MaxAttempts: 10, Jitter: false}
	sentinel := errors.New("lock wait timeout exceeded")
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	err := Do(ctx, p, func(context.Context, int) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the underlying failure to survive cancellation, got %v", err)
	}
}

func TestExhausted(t *testing.T) {
	p := Policy{MaxAttempts: 3}
	if p.Exhausted(2) {
		t.Fatal("2 of 3 attempts is not exhausted")
	}
	if !p.Exhausted(3) {
		t.Fatal("3 of 3 attempts is exhausted")
	}
	unlimited := Policy{MaxAttempts: 0}
	if unlimited.Exhausted(1_000_000) {
		t.Fatal("zero MaxAttempts means unlimited")
	}
}

func TestNextAtIsMonotonicFromNow(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, Multiplier: 2, Jitter: false}
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	if got := p.NextAt(now, 3); !got.Equal(now.Add(4 * time.Second)) {
		t.Fatalf("got %s, want %s", got, now.Add(4*time.Second))
	}
}

func TestValidateRejectsBaseAboveMax(t *testing.T) {
	p := Policy{Base: time.Minute, Max: time.Second, Multiplier: 2}
	if err := p.Validate(); err == nil {
		t.Fatal("expected validation error when base exceeds max")
	}
}

func TestSleepReturnsFalseOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Second) {
		t.Fatal("expected Sleep to report interruption")
	}
	if !Sleep(context.Background(), 0) {
		t.Fatal("zero duration should return true immediately")
	}
}

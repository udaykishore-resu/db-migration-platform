// Package retryx implements the backoff policy used by every retrying component.
//
// The policy is exponential backoff with full jitter. Full jitter, rather than
// the more common "exponential plus a small random nudge", matters here: when a
// target database sheds load, every in-flight worker fails at almost the same
// instant. Without full jitter they all retry at the same instant too, and the
// recovering database is knocked over by the retry storm it just caused. Full
// jitter spreads the retries uniformly across the whole backoff window and turns
// a thundering herd into a smooth ramp.
package retryx

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
)

// Policy describes an exponential backoff schedule.
type Policy struct {
	// Base is the delay before the first retry.
	Base time.Duration
	// Max caps the delay of any single retry.
	Max time.Duration
	// Multiplier is applied per attempt; 2.0 doubles each time.
	Multiplier float64
	// MaxAttempts bounds total attempts including the first. Zero means
	// unlimited, which is only appropriate for a supervisor loop.
	MaxAttempts int
	// Jitter enables full jitter. Disable only in tests.
	Jitter bool
}

// DefaultPolicy is tuned for the apply path: fast enough that a deadlock retry
// is invisible, slow enough that a hard outage does not amplify load.
func DefaultPolicy() Policy {
	return Policy{
		Base:        100 * time.Millisecond,
		Max:         30 * time.Second,
		Multiplier:  2.0,
		MaxAttempts: 8,
		Jitter:      true,
	}
}

// DLQPolicy is the slower schedule used when draining the dead-letter store.
// Dead-lettered rows are already durable, so patience costs nothing and gives a
// genuine outage room to resolve before attempts are exhausted.
func DLQPolicy() Policy {
	return Policy{
		Base:        30 * time.Second,
		Max:         2 * time.Hour,
		Multiplier:  3.0,
		MaxAttempts: 12,
		Jitter:      true,
	}
}

// Validate fills in sane defaults and rejects nonsensical configuration.
func (p *Policy) Validate() error {
	if p.Base <= 0 {
		p.Base = 100 * time.Millisecond
	}
	if p.Max <= 0 {
		p.Max = 30 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 2.0
	}
	if p.Base > p.Max {
		return errors.New("retry policy: base delay exceeds max delay")
	}
	if p.MaxAttempts < 0 {
		return errors.New("retry policy: negative max attempts")
	}
	return nil
}

// Delay returns the backoff before the given attempt. Attempt 1 is the first
// retry (i.e. after the initial try failed). The result is uniformly distributed
// in [0, min(Max, Base*Multiplier^(attempt-1))] when jitter is enabled.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := float64(p.Base) * math.Pow(p.Multiplier, float64(attempt-1))
	if math.IsInf(base, 0) || base > float64(p.Max) {
		base = float64(p.Max)
	}
	if !p.Jitter {
		return time.Duration(base)
	}
	if base <= 0 {
		return 0
	}
	// Full jitter: uniform over the whole window.
	return time.Duration(rand.Int63n(int64(base) + 1)) //nolint:gosec // jitter needs no CSPRNG
}

// NextAt returns the wall-clock time at which the given attempt should run.
// The dead-letter store persists this so that a restart does not reset backoff.
func (p Policy) NextAt(now time.Time, attempt int) time.Time {
	return now.Add(p.Delay(attempt))
}

// Exhausted reports whether the attempt budget has been used up.
func (p Policy) Exhausted(attempts int) bool {
	return p.MaxAttempts > 0 && attempts >= p.MaxAttempts
}

// Do runs fn with retries according to the policy. It stops early on a permanent
// error, on context cancellation, or when the attempt budget is exhausted. The
// error returned is the last error observed, so the caller can classify and
// dead-letter it with full fidelity.
func Do(ctx context.Context, p Policy, fn func(ctx context.Context, attempt int) error) error {
	if err := p.Validate(); err != nil {
		return err
	}
	var last error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return last
			}
			return err
		}

		last = fn(ctx, attempt)
		if last == nil {
			return nil
		}

		// A permanent error is not made better by waiting.
		if errclass.Classify(last) == errclass.Permanent {
			return last
		}
		if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
			return last
		}

		select {
		case <-ctx.Done():
			return last
		case <-time.After(p.Delay(attempt)):
		}
	}
}

// Sleep waits for d or until ctx is done, reporting whether it slept fully.
func Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

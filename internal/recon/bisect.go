// Package recon verifies that the target actually matches the source.
//
// Every other component in the platform is built to avoid losing data. This one
// exists because none of them can be trusted to have succeeded. A migration that
// cannot demonstrate correctness has not finished, it has only stopped, and
// "the counts match" is not a demonstration: a row whose balance was written
// with the wrong scale, a CHAR column that lost its padding, a NULL that became
// an empty string, and a row that silently failed to apply all leave the counts
// identical.
//
// The strategy is a hierarchical digest comparison. Rather than compare rows —
// which would mean pulling both tables across the network — the two databases
// each compute an order-independent digest over a key range and only the digests
// are compared. When a range's digests disagree, the range is bisected and the
// comparison repeats on each half, so the cost of localising a discrepancy is
// logarithmic in table size rather than linear. Only at the leaves, over a range
// small enough to be cheap, are actual rows fetched to say precisely which keys
// differ and how.
//
// The important consequence: verifying a billion-row table that is correct costs
// one digest query per side, and verifying one that has three bad rows costs a
// few dozen. That is what makes continuous verification affordable enough to run
// during the migration rather than only at the end, when it is too late to do
// anything but start over.
package recon

import (
	"math/big"
	"strings"
	"time"
)

// Bisector splits a key range in half so that a mismatching range can be
// localised by binary search.
type Bisector interface {
	// Bisect returns a value strictly between low and high. It reports false
	// when the range cannot be split further, which is the signal to stop
	// descending and compare actual rows.
	//
	// A nil bound means unbounded on that side.
	Bisect(low, high any) (any, bool)
}

// BisectorFor picks a bisector from a sample value. Choosing from the data
// rather than from a declared type means a key column whose driver returns
// []byte for a VARCHAR still bisects correctly.
func BisectorFor(sample any) Bisector {
	switch sample.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return IntBisector{}
	case time.Time:
		return TimeBisector{}
	default:
		return StringBisector{}
	}
}

// IntBisector splits integer key spaces.
type IntBisector struct{}

// Bisect returns the arithmetic midpoint.
//
// big.Int rather than int64 arithmetic: the obvious (low+high)/2 overflows when
// the bounds are near the extremes of the type, and an overflowing midpoint sends
// the descent to a nonsensical range that silently reports no differences.
func (IntBisector) Bisect(low, high any) (any, bool) {
	l, lok := toBigInt(low)
	h, hok := toBigInt(high)

	switch {
	case !lok && !hok:
		// Both unbounded: split at zero, which at least halves the space.
		return int64(0), true
	case !lok:
		// Unbounded below. Step down by a large but finite amount rather than
		// guessing at the type's minimum, which may not be representable.
		mid := new(big.Int).Sub(h, big.NewInt(1<<20))
		return bigToInt64(mid), true
	case !hok:
		mid := new(big.Int).Add(l, big.NewInt(1<<20))
		return bigToInt64(mid), true
	}

	sum := new(big.Int).Add(l, h)
	mid := new(big.Int).Rsh(sum, 1)
	if mid.Cmp(l) <= 0 || mid.Cmp(h) >= 0 {
		// The bounds are adjacent; there is no value strictly between them.
		return nil, false
	}
	return bigToInt64(mid), true
}

func toBigInt(v any) (*big.Int, bool) {
	switch t := v.(type) {
	case nil:
		return nil, false
	case int:
		return big.NewInt(int64(t)), true
	case int8:
		return big.NewInt(int64(t)), true
	case int16:
		return big.NewInt(int64(t)), true
	case int32:
		return big.NewInt(int64(t)), true
	case int64:
		return big.NewInt(t), true
	case uint:
		return new(big.Int).SetUint64(uint64(t)), true
	case uint8:
		return big.NewInt(int64(t)), true
	case uint16:
		return big.NewInt(int64(t)), true
	case uint32:
		return big.NewInt(int64(t)), true
	case uint64:
		return new(big.Int).SetUint64(t), true
	case float32:
		return big.NewInt(int64(t)), true
	case float64:
		return big.NewInt(int64(t)), true
	default:
		return nil, false
	}
}

func bigToInt64(b *big.Int) int64 {
	if !b.IsInt64() {
		// Clamp rather than wrap. A clamped midpoint still makes progress; a
		// wrapped one inverts the range and the descent finds nothing.
		if b.Sign() > 0 {
			return 1<<63 - 1
		}
		return -1 << 63
	}
	return b.Int64()
}

// StringBisector splits text key spaces, which covers UUIDs, account numbers and
// any other identifier that is compared lexicographically.
type StringBisector struct{}

// Bisect finds a string lexicographically between low and high.
//
// The implementation treats the two strings as base-256 fractions and takes
// their midpoint. Comparing to the naive alternative — bisect only on the first
// differing character — this keeps making progress when keys share a long common
// prefix, which is exactly the situation in tables keyed by something like
// "ACCT-2026-08-000000001". The naive approach stalls there and the descent
// degenerates into a linear scan.
func (StringBisector) Bisect(low, high any) (any, bool) {
	l, lok := toString(low)
	h, hok := toString(high)

	switch {
	case !lok && !hok:
		return "", false
	case !lok:
		// Unbounded below: bisect between the empty string and high.
		l = ""
	case !hok:
		// Unbounded above: extend low by one maximal character, which is above
		// every string sharing low as a prefix.
		return l + "\xff", true
	}

	if l >= h {
		return nil, false
	}

	mid := midpointString(l, h)
	if mid <= l || mid >= h {
		return nil, false
	}
	return mid, true
}

// midpointString computes the lexicographic midpoint of two strings.
func midpointString(lo, hi string) string {
	const maxExtra = 8
	n := len(lo)
	if len(hi) > n {
		n = len(hi)
	}
	n += maxExtra

	l := new(big.Int)
	h := new(big.Int)
	for i := 0; i < n; i++ {
		l.Lsh(l, 8)
		h.Lsh(h, 8)
		if i < len(lo) {
			l.Or(l, big.NewInt(int64(lo[i])))
		}
		if i < len(hi) {
			h.Or(h, big.NewInt(int64(hi[i])))
		}
	}

	mid := new(big.Int).Add(l, h)
	mid.Rsh(mid, 1)

	// FillBytes writes the big-endian representation zero-padded into buf, which
	// is exactly the fixed-width encoding this function assumed — and it replaces
	// a shift-and-mask loop whose per-byte narrowing a static analyser cannot
	// distinguish from a genuine overflow.
	buf := make([]byte, n)
	mid.FillBytes(buf)
	// Trailing NULs are an artefact of the fixed-width encoding, not part of the
	// value, and leaving them in produces a key no database will ever match.
	return strings.TrimRight(string(buf), "\x00")
}

func toString(v any) (string, bool) {
	switch t := v.(type) {
	case nil:
		return "", false
	case string:
		return t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}

// TimeBisector splits timestamp key spaces, used for tables chunked by an
// event or created-at column rather than by an identifier.
type TimeBisector struct{}

// Bisect returns the instant halfway between the bounds.
func (TimeBisector) Bisect(low, high any) (any, bool) {
	l, lok := low.(time.Time)
	h, hok := high.(time.Time)

	switch {
	case !lok && !hok:
		return nil, false
	case !lok:
		return h.Add(-24 * time.Hour), true
	case !hok:
		return l.Add(24 * time.Hour), true
	}

	if !l.Before(h) {
		return nil, false
	}
	mid := l.Add(h.Sub(l) / 2)
	if !mid.After(l) || !mid.Before(h) {
		return nil, false
	}
	return mid, true
}

package recon

import (
	"math"
	"testing"
	"time"
)

func TestIntBisectorFindsMidpoint(t *testing.T) {
	mid, ok := IntBisector{}.Bisect(int64(0), int64(100))
	if !ok || mid.(int64) != 50 {
		t.Fatalf("got %v, %v; want 50", mid, ok)
	}
}

// The obvious (low+high)/2 overflows near the extremes of int64, and an
// overflowing midpoint sends the descent to a nonsensical range where it
// silently finds nothing.
func TestIntBisectorDoesNotOverflow(t *testing.T) {
	mid, ok := IntBisector{}.Bisect(int64(math.MaxInt64-2), int64(math.MaxInt64))
	if !ok {
		t.Fatal("expected a midpoint near MaxInt64")
	}
	// The only value strictly between MaxInt64-2 and MaxInt64.
	if m := mid.(int64); m != math.MaxInt64-1 {
		t.Fatalf("midpoint %d is not strictly between the bounds", m)
	}

	mid, ok = (IntBisector{}).Bisect(int64(math.MinInt64), int64(math.MinInt64+2))
	if !ok {
		t.Fatal("expected a midpoint near MinInt64")
	}
	if m := mid.(int64); m != math.MinInt64+1 {
		t.Fatalf("midpoint %d is not strictly between the bounds", m)
	}
}

// Adjacent bounds have nothing between them; the descent must be told to stop
// rather than loop forever on the same range.
func TestIntBisectorReportsUnsplittableRange(t *testing.T) {
	if _, ok := (IntBisector{}).Bisect(int64(5), int64(6)); ok {
		t.Fatal("adjacent integers must not be splittable")
	}
	if _, ok := (IntBisector{}).Bisect(int64(5), int64(5)); ok {
		t.Fatal("an empty range must not be splittable")
	}
}

func TestIntBisectorHandlesUnboundedSides(t *testing.T) {
	if mid, ok := (IntBisector{}).Bisect(nil, int64(1000)); !ok || mid.(int64) >= 1000 {
		t.Fatalf("unbounded-below split failed: %v %v", mid, ok)
	}
	if mid, ok := (IntBisector{}).Bisect(int64(1000), nil); !ok || mid.(int64) <= 1000 {
		t.Fatalf("unbounded-above split failed: %v %v", mid, ok)
	}
	if mid, ok := (IntBisector{}).Bisect(nil, nil); !ok || mid.(int64) != 0 {
		t.Fatalf("fully unbounded split should pick zero: %v %v", mid, ok)
	}
}

func TestStringBisectorFindsMidpoint(t *testing.T) {
	mid, ok := StringBisector{}.Bisect("a", "z")
	if !ok {
		t.Fatal("expected a midpoint")
	}
	s := mid.(string)
	if s <= "a" || s >= "z" {
		t.Fatalf("midpoint %q is not strictly between the bounds", s)
	}
}

// Keys like "ACCT-2026-08-000000001" share a long prefix. Bisecting only on the
// first differing character stalls there and the descent degrades into a linear
// scan; the base-256 midpoint keeps making progress.
func TestStringBisectorMakesProgressOnLongCommonPrefixes(t *testing.T) {
	lo := "ACCT-2026-08-000000001"
	hi := "ACCT-2026-08-000000009"

	cur := lo
	for i := 0; i < 8; i++ {
		mid, ok := StringBisector{}.Bisect(cur, hi)
		if !ok {
			break
		}
		s := mid.(string)
		if s <= cur || s >= hi {
			t.Fatalf("iteration %d produced %q, outside (%q, %q)", i, s, cur, hi)
		}
		cur = s
	}
	if cur == lo {
		t.Fatal("bisector made no progress on a long shared prefix")
	}
}

func TestStringBisectorReportsUnsplittableRange(t *testing.T) {
	if _, ok := (StringBisector{}).Bisect("m", "m"); ok {
		t.Fatal("identical bounds must not be splittable")
	}
	if _, ok := (StringBisector{}).Bisect("z", "a"); ok {
		t.Fatal("inverted bounds must not be splittable")
	}
}

func TestStringBisectorHandlesUnboundedAbove(t *testing.T) {
	mid, ok := StringBisector{}.Bisect("zzz", nil)
	if !ok || mid.(string) <= "zzz" {
		t.Fatalf("unbounded-above split failed: %v %v", mid, ok)
	}
}

// A midpoint padded with trailing NUL bytes is a key no database will ever
// match, which would make the upper half of the descent scan nothing.
func TestStringMidpointHasNoTrailingNULs(t *testing.T) {
	mid, ok := StringBisector{}.Bisect("aaa", "aab")
	if !ok {
		t.Skip("range not splittable, which is also acceptable here")
	}
	s := mid.(string)
	if s != "" && s[len(s)-1] == 0 {
		t.Fatalf("midpoint %q ends in a NUL byte", s)
	}
}

func TestTimeBisectorFindsMidpoint(t *testing.T) {
	lo := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	hi := time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)
	mid, ok := TimeBisector{}.Bisect(lo, hi)
	if !ok {
		t.Fatal("expected a midpoint")
	}
	m := mid.(time.Time)
	if !m.After(lo) || !m.Before(hi) {
		t.Fatalf("midpoint %s is not strictly between the bounds", m)
	}
}

func TestTimeBisectorReportsUnsplittableRange(t *testing.T) {
	now := time.Now()
	if _, ok := (TimeBisector{}).Bisect(now, now); ok {
		t.Fatal("identical instants must not be splittable")
	}
	if _, ok := (TimeBisector{}).Bisect(now, now.Add(time.Nanosecond)); ok {
		t.Fatal("adjacent nanoseconds must not be splittable")
	}
}

// The bisector is chosen from the value's runtime type, so a VARCHAR key that a
// driver returns as []byte still bisects as a string.
func TestBisectorForChoosesByRuntimeType(t *testing.T) {
	if _, ok := BisectorFor(int64(1)).(IntBisector); !ok {
		t.Error("int64 should select the integer bisector")
	}
	if _, ok := BisectorFor("x").(StringBisector); !ok {
		t.Error("string should select the string bisector")
	}
	if _, ok := BisectorFor([]byte("x")).(StringBisector); !ok {
		t.Error("[]byte should select the string bisector")
	}
	if _, ok := BisectorFor(time.Now()).(TimeBisector); !ok {
		t.Error("time.Time should select the time bisector")
	}
}

// Repeated bisection must terminate. A bisector that can return a value equal to
// a bound would spin forever inside the descent.
func TestRepeatedBisectionTerminates(t *testing.T) {
	lo, hi := any(int64(0)), any(int64(1_000_000))
	for i := 0; i < 100; i++ {
		mid, ok := IntBisector{}.Bisect(lo, hi)
		if !ok {
			return
		}
		lo = mid
	}
	t.Fatal("bisection did not terminate within 100 iterations")
}

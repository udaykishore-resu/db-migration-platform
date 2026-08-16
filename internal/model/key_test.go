package model

import (
	"testing"
	"time"
)

func TestRowKeyCanonicalIsOrderIndependent(t *testing.T) {
	a := NewRowKey(map[string]any{"b": 2, "a": 1})
	b := NewRowKey(map[string]any{"a": 1, "b": 2})
	if !a.Equal(b) {
		t.Fatalf("keys built from the same map in different order must match: %s vs %s", a.Canonical(), b.Canonical())
	}
}

// A numeric key read from the snapshot as int64 and from a JSON change event as
// float64 must hash identically, or snapshot/CDC deduplication silently fails.
func TestRowKeyNumericTypeNormalisation(t *testing.T) {
	fromSnapshot := NewRowKey(map[string]any{"id": int64(42)})
	fromJSON := NewRowKey(map[string]any{"id": float64(42)})
	if !fromSnapshot.Equal(fromJSON) {
		t.Fatalf("int64(42) and float64(42) must produce the same key: %q vs %q",
			fromSnapshot.Canonical(), fromJSON.Canonical())
	}
	if fromSnapshot.Hash() != fromJSON.Hash() {
		t.Fatal("hashes must match for equal keys")
	}
}

func TestRowKeyLengthPrefixPreventsCollision(t *testing.T) {
	// Without length prefixing, {a:"b", c:"d"} and {a:"b=2:c", ...} style values
	// could collide. Verify adjacent-value ambiguity does not collide.
	a := NewRowKey(map[string]any{"k1": "ab", "k2": "c"})
	b := NewRowKey(map[string]any{"k1": "a", "k2": "bc"})
	if a.Equal(b) {
		t.Fatalf("distinct keys collided: %q", a.Canonical())
	}
}

func TestRowKeyNullDistinctFromEmptyString(t *testing.T) {
	withNull := NewRowKey(map[string]any{"id": nil})
	withEmpty := NewRowKey(map[string]any{"id": ""})
	if withNull.Equal(withEmpty) {
		t.Fatal("NULL and empty string must not produce the same key")
	}
}

func TestRowKeyPartitionIsStableAndBounded(t *testing.T) {
	k := NewRowKey(map[string]any{"id": "consumer-9931"})
	const parts = 12
	first := k.Partition(parts)
	for i := 0; i < 100; i++ {
		if got := k.Partition(parts); got != first {
			t.Fatalf("partition assignment must be stable: got %d then %d", first, got)
		}
	}
	if first < 0 || first >= parts {
		t.Fatalf("partition %d out of range [0,%d)", first, parts)
	}
	if got := k.Partition(1); got != 0 {
		t.Fatalf("single-partition topic must always map to 0, got %d", got)
	}
}

// Different rows should spread across partitions; a hash that collapses
// everything onto one partition would serialise the entire migration.
func TestRowKeyPartitionSpread(t *testing.T) {
	const parts = 8
	hits := make(map[int32]int)
	for i := 0; i < 2000; i++ {
		k := NewRowKey(map[string]any{"id": int64(i)})
		hits[k.Partition(parts)]++
	}
	if len(hits) != parts {
		t.Fatalf("expected all %d partitions to be used, got %d", parts, len(hits))
	}
	for p, n := range hits {
		if n < 2000/parts/2 {
			t.Fatalf("partition %d badly under-filled with %d rows", p, n)
		}
	}
}

func TestRowKeyHashDoesNotLeakValues(t *testing.T) {
	k := NewRowKey(map[string]any{"ssn": "123-45-6789"})
	if got := k.String(); got == "123-45-6789" || len(got) == 0 {
		t.Fatalf("key string form must be a digest, got %q", got)
	}
	for _, s := range []string{"123", "6789", "ssn"} {
		if contains(k.Hash(), s) {
			t.Fatalf("hash %q leaked substring %q", k.Hash(), s)
		}
	}
}

func TestRowKeyTimeNormalisedToUTC(t *testing.T) {
	loc := time.FixedZone("EST", -5*3600)
	utc := NewRowKey(map[string]any{"ts": time.Date(2026, 8, 16, 17, 0, 0, 0, time.UTC)})
	est := NewRowKey(map[string]any{"ts": time.Date(2026, 8, 16, 12, 0, 0, 0, loc)})
	if !utc.Equal(est) {
		t.Fatalf("equal instants in different zones must match: %q vs %q", utc.Canonical(), est.Canonical())
	}
}

func TestRowKeyOrderedSelectsOnlyKeyColumns(t *testing.T) {
	row := map[string]any{"id": 1, "name": "x", "email": "y"}
	k := NewRowKeyOrdered([]string{"id"}, row)
	if k.Len() != 1 {
		t.Fatalf("expected 1 key column, got %d", k.Len())
	}
	if k.Names()[0] != "id" {
		t.Fatalf("expected key column id, got %s", k.Names()[0])
	}
}

func contains(haystack, needle string) bool {
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

package model

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// KeyColumn is one component of a primary key.
type KeyColumn struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

// RowKey is the ordered primary key of a row. Ordering is canonicalised on
// construction so that two keys describing the same row always produce the same
// canonical form, hash and partition — this is what guarantees that all changes
// to a row land on the same Kafka partition and are therefore applied in order.
type RowKey struct {
	cols []KeyColumn
}

// NewRowKey builds a canonical RowKey from a map of primary key columns.
// Column order is normalised alphabetically, so callers need not be careful.
func NewRowKey(values map[string]any) RowKey {
	cols := make([]KeyColumn, 0, len(values))
	for name, v := range values {
		cols = append(cols, KeyColumn{Name: name, Value: v})
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
	return RowKey{cols: cols}
}

// NewRowKeyOrdered builds a RowKey from an explicit column order, used when the
// caller already knows the declared primary key order for a table.
func NewRowKeyOrdered(names []string, values map[string]any) RowKey {
	subset := make(map[string]any, len(names))
	for _, n := range names {
		if v, ok := values[n]; ok {
			subset[n] = v
		}
	}
	return NewRowKey(subset)
}

// Len returns the number of columns in the key.
func (k RowKey) Len() int { return len(k.cols) }

// Columns returns a copy of the key columns in canonical order.
func (k RowKey) Columns() []KeyColumn {
	out := make([]KeyColumn, len(k.cols))
	copy(out, k.cols)
	return out
}

// Names returns the key column names in canonical order.
func (k RowKey) Names() []string {
	out := make([]string, len(k.cols))
	for i, c := range k.cols {
		out[i] = c.Name
	}
	return out
}

// Values returns the key column values in canonical order.
func (k RowKey) Values() []any {
	out := make([]any, len(k.cols))
	for i, c := range k.cols {
		out[i] = c.Value
	}
	return out
}

// Map returns the key as a map, convenient for building SQL predicates.
func (k RowKey) Map() map[string]any {
	out := make(map[string]any, len(k.cols))
	for _, c := range k.cols {
		out[c.Name] = c.Value
	}
	return out
}

// Canonical renders a deterministic, collision-resistant string form of the key.
// Values are length-prefixed so that ("ab","c") and ("a","bc") never collide.
func (k RowKey) Canonical() string {
	var b strings.Builder
	for _, c := range k.cols {
		v := CanonicalValue(c.Value)
		b.WriteString(strconv.Itoa(len(c.Name)))
		b.WriteByte(':')
		b.WriteString(c.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
		b.WriteByte(';')
	}
	return b.String()
}

// Hash returns a short, stable, non-reversible digest of the key. Logs and
// metrics use this instead of the raw key so that primary keys — which are
// frequently PII, such as a consumer identifier — never reach a log sink.
func (k RowKey) Hash() string {
	sum := sha256.Sum256([]byte(k.Canonical()))
	return base64.RawURLEncoding.EncodeToString(sum[:9])
}

// Partition maps the key onto one of n partitions. All changes to the same row
// therefore route to the same partition and are consumed in commit order, which
// is the property that prevents an older UPDATE from overwriting a newer one.
func (k RowKey) Partition(n int32) int32 {
	if n <= 1 {
		return 0
	}
	sum := sha256.Sum256([]byte(k.Canonical()))
	v := binary.BigEndian.Uint64(sum[:8])
	return int32(v % uint64(n)) //nolint:gosec // modulo of a positive int32 bound
}

// Equal reports whether two keys identify the same row.
func (k RowKey) Equal(other RowKey) bool { return k.Canonical() == other.Canonical() }

// String implements fmt.Stringer using the safe hash, never the raw values.
func (k RowKey) String() string { return k.Hash() }

// CanonicalValue renders a value into a stable textual form. Type normalisation
// matters here: a numeric primary key arriving as int64 from the snapshot reader
// and as float64 from a JSON-decoded change event must render identically, or
// the deduplication window in the snapshot protocol, the tokeniser and the
// reconciler all silently disagree about whether two rows are the same row.
//
// It is exported because the crypto and reconciliation layers must normalise
// values exactly the same way the key layer does; two independent
// implementations would drift apart and produce phantom mismatches that are
// extremely hard to diagnose.
func CanonicalValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "\x00NULL"
	case string:
		return t
	case []byte:
		return string(t)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint:
		return strconv.FormatUint(uint64(t), 10)
	case uint8:
		return strconv.FormatUint(uint64(t), 10)
	case uint16:
		return strconv.FormatUint(uint64(t), 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case float32:
		return normaliseFloat(float64(t))
	case float64:
		return normaliseFloat(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// normaliseFloat renders whole-valued floats as integers so that a key arriving
// as JSON number 42 (float64) matches the same key read as int64 42.
func normaliseFloat(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

package model

import (
	"fmt"
	"sort"
	"strings"
)

// TableRef identifies a table in a schema.
type TableRef struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

// String renders schema-qualified name, or the bare name when unqualified.
func (t TableRef) String() string {
	if t.Schema == "" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

// ParseTableRef parses "schema.table" or "table".
func ParseTableRef(s string) TableRef {
	if i := strings.LastIndex(s, "."); i >= 0 {
		return TableRef{Schema: s[:i], Name: s[i+1:]}
	}
	return TableRef{Name: s}
}

// Protection describes how a column is protected before it leaves the source
// network boundary. This is the field that decides whether the target database
// ever holds plaintext for a column.
type Protection string

const (
	// ProtectNone leaves the column as-is. Appropriate for non-sensitive data.
	ProtectNone Protection = "none"

	// ProtectTokenize replaces the value with a deterministic, format-preserving
	// token before it leaves the source network. The target stores the token
	// permanently and never sees plaintext, which keeps the target out of PCI
	// scope for that column while still supporting equality joins and indexes.
	ProtectTokenize Protection = "tokenize"

	// ProtectEncrypt applies non-deterministic authenticated encryption. Safer
	// than tokenisation against frequency analysis, but the ciphertext cannot be
	// joined, indexed for equality, or reconciled without decrypting.
	ProtectEncrypt Protection = "encrypt"

	// ProtectRedact drops the value entirely. Used for columns that must not be
	// migrated at all, such as raw magnetic stripe data.
	ProtectRedact Protection = "redact"
)

// Valid reports whether the protection mode is recognised.
func (p Protection) Valid() bool {
	switch p {
	case ProtectNone, ProtectTokenize, ProtectEncrypt, ProtectRedact:
		return true
	default:
		return false
	}
}

// Deterministic reports whether the mode produces stable ciphertext for a given
// plaintext. Only deterministic modes can be reconciled by comparing protected
// values on both sides without decrypting anything.
func (p Protection) Deterministic() bool {
	return p == ProtectNone || p == ProtectTokenize
}

// ColumnSpec describes one column of a migrated table.
type ColumnSpec struct {
	Name string `json:"name"`
	// TargetName allows renaming during migration. Empty means unchanged.
	TargetName string `json:"target_name,omitempty"`
	// Type is the logical type used for normalisation during reconciliation.
	Type LogicalType `json:"type"`
	// Protect controls the confidentiality treatment applied at the source edge.
	Protect Protection `json:"protect"`
	// Nullable affects null-sentinel handling in checksum computation.
	Nullable bool `json:"nullable"`
	// TrimTrailingSpace handles fixed-width CHAR semantics. DB2 pads CHAR to the
	// declared width; Postgres and MySQL do not return the padding the same way.
	// Without this, every CHAR column reconciles as a mismatch.
	TrimTrailingSpace bool `json:"trim_trailing_space"`
	// Scale pins decimal scale so that DECFLOAT and NUMERIC compare equal across
	// engines instead of differing in trailing zeros.
	Scale int `json:"scale,omitempty"`
}

// Target returns the column name on the target side.
func (c ColumnSpec) Target() string {
	if c.TargetName != "" {
		return c.TargetName
	}
	return c.Name
}

// LogicalType is an engine-neutral type used to drive value normalisation.
type LogicalType string

// Logical types recognised by the normalisation and checksum layers.
const (
	TypeString    LogicalType = "string"
	TypeInt       LogicalType = "int"
	TypeDecimal   LogicalType = "decimal"
	TypeFloat     LogicalType = "float"
	TypeBool      LogicalType = "bool"
	TypeTimestamp LogicalType = "timestamp"
	TypeDate      LogicalType = "date"
	TypeBytes     LogicalType = "bytes"
	TypeJSON      LogicalType = "json"
)

// TableSpec is the full migration contract for one table: what to read, how to
// protect it, how to chunk it, and how to verify it.
type TableSpec struct {
	Source TableRef `json:"source"`
	Target TableRef `json:"target"`

	// PrimaryKey lists the key columns in declared order.
	PrimaryKey []string `json:"primary_key"`

	// Columns lists every column to migrate, in declared order.
	Columns []ColumnSpec `json:"columns"`

	// ChunkColumn is the column used to split the table into snapshot ranges.
	// It must be indexed and monotonically comparable; it defaults to the first
	// primary key column.
	ChunkColumn string `json:"chunk_column,omitempty"`

	// ChunkSize is the target number of rows per snapshot chunk.
	ChunkSize int `json:"chunk_size,omitempty"`

	// ReconcileInterval overrides the global continuous-reconciliation cadence
	// for hot tables that warrant tighter verification.
	ReconcileIntervalSeconds int `json:"reconcile_interval_seconds,omitempty"`

	// Exclude drops rows matching a source-side predicate, for tables where only
	// a subset migrates. Rendered verbatim into the snapshot query's WHERE.
	Exclude string `json:"exclude,omitempty"`
}

// Key returns the chunking column, defaulting to the first primary key column.
func (t TableSpec) Key() string {
	if t.ChunkColumn != "" {
		return t.ChunkColumn
	}
	if len(t.PrimaryKey) > 0 {
		return t.PrimaryKey[0]
	}
	return ""
}

// ColumnNames returns source column names in declared order.
func (t TableSpec) ColumnNames() []string {
	out := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		out[i] = c.Name
	}
	return out
}

// TargetColumnNames returns target column names in declared order.
func (t TableSpec) TargetColumnNames() []string {
	out := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		out[i] = c.Target()
	}
	return out
}

// Column looks a column up by source name.
func (t TableSpec) Column(name string) (ColumnSpec, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return ColumnSpec{}, false
}

// ProtectedColumns returns the columns that receive confidentiality treatment.
func (t TableSpec) ProtectedColumns() []ColumnSpec {
	var out []ColumnSpec
	for _, c := range t.Columns {
		if c.Protect != "" && c.Protect != ProtectNone {
			out = append(out, c)
		}
	}
	return out
}

// ComparableColumns returns the columns that can participate in a cross-engine
// checksum. Non-deterministically protected and redacted columns are excluded,
// because their representations differ on the two sides by design.
func (t TableSpec) ComparableColumns() []ColumnSpec {
	var out []ColumnSpec
	for _, c := range t.Columns {
		if c.Protect == ProtectEncrypt || c.Protect == ProtectRedact {
			continue
		}
		out = append(out, c)
	}
	return out
}

// Validate checks a table specification for internal consistency. Catching these
// at config load is far cheaper than discovering them mid-migration.
func (t TableSpec) Validate() error {
	if t.Source.Name == "" {
		return fmt.Errorf("table spec missing source name")
	}
	if t.Target.Name == "" {
		return fmt.Errorf("table %s: missing target name", t.Source)
	}
	if len(t.PrimaryKey) == 0 {
		return fmt.Errorf("table %s: primary key is required; keyless tables cannot be applied idempotently", t.Source)
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("table %s: no columns declared", t.Source)
	}

	seen := make(map[string]bool, len(t.Columns))
	targets := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		if c.Name == "" {
			return fmt.Errorf("table %s: column with empty name", t.Source)
		}
		if seen[c.Name] {
			return fmt.Errorf("table %s: duplicate column %q", t.Source, c.Name)
		}
		seen[c.Name] = true
		if targets[c.Target()] {
			return fmt.Errorf("table %s: duplicate target column %q", t.Source, c.Target())
		}
		targets[c.Target()] = true
		if c.Protect != "" && !c.Protect.Valid() {
			return fmt.Errorf("table %s column %s: unknown protection %q", t.Source, c.Name, c.Protect)
		}
	}

	for _, pk := range t.PrimaryKey {
		col, ok := t.Column(pk)
		if !ok {
			return fmt.Errorf("table %s: primary key column %q is not in the column list", t.Source, pk)
		}
		// A non-deterministically protected primary key cannot be looked up on
		// the target, which breaks both the apply path and reconciliation.
		if !col.Protect.Deterministic() && col.Protect != "" {
			return fmt.Errorf("table %s: primary key column %q uses non-deterministic protection %q; use %q instead",
				t.Source, pk, col.Protect, ProtectTokenize)
		}
	}

	if ck := t.Key(); ck != "" {
		if _, ok := t.Column(ck); !ok {
			return fmt.Errorf("table %s: chunk column %q is not in the column list", t.Source, ck)
		}
	}
	if t.ChunkSize < 0 {
		return fmt.Errorf("table %s: negative chunk size", t.Source)
	}
	return nil
}

// Plan is the complete set of tables in a migration, plus ordering information.
type Plan struct {
	Name   string      `json:"name"`
	Tables []TableSpec `json:"tables"`
}

// Validate checks every table and the plan as a whole.
func (p Plan) Validate() error {
	if len(p.Tables) == 0 {
		return fmt.Errorf("migration plan %q has no tables", p.Name)
	}
	seen := make(map[string]bool, len(p.Tables))
	for _, t := range p.Tables {
		if err := t.Validate(); err != nil {
			return err
		}
		if seen[t.Source.String()] {
			return fmt.Errorf("duplicate table %s in plan", t.Source)
		}
		seen[t.Source.String()] = true
	}
	return nil
}

// Table looks up a table spec by source reference. Lookup is tolerant of an
// unqualified name so that connectors which omit the schema still match.
func (p Plan) Table(ref TableRef) (TableSpec, bool) {
	for _, t := range p.Tables {
		if t.Source == ref {
			return t, true
		}
	}
	for _, t := range p.Tables {
		if t.Source.Name == ref.Name && (ref.Schema == "" || t.Source.Schema == "") {
			return t, true
		}
	}
	return TableSpec{}, false
}

// SortedTableNames returns source table names in stable order, for logs and
// deterministic scheduling.
func (p Plan) SortedTableNames() []string {
	out := make([]string, len(p.Tables))
	for i, t := range p.Tables {
		out[i] = t.Source.String()
	}
	sort.Strings(out)
	return out
}

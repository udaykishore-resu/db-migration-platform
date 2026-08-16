// Package dialect isolates every difference between target database engines.
//
// The platform supports Aurora PostgreSQL and Aurora MySQL. Rather than
// sprinkling `if postgres { ... }` through the apply, load and reconciliation
// paths, all of it is concentrated here behind one interface. The rule the
// package enforces is that no other package in the repository may contain a
// vendor-specific SQL string.
//
// The most consequential thing this package owns is the row digest. For
// reconciliation to mean anything, both engines must hash the same logical row
// to the same value, and they do not agree by default: DB2 pads CHAR to the
// declared width and Postgres does not, MySQL and Postgres disagree about
// trailing zeros in DECIMAL, timestamps carry different precision, and the two
// engines' string concatenation treats NULL differently. Every one of those
// differences shows up as a phantom mismatch on every single row unless it is
// normalised away here, once, deliberately.
package dialect

import (
	"fmt"
	"strings"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// Metadata columns the platform adds to every migrated target table.
const (
	// ColLSN records the source change sequence number of the last write that
	// touched the row. It is the fence: a write is only applied if its LSN is at
	// least as new as the LSN already on the row.
	//
	// This one column is what makes the whole pipeline replay-safe. Kafka
	// redelivers, retries re-apply, a stale snapshot part can land after a fresh
	// change event, and the repair worker replays dead letters out of order —
	// under all of those, the fenced write converges to the same correct state.
	ColLSN = "_mig_lsn"

	// ColDeleted marks a row as deleted rather than removing it.
	//
	// A hard DELETE loses the row's LSN, so a delayed UPDATE carrying an older
	// LSN would re-insert the row and resurrect a record the source deleted. The
	// tombstone keeps the LSN available to reject that write. Tombstones are
	// purged after cutover.
	ColDeleted = "_mig_deleted"

	// ColUpdatedAt records when the platform last wrote the row, for operator
	// forensics rather than for correctness.
	ColUpdatedAt = "_mig_updated_at"
)

// ControlSchema is the schema holding the platform's own bookkeeping tables.
const ControlSchema = "migration_ctl"

// Name identifies a supported engine.
type Name string

// Supported engines.
const (
	Postgres Name = "postgres"
	MySQL    Name = "mysql"
)

// Range bounds a chunk of a table by its chunking column, half-open as
// (Low, High] so that adjacent chunks tile the key space without overlapping.
// Overlapping ranges would double-count rows in reconciliation and double-apply
// them in the snapshot.
type Range struct {
	Column string
	// Low is exclusive. Nil means unbounded below.
	Low any
	// High is inclusive. Nil means unbounded above.
	High any
}

// Bounded reports whether the range constrains anything.
func (r Range) Bounded() bool { return r.Low != nil || r.High != nil }

// S3Source describes a staged part for a native bulk import.
type S3Source struct {
	Bucket string
	Key    string
	Region string
	// Compressed selects gzip decoding during import.
	Compressed bool
	// Delimiter and NullSentinel must match what the extractor wrote.
	Delimiter    string
	NullSentinel string
}

// URI renders the canonical s3:// form.
func (s S3Source) URI() string { return "s3://" + s.Bucket + "/" + s.Key }

// Digest is the pair of values that identify a range's contents. Two independent
// 60-bit sums are used rather than one: a single additive digest can be defeated
// by a pair of compensating errors (one row too high, another too low by the same
// amount), which is unlikely but not impossible across billions of rows. Two
// independent projections of the same hash make that essentially impossible while
// staying computable in portable SQL on both engines.
type Digest struct {
	Rows   int64
	SumLow string
	SumHi  string
}

// Equal compares two digests.
func (d Digest) Equal(other Digest) bool {
	return d.Rows == other.Rows &&
		normaliseNumeric(d.SumLow) == normaliseNumeric(other.SumLow) &&
		normaliseNumeric(d.SumHi) == normaliseNumeric(other.SumHi)
}

// Empty reports whether the range contained no rows.
func (d Digest) Empty() bool { return d.Rows == 0 }

// normaliseNumeric strips representational differences between engines: Postgres
// returns a numeric sum as "12345" while MySQL may return "12345.0000" from a
// DECIMAL aggregate, and an empty range yields NULL on one and "0" on the other.
func normaliseNumeric(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "null") {
		return "0"
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		frac := strings.TrimRight(s[i+1:], "0")
		if frac == "" {
			s = s[:i]
		} else {
			s = s[:i+1] + frac
		}
	}
	s = strings.TrimLeft(s, "+")
	if s == "" || s == "-" {
		return "0"
	}
	return s
}

// Dialect renders every engine-specific SQL construct the platform needs.
type Dialect interface {
	// Name identifies the engine.
	Name() Name
	// Driver is the database/sql driver name to open a connection with.
	Driver() string

	// Quote renders an identifier safely.
	Quote(ident string) string
	// QuoteTable renders a schema-qualified table name.
	QuoteTable(t model.TableRef) string
	// Placeholder renders the nth (1-based) bind parameter.
	Placeholder(n int) string

	// FencedUpsert renders a multi-row upsert that only overwrites a row when the
	// incoming LSN is at least as new as the LSN already stored on it. Args are
	// expected in row-major order, with the LSN appended to each row's values.
	FencedUpsert(spec model.TableSpec, columns []string, rows int) string

	// FencedDelete renders the tombstone write for a delete event.
	FencedDelete(spec model.TableSpec) string

	// CreateStagingTable renders DDL for an unlogged/temporary staging table
	// shaped like the target, used as the landing zone for a bulk part load.
	CreateStagingTable(target, staging model.TableRef, columns []string) string

	// DropStagingTable renders the cleanup DDL.
	DropStagingTable(staging model.TableRef) string

	// BulkImport renders the engine's native S3 import into a staging table. This
	// is materially faster than streaming rows through the application, and it
	// keeps the data path between object storage and the database rather than
	// routing terabytes through a worker process.
	BulkImport(staging model.TableRef, columns []string, src S3Source) (string, []any)

	// MergeStaging renders the single set-based statement that moves a staged
	// part into the live table under the LSN fence. Doing this as one statement
	// rather than a row loop is both far faster and makes "a stale part can never
	// overwrite a fresher row" a property of the SQL rather than of a loop that
	// has to be trusted.
	MergeStaging(spec model.TableSpec, staging model.TableRef, columns []string, extractLSN uint64) string

	// RowDigestExpr renders the per-row hash expression with all cross-engine
	// normalisation applied.
	RowDigestExpr(alias string, columns []model.ColumnSpec) string

	// RangeDigestQuery renders the order-independent digest of a key range.
	RangeDigestQuery(t model.TableRef, columns []model.ColumnSpec, r Range, includeDeleted bool) (string, []any)

	// CountQuery renders a bounded row count.
	CountQuery(t model.TableRef, r Range, includeDeleted bool) (string, []any)

	// KeysetPageQuery renders the query that walks the key space in order to
	// discover chunk boundaries without an expensive OFFSET scan.
	KeysetPageQuery(t model.TableRef, keyColumn string, limit int, after any) (string, []any)

	// RowsInRangeQuery selects full row images for a key range, used both by the
	// snapshot reader and by the reconciler when it descends to row level.
	RowsInRangeQuery(t model.TableRef, columns []string, r Range, includeDeleted bool) (string, []any)

	// SelectClaimableDeadLetters renders the work-claiming query for the repair
	// worker. SKIP LOCKED is what allows many workers to drain the same queue
	// concurrently without contending on the same rows, and without the whole
	// pool serialising behind one slow item.
	//
	// Claiming is deliberately split into a locking SELECT followed by an UPDATE
	// rather than expressed as a single statement. Postgres can do it in one;
	// MySQL cannot lock rows in a subquery of an UPDATE against the same table.
	// Using the two-statement shape on both engines keeps the repair worker's
	// logic identical rather than forking on dialect.
	SelectClaimableDeadLetters(limit int) string

	// MarkDeadLettersClaimed renders the UPDATE that takes ownership of the rows
	// returned by SelectClaimableDeadLetters, within the same transaction.
	MarkDeadLettersClaimed(ids int) string

	// UpsertOffset renders the statement that records stream progress. It is
	// executed inside the same transaction as the data writes, which is what
	// upgrades at-least-once delivery into effectively-once application.
	UpsertOffset() string

	// SelectOffsets renders the query that restores progress on startup.
	SelectOffsets() string

	// PurgeTombstones renders the post-cutover cleanup of soft-deleted rows.
	PurgeTombstones(t model.TableRef) string

	// BulkLoadSessionSettings returns session-scoped statements that make a bulk
	// load materially faster and that are safe to apply to one connection.
	//
	// Note what is deliberately absent: nothing here drops or invalidates an
	// index. Both engines have folklore tricks for that — editing pg_index by
	// hand, ALTER TABLE ... DISABLE KEYS — and both are either unsupported on the
	// storage engine in question or capable of leaving the catalogue in a state
	// that needs a restore to fix. Dropping and recreating indexes around a bulk
	// load is a real and worthwhile optimisation, but it is a deliberate operator
	// procedure with a rollback plan, documented in the load runbook, not
	// something a worker process should do to a production table on its own.
	BulkLoadSessionSettings() []string

	// RestoreSessionSettings undoes BulkLoadSessionSettings.
	RestoreSessionSettings() []string
}

// For returns the dialect for an engine name.
func For(name Name) (Dialect, error) {
	switch name {
	case Postgres:
		return NewPostgres(), nil
	case MySQL:
		return NewMySQL(), nil
	default:
		return nil, fmt.Errorf("dialect: unsupported engine %q (supported: %s, %s)", name, Postgres, MySQL)
	}
}

// NullSentinelSQL is the literal used inside digest expressions to stand in for
// SQL NULL. It must be a value that cannot occur in the data, or a row holding
// the sentinel as a real value would hash identically to a row holding NULL.
const NullSentinelSQL = `\x00NULL`

// digestSeparator joins column values inside a row digest. A separator that
// could occur in the data would let ("ab","c") and ("a","bc") hash identically,
// so it uses a control character that cannot appear in a text column.
const digestSeparator = "\x1f"

// columnsForDigest filters a spec's columns down to those that can be compared
// across the two engines and returns them in a stable order. Ordering must be
// deterministic and identical on both sides, or the concatenation differs and
// every row mismatches.
func columnsForDigest(columns []model.ColumnSpec) []model.ColumnSpec {
	out := make([]model.ColumnSpec, 0, len(columns))
	for _, c := range columns {
		if c.Protect == model.ProtectRedact || c.Protect == model.ProtectEncrypt {
			// Redacted columns do not exist on the target, and randomised
			// ciphertext differs on every write by design. Including either
			// would guarantee a mismatch on every row.
			continue
		}
		out = append(out, c)
	}
	return out
}

// buildInsertPlaceholders renders "($1,$2,$3),($4,$5,$6)" style tuples.
func buildInsertPlaceholders(d Dialect, columns, rows int) string {
	var b strings.Builder
	n := 1
	for r := 0; r < rows; r++ {
		if r > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for c := 0; c < columns; c++ {
			if c > 0 {
				b.WriteString(", ")
			}
			b.WriteString(d.Placeholder(n))
			n++
		}
		b.WriteByte(')')
	}
	return b.String()
}

// quoteAll renders a list of identifiers.
func quoteAll(d Dialect, names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = d.Quote(n)
	}
	return out
}

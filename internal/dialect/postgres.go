package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// PostgresDialect renders SQL for Aurora PostgreSQL and stock PostgreSQL.
type PostgresDialect struct{}

// NewPostgres returns the PostgreSQL dialect.
func NewPostgres() *PostgresDialect { return &PostgresDialect{} }

// Name identifies the engine.
func (p *PostgresDialect) Name() Name { return Postgres }

// Driver returns the database/sql driver name.
func (p *PostgresDialect) Driver() string { return "postgres" }

// Quote renders an identifier, doubling any embedded quote character so that a
// column named `weird"name` cannot terminate the quoting and inject SQL.
func (p *PostgresDialect) Quote(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// QuoteTable renders a schema-qualified table name.
func (p *PostgresDialect) QuoteTable(t model.TableRef) string {
	if t.Schema == "" {
		return p.Quote(t.Name)
	}
	return p.Quote(t.Schema) + "." + p.Quote(t.Name)
}

// Placeholder renders the nth bind parameter.
func (p *PostgresDialect) Placeholder(n int) string { return "$" + strconv.Itoa(n) }

// FencedUpsert renders a multi-row upsert guarded by the LSN fence.
//
// The WHERE clause on DO UPDATE is the entire point. Without it the last write
// to arrive wins, which is wrong whenever writes can arrive out of order — and
// they always can: Kafka redelivers after a rebalance, the repair worker drains
// dead letters minutes late, and a snapshot part loads after a change event that
// was captured while the extract ran. With it, the newest LSN always wins
// regardless of arrival order, and the pipeline becomes safe to replay.
func (p *PostgresDialect) FencedUpsert(spec model.TableSpec, columns []string, rows int) string {
	target := p.QuoteTable(spec.Target)
	cols := append(quoteAll(p, columns), p.Quote(ColLSN), p.Quote(ColDeleted), p.Quote(ColUpdatedAt))

	// Each row supplies its column values plus the LSN; deleted=false and the
	// timestamp are rendered as literals.
	var tuples strings.Builder
	n := 1
	for r := 0; r < rows; r++ {
		if r > 0 {
			tuples.WriteString(", ")
		}
		tuples.WriteByte('(')
		for c := 0; c < len(columns); c++ {
			if c > 0 {
				tuples.WriteString(", ")
			}
			tuples.WriteString(p.Placeholder(n))
			n++
		}
		fmt.Fprintf(&tuples, ", %s, FALSE, now()", p.Placeholder(n))
		n++
		tuples.WriteByte(')')
	}

	conflict := strings.Join(quoteAll(p, spec.PrimaryKey), ", ")

	assignments := make([]string, 0, len(columns)+3)
	pk := make(map[string]bool, len(spec.PrimaryKey))
	for _, k := range spec.PrimaryKey {
		pk[k] = true
	}
	for _, c := range columns {
		if pk[c] {
			continue
		}
		assignments = append(assignments, fmt.Sprintf("%s = EXCLUDED.%s", p.Quote(c), p.Quote(c)))
	}
	assignments = append(assignments,
		fmt.Sprintf("%s = EXCLUDED.%s", p.Quote(ColLSN), p.Quote(ColLSN)),
		fmt.Sprintf("%s = FALSE", p.Quote(ColDeleted)),
		fmt.Sprintf("%s = now()", p.Quote(ColUpdatedAt)),
	)

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s WHERE %s.%s <= EXCLUDED.%s",
		target,
		strings.Join(cols, ", "),
		tuples.String(),
		conflict,
		strings.Join(assignments, ", "),
		target, p.Quote(ColLSN), p.Quote(ColLSN),
	)
}

// FencedDelete writes a tombstone rather than removing the row.
//
// Removing the row would discard its LSN, and a delayed UPDATE carrying an older
// LSN would then re-insert it — resurrecting a record the source deleted, with no
// error anywhere. Keeping a tombstone preserves the LSN so the fence can reject
// that write. Tombstones are purged after cutover.
func (p *PostgresDialect) FencedDelete(spec model.TableSpec) string {
	target := p.QuoteTable(spec.Target)
	cols := append(quoteAll(p, spec.PrimaryKey), p.Quote(ColLSN), p.Quote(ColDeleted), p.Quote(ColUpdatedAt))

	n := 1
	vals := make([]string, 0, len(spec.PrimaryKey)+1)
	for range spec.PrimaryKey {
		vals = append(vals, p.Placeholder(n))
		n++
	}
	vals = append(vals, p.Placeholder(n), "TRUE", "now()")

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s = TRUE, %s = EXCLUDED.%s, %s = now() WHERE %s.%s <= EXCLUDED.%s",
		target,
		strings.Join(cols, ", "),
		strings.Join(vals, ", "),
		strings.Join(quoteAll(p, spec.PrimaryKey), ", "),
		p.Quote(ColDeleted), p.Quote(ColLSN), p.Quote(ColLSN), p.Quote(ColUpdatedAt),
		target, p.Quote(ColLSN), p.Quote(ColLSN),
	)
}

// CreateStagingTable creates an UNLOGGED table shaped like the target.
//
// UNLOGGED skips WAL for the staging table. The data is reproducible from the
// part file, so durability of the staging copy buys nothing, and skipping WAL
// roughly halves the write cost of the bulk load.
func (p *PostgresDialect) CreateStagingTable(target, staging model.TableRef, _ []string) string {
	return fmt.Sprintf(
		"CREATE UNLOGGED TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES)",
		p.QuoteTable(staging), p.QuoteTable(target),
	)
}

// DropStagingTable removes a staging table.
func (p *PostgresDialect) DropStagingTable(staging model.TableRef) string {
	return "DROP TABLE IF EXISTS " + p.QuoteTable(staging)
}

// BulkImport renders the aws_s3 extension's native import. The database reads
// directly from S3, so terabytes never traverse the worker process.
func (p *PostgresDialect) BulkImport(staging model.TableRef, columns []string, src S3Source) (string, []any) {
	opts := fmt.Sprintf("(FORMAT csv, DELIMITER %s, NULL %s, QUOTE '\"')",
		quoteLiteral(src.Delimiter), quoteLiteral(src.NullSentinel))

	q := fmt.Sprintf(
		"SELECT aws_s3.table_import_from_s3(%s, %s, %s, aws_commons.create_s3_uri(%s, %s, %s))",
		p.Placeholder(1), p.Placeholder(2), p.Placeholder(3),
		p.Placeholder(4), p.Placeholder(5), p.Placeholder(6),
	)
	args := []any{
		staging.String(),
		strings.Join(columns, ","),
		opts,
		src.Bucket, src.Key, src.Region,
	}
	return q, args
}

// MergeStaging moves a staged part into the live table in one statement.
func (p *PostgresDialect) MergeStaging(spec model.TableSpec, staging model.TableRef, columns []string, extractLSN uint64) string {
	target := p.QuoteTable(spec.Target)
	quoted := quoteAll(p, columns)
	insertCols := append(append([]string(nil), quoted...), p.Quote(ColLSN), p.Quote(ColDeleted), p.Quote(ColUpdatedAt))

	selectCols := make([]string, 0, len(columns)+3)
	for _, c := range columns {
		selectCols = append(selectCols, "s."+p.Quote(c))
	}
	selectCols = append(selectCols, strconv.FormatUint(extractLSN, 10), "FALSE", "now()")

	pk := make(map[string]bool, len(spec.PrimaryKey))
	for _, k := range spec.PrimaryKey {
		pk[k] = true
	}
	assignments := make([]string, 0, len(columns)+3)
	for _, c := range columns {
		if pk[c] {
			continue
		}
		assignments = append(assignments, fmt.Sprintf("%s = EXCLUDED.%s", p.Quote(c), p.Quote(c)))
	}
	assignments = append(assignments,
		fmt.Sprintf("%s = EXCLUDED.%s", p.Quote(ColLSN), p.Quote(ColLSN)),
		fmt.Sprintf("%s = now()", p.Quote(ColUpdatedAt)),
	)

	// DISTINCT ON collapses duplicate keys inside a single part. Without it, a
	// part containing the same key twice makes ON CONFLICT fail outright with
	// "cannot affect row a second time", failing the whole part rather than the
	// one bad row. The SELECT is wrapped in a subquery so that its ORDER BY —
	// which DISTINCT ON requires — cannot be mistaken for part of the INSERT.
	pkList := strings.Join(quoteAll(p, spec.PrimaryKey), ", ")
	sPK := strings.Join(prefixAll("s.", quoteAll(p, spec.PrimaryKey)), ", ")

	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT * FROM (SELECT DISTINCT ON (%s) %s FROM %s AS s ORDER BY %s) AS d ON CONFLICT (%s) DO UPDATE SET %s WHERE %s.%s <= EXCLUDED.%s",
		target,
		strings.Join(insertCols, ", "),
		sPK,
		strings.Join(selectCols, ", "),
		p.QuoteTable(staging),
		sPK,
		pkList,
		strings.Join(assignments, ", "),
		target, p.Quote(ColLSN), p.Quote(ColLSN),
	)
}

// RowDigestExpr renders the normalised per-row hash.
func (p *PostgresDialect) RowDigestExpr(alias string, columns []model.ColumnSpec) string {
	cols := columnsForDigest(columns)
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, p.normalise(alias, c))
	}
	return fmt.Sprintf("md5(concat_ws(%s, %s))", quoteLiteral(digestSeparator), strings.Join(parts, ", "))
}

// normalise renders one column into a canonical text form that MySQL's
// equivalent expression will agree with exactly.
func (p *PostgresDialect) normalise(alias string, c model.ColumnSpec) string {
	ref := p.Quote(c.Name)
	if alias != "" {
		ref = alias + "." + ref
	}

	var expr string
	switch c.Type {
	case model.TypeTimestamp:
		// Force UTC and a fixed precision. Two engines storing the same instant
		// at different precisions otherwise hash differently on every row.
		expr = fmt.Sprintf("to_char((%s AT TIME ZONE 'UTC'), 'YYYY-MM-DD\"T\"HH24:MI:SS.US')", ref)
	case model.TypeDate:
		expr = fmt.Sprintf("to_char(%s, 'YYYY-MM-DD')", ref)
	case model.TypeDecimal:
		// Round to the declared scale on both sides so that 1.50 and 1.5 —
		// which DB2 and Postgres disagree about preserving — compare equal.
		expr = fmt.Sprintf("trim(trailing '.' from to_char(round(%s::numeric, %d), 'FM999999999999999999990.%s'))",
			ref, c.Scale, strings.Repeat("9", maxInt(c.Scale, 0)))
		if c.Scale <= 0 {
			expr = fmt.Sprintf("to_char(round(%s::numeric, 0), 'FM999999999999999999990')", ref)
		}
	case model.TypeFloat:
		// Fixed significant digits: the two engines' default float-to-text
		// rendering differs in the last place often enough to matter.
		expr = fmt.Sprintf("to_char(%s::numeric, 'FM999999999999999999990.999999999999')", ref)
	case model.TypeBool:
		expr = fmt.Sprintf("CASE WHEN %s THEN '1' ELSE '0' END", ref)
	case model.TypeBytes:
		expr = fmt.Sprintf("encode(%s, 'hex')", ref)
	case model.TypeJSON:
		// Text form, not jsonb: key ordering and whitespace must not matter, and
		// jsonb's canonical form is the only stable one across both engines.
		expr = fmt.Sprintf("%s::jsonb::text", ref)
	default:
		expr = fmt.Sprintf("%s::text", ref)
	}

	if c.TrimTrailingSpace {
		// DB2 pads CHAR to the declared width; Postgres and MySQL do not return
		// that padding identically. Untrimmed, every CHAR column mismatches.
		expr = fmt.Sprintf("rtrim(%s)", expr)
	}
	// NULL must map to a sentinel that cannot occur in the data, or a NULL and a
	// row genuinely containing the sentinel string hash identically.
	return fmt.Sprintf("coalesce(%s, %s)", expr, quoteLiteral(NullSentinelSQL))
}

// RangeDigestQuery renders the order-independent range digest.
//
// Order independence is what allows the two sides to be scanned in whatever
// order each engine finds cheapest, and allows the scan to be parallelised,
// without the digests disagreeing. Summation gives that for free; an ordered
// concatenation would not.
func (p *PostgresDialect) RangeDigestQuery(t model.TableRef, columns []model.ColumnSpec, r Range, includeDeleted bool) (string, []any) {
	digest := p.RowDigestExpr("t", columns)
	where, args := p.whereClause("t", r, includeDeleted, 1)

	q := fmt.Sprintf(`SELECT count(*)::bigint,
       coalesce(sum(('x'||substr(%s,1,15))::bit(60)::bigint), 0)::text,
       coalesce(sum(('x'||substr(%s,17,15))::bit(60)::bigint), 0)::text
  FROM %s AS t%s`,
		digest, digest, p.QuoteTable(t), where)
	return q, args
}

// CountQuery renders a bounded row count.
func (p *PostgresDialect) CountQuery(t model.TableRef, r Range, includeDeleted bool) (string, []any) {
	where, args := p.whereClause("t", r, includeDeleted, 1)
	return fmt.Sprintf("SELECT count(*)::bigint FROM %s AS t%s", p.QuoteTable(t), where), args
}

// KeysetPageQuery walks the key space in order.
//
// Keyset pagination rather than OFFSET: OFFSET makes the database scan and
// discard every preceding row, so chunk N costs O(N) and chunking a large table
// degrades into a quadratic scan.
func (p *PostgresDialect) KeysetPageQuery(t model.TableRef, keyColumn string, limit int, after any) (string, []any) {
	col := p.Quote(keyColumn)
	if after == nil {
		return fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d", col, p.QuoteTable(t), col, limit), nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s > %s ORDER BY %s LIMIT %d",
		col, p.QuoteTable(t), col, p.Placeholder(1), col, limit), []any{after}
}

// RowsInRangeQuery selects full row images for a key range.
func (p *PostgresDialect) RowsInRangeQuery(t model.TableRef, columns []string, r Range, includeDeleted bool) (string, []any) {
	where, args := p.whereClause("t", r, includeDeleted, 1)
	order := ""
	if r.Column != "" {
		order = " ORDER BY t." + p.Quote(r.Column)
	}
	return fmt.Sprintf("SELECT %s FROM %s AS t%s%s",
		strings.Join(prefixAll("t.", quoteAll(p, columns)), ", "), p.QuoteTable(t), where, order), args
}

func (p *PostgresDialect) whereClause(alias string, r Range, includeDeleted bool, start int) (string, []any) {
	var conds []string
	var args []any
	n := start

	if r.Column != "" {
		col := alias + "." + p.Quote(r.Column)
		if r.Low != nil {
			conds = append(conds, fmt.Sprintf("%s > %s", col, p.Placeholder(n)))
			args = append(args, r.Low)
			n++
		}
		if r.High != nil {
			conds = append(conds, fmt.Sprintf("%s <= %s", col, p.Placeholder(n)))
			args = append(args, r.High)
			n++
		}
	}
	if !includeDeleted {
		conds = append(conds, fmt.Sprintf("coalesce(%s.%s, FALSE) = FALSE", alias, p.Quote(ColDeleted)))
	}
	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// SelectClaimableDeadLetters locks a batch of due dead letters with SKIP LOCKED.
func (p *PostgresDialect) SelectClaimableDeadLetters(limit int) string {
	return fmt.Sprintf(`SELECT id, migration_id, source_table, op, row_key_hash, payload, payload_encrypted,
       error_class, last_error, attempts, first_seen_at, next_retry_at, source_lsn, topic, partition, "offset"
  FROM %s.dead_letter
 WHERE status = 'pending' AND next_retry_at <= now()
 ORDER BY next_retry_at
 LIMIT %d
 FOR UPDATE SKIP LOCKED`, ControlSchema, limit)
}

// MarkDeadLettersClaimed takes ownership of the locked rows.
func (p *PostgresDialect) MarkDeadLettersClaimed(ids int) string {
	ph := make([]string, ids)
	for i := range ph {
		ph[i] = p.Placeholder(i + 2)
	}
	return fmt.Sprintf(`UPDATE %s.dead_letter
   SET status = 'retrying', claimed_at = now(), claimed_by = $1
 WHERE id IN (%s)`, ControlSchema, strings.Join(ph, ", "))
}

// UpsertOffset records stream progress inside the data transaction.
func (p *PostgresDialect) UpsertOffset() string {
	return fmt.Sprintf(`INSERT INTO %s.applied_offset (migration_id, topic, partition, "offset", last_lsn, updated_at)
 VALUES ($1, $2, $3, $4, $5, now())
 ON CONFLICT (migration_id, topic, partition)
 DO UPDATE SET "offset" = EXCLUDED."offset", last_lsn = EXCLUDED.last_lsn, updated_at = now()
 WHERE %s.applied_offset."offset" <= EXCLUDED."offset"`, ControlSchema, ControlSchema)
}

// SelectOffsets restores progress on startup.
func (p *PostgresDialect) SelectOffsets() string {
	return fmt.Sprintf(`SELECT topic, partition, "offset", last_lsn FROM %s.applied_offset WHERE migration_id = $1`, ControlSchema)
}

// PurgeTombstones removes soft-deleted rows after cutover.
func (p *PostgresDialect) PurgeTombstones(t model.TableRef) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = TRUE", p.QuoteTable(t), p.Quote(ColDeleted))
}

// BulkLoadSessionSettings tunes one connection for bulk loading.
//
// synchronous_commit is the significant one: it lets the transaction return
// before its WAL record reaches disk. That is normally unacceptable, but a
// staged part is fully reproducible from the part file and its digest, so the
// worst case of losing an unflushed commit is reloading one part — which the
// loader already knows how to do.
func (p *PostgresDialect) BulkLoadSessionSettings() []string {
	return []string{
		"SET LOCAL synchronous_commit = OFF",
		"SET LOCAL maintenance_work_mem = '1GB'",
		"SET LOCAL statement_timeout = 0",
	}
}

// RestoreSessionSettings returns the session to its defaults.
func (p *PostgresDialect) RestoreSessionSettings() []string {
	return []string{
		"SET LOCAL synchronous_commit = ON",
		"SET LOCAL statement_timeout = '60s'",
	}
}

// quoteLiteral renders a single-quoted SQL string literal with embedded quotes
// doubled. Used only for values that are structurally part of a statement, such
// as a format specifier; row data always travels as a bind parameter.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func prefixAll(prefix string, items []string) []string {
	out := make([]string, len(items))
	for i, s := range items {
		out[i] = prefix + s
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

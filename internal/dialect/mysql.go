package dialect

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// MySQLDialect renders SQL for Aurora MySQL and MySQL 8.0.
//
// It targets 8.0.19 or later, which is what Aurora MySQL 3 provides. That
// version introduced the row-alias form of ON DUPLICATE KEY UPDATE, which this
// dialect depends on: the deprecated VALUES() function cannot be used inside the
// conditional expression that implements the LSN fence.
type MySQLDialect struct{}

// NewMySQL returns the MySQL dialect.
func NewMySQL() *MySQLDialect { return &MySQLDialect{} }

// Name identifies the engine.
func (m *MySQLDialect) Name() Name { return MySQL }

// Driver returns the database/sql driver name.
func (m *MySQLDialect) Driver() string { return "mysql" }

// Quote renders an identifier, doubling any embedded backtick.
func (m *MySQLDialect) Quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// QuoteTable renders a schema-qualified table name.
func (m *MySQLDialect) QuoteTable(t model.TableRef) string {
	if t.Schema == "" {
		return m.Quote(t.Name)
	}
	return m.Quote(t.Schema) + "." + m.Quote(t.Name)
}

// Placeholder renders a bind parameter. MySQL uses positional question marks, so
// the index is ignored — but the signature stays uniform so that shared helpers
// can build statements for either engine.
func (m *MySQLDialect) Placeholder(int) string { return "?" }

// newRowAlias names the incoming row in an upsert.
const newRowAlias = "`new_row`"

// FencedUpsert renders a multi-row upsert guarded by the LSN fence.
//
// MySQL has no WHERE clause on ON DUPLICATE KEY UPDATE, so the fence cannot be
// expressed once for the statement the way Postgres allows. Instead every
// assignment becomes conditional: keep the incoming value when its LSN is at
// least as new, otherwise write the column back to itself. The effect is
// identical — a stale write cannot win — but it is worth understanding that this
// version still performs a write, it just writes the existing values, so the
// affected-rows count means something different on the two engines.
func (m *MySQLDialect) FencedUpsert(spec model.TableSpec, columns []string, rows int) string {
	target := m.QuoteTable(spec.Target)
	cols := append(quoteAll(m, columns), m.Quote(ColLSN), m.Quote(ColDeleted), m.Quote(ColUpdatedAt))

	var tuples strings.Builder
	for r := 0; r < rows; r++ {
		if r > 0 {
			tuples.WriteString(", ")
		}
		tuples.WriteByte('(')
		for c := 0; c < len(columns); c++ {
			if c > 0 {
				tuples.WriteString(", ")
			}
			tuples.WriteByte('?')
		}
		tuples.WriteString(", ?, 0, UTC_TIMESTAMP(6))")
	}

	fence := fmt.Sprintf("%s.%s <= %s.%s", target, m.Quote(ColLSN), newRowAlias, m.Quote(ColLSN))

	pk := make(map[string]bool, len(spec.PrimaryKey))
	for _, k := range spec.PrimaryKey {
		pk[k] = true
	}
	assignments := make([]string, 0, len(columns)+3)
	for _, c := range columns {
		if pk[c] {
			continue
		}
		q := m.Quote(c)
		assignments = append(assignments, fmt.Sprintf("%s = IF(%s, %s.%s, %s.%s)", q, fence, newRowAlias, q, target, q))
	}
	assignments = append(assignments,
		fmt.Sprintf("%s = IF(%s, %s.%s, %s.%s)", m.Quote(ColLSN), fence, newRowAlias, m.Quote(ColLSN), target, m.Quote(ColLSN)),
		fmt.Sprintf("%s = IF(%s, 0, %s.%s)", m.Quote(ColDeleted), fence, target, m.Quote(ColDeleted)),
		fmt.Sprintf("%s = IF(%s, UTC_TIMESTAMP(6), %s.%s)", m.Quote(ColUpdatedAt), fence, target, m.Quote(ColUpdatedAt)),
	)

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS %s ON DUPLICATE KEY UPDATE %s",
		target, strings.Join(cols, ", "), tuples.String(), newRowAlias, strings.Join(assignments, ", "))
}

// FencedDelete writes a tombstone rather than removing the row, preserving the
// LSN so that a delayed older UPDATE cannot resurrect a deleted record.
func (m *MySQLDialect) FencedDelete(spec model.TableSpec) string {
	target := m.QuoteTable(spec.Target)
	cols := append(quoteAll(m, spec.PrimaryKey), m.Quote(ColLSN), m.Quote(ColDeleted), m.Quote(ColUpdatedAt))

	vals := make([]string, 0, len(spec.PrimaryKey)+3)
	for range spec.PrimaryKey {
		vals = append(vals, "?")
	}
	vals = append(vals, "?", "1", "UTC_TIMESTAMP(6)")

	fence := fmt.Sprintf("%s.%s <= %s.%s", target, m.Quote(ColLSN), newRowAlias, m.Quote(ColLSN))

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) AS %s ON DUPLICATE KEY UPDATE %s = IF(%s, 1, %s.%s), %s = IF(%s, %s.%s, %s.%s), %s = IF(%s, UTC_TIMESTAMP(6), %s.%s)",
		target, strings.Join(cols, ", "), strings.Join(vals, ", "), newRowAlias,
		m.Quote(ColDeleted), fence, target, m.Quote(ColDeleted),
		m.Quote(ColLSN), fence, newRowAlias, m.Quote(ColLSN), target, m.Quote(ColLSN),
		m.Quote(ColUpdatedAt), fence, target, m.Quote(ColUpdatedAt),
	)
}

// CreateStagingTable creates a table shaped like the target, without secondary
// indexes so that the bulk load does not pay for index maintenance it does not
// need — the merge reads the staging table once, sequentially.
func (m *MySQLDialect) CreateStagingTable(target, staging model.TableRef, _ []string) string {
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s LIKE %s", m.QuoteTable(staging), m.QuoteTable(target))
}

// DropStagingTable removes a staging table.
func (m *MySQLDialect) DropStagingTable(staging model.TableRef) string {
	return "DROP TABLE IF EXISTS " + m.QuoteTable(staging)
}

// BulkImport renders Aurora MySQL's LOAD DATA FROM S3.
//
// The NULL sentinel is handled by a per-column SET expression rather than a
// dedicated option, because MySQL's loader only treats a literal \N as NULL for
// unquoted fields — and the CSV writer quotes anything containing a delimiter.
// Without the explicit NULLIF, every quoted NULL would load as the literal
// two-character string.
func (m *MySQLDialect) BulkImport(staging model.TableRef, columns []string, src S3Source) (query string, args []any) {
	vars := make([]string, len(columns))
	sets := make([]string, len(columns))
	for i, c := range columns {
		v := "@v" + strconv.Itoa(i)
		vars[i] = v
		sets[i] = fmt.Sprintf("%s = NULLIF(%s, %s)", m.Quote(c), v, quoteLiteralMySQL(src.NullSentinel))
	}

	q := fmt.Sprintf(
		"LOAD DATA FROM S3 %s INTO TABLE %s FIELDS TERMINATED BY %s ENCLOSED BY '\"' LINES TERMINATED BY '\\n' (%s) SET %s",
		quoteLiteralMySQL(src.URI()),
		m.QuoteTable(staging),
		quoteLiteralMySQL(src.Delimiter),
		strings.Join(vars, ", "),
		strings.Join(sets, ", "),
	)
	return q, nil
}

// MergeStaging moves a staged part into the live table in one statement.
func (m *MySQLDialect) MergeStaging(spec model.TableSpec, staging model.TableRef, columns []string, extractLSN uint64) string {
	target := m.QuoteTable(spec.Target)
	insertCols := append(quoteAll(m, columns), m.Quote(ColLSN), m.Quote(ColDeleted), m.Quote(ColUpdatedAt))

	fence := fmt.Sprintf("%s.%s <= %s.%s", target, m.Quote(ColLSN), newRowAlias, m.Quote(ColLSN))

	pk := make(map[string]bool, len(spec.PrimaryKey))
	for _, k := range spec.PrimaryKey {
		pk[k] = true
	}
	assignments := make([]string, 0, len(columns)+2)
	for _, c := range columns {
		if pk[c] {
			continue
		}
		q := m.Quote(c)
		assignments = append(assignments, fmt.Sprintf("%s = IF(%s, %s.%s, %s.%s)", q, fence, newRowAlias, q, target, q))
	}
	assignments = append(assignments,
		fmt.Sprintf("%s = IF(%s, %s.%s, %s.%s)", m.Quote(ColLSN), fence, newRowAlias, m.Quote(ColLSN), target, m.Quote(ColLSN)),
		fmt.Sprintf("%s = IF(%s, UTC_TIMESTAMP(6), %s.%s)", m.Quote(ColUpdatedAt), fence, target, m.Quote(ColUpdatedAt)),
	)

	// The inner SELECT is aliased as a derived table because MySQL only accepts
	// the new-row alias on the VALUES form of INSERT; for INSERT ... SELECT the
	// documented way to reference the incoming row is a derived table alias.
	//
	// ROW_NUMBER collapses duplicate keys inside a single part. A part containing
	// the same key twice would otherwise apply the row twice, and under
	// ONLY_FULL_GROUP_BY a GROUP BY would be rejected outright — so the window
	// function is both the correct and the portable choice.
	inner := make([]string, 0, len(columns)+4)
	for _, c := range columns {
		inner = append(inner, fmt.Sprintf("s.%s AS %s", m.Quote(c), m.Quote(c)))
	}
	inner = append(inner,
		fmt.Sprintf("%d AS %s", extractLSN, m.Quote(ColLSN)),
		fmt.Sprintf("0 AS %s", m.Quote(ColDeleted)),
		fmt.Sprintf("UTC_TIMESTAMP(6) AS %s", m.Quote(ColUpdatedAt)),
		fmt.Sprintf("ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS %s",
			strings.Join(prefixAll("s.", quoteAll(m, spec.PrimaryKey)), ", "),
			strings.Join(prefixAll("s.", quoteAll(m, spec.PrimaryKey)), ", "),
			m.Quote("_mig_rn")),
	)

	outer := make([]string, 0, len(columns)+3)
	for _, c := range columns {
		outer = append(outer, newRowAlias+"."+m.Quote(c))
	}
	outer = append(outer,
		newRowAlias+"."+m.Quote(ColLSN),
		newRowAlias+"."+m.Quote(ColDeleted),
		newRowAlias+"."+m.Quote(ColUpdatedAt),
	)

	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM (SELECT %s FROM %s AS s) AS %s WHERE %s.%s = 1 ON DUPLICATE KEY UPDATE %s",
		target,
		strings.Join(insertCols, ", "),
		strings.Join(outer, ", "),
		strings.Join(inner, ", "),
		m.QuoteTable(staging),
		newRowAlias,
		newRowAlias, m.Quote("_mig_rn"),
		strings.Join(assignments, ", "),
	)
}

// RowDigestExpr renders the normalised per-row hash. Every branch here has a
// counterpart in the Postgres dialect that must produce byte-identical output
// for the same logical row.
func (m *MySQLDialect) RowDigestExpr(alias string, columns []model.ColumnSpec) string {
	cols := columnsForDigest(columns)
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, m.normalise(alias, c))
	}
	return fmt.Sprintf("MD5(CONCAT_WS(%s, %s))", quoteLiteralMySQL(digestSeparator), strings.Join(parts, ", "))
}

func (m *MySQLDialect) normalise(alias string, c model.ColumnSpec) string {
	ref := m.Quote(c.Name)
	if alias != "" {
		ref = alias + "." + ref
	}

	var expr string
	switch c.Type {
	case model.TypeTimestamp:
		expr = fmt.Sprintf("DATE_FORMAT(CONVERT_TZ(%s, @@session.time_zone, '+00:00'), '%%Y-%%m-%%dT%%H:%%i:%%s.%%f')", ref)
	case model.TypeDate:
		expr = fmt.Sprintf("DATE_FORMAT(%s, '%%Y-%%m-%%d')", ref)
	case model.TypeDecimal:
		// FORMAT pads to exactly the requested scale, which is what the Postgres
		// side is now made to match. It also inserts thousands separators, which
		// Postgres does not, so those are stripped.
		if c.Scale > 0 {
			expr = fmt.Sprintf("REPLACE(FORMAT(ROUND(CAST(%s AS DECIMAL(65,%d)), %d), %d, 'en_US'), ',', '')",
				ref, c.Scale, c.Scale, c.Scale)
		} else {
			expr = fmt.Sprintf("CAST(ROUND(CAST(%s AS DECIMAL(65,0)), 0) AS CHAR)", ref)
		}
	case model.TypeFloat:
		expr = fmt.Sprintf("REPLACE(FORMAT(CAST(%s AS DECIMAL(65,12)), 12, 'en_US'), ',', '')", ref)
	case model.TypeBool:
		expr = fmt.Sprintf("IF(%s, '1', '0')", ref)
	case model.TypeBytes:
		expr = fmt.Sprintf("LOWER(HEX(%s))", ref)
	case model.TypeJSON:
		expr = fmt.Sprintf("CAST(%s AS CHAR)", ref)
	default:
		expr = fmt.Sprintf("CAST(%s AS CHAR)", ref)
	}

	if c.TrimTrailingSpace {
		expr = fmt.Sprintf("RTRIM(%s)", expr)
	}
	return fmt.Sprintf("IFNULL(%s, %s)", expr, quoteLiteralMySQL(NullSentinelSQL))
}

// RangeDigestQuery renders the order-independent range digest, matching the
// Postgres formulation: two independent 60-bit projections of the row hash,
// summed, plus a row count.
func (m *MySQLDialect) RangeDigestQuery(t model.TableRef, columns []model.ColumnSpec, r Range, includeDeleted bool) (query string, args []any) {
	digest := m.RowDigestExpr("t", columns)
	where, args := m.whereClause("t", r, includeDeleted)

	q := fmt.Sprintf(`SELECT CAST(COUNT(*) AS SIGNED),
       CAST(IFNULL(SUM(CAST(CONV(SUBSTR(%s,1,15),16,10) AS DECIMAL(65,0))), 0) AS CHAR),
       CAST(IFNULL(SUM(CAST(CONV(SUBSTR(%s,17,15),16,10) AS DECIMAL(65,0))), 0) AS CHAR)
  FROM %s AS t%s`,
		digest, digest, m.QuoteTable(t), where)
	return q, args
}

// CountQuery renders a bounded row count.
func (m *MySQLDialect) CountQuery(t model.TableRef, r Range, includeDeleted bool) (query string, args []any) {
	where, args := m.whereClause("t", r, includeDeleted)
	return fmt.Sprintf("SELECT CAST(COUNT(*) AS SIGNED) FROM %s AS t%s", m.QuoteTable(t), where), args
}

// KeysetPageQuery walks the key space in order without an OFFSET scan.
func (m *MySQLDialect) KeysetPageQuery(t model.TableRef, keyColumn string, limit int, after any) (query string, args []any) {
	col := m.Quote(keyColumn)
	if after == nil {
		return fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d", col, m.QuoteTable(t), col, limit), nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT %d",
		col, m.QuoteTable(t), col, col, limit), []any{after}
}

// RowsInRangeQuery selects full row images for a key range.
func (m *MySQLDialect) RowsInRangeQuery(t model.TableRef, columns []string, r Range, includeDeleted bool) (query string, args []any) {
	where, args := m.whereClause("t", r, includeDeleted)
	order := ""
	if r.Column != "" {
		order = " ORDER BY t." + m.Quote(r.Column)
	}
	return fmt.Sprintf("SELECT %s FROM %s AS t%s%s",
		strings.Join(prefixAll("t.", quoteAll(m, columns)), ", "), m.QuoteTable(t), where, order), args
}

func (m *MySQLDialect) whereClause(alias string, r Range, includeDeleted bool) (query string, args []any) {
	var conds []string

	if r.Column != "" {
		col := alias + "." + m.Quote(r.Column)
		if r.Low != nil {
			conds = append(conds, col+" > ?")
			args = append(args, r.Low)
		}
		if r.High != nil {
			conds = append(conds, col+" <= ?")
			args = append(args, r.High)
		}
	}
	if !includeDeleted {
		conds = append(conds, fmt.Sprintf("IFNULL(%s.%s, 0) = 0", alias, m.Quote(ColDeleted)))
	}
	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// SelectClaimableDeadLetters locks a batch of due dead letters with SKIP LOCKED.
func (m *MySQLDialect) SelectClaimableDeadLetters(limit int) string {
	return fmt.Sprintf("SELECT id, migration_id, source_table, op, row_key_hash, payload, payload_encrypted,"+
		" error_class, last_error, attempts, first_seen_at, next_retry_at, source_lsn, topic, `partition`, `offset`"+
		" FROM %s.dead_letter"+
		" WHERE status = 'pending' AND next_retry_at <= UTC_TIMESTAMP(6)"+
		" ORDER BY next_retry_at LIMIT %d FOR UPDATE SKIP LOCKED", ControlSchema, limit)
}

// MarkDeadLettersClaimed takes ownership of the locked rows.
func (m *MySQLDialect) MarkDeadLettersClaimed(ids int) string {
	ph := strings.TrimSuffix(strings.Repeat("?, ", ids), ", ")
	return fmt.Sprintf("UPDATE %s.dead_letter SET status = 'retrying', claimed_at = UTC_TIMESTAMP(6), claimed_by = ?"+
		" WHERE id IN (%s)", ControlSchema, ph)
}

// UpsertOffset records stream progress inside the data transaction.
func (m *MySQLDialect) UpsertOffset() string {
	return fmt.Sprintf("INSERT INTO %s.applied_offset (migration_id, topic, `partition`, `offset`, last_lsn, updated_at)"+
		" VALUES (?, ?, ?, ?, ?, UTC_TIMESTAMP(6)) AS %s"+
		" ON DUPLICATE KEY UPDATE"+
		" `offset` = IF(%s.applied_offset.`offset` <= %s.`offset`, %s.`offset`, %s.applied_offset.`offset`),"+
		" last_lsn = IF(%s.applied_offset.`offset` <= %s.`offset`, %s.last_lsn, %s.applied_offset.last_lsn),"+
		" updated_at = UTC_TIMESTAMP(6)",
		ControlSchema, newRowAlias,
		ControlSchema, newRowAlias, newRowAlias, ControlSchema,
		ControlSchema, newRowAlias, newRowAlias, ControlSchema)
}

// SelectOffsets restores progress on startup.
func (m *MySQLDialect) SelectOffsets() string {
	return fmt.Sprintf("SELECT topic, `partition`, `offset`, last_lsn FROM %s.applied_offset WHERE migration_id = ?", ControlSchema)
}

// PurgeTombstones removes soft-deleted rows after cutover.
func (m *MySQLDialect) PurgeTombstones(t model.TableRef) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = 1", m.QuoteTable(t), m.Quote(ColDeleted))
}

// BulkLoadSessionSettings tunes one session for bulk loading.
//
// unique_checks and foreign_key_checks are safe to relax here because the merge
// statement enforces key uniqueness itself and the load order across tables is
// controlled by the plan. They are restored immediately afterwards so that no
// ordinary transaction ever runs with them off.
func (m *MySQLDialect) BulkLoadSessionSettings() []string {
	return []string{
		"SET SESSION unique_checks = 0",
		"SET SESSION foreign_key_checks = 0",
		"SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'",
	}
}

// RestoreSessionSettings returns the session to its defaults.
func (m *MySQLDialect) RestoreSessionSettings() []string {
	return []string{
		"SET SESSION unique_checks = 1",
		"SET SESSION foreign_key_checks = 1",
	}
}

// quoteLiteralMySQL renders a single-quoted literal with backslash and quote
// escaping, matching MySQL's default (non-ANSI_QUOTES) string semantics.
func quoteLiteralMySQL(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(s) + "'"
}

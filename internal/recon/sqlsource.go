package recon

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// SQLPair computes digests and reads rows from a real source and target.
//
// The two sides can be different engines, which is the whole point: the digest
// expressions are generated per dialect specifically so that a DB2-shaped row and
// an Aurora-shaped row of the same logical content hash to the same value.
type SQLPair struct {
	Source     *sql.DB
	Target     *sql.DB
	SourceDial dialect.Dialect
	TargetDial dialect.Dialect
	Spec       model.TableSpec
}

// Digest computes the range digest on one side.
func (p *SQLPair) Digest(ctx context.Context, side Side, r dialect.Range) (dialect.Digest, error) {
	db, d, table := p.pick(side)
	if r.Column == "" {
		r.Column = p.Spec.Key()
	}

	// Tombstones exist only on the target and have no counterpart on the source,
	// so including them would report a discrepancy for every deleted row.
	q, args := d.RangeDigestQuery(table, p.Spec.ComparableColumns(), r, false)

	var digest dialect.Digest
	if err := db.QueryRowContext(ctx, q, args...).Scan(&digest.Rows, &digest.SumLow, &digest.SumHi); err != nil {
		return dialect.Digest{}, fmt.Errorf("recon: digesting %s on the %s: %w", table, side, err)
	}
	return digest, nil
}

// Rows reads per-row digests for a narrow range.
func (p *SQLPair) Rows(ctx context.Context, side Side, r dialect.Range) (map[string]RowDigest, error) {
	db, d, table := p.pick(side)
	if r.Column == "" {
		r.Column = p.Spec.Key()
	}

	keyCols := p.Spec.PrimaryKey
	selectCols := make([]string, 0, len(keyCols))
	selectCols = append(selectCols, keyCols...)

	where, args := rangePredicate(d, r)
	q := fmt.Sprintf("SELECT %s, %s FROM %s AS t%s",
		joinQuoted(d, "t", selectCols),
		d.RowDigestExpr("t", p.Spec.ComparableColumns()),
		d.QuoteTable(table), where)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("recon: reading rows from %s on the %s: %w", table, side, err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]RowDigest)
	for rows.Next() {
		scan := make([]any, len(keyCols)+1)
		holders := make([]any, len(keyCols))
		for i := range keyCols {
			holders[i] = new(any)
			scan[i] = holders[i]
		}
		var digest sql.NullString
		scan[len(keyCols)] = &digest

		if err := rows.Scan(scan...); err != nil {
			return nil, fmt.Errorf("recon: scanning row from %s: %w", table, err)
		}

		values := make(map[string]any, len(keyCols))
		for i, name := range keyCols {
			v := *(holders[i].(*any))
			// Drivers return text columns as []byte on one engine and string on
			// the other. Normalising here is what stops every row on a
			// string-keyed table from being reported as missing on both sides.
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			values[name] = v
		}
		key := model.NewRowKeyOrdered(keyCols, values)
		out[key.Canonical()] = RowDigest{Key: key, Digest: digest.String}
	}
	return out, rows.Err()
}

func (p *SQLPair) pick(side Side) (*sql.DB, dialect.Dialect, model.TableRef) {
	if side == TargetSide {
		return p.Target, p.TargetDial, p.Spec.Target
	}
	return p.Source, p.SourceDial, p.Spec.Source
}

// rangePredicate renders the half-open (low, high] predicate for either dialect.
func rangePredicate(d dialect.Dialect, r dialect.Range) (string, []any) {
	if r.Column == "" || (r.Low == nil && r.High == nil) {
		return "", nil
	}
	col := "t." + d.Quote(r.Column)
	var clause string
	var args []any
	n := 1
	if r.Low != nil {
		clause = " WHERE " + col + " > " + d.Placeholder(n)
		args = append(args, r.Low)
		n++
	}
	if r.High != nil {
		if clause == "" {
			clause = " WHERE "
		} else {
			clause += " AND "
		}
		clause += col + " <= " + d.Placeholder(n)
		args = append(args, r.High)
	}
	return clause, args
}

func joinQuoted(d dialect.Dialect, alias string, names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ", "
		}
		out += alias + "." + d.Quote(n)
	}
	return out
}

// FullRange returns an unbounded range over a table's chunk column, which is
// where every reconciliation run starts.
func FullRange(spec model.TableSpec) dialect.Range {
	return dialect.Range{Column: spec.Key()}
}

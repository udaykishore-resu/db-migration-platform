//go:build integration

// Package integration exercises the generated SQL against real database engines.
//
// The unit tests assert the *shape* of the generated SQL. That catches most
// mistakes but not the ones that matter most, because the interesting claims —
// "a stale write cannot win", "a delayed update cannot resurrect a deleted row",
// "both engines hash the same logical row identically" — are claims about how
// PostgreSQL and MySQL actually behave, not about string contents. Those can only
// be verified by running the statements.
//
// Run with:  make up && make integration
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/sink"
)

func testSpec(schema string) model.TableSpec {
	return model.TableSpec{
		Source:     model.TableRef{Schema: schema, Name: "accounts"},
		Target:     model.TableRef{Schema: schema, Name: "accounts"},
		PrimaryKey: []string{"account_id"},
		Columns: []model.ColumnSpec{
			{Name: "account_id", Type: model.TypeString},
			{Name: "branch_code", Type: model.TypeString, TrimTrailingSpace: true},
			{Name: "balance", Type: model.TypeDecimal, Scale: 2},
			{Name: "opened_on", Type: model.TypeDate},
			{Name: "note", Type: model.TypeString, Nullable: true},
		},
	}
}

type engine struct {
	name    string
	db      *sql.DB
	dialect dialect.Dialect
	schema  string
}

func openEngines(t *testing.T) []engine {
	t.Helper()
	var out []engine

	if dsn := os.Getenv("PG_DSN"); dsn != "" {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatalf("opening postgres: %v", err)
		}
		if err := db.Ping(); err != nil {
			t.Fatalf("pinging postgres: %v", err)
		}
		out = append(out, engine{name: "postgres", db: db, dialect: dialect.NewPostgres(), schema: "public"})
	}
	if dsn := os.Getenv("MYSQL_DSN"); dsn != "" {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("opening mysql: %v", err)
		}
		if err := db.Ping(); err != nil {
			t.Fatalf("pinging mysql: %v", err)
		}
		out = append(out, engine{name: "mysql", db: db, dialect: dialect.NewMySQL(), schema: "target"})
	}

	if len(out) == 0 {
		t.Skip("neither PG_DSN nor MYSQL_DSN is set")
	}
	t.Cleanup(func() {
		for _, e := range out {
			_ = e.db.Close()
		}
	})
	return out
}

// createTable builds the migrated table plus the platform's metadata columns.
func createTable(t *testing.T, e engine, name string) model.TableRef {
	t.Helper()
	ref := model.TableRef{Schema: e.schema, Name: name}
	q := e.dialect.QuoteTable(ref)

	var ddl string
	if e.dialect.Name() == dialect.Postgres {
		ddl = fmt.Sprintf(`CREATE TABLE %s (
			account_id       VARCHAR(64) PRIMARY KEY,
			branch_code      CHAR(8),
			balance          NUMERIC(18,4),
			opened_on        DATE,
			note             TEXT,
			_mig_lsn         BIGINT      NOT NULL DEFAULT 0,
			_mig_deleted     BOOLEAN     NOT NULL DEFAULT FALSE,
			_mig_updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, q)
	} else {
		ddl = fmt.Sprintf(`CREATE TABLE %s (
			account_id       VARCHAR(64) NOT NULL PRIMARY KEY,
			branch_code      CHAR(8),
			balance          DECIMAL(18,4),
			opened_on        DATE,
			note             TEXT,
			_mig_lsn         BIGINT      NOT NULL DEFAULT 0,
			_mig_deleted     TINYINT(1)  NOT NULL DEFAULT 0,
			_mig_updated_at  DATETIME(6) NOT NULL DEFAULT UTC_TIMESTAMP(6)
		) ENGINE=InnoDB`, q)
	}

	ctx := context.Background()
	if _, err := e.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+q); err != nil {
		t.Fatalf("%s: dropping %s: %v", e.name, name, err)
	}
	if _, err := e.db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("%s: creating %s: %v", e.name, name, err)
	}
	t.Cleanup(func() { _, _ = e.db.Exec("DROP TABLE IF EXISTS " + q) })
	return ref
}

func upsert(t *testing.T, e engine, spec model.TableSpec, lsn uint64, values ...any) {
	t.Helper()
	cols := spec.TargetColumnNames()
	q := e.dialect.FencedUpsert(spec, cols, 1)
	args := append(append([]any{}, values...), int64(lsn))
	if _, err := e.db.Exec(q, args...); err != nil {
		t.Fatalf("%s: upsert: %v\nSQL: %s", e.name, err, q)
	}
}

func readBalance(t *testing.T, e engine, ref model.TableRef, id string) (string, uint64, bool) {
	t.Helper()
	q := fmt.Sprintf("SELECT balance, _mig_lsn, _mig_deleted FROM %s WHERE account_id = %s",
		e.dialect.QuoteTable(ref), e.dialect.Placeholder(1))
	var balance sql.NullString
	var lsn int64
	var deleted bool
	if err := e.db.QueryRow(q, id).Scan(&balance, &lsn, &deleted); err != nil {
		t.Fatalf("%s: reading row: %v", e.name, err)
	}
	return balance.String, uint64(lsn), deleted
}

// ---------------------------------------------------------------- the fence

// The central correctness claim of the platform: a write carrying an older LSN
// cannot overwrite a row that a newer write already touched. This is what makes
// replay, retry and reordering safe, and it is a claim about engine behaviour
// rather than about SQL text.
func TestFencedUpsertRejectsStaleWrite(t *testing.T) {
	for _, e := range openEngines(t) {
		t.Run(e.name, func(t *testing.T) {
			ref := createTable(t, e, "accounts")
			spec := testSpec(e.schema)
			spec.Source, spec.Target = ref, ref

			day := "2026-01-01"
			upsert(t, e, spec, 100, "A-1", "BR01    ", "500.00", day, "fresh")

			// A stale replay arrives late: same row, older LSN.
			upsert(t, e, spec, 50, "A-1", "BR01    ", "1.00", day, "stale")

			balance, lsn, _ := readBalance(t, e, ref, "A-1")
			if lsn != 100 {
				t.Fatalf("stale write moved the LSN to %d", lsn)
			}
			if !approxEqual(balance, 500) {
				t.Fatalf("stale write won: balance is %q, expected 500", balance)
			}

			// A genuinely newer write must still win.
			upsert(t, e, spec, 200, "A-1", "BR01    ", "750.00", day, "newer")
			balance, lsn, _ = readBalance(t, e, ref, "A-1")
			if lsn != 200 || !approxEqual(balance, 750) {
				t.Fatalf("newer write did not win: balance=%q lsn=%d", balance, lsn)
			}
		})
	}
}

// A hard DELETE discards the row's LSN, so a delayed older UPDATE re-inserts it
// and resurrects a record the source deleted — silently. The tombstone keeps the
// LSN available to reject that write.
func TestTombstonePreventsResurrection(t *testing.T) {
	for _, e := range openEngines(t) {
		t.Run(e.name, func(t *testing.T) {
			ref := createTable(t, e, "accounts")
			spec := testSpec(e.schema)
			spec.Source, spec.Target = ref, ref

			day := "2026-01-01"
			upsert(t, e, spec, 100, "A-1", "BR01    ", "500.00", day, "live")

			// Delete at LSN 200.
			if _, err := e.db.Exec(e.dialect.FencedDelete(spec), "A-1", int64(200)); err != nil {
				t.Fatalf("%s: delete: %v", e.name, err)
			}
			if _, _, deleted := readBalance(t, e, ref, "A-1"); !deleted {
				t.Fatal("row was not tombstoned")
			}

			// A delayed UPDATE from before the delete arrives.
			upsert(t, e, spec, 150, "A-1", "BR01    ", "999.00", day, "zombie")

			balance, lsn, deleted := readBalance(t, e, ref, "A-1")
			if !deleted {
				t.Fatalf("a delayed older update resurrected a deleted row (balance=%q lsn=%d)", balance, lsn)
			}
			if lsn != 200 {
				t.Fatalf("tombstone LSN was overwritten: %d", lsn)
			}
		})
	}
}

// A multi-row upsert containing the same key twice makes PostgreSQL raise
// "cannot affect row a second time" and abort the whole statement. Coalescing is
// what prevents a hot row from failing the batch it appears in — verify the
// engine really does behave that way, so the mitigation is not cargo cult.
func TestDuplicateKeyInOneStatementFailsWithoutCoalescing(t *testing.T) {
	for _, e := range openEngines(t) {
		if e.dialect.Name() != dialect.Postgres {
			continue
		}
		t.Run(e.name, func(t *testing.T) {
			ref := createTable(t, e, "accounts")
			spec := testSpec(e.schema)
			spec.Source, spec.Target = ref, ref

			q := e.dialect.FencedUpsert(spec, spec.TargetColumnNames(), 2)
			_, err := e.db.Exec(q,
				"A-1", "BR01    ", "1.00", "2026-01-01", "first", int64(10),
				"A-1", "BR01    ", "2.00", "2026-01-01", "second", int64(20),
			)
			if err == nil {
				t.Fatal("expected the engine to reject a duplicate key within one statement; " +
					"if this now succeeds, revisit whether coalescing is still required")
			}

			// And the coalesced form succeeds.
			upsert(t, e, spec, 20, "A-1", "BR01    ", "2.00", "2026-01-01", "second")
		})
	}
}

// -------------------------------------------------------- cross-engine digests

// The claim that makes reconciliation meaningful: the same logical row, stored in
// PostgreSQL and in MySQL, must produce the same digest. Every normalisation in
// the dialect layer exists for this test, and without it every row on every table
// reconciles as a mismatch.
func TestDigestsAgreeAcrossEngines(t *testing.T) {
	engines := openEngines(t)
	if len(engines) < 2 {
		t.Skip("both PG_DSN and MYSQL_DSN are required to compare engines")
	}

	rows := [][]any{
		// Trailing-padded CHAR, which DB2 and Postgres disagree about.
		{"A-1", "BR01    ", "500.00", "2026-01-01", "plain"},
		// Decimal with trailing zeros, rendered differently by each engine.
		{"A-2", "BR02", "1.50", "2026-02-15", "decimal"},
		// A NULL, which must not compare equal to an empty string.
		{"A-3", "BR03", "0.00", "2026-03-20", nil},
		// An empty string, distinct from the NULL above.
		{"A-4", "BR04", "-25.75", "2026-04-01", ""},
		// Unicode, to catch collation differences.
		{"A-5", "BR05", "10.10", "2026-05-05", "café ☕"},
	}

	digests := make(map[string]dialect.Digest, len(engines))
	for _, e := range engines {
		ref := createTable(t, e, "accounts")
		spec := testSpec(e.schema)
		spec.Source, spec.Target = ref, ref

		for i, r := range rows {
			upsert(t, e, spec, uint64(i+1), r...)
		}

		q, args := e.dialect.RangeDigestQuery(ref, spec.Columns, dialect.Range{Column: "account_id"}, false)
		var d dialect.Digest
		if err := e.db.QueryRow(q, args...).Scan(&d.Rows, &d.SumLow, &d.SumHi); err != nil {
			t.Fatalf("%s: digest query failed: %v\nSQL: %s", e.name, err, q)
		}
		digests[e.name] = d
		t.Logf("%s digest: rows=%d low=%s hi=%s", e.name, d.Rows, d.SumLow, d.SumHi)
	}

	a, b := digests[engines[0].name], digests[engines[1].name]
	if a.Rows != b.Rows {
		t.Fatalf("row counts differ: %s=%d %s=%d", engines[0].name, a.Rows, engines[1].name, b.Rows)
	}
	if !a.Equal(b) {
		t.Fatalf("digests disagree across engines for identical logical data:\n  %s: %+v\n  %s: %+v\n"+
			"this means a normalisation in internal/dialect is wrong, and every row would reconcile as a mismatch",
			engines[0].name, a, engines[1].name, b)
	}
}

// A change on one side must change that side's digest. A digest insensitive to
// the data would compare equal forever and report a broken migration as clean.
func TestDigestDetectsASingleChangedValue(t *testing.T) {
	for _, e := range openEngines(t) {
		t.Run(e.name, func(t *testing.T) {
			ref := createTable(t, e, "accounts")
			spec := testSpec(e.schema)
			spec.Source, spec.Target = ref, ref

			for i := 0; i < 50; i++ {
				upsert(t, e, spec, uint64(i+1),
					fmt.Sprintf("A-%03d", i), "BR01    ", "100.00", "2026-01-01", "note")
			}

			q, args := e.dialect.RangeDigestQuery(ref, spec.Columns, dialect.Range{Column: "account_id"}, false)
			var before dialect.Digest
			if err := e.db.QueryRow(q, args...).Scan(&before.Rows, &before.SumLow, &before.SumHi); err != nil {
				t.Fatal(err)
			}

			// Change one row by one cent.
			upsert(t, e, spec, 999, "A-025", "BR01    ", "100.01", "2026-01-01", "note")

			var after dialect.Digest
			if err := e.db.QueryRow(q, args...).Scan(&after.Rows, &after.SumLow, &after.SumHi); err != nil {
				t.Fatal(err)
			}
			if before.Equal(after) {
				t.Fatal("the digest did not change when a value changed by one cent")
			}
			if before.Rows != after.Rows {
				t.Fatal("row count should be unchanged by an update")
			}
		})
	}
}

// Tombstoned rows exist only on the target and have no counterpart on the source,
// so including them would report a discrepancy for every deleted row.
func TestDigestExcludesTombstones(t *testing.T) {
	for _, e := range openEngines(t) {
		t.Run(e.name, func(t *testing.T) {
			ref := createTable(t, e, "accounts")
			spec := testSpec(e.schema)
			spec.Source, spec.Target = ref, ref

			for i := 0; i < 10; i++ {
				upsert(t, e, spec, uint64(i+1), fmt.Sprintf("A-%02d", i), "BR", "1.00", "2026-01-01", "n")
			}
			if _, err := e.db.Exec(e.dialect.FencedDelete(spec), "A-05", int64(500)); err != nil {
				t.Fatal(err)
			}

			q, args := e.dialect.RangeDigestQuery(ref, spec.Columns, dialect.Range{Column: "account_id"}, false)
			var d dialect.Digest
			if err := e.db.QueryRow(q, args...).Scan(&d.Rows, &d.SumLow, &d.SumHi); err != nil {
				t.Fatal(err)
			}
			if d.Rows != 9 {
				t.Fatalf("tombstone counted in the digest: %d rows, expected 9", d.Rows)
			}
		})
	}
}

// ------------------------------------------------------------ staging merge

// The bulk-load path's guarantee: a part carrying an older extract LSN cannot
// overwrite a row the change stream already advanced past. This is the exact
// snapshot-clobbers-CDC bug, verified against the real engine.
func TestMergeFromStagingIsFenced(t *testing.T) {
	for _, e := range openEngines(t) {
		t.Run(e.name, func(t *testing.T) {
			ref := createTable(t, e, "accounts")
			spec := testSpec(e.schema)
			spec.Source, spec.Target = ref, ref

			// The change stream has already applied a fresh value at LSN 1000.
			upsert(t, e, spec, 1000, "A-1", "BR01    ", "999.00", "2026-01-01", "from-cdc")

			// A snapshot part extracted at LSN 500 arrives afterwards.
			staging := createTable(t, e, "stg_accounts")
			ins := fmt.Sprintf("INSERT INTO %s (account_id, branch_code, balance, opened_on, note) VALUES (%s, %s, %s, %s, %s)",
				e.dialect.QuoteTable(staging),
				e.dialect.Placeholder(1), e.dialect.Placeholder(2), e.dialect.Placeholder(3),
				e.dialect.Placeholder(4), e.dialect.Placeholder(5))
			if _, err := e.db.Exec(ins, "A-1", "BR01    ", "1.00", "2026-01-01", "from-snapshot"); err != nil {
				t.Fatal(err)
			}
			// A row the stream has never seen, which must be inserted.
			if _, err := e.db.Exec(ins, "A-2", "BR02    ", "42.00", "2026-01-01", "new"); err != nil {
				t.Fatal(err)
			}

			mergeSQL := e.dialect.MergeStaging(spec, staging, spec.TargetColumnNames(), 500)
			if _, err := e.db.Exec(mergeSQL); err != nil {
				t.Fatalf("%s: merge failed: %v\nSQL: %s", e.name, err, mergeSQL)
			}

			balance, lsn, _ := readBalance(t, e, ref, "A-1")
			if !approxEqual(balance, 999) || lsn != 1000 {
				t.Fatalf("a stale snapshot part overwrote a fresher CDC row: balance=%q lsn=%d", balance, lsn)
			}
			balance, lsn, _ = readBalance(t, e, ref, "A-2")
			if !approxEqual(balance, 42) || lsn != 500 {
				t.Fatalf("a genuinely new row was not inserted by the merge: balance=%q lsn=%d", balance, lsn)
			}
		})
	}
}

// --------------------------------------------------------------- offsets

// The offset upsert must be monotonic, or a replayed older batch moves the
// committed offset backwards and the whole partition is reprocessed.
func TestOffsetUpsertIsMonotonic(t *testing.T) {
	for _, e := range openEngines(t) {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			mig := fmt.Sprintf("itest-%d", time.Now().UnixNano())
			q := e.dialect.UpsertOffset()

			exec := func(off int64) {
				if _, err := e.db.ExecContext(ctx, q, mig, "t1", 0, off, off); err != nil {
					t.Fatalf("%s: offset upsert: %v\nSQL: %s", e.name, err, q)
				}
			}
			exec(100)
			exec(50) // a replayed older batch
			exec(150)

			var got int64
			if err := e.db.QueryRowContext(ctx, e.dialect.SelectOffsets(), mig).Scan(new(string), new(int), &got, new(int64)); err != nil {
				t.Fatalf("%s: reading offset: %v", e.name, err)
			}
			if got != 150 {
				t.Fatalf("offset is %d; a replayed older batch moved it backwards", got)
			}
			_, _ = e.db.ExecContext(ctx, fmt.Sprintf(
				"DELETE FROM %s.applied_offset WHERE migration_id = %s",
				dialect.ControlSchema, e.dialect.Placeholder(1)), mig)
		})
	}
}

// --------------------------------------------------------------- applier

// End to end through the real applier: the offset lands in the same transaction
// as the data, so the two can never disagree.
func TestApplierCommitsDataAndOffsetTogether(t *testing.T) {
	for _, e := range openEngines(t) {
		t.Run(e.name, func(t *testing.T) {
			ref := createTable(t, e, "accounts")
			spec := testSpec(e.schema)
			spec.Source, spec.Target = ref, ref

			mig := fmt.Sprintf("itest-%d", time.Now().UnixNano())
			applier := sink.New(e.db, e.dialect, model.Plan{Name: "t", Tables: []model.TableSpec{spec}},
				sink.Options{MigrationID: mig, MaxRowsPerStatement: 100})

			events := []*model.ChangeEvent{{
				Table: ref, Op: model.OpCreate,
				Key: model.NewRowKey(map[string]any{"account_id": "A-9"}),
				After: map[string]any{
					"account_id": "A-9", "branch_code": "BR09", "balance": "77.00",
					"opened_on": "2026-06-01", "note": "applied",
				},
				Source: model.SourceMeta{LSN: 4242}, Topic: "t1", Partition: 0, Offset: 77,
			}}

			res, err := applier.Apply(context.Background(), events, sink.HighWaterMarks(events))
			if err != nil {
				t.Fatalf("%s: apply: %v", e.name, err)
			}
			if res.Applied != 1 {
				t.Fatalf("expected 1 row applied, got %d", res.Applied)
			}

			balance, lsn, _ := readBalance(t, e, ref, "A-9")
			if !approxEqual(balance, 77) || lsn != 4242 {
				t.Fatalf("row not applied correctly: balance=%q lsn=%d", balance, lsn)
			}

			var off int64
			if err := e.db.QueryRow(e.dialect.SelectOffsets(), mig).Scan(new(string), new(int), &off, new(int64)); err != nil {
				t.Fatalf("%s: offset not committed with the data: %v", e.name, err)
			}
			if off != 77 {
				t.Fatalf("offset is %d, expected 77", off)
			}
			_, _ = e.db.Exec(fmt.Sprintf("DELETE FROM %s.applied_offset WHERE migration_id = %s",
				dialect.ControlSchema, e.dialect.Placeholder(1)), mig)
		})
	}
}

// approxEqual compares a numeric column rendered as text, tolerating the trailing
// zeros the two engines disagree about.
func approxEqual(got string, want float64) bool {
	var f float64
	if _, err := fmt.Sscanf(got, "%g", &f); err != nil {
		return false
	}
	return math.Abs(f-want) < 0.001
}

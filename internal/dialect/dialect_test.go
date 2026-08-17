package dialect

import (
	"strings"
	"testing"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

func spec() model.TableSpec {
	return model.TableSpec{
		Source:     model.TableRef{Schema: "app", Name: "accounts"},
		Target:     model.TableRef{Schema: "app", Name: "accounts"},
		PrimaryKey: []string{"account_id"},
		Columns: []model.ColumnSpec{
			{Name: "account_id", Type: model.TypeString},
			{Name: "balance", Type: model.TypeDecimal, Scale: 2},
			{Name: "opened_on", Type: model.TypeDate},
			{Name: "notes", Type: model.TypeString, Protect: model.ProtectEncrypt},
			{Name: "track_data", Type: model.TypeString, Protect: model.ProtectRedact},
			{Name: "branch_code", Type: model.TypeString, TrimTrailingSpace: true},
		},
	}
}

func allDialects() []Dialect { return []Dialect{NewPostgres(), NewMySQL()} }

func TestForReturnsBothEnginesAndRejectsUnknown(t *testing.T) {
	for _, n := range []Name{Postgres, MySQL} {
		d, err := For(n)
		if err != nil || d.Name() != n {
			t.Fatalf("For(%s) = %v, %v", n, d, err)
		}
	}
	if _, err := For("oracle"); err == nil {
		t.Fatal("expected an error for an unsupported engine")
	}
}

// An identifier containing the engine's own quote character must not be able to
// terminate the quoting and inject SQL.
func TestQuotingEscapesEmbeddedQuoteCharacters(t *testing.T) {
	if got := NewPostgres().Quote(`we"ird`); got != `"we""ird"` {
		t.Errorf("postgres: got %s", got)
	}
	if got := NewMySQL().Quote("we`ird"); got != "`we``ird`" {
		t.Errorf("mysql: got %s", got)
	}
}

func TestQuoteTableHandlesUnqualifiedNames(t *testing.T) {
	for _, d := range allDialects() {
		got := d.QuoteTable(model.TableRef{Name: "accounts"})
		if strings.Contains(got, ".") {
			t.Errorf("%s: unqualified table gained a schema: %s", d.Name(), got)
		}
	}
}

// The LSN fence is what makes the pipeline replay-safe. Both engines must
// express it, even though only Postgres can put it in a WHERE clause.
func TestFencedUpsertCarriesTheLSNFence(t *testing.T) {
	s := spec()
	cols := []string{"account_id", "balance"}

	pg := NewPostgres().FencedUpsert(s, cols, 2)
	if !strings.Contains(pg, "ON CONFLICT") || !strings.Contains(pg, "DO UPDATE SET") {
		t.Fatalf("postgres upsert missing conflict clause:\n%s", pg)
	}
	if !strings.Contains(pg, `WHERE "app"."accounts"."_mig_lsn" <= EXCLUDED."_mig_lsn"`) {
		t.Fatalf("postgres upsert missing the LSN fence:\n%s", pg)
	}
	// Two rows of two columns plus one LSN each: six placeholders.
	if n := strings.Count(pg, "$"); n != 6 {
		t.Fatalf("expected 6 placeholders, got %d:\n%s", n, pg)
	}

	my := NewMySQL().FencedUpsert(s, cols, 2)
	if !strings.Contains(my, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("mysql upsert missing conflict clause:\n%s", my)
	}
	// MySQL has no WHERE on ODKU, so the fence must appear in every assignment.
	if !strings.Contains(my, "IF(`app`.`accounts`.`_mig_lsn` <= `new_row`.`_mig_lsn`") {
		t.Fatalf("mysql upsert missing the conditional LSN fence:\n%s", my)
	}
	if n := strings.Count(my, "?"); n != 6 {
		t.Fatalf("expected 6 placeholders, got %d:\n%s", n, my)
	}
}

// The primary key must never appear in the update assignments: assigning a key
// column to itself is at best pointless and at worst rejected by the engine.
func TestUpsertDoesNotAssignPrimaryKeyColumns(t *testing.T) {
	s := spec()
	for _, d := range allDialects() {
		sql := d.FencedUpsert(s, []string{"account_id", "balance"}, 1)
		i := strings.Index(sql, "UPDATE")
		if i < 0 {
			t.Fatalf("%s upsert has no update clause:\n%s", d.Name(), sql)
		}
		update := sql[i:]
		if strings.Contains(update, "account_id =") {
			t.Errorf("%s assigns the primary key in the update clause:\n%s", d.Name(), sql)
		}
	}
}

// A hard DELETE discards the row's LSN, so a delayed older UPDATE would
// re-insert it and resurrect a deleted record. Both dialects must tombstone.
func TestDeleteWritesATombstoneRatherThanRemovingTheRow(t *testing.T) {
	for _, d := range allDialects() {
		sql := d.FencedDelete(spec())
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "DELETE") {
			t.Errorf("%s issues a hard delete, which loses the LSN:\n%s", d.Name(), sql)
		}
		if !strings.Contains(sql, ColDeleted) {
			t.Errorf("%s delete does not set the tombstone column:\n%s", d.Name(), sql)
		}
		if !strings.Contains(sql, ColLSN) {
			t.Errorf("%s delete does not carry the LSN:\n%s", d.Name(), sql)
		}
	}
}

// Columns whose representation differs on the two sides by design must be
// excluded, or every single row reconciles as a mismatch.
func TestRowDigestExcludesIncomparableColumns(t *testing.T) {
	for _, d := range allDialects() {
		expr := d.RowDigestExpr("t", spec().Columns)
		if strings.Contains(expr, "track_data") {
			t.Errorf("%s digest includes a redacted column, which does not exist on the target:\n%s", d.Name(), expr)
		}
		if strings.Contains(expr, "notes") {
			t.Errorf("%s digest includes a randomly-encrypted column, which differs on every write:\n%s", d.Name(), expr)
		}
		if !strings.Contains(expr, "account_id") || !strings.Contains(expr, "balance") {
			t.Errorf("%s digest dropped a comparable column:\n%s", d.Name(), expr)
		}
	}
}

// Every normalisation that exists to reconcile a known cross-engine disagreement
// must actually appear in the generated expression.
func TestRowDigestAppliesCrossEngineNormalisation(t *testing.T) {
	pg := NewPostgres().RowDigestExpr("t", spec().Columns)
	my := NewMySQL().RowDigestExpr("t", spec().Columns)

	// NULL must map to a sentinel that cannot occur in the data.
	if !strings.Contains(pg, "coalesce") || !strings.Contains(my, "IFNULL") {
		t.Error("null handling missing from one of the digests")
	}
	// CHAR padding: DB2 pads, the targets do not.
	if !strings.Contains(pg, "rtrim") || !strings.Contains(my, "RTRIM") {
		t.Error("trailing-space trimming missing from one of the digests")
	}
	// Decimal scale: engines disagree about trailing zeros.
	if !strings.Contains(pg, "round") || !strings.Contains(my, "ROUND") {
		t.Error("decimal rounding missing from one of the digests")
	}
	// Dates must render in one fixed format.
	if !strings.Contains(pg, "YYYY-MM-DD") || !strings.Contains(my, "%Y-%m-%d") {
		t.Error("date formatting missing from one of the digests")
	}
	// A separator that could occur in data would let ("ab","c") and ("a","bc")
	// hash identically.
	if !strings.Contains(pg, "\x1f") || !strings.Contains(my, "\x1f") {
		t.Error("digest separator is not the unit separator control character")
	}
}

// Both engines must aggregate the row hashes in an order-independent way, so the
// scan can be parallelised and each engine can choose its own access path.
func TestRangeDigestIsOrderIndependentOnBothEngines(t *testing.T) {
	r := Range{Column: "account_id", Low: "A", High: "M"}

	pgSQL, pgArgs := NewPostgres().RangeDigestQuery(spec().Target, spec().Columns, r, false)
	if !strings.Contains(pgSQL, "sum(") || strings.Contains(strings.ToUpper(pgSQL), "ORDER BY") {
		t.Errorf("postgres digest is not order-independent:\n%s", pgSQL)
	}
	if len(pgArgs) != 2 {
		t.Errorf("expected 2 bound args, got %d", len(pgArgs))
	}

	mySQL, myArgs := NewMySQL().RangeDigestQuery(spec().Target, spec().Columns, r, false)
	if !strings.Contains(mySQL, "SUM(") || strings.Contains(strings.ToUpper(mySQL), "ORDER BY") {
		t.Errorf("mysql digest is not order-independent:\n%s", mySQL)
	}
	if len(myArgs) != 2 {
		t.Errorf("expected 2 bound args, got %d", len(myArgs))
	}
}

// A single additive digest can be defeated by two compensating errors. Two
// independent projections of the row hash make that essentially impossible.
func TestRangeDigestUsesTwoIndependentProjections(t *testing.T) {
	for _, d := range allDialects() {
		sql, _ := d.RangeDigestQuery(spec().Target, spec().Columns, Range{}, false)
		if !strings.Contains(sql, ",1,15") || !strings.Contains(sql, ",17,15") {
			t.Errorf("%s digest does not take two disjoint hash slices:\n%s", d.Name(), sql)
		}
	}
}

// Ranges are half-open (low, high] so that adjacent chunks tile the key space
// without overlapping. An overlap double-counts rows in both the snapshot and
// the reconciler.
func TestRangePredicatesAreHalfOpen(t *testing.T) {
	r := Range{Column: "id", Low: 100, High: 200}
	for _, d := range allDialects() {
		sql, _ := d.CountQuery(spec().Target, r, false)
		if !strings.Contains(sql, "> ") || !strings.Contains(sql, "<= ") {
			t.Errorf("%s range is not half-open (low, high]:\n%s", d.Name(), sql)
		}
	}
}

// Tombstoned rows are an implementation detail of the pipeline and do not exist
// on the source, so they must be excluded from counts and digests by default.
func TestQueriesExcludeTombstonesByDefault(t *testing.T) {
	for _, d := range allDialects() {
		sql, _ := d.CountQuery(spec().Target, Range{}, false)
		if !strings.Contains(sql, ColDeleted) {
			t.Errorf("%s count does not exclude tombstones:\n%s", d.Name(), sql)
		}
		sql, _ = d.CountQuery(spec().Target, Range{}, true)
		if strings.Contains(sql, ColDeleted) {
			t.Errorf("%s count should include tombstones when asked:\n%s", d.Name(), sql)
		}
	}
}

// OFFSET pagination makes the database scan and discard every preceding row, so
// chunking a large table degrades into a quadratic scan.
func TestKeysetPaginationAvoidsOffset(t *testing.T) {
	for _, d := range allDialects() {
		sql, args := d.KeysetPageQuery(spec().Source, "account_id", 1000, nil)
		if strings.Contains(strings.ToUpper(sql), "OFFSET") {
			t.Errorf("%s uses OFFSET:\n%s", d.Name(), sql)
		}
		if len(args) != 0 {
			t.Errorf("%s first page should bind no args", d.Name())
		}

		sql, args = d.KeysetPageQuery(spec().Source, "account_id", 1000, "A-500")
		if !strings.Contains(sql, "ORDER BY") || len(args) != 1 {
			t.Errorf("%s subsequent page malformed:\n%s", d.Name(), sql)
		}
	}
}

// Many repair workers must be able to drain the same queue concurrently without
// contending on the same rows.
func TestDeadLetterClaimUsesSkipLocked(t *testing.T) {
	for _, d := range allDialects() {
		sql := d.SelectClaimableDeadLetters(50)
		if !strings.Contains(sql, "FOR UPDATE SKIP LOCKED") {
			t.Errorf("%s claim does not use SKIP LOCKED:\n%s", d.Name(), sql)
		}
		if !strings.Contains(sql, "next_retry_at") {
			t.Errorf("%s claim ignores the retry schedule:\n%s", d.Name(), sql)
		}
	}
}

func TestMarkDeadLettersClaimedBindsEveryID(t *testing.T) {
	for _, d := range allDialects() {
		sql := d.MarkDeadLettersClaimed(3)
		if n := strings.Count(sql, "$") + strings.Count(sql, "?"); n != 4 {
			t.Errorf("%s: expected 4 placeholders (worker + 3 ids), got %d:\n%s", d.Name(), n, sql)
		}
	}
}

// The offset upsert must itself be fenced, or a replayed older batch could move
// the committed offset backwards and cause the whole partition to be reprocessed.
func TestOffsetUpsertIsMonotonic(t *testing.T) {
	for _, d := range allDialects() {
		sql := d.UpsertOffset()
		if !strings.Contains(sql, "<=") {
			t.Errorf("%s offset upsert is not monotonic:\n%s", d.Name(), sql)
		}
	}
}

func TestMergeStagingIsFencedAndDeduplicates(t *testing.T) {
	staging := model.TableRef{Schema: "migration_ctl", Name: "stg_accounts"}
	cols := []string{"account_id", "balance"}

	pg := NewPostgres().MergeStaging(spec(), staging, cols, 4242)
	if !strings.Contains(pg, "DISTINCT ON") {
		t.Errorf("postgres merge does not deduplicate keys within a part:\n%s", pg)
	}
	if !strings.Contains(pg, "4242") {
		t.Errorf("postgres merge does not stamp the extract LSN:\n%s", pg)
	}
	if !strings.Contains(pg, `WHERE "app"."accounts"."_mig_lsn" <= EXCLUDED."_mig_lsn"`) {
		t.Errorf("postgres merge is not LSN-fenced:\n%s", pg)
	}

	my := NewMySQL().MergeStaging(spec(), staging, cols, 4242)
	if !strings.Contains(my, "ROW_NUMBER()") {
		t.Errorf("mysql merge does not deduplicate keys within a part:\n%s", my)
	}
	if !strings.Contains(my, "4242") {
		t.Errorf("mysql merge does not stamp the extract LSN:\n%s", my)
	}
	if !strings.Contains(my, "IF(`app`.`accounts`.`_mig_lsn` <= `new_row`.`_mig_lsn`") {
		t.Errorf("mysql merge is not LSN-fenced:\n%s", my)
	}
}

func TestBulkImportUsesTheEngineNativePath(t *testing.T) {
	src := S3Source{Bucket: "mig-parts", Key: "accounts/app.accounts.dat.00001", Region: "us-east-1",
		Delimiter: ",", NullSentinel: `\N`}
	staging := model.TableRef{Schema: "migration_ctl", Name: "stg_accounts"}

	pg, args := NewPostgres().BulkImport(staging, []string{"account_id", "balance"}, src)
	if !strings.Contains(pg, "aws_s3.table_import_from_s3") {
		t.Errorf("postgres import does not use the native path:\n%s", pg)
	}
	if len(args) != 6 {
		t.Errorf("expected 6 bound args, got %d", len(args))
	}

	my, _ := NewMySQL().BulkImport(staging, []string{"account_id", "balance"}, src)
	if !strings.Contains(my, "LOAD DATA FROM S3") {
		t.Errorf("mysql import does not use the native path:\n%s", my)
	}
	if !strings.Contains(my, "s3://mig-parts/accounts/app.accounts.dat.00001") {
		t.Errorf("mysql import lost the object URI:\n%s", my)
	}
	// A quoted NULL sentinel only becomes NULL with an explicit NULLIF.
	if !strings.Contains(my, "NULLIF") {
		t.Errorf("mysql import will load quoted NULLs as literal text:\n%s", my)
	}
}

// Nothing in the automated path may drop or invalidate an index: both engines'
// folklore tricks for that are either unsupported or capable of leaving the
// catalogue needing a restore.
func TestBulkLoadSettingsAreSessionScopedAndNonDestructive(t *testing.T) {
	for _, d := range allDialects() {
		for _, stmt := range append(d.BulkLoadSessionSettings(), d.RestoreSessionSettings()...) {
			upper := strings.ToUpper(stmt)
			if !strings.HasPrefix(upper, "SET ") {
				t.Errorf("%s: non-SET statement in session settings: %s", d.Name(), stmt)
			}
			for _, forbidden := range []string{"DROP", "ALTER", "DELETE", "UPDATE PG_", "TRUNCATE"} {
				if strings.Contains(upper, forbidden) {
					t.Errorf("%s: destructive statement in session settings: %s", d.Name(), stmt)
				}
			}
		}
	}
}

func TestDigestEqualityNormalisesEngineNumericFormatting(t *testing.T) {
	// Postgres returns a numeric sum as "12345"; MySQL's DECIMAL aggregate may
	// return "12345.0000"; an empty range yields NULL on one and "0" on the other.
	cases := [][2]string{
		{"12345", "12345.0000"},
		{"0", ""},
		{"0", "NULL"},
		{"-17", "-17.00"},
	}
	for _, c := range cases {
		a := Digest{Rows: 1, SumLow: c[0], SumHi: c[0]}
		b := Digest{Rows: 1, SumLow: c[1], SumHi: c[1]}
		if !a.Equal(b) {
			t.Errorf("%q and %q should compare equal", c[0], c[1])
		}
	}
	if (Digest{Rows: 1, SumLow: "1"}).Equal(Digest{Rows: 2, SumLow: "1"}) {
		t.Error("digests with different row counts must not compare equal")
	}
	if (Digest{Rows: 1, SumLow: "1", SumHi: "2"}).Equal(Digest{Rows: 1, SumLow: "1", SumHi: "3"}) {
		t.Error("digests differing in the second projection must not compare equal")
	}
}

func TestPlaceholdersMatchEngineConvention(t *testing.T) {
	if got := NewPostgres().Placeholder(3); got != "$3" {
		t.Errorf("postgres placeholder: %s", got)
	}
	if got := NewMySQL().Placeholder(3); got != "?" {
		t.Errorf("mysql placeholder: %s", got)
	}
}

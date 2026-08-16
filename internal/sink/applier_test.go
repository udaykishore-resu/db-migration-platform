package sink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// ---------------------------------------------------------------- fake driver
//
// A minimal database/sql driver. Faking at the driver boundary rather than
// wrapping the applier in an interface means the transaction semantics under
// test are the real ones: real BeginTx, real rollback-on-error, real argument
// marshalling.

type stmtRecord struct {
	query string
	args  []driver.NamedValue
}

type fakeDB struct {
	mu        sync.Mutex
	stmts     []stmtRecord
	commits   int
	rollbacks int
	// failOn returns a non-nil error to make a statement fail.
	failOn func(query string, args []driver.NamedValue) error
}

func (f *fakeDB) record(q string, a []driver.NamedValue) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stmts = append(f.stmts, stmtRecord{query: q, args: a})
	if f.failOn != nil {
		return f.failOn(q, a)
	}
	return nil
}

func (f *fakeDB) queries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.stmts))
	for i, s := range f.stmts {
		out[i] = s.query
	}
	return out
}

func (f *fakeDB) countMatching(sub string) int {
	n := 0
	for _, q := range f.queries() {
		if strings.Contains(q, sub) {
			n++
		}
	}
	return n
}

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return &fakeTx{db: c.db}, nil }

func (c *fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &fakeTx{db: c.db}, nil
}

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.db.record(query, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

type fakeTx struct{ db *fakeDB }

func (t *fakeTx) Commit() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.db.commits++
	return nil
}

func (t *fakeTx) Rollback() error {
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.db.rollbacks++
	return nil
}

type fakeConnector struct{ db *fakeDB }

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeConn{db: c.db}, nil
}
func (c *fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, io.EOF }

// ------------------------------------------------------------------- fixtures

func testPlan() model.Plan {
	return model.Plan{
		Name: "db2-to-aurora",
		Tables: []model.TableSpec{
			{
				Source:     model.TableRef{Schema: "app", Name: "accounts"},
				Target:     model.TableRef{Schema: "app", Name: "accounts"},
				PrimaryKey: []string{"account_id"},
				Columns: []model.ColumnSpec{
					{Name: "account_id", Type: model.TypeString},
					{Name: "balance", Type: model.TypeDecimal, Scale: 2},
				},
			},
			{
				Source:     model.TableRef{Schema: "app", Name: "disputes"},
				Target:     model.TableRef{Schema: "app", Name: "disputes"},
				PrimaryKey: []string{"dispute_id"},
				Columns: []model.ColumnSpec{
					{Name: "dispute_id", Type: model.TypeString},
					{Name: "status", Type: model.TypeString},
				},
			},
		},
	}
}

func newApplier(t *testing.T, f *fakeDB) *Applier {
	t.Helper()
	db := sql.OpenDB(&fakeConnector{db: f})
	t.Cleanup(func() { _ = db.Close() })
	return New(db, dialect.NewPostgres(), testPlan(), Options{MigrationID: "mig-1", MaxRowsPerStatement: 100})
}

func ev(table, key string, lsn uint64, op model.Op, offset int64) *model.ChangeEvent {
	keyCol := "account_id"
	if table == "disputes" {
		keyCol = "dispute_id"
	}
	values := map[string]any{keyCol: key, "balance": 100, "status": "open"}
	return &model.ChangeEvent{
		Table:     model.TableRef{Schema: "app", Name: table},
		Op:        op,
		Key:       model.NewRowKey(map[string]any{keyCol: key}),
		After:     values,
		Before:    values,
		Source:    model.SourceMeta{LSN: lsn},
		Topic:     "cdc.app." + table,
		Partition: 0,
		Offset:    offset,
	}
}

// ---------------------------------------------------------------------- tests

// The offset must be written inside the same transaction as the data it accounts
// for. If they can commit separately, a crash between them re-applies a whole
// batch — or worse, skips one.
func TestOffsetIsCommittedInTheSameTransactionAsTheData(t *testing.T) {
	f := &fakeDB{}
	a := newApplier(t, f)

	events := []*model.ChangeEvent{ev("accounts", "A-1", 10, model.OpCreate, 1)}
	_, err := a.Apply(context.Background(), events, HighWaterMarks(events))
	if err != nil {
		t.Fatal(err)
	}

	if f.commits != 1 {
		t.Fatalf("expected exactly 1 commit, got %d", f.commits)
	}
	if f.countMatching("INSERT INTO \"app\".\"accounts\"") != 1 {
		t.Fatalf("expected one data statement:\n%v", f.queries())
	}
	if f.countMatching("applied_offset") != 1 {
		t.Fatalf("expected the offset write in the same transaction:\n%v", f.queries())
	}
}

func TestFailedBatchRollsBackAndCommitsNothing(t *testing.T) {
	f := &fakeDB{failOn: func(q string, _ []driver.NamedValue) error {
		if strings.Contains(q, "accounts") {
			return errors.New("pq: deadlock detected")
		}
		return nil
	}}
	a := newApplier(t, f)

	events := []*model.ChangeEvent{ev("accounts", "A-1", 10, model.OpCreate, 1)}
	if _, err := a.Apply(context.Background(), events, HighWaterMarks(events)); err == nil {
		t.Fatal("expected the apply to fail")
	}
	if f.commits != 0 {
		t.Fatal("a failed batch must not commit")
	}
	if f.rollbacks == 0 {
		t.Fatal("a failed batch must roll back")
	}
	// Critically, the offset must not have advanced past data that was not
	// written.
	if f.countMatching("applied_offset") != 0 {
		t.Fatal("offset was written despite the data failing")
	}
}

// A hot row updated many times in one batch produces one final state. Keeping
// only the newest is both far cheaper and necessary: two tuples with the same key
// inside one multi-row upsert make Postgres abort the entire statement.
func TestCoalesceKeepsOnlyTheNewestVersionOfARow(t *testing.T) {
	events := []*model.ChangeEvent{
		ev("accounts", "A-1", 10, model.OpCreate, 1),
		ev("accounts", "A-2", 11, model.OpCreate, 2),
		ev("accounts", "A-1", 12, model.OpUpdate, 3),
		ev("accounts", "A-1", 15, model.OpUpdate, 4),
	}
	got := Coalesce(events)
	if len(got) != 2 {
		t.Fatalf("expected 2 events after coalescing, got %d", len(got))
	}
	for _, e := range got {
		if e.Key.Equal(model.NewRowKey(map[string]any{"account_id": "A-1"})) && e.Source.LSN != 15 {
			t.Fatalf("coalescing kept LSN %d instead of the newest, 15", e.Source.LSN)
		}
	}
}

// Out-of-order arrival is normal after a rebalance or a dead-letter replay. The
// newest version must still win.
func TestCoalescePrefersHighestLSNRegardlessOfArrivalOrder(t *testing.T) {
	events := []*model.ChangeEvent{
		ev("accounts", "A-1", 99, model.OpUpdate, 1),
		ev("accounts", "A-1", 12, model.OpUpdate, 2),
	}
	got := Coalesce(events)
	if len(got) != 1 || got[0].Source.LSN != 99 {
		t.Fatalf("expected the LSN-99 event to survive, got %+v", got)
	}
}

// Two changes to the same row inside one source transaction share an LSN, so the
// tie must be broken by stream position rather than left to map ordering.
func TestCoalesceBreaksLSNTiesByStreamPosition(t *testing.T) {
	events := []*model.ChangeEvent{
		ev("accounts", "A-1", 50, model.OpUpdate, 7),
		ev("accounts", "A-1", 50, model.OpUpdate, 9),
	}
	got := Coalesce(events)
	if len(got) != 1 || got[0].Offset != 9 {
		t.Fatalf("expected the later offset to win, got offset %d", got[0].Offset)
	}
}

func TestCoalescePreservesArrivalOrderOfSurvivors(t *testing.T) {
	events := []*model.ChangeEvent{
		ev("accounts", "A-3", 1, model.OpCreate, 1),
		ev("accounts", "A-1", 2, model.OpCreate, 2),
		ev("accounts", "A-2", 3, model.OpCreate, 3),
	}
	got := Coalesce(events)
	if len(got) != 3 {
		t.Fatalf("got %d", len(got))
	}
	if got[0].Offset != 1 || got[1].Offset != 2 || got[2].Offset != 3 {
		t.Fatal("arrival order not preserved; the target's write pattern would become random")
	}
}

// Rows from different tables must be locked in a deterministic order, or two
// appliers with overlapping batches deadlock — a failure that only appears under
// production concurrency.
func TestTablesAreWrittenInDeterministicOrder(t *testing.T) {
	var first string
	for i := 0; i < 25; i++ {
		f := &fakeDB{}
		a := newApplier(t, f)
		events := []*model.ChangeEvent{
			ev("disputes", "D-1", 1, model.OpCreate, 1),
			ev("accounts", "A-1", 2, model.OpCreate, 2),
		}
		if _, err := a.Apply(context.Background(), events, nil); err != nil {
			t.Fatal(err)
		}
		order := strings.Join(f.queries(), "|")
		if i == 0 {
			first = order
			continue
		}
		if order != first {
			t.Fatal("table write order varies between runs; this is a latent deadlock")
		}
	}
}

// A single unmigratable row must not halt the partition. The applier bisects to
// find it, applies everything else, and dead-letters only what failed.
func TestOneBadRowIsIsolatedAndTheRestAreApplied(t *testing.T) {
	f := &fakeDB{failOn: func(q string, args []driver.NamedValue) error {
		if !strings.Contains(q, "accounts") {
			return nil
		}
		for _, a := range args {
			if s, ok := a.Value.(string); ok && s == "POISON" {
				return errors.New(`pq: value too long for type character varying(10)`)
			}
		}
		return nil
	}}
	a := newApplier(t, f)

	var events []*model.ChangeEvent
	for i := 0; i < 16; i++ {
		events = append(events, ev("accounts", "A-"+string(rune('a'+i)), uint64(i+1), model.OpCreate, int64(i+1)))
	}
	events = append(events, ev("accounts", "POISON", 99, model.OpCreate, 99))

	res, err := a.ApplyWithIsolation(context.Background(), events, HighWaterMarks(events))
	if err != nil {
		t.Fatalf("isolation should succeed by dead-lettering the bad row, got %v", err)
	}
	if len(res.DeadLettered) != 1 {
		t.Fatalf("expected exactly 1 dead letter, got %d", len(res.DeadLettered))
	}
	if !res.DeadLettered[0].Event.Key.Equal(model.NewRowKey(map[string]any{"account_id": "POISON"})) {
		t.Fatalf("the wrong record was dead-lettered: %s", res.DeadLettered[0].Event.Key.Hash())
	}
	if res.Applied != 16 {
		t.Fatalf("expected the other 16 rows to be applied, got %d", res.Applied)
	}
	// Bisection should cost a handful of extra round trips, not one per row.
	if attempts := f.countMatching("INSERT INTO \"app\".\"accounts\""); attempts > 14 {
		t.Fatalf("bisection was not logarithmic: %d statements for 17 rows", attempts)
	}
}

// A transient failure is about the database, not the data. Bisecting would just
// repeat the same failing write several more times against a struggling target.
func TestTransientFailuresAreNotBisected(t *testing.T) {
	f := &fakeDB{failOn: func(q string, _ []driver.NamedValue) error {
		if strings.Contains(q, "accounts") {
			return errors.New("pq: deadlock detected")
		}
		return nil
	}}
	a := newApplier(t, f)

	var events []*model.ChangeEvent
	for i := 0; i < 8; i++ {
		events = append(events, ev("accounts", "A-"+string(rune('a'+i)), uint64(i+1), model.OpCreate, int64(i+1)))
	}

	res, err := a.ApplyWithIsolation(context.Background(), events, nil)
	if err == nil {
		t.Fatal("a transient failure must surface as an error, not as dead letters")
	}
	if errclass.Classify(err) != errclass.Transient {
		t.Fatalf("error lost its classification: %v", err)
	}
	if len(res.DeadLettered) != 0 {
		t.Fatal("good records were dead-lettered because the database was unavailable")
	}
	if n := f.countMatching("INSERT INTO \"app\".\"accounts\""); n > 1 {
		t.Fatalf("transient failure was retried %d times inside the applier", n)
	}
}

// Deletes must tombstone rather than remove, so the row keeps its LSN and a
// delayed older update cannot resurrect it.
func TestDeleteIssuesATombstoneWrite(t *testing.T) {
	f := &fakeDB{}
	a := newApplier(t, f)

	events := []*model.ChangeEvent{ev("accounts", "A-1", 10, model.OpDelete, 1)}
	res, err := a.Apply(context.Background(), events, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 {
		t.Fatalf("expected 1 delete, got %d", res.Deleted)
	}
	for _, q := range f.queries() {
		if strings.HasPrefix(strings.TrimSpace(q), "DELETE") {
			t.Fatalf("a hard delete was issued, which discards the LSN:\n%s", q)
		}
	}
	if f.countMatching("_mig_deleted") == 0 {
		t.Fatal("no tombstone column was written")
	}
}

// An event for a table nobody declared is a plan or configuration error, and no
// amount of retrying will fix it.
func TestUnknownTableIsAPermanentError(t *testing.T) {
	f := &fakeDB{}
	a := newApplier(t, f)

	e := ev("accounts", "A-1", 1, model.OpCreate, 1)
	e.Table = model.TableRef{Schema: "app", Name: "not_in_plan"}

	_, err := a.Apply(context.Background(), []*model.ChangeEvent{e}, nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if errclass.Classify(err) != errclass.Permanent {
		t.Fatalf("expected a permanent classification, got %s", errclass.Classify(err))
	}
}

func TestLargeBatchesAreChunkedIntoBoundedStatements(t *testing.T) {
	f := &fakeDB{}
	db := sql.OpenDB(&fakeConnector{db: f})
	t.Cleanup(func() { _ = db.Close() })
	a := New(db, dialect.NewPostgres(), testPlan(), Options{MigrationID: "m", MaxRowsPerStatement: 10})

	var events []*model.ChangeEvent
	for i := 0; i < 95; i++ {
		events = append(events, ev("accounts", "A-"+itoa(i), uint64(i+1), model.OpCreate, int64(i+1)))
	}
	if _, err := a.Apply(context.Background(), events, nil); err != nil {
		t.Fatal(err)
	}
	if n := f.countMatching("INSERT INTO \"app\".\"accounts\""); n != 10 {
		t.Fatalf("expected 10 chunked statements for 95 rows at 10 per statement, got %d", n)
	}
}

func TestHighWaterMarksTakeTheMaximumPerPartition(t *testing.T) {
	events := []*model.ChangeEvent{
		{Topic: "t1", Partition: 0, Offset: 5, Source: model.SourceMeta{LSN: 5}},
		{Topic: "t1", Partition: 0, Offset: 9, Source: model.SourceMeta{LSN: 9}},
		{Topic: "t1", Partition: 1, Offset: 3, Source: model.SourceMeta{LSN: 3}},
		{Topic: "t2", Partition: 0, Offset: 1, Source: model.SourceMeta{LSN: 1}},
	}
	got := HighWaterMarks(events)
	if len(got) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(got))
	}
	if got[0].Topic != "t1" || got[0].Partition != 0 || got[0].Offset != 9 {
		t.Fatalf("wrong high-water mark: %+v", got[0])
	}
	// Deterministic ordering keeps the generated statements stable.
	if got[1].Partition != 1 || got[2].Topic != "t2" {
		t.Fatalf("positions are not deterministically ordered: %+v", got)
	}
}

func TestEmptyBatchIsANoOp(t *testing.T) {
	f := &fakeDB{}
	a := newApplier(t, f)
	res, err := a.Apply(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 0 || f.commits != 0 {
		t.Fatal("an empty batch should not open a transaction")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

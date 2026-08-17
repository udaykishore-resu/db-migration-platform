package cdc

import (
	"testing"

	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

func decoder() *DebeziumDecoder {
	return &DebeziumDecoder{
		KeyColumns:  map[string][]string{"app.accounts": {"account_id"}},
		SignalTable: "migration_ctl.snapshot_signal",
	}
}

const insertMsg = `{
  "before": null,
  "after": {"account_id": "A-1", "balance": 100},
  "op": "c",
  "ts_ms": 1755345600000,
  "source": {"connector":"db2","db":"APPDB","schema":"app","table":"accounts","lsn":"0/1A2B3C4D","ts_ms":1755345599000,"txId":9911}
}`

func TestDecodeInsert(t *testing.T) {
	ev, err := decoder().Decode("cdc.app.accounts", 2, 77, []byte(`{"account_id":"A-1"}`), []byte(insertMsg))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Op != model.OpCreate {
		t.Errorf("op = %s", ev.Op)
	}
	if ev.Table.String() != "app.accounts" {
		t.Errorf("table = %s", ev.Table)
	}
	if ev.Topic != "cdc.app.accounts" || ev.Partition != 2 || ev.Offset != 77 {
		t.Error("stream position not captured")
	}
	if ev.Source.TxID != "9911" {
		t.Errorf("txid = %q", ev.Source.TxID)
	}
	// The raw bytes must be retained so a failed event can be dead-lettered
	// byte-for-byte rather than reconstructed from a partial parse.
	if len(ev.Raw) != len(insertMsg) {
		t.Error("raw payload not retained")
	}
}

// Postgres and DB2 render an LSN as "hi/lo" in hex. Parsing it as a decimal
// number, or not at all, silently collapses every event to LSN zero — and the
// fence stops fencing.
func TestPostgresStyleLSNIsParsed(t *testing.T) {
	ev, err := decoder().Decode("t", 0, 0, nil, []byte(insertMsg))
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(0)<<32 | 0x1A2B3C4D
	if ev.Source.LSN != want {
		t.Fatalf("lsn = %d, want %d", ev.Source.LSN, want)
	}
}

// A MySQL binlog position resets to zero on every log rotation, so using it raw
// makes each rotation look like the stream jumping backwards — which would let
// stale events overwrite fresh ones.
func TestMySQLBinlogPositionIsCombinedWithFileOrdinal(t *testing.T) {
	early := `{"after":{"account_id":"A-1"},"op":"c","source":{"schema":"app","table":"accounts","file":"mysql-bin.000041","pos":999999}}`
	afterRotate := `{"after":{"account_id":"A-1"},"op":"c","source":{"schema":"app","table":"accounts","file":"mysql-bin.000042","pos":4}}`

	a, err := decoder().Decode("t", 0, 0, nil, []byte(early))
	if err != nil {
		t.Fatal(err)
	}
	b, err := decoder().Decode("t", 0, 1, nil, []byte(afterRotate))
	if err != nil {
		t.Fatal(err)
	}
	if b.Source.LSN <= a.Source.LSN {
		t.Fatalf("LSN went backwards across a binlog rotation: %d then %d", a.Source.LSN, b.Source.LSN)
	}
}

func TestSchemaWrappedPayloadIsUnwrapped(t *testing.T) {
	wrapped := `{"schema":{"type":"struct"},"payload":{"after":{"account_id":"A-9"},"op":"c","source":{"schema":"app","table":"accounts","lsn":42}}}`
	ev, err := decoder().Decode("t", 0, 0, nil, []byte(wrapped))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Op != model.OpCreate || ev.Source.LSN != 42 {
		t.Fatalf("schema-wrapped envelope not unwrapped: %+v", ev)
	}
}

// A delete's identifying image lives in "before"; reading only "after" would
// produce an event with no key.
func TestDeleteTakesItsKeyFromBefore(t *testing.T) {
	del := `{"before":{"account_id":"A-7","balance":5},"after":null,"op":"d","source":{"schema":"app","table":"accounts","lsn":100}}`
	ev, err := decoder().Decode("t", 0, 0, nil, []byte(del))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Op != model.OpDelete {
		t.Fatalf("op = %s", ev.Op)
	}
	want := model.NewRowKey(map[string]any{"account_id": "A-7"})
	if !ev.Key.Equal(want) {
		t.Fatalf("key = %s, want %s", ev.Key.Canonical(), want.Canonical())
	}
}

// The tombstone that follows a delete exists only so log compaction can drop the
// key. Treating it as data would produce a keyless event on every delete.
func TestNullValueTombstoneIsSkipped(t *testing.T) {
	ev, err := decoder().Decode("t", 0, 0, []byte(`{"account_id":"A-1"}`), nil)
	if err != nil {
		t.Fatalf("a tombstone must not be an error: %v", err)
	}
	if ev != nil {
		t.Fatalf("expected the tombstone to be skipped, got %+v", ev)
	}
}

// A message that does not parse will not parse on the next attempt either.
// Retrying it forever stalls every record behind it on the same partition.
func TestMalformedMessagesAreClassifiedPermanent(t *testing.T) {
	cases := map[string]string{
		"invalid json":  `{not json`,
		"unknown op":    `{"after":{"account_id":"A"},"op":"z","source":{"schema":"app","table":"accounts"}}`,
		"no key at all": `{"after":{"balance":1},"op":"c","source":{"schema":"app","table":"other"}}`,
	}
	for name, msg := range cases {
		_, err := decoder().Decode("t", 0, 0, nil, []byte(msg))
		if err == nil {
			t.Errorf("%s: expected an error", name)
			continue
		}
		if got := errclass.Classify(err); got != errclass.Permanent {
			t.Errorf("%s: classified %s, want permanent — a retry loop here stalls the partition", name, got)
		}
	}
}

// The declared key order makes the row key canonical regardless of how the
// connector happens to order fields in a given version.
func TestDeclaredKeyColumnsTakePrecedence(t *testing.T) {
	d := &DebeziumDecoder{KeyColumns: map[string][]string{"app.accounts": {"account_id"}}}
	msg := `{"after":{"account_id":"A-1","balance":9},"op":"c","source":{"schema":"app","table":"accounts","lsn":1}}`
	ev, err := d.Decode("t", 0, 0, []byte(`{"wrong_field":"ignored"}`), []byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	want := model.NewRowKey(map[string]any{"account_id": "A-1"})
	if !ev.Key.Equal(want) {
		t.Fatalf("declared key columns ignored: %s", ev.Key.Canonical())
	}
}

func TestFallsBackToConnectorKeyMessage(t *testing.T) {
	d := &DebeziumDecoder{} // no declared key columns
	msg := `{"after":{"balance":9},"op":"c","source":{"schema":"app","table":"accounts","lsn":1}}`
	ev, err := d.Decode("t", 0, 0, []byte(`{"payload":{"account_id":"A-3"}}`), []byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Key.Equal(model.NewRowKey(map[string]any{"account_id": "A-3"})) {
		t.Fatalf("connector key message not used: %s", ev.Key.Canonical())
	}
}

// Watermarks travel through the source's own transaction log so they are ordered
// against the data changes. The decoder has to recognise them and route them to
// the snapshot window rather than to the applier.
func TestWatermarkEventsAreRecognised(t *testing.T) {
	d := decoder()
	msg := `{"after":{"chunk_id":"accounts-0007","kind":"low","table_name":"app.accounts"},"op":"c","source":{"schema":"migration_ctl","table":"snapshot_signal","lsn":500}}`
	ev, err := d.Decode("t", 0, 0, []byte(`{"signal_id":"s1"}`), []byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	chunk, kind, ok := d.IsWatermark(ev)
	if !ok || chunk != "accounts-0007" || kind != "low" {
		t.Fatalf("watermark not recognised: chunk=%q kind=%q ok=%v", chunk, kind, ok)
	}

	dataEv, _ := d.Decode("t", 0, 0, nil, []byte(insertMsg))
	if _, _, ok := d.IsWatermark(dataEv); ok {
		t.Fatal("an ordinary data event was mistaken for a watermark")
	}
}

func TestSnapshotFlagIsCarriedThrough(t *testing.T) {
	msg := `{"after":{"account_id":"A-1"},"op":"r","source":{"schema":"app","table":"accounts","lsn":7,"snapshot":"incremental"}}`
	ev, err := decoder().Decode("t", 0, 0, nil, []byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Source.Snapshot {
		t.Fatal("snapshot marker lost")
	}
	if ev.Op != model.OpRead {
		t.Fatalf("op = %s, want r", ev.Op)
	}
}

// Without a sequence number the fence has nothing to compare, so commit time is
// used as a coarse but still monotonic substitute rather than defaulting to zero.
func TestFallsBackToCommitTimeWhenNoSequenceNumberExists(t *testing.T) {
	msg := `{"after":{"account_id":"A-1"},"op":"c","source":{"schema":"app","table":"accounts","ts_ms":1755345599000}}`
	ev, err := decoder().Decode("t", 0, 0, nil, []byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Source.LSN != 1755345599000 {
		t.Fatalf("lsn = %d, want the commit timestamp", ev.Source.LSN)
	}
}

func TestLagIsMeasuredFromSourceCommit(t *testing.T) {
	ev, err := decoder().Decode("t", 0, 0, nil, []byte(insertMsg))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Lag() <= 0 {
		t.Fatal("expected a positive lag from a past commit timestamp")
	}
}

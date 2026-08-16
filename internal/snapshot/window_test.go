package snapshot

import (
	"errors"
	"sync"
	"testing"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

func keys(ids ...int) []model.RowKey {
	out := make([]model.RowKey, len(ids))
	for i, id := range ids {
		out[i] = model.NewRowKey(map[string]any{"id": int64(id)})
	}
	return out
}

func rowsFor(ids ...int) []map[string]any {
	out := make([]map[string]any, len(ids))
	for i, id := range ids {
		out[i] = map[string]any{"id": int64(id), "balance": id * 10}
	}
	return out
}

func emittedIDs(rows []EmittedRow) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r.Values["id"].(int64)
	}
	return out
}

// The whole point of the protocol: a row updated while its chunk was in flight
// must be dropped from the snapshot, because the change stream carries a fresher
// version. Without this, the stale snapshot image silently overwrites the fresh
// one and the target is quietly wrong.
func TestConcurrentUpdateEvictsStaleSnapshotRow(t *testing.T) {
	w := NewWindow()
	if err := w.Buffer("chunk-1", keys(1, 2, 3), rowsFor(1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	if !w.OnLowWatermark("chunk-1") {
		t.Fatal("low watermark should open the window")
	}

	// Row 2 is updated on the source while the chunk is in flight.
	if !w.OnEvent(model.NewRowKey(map[string]any{"id": int64(2)})) {
		t.Fatal("expected row 2 to be evicted")
	}

	out, ok := w.OnHighWatermark("chunk-1")
	if !ok {
		t.Fatal("high watermark should close the window")
	}
	got := emittedIDs(out)
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("expected rows 1 and 3 to survive, got %v", got)
	}
	if s := w.Stats(); s.RowsEvicted != 1 || s.RowsEmitted != 2 || s.ChunksCompleted != 1 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

// Events arriving before the low watermark belong to an earlier position in the
// stream than the chunk read, so the snapshot image is the fresher one and must
// survive.
func TestEventsBeforeLowWatermarkDoNotEvict(t *testing.T) {
	w := NewWindow()
	_ = w.Buffer("chunk-1", keys(1, 2), rowsFor(1, 2))

	if w.OnEvent(model.NewRowKey(map[string]any{"id": int64(1)})) {
		t.Fatal("event before the low watermark must not evict")
	}
	w.OnLowWatermark("chunk-1")
	out, _ := w.OnHighWatermark("chunk-1")
	if len(out) != 2 {
		t.Fatalf("expected both rows to survive, got %v", emittedIDs(out))
	}
}

// Events arriving after the window has closed are ordered after the emitted
// snapshot rows, so they will be applied on top by the normal apply path and
// must not affect the window.
func TestEventsAfterHighWatermarkAreIgnored(t *testing.T) {
	w := NewWindow()
	_ = w.Buffer("chunk-1", keys(1), rowsFor(1))
	w.OnLowWatermark("chunk-1")
	if _, ok := w.OnHighWatermark("chunk-1"); !ok {
		t.Fatal("expected close")
	}
	if w.OnEvent(model.NewRowKey(map[string]any{"id": int64(1)})) {
		t.Fatal("event after close must not evict")
	}
	if w.State() != WindowIdle {
		t.Fatalf("expected idle, got %s", w.State())
	}
}

// After a restart the consumer replays from the last committed offset, so
// watermarks for chunks that already completed reappear. They must be inert
// rather than corrupting the chunk currently in flight.
func TestReplayedWatermarksForOtherChunksAreIgnored(t *testing.T) {
	w := NewWindow()
	_ = w.Buffer("chunk-7", keys(1, 2), rowsFor(1, 2))
	if w.OnLowWatermark("chunk-3") {
		t.Fatal("stale low watermark must not open the window")
	}
	if w.State() != WindowBuffered {
		t.Fatalf("state changed on stale watermark: %s", w.State())
	}
	w.OnLowWatermark("chunk-7")
	if _, ok := w.OnHighWatermark("chunk-3"); ok {
		t.Fatal("stale high watermark must not close the window")
	}
	out, ok := w.OnHighWatermark("chunk-7")
	if !ok || len(out) != 2 {
		t.Fatalf("correct high watermark should close and emit 2 rows, got ok=%v rows=%v", ok, emittedIDs(out))
	}
}

// Read order is preserved so that the target receives rows in primary key order,
// which keeps index maintenance sequential instead of a random scatter.
func TestEmissionPreservesReadOrder(t *testing.T) {
	w := NewWindow()
	_ = w.Buffer("c", keys(10, 20, 30, 40, 50), rowsFor(10, 20, 30, 40, 50))
	w.OnLowWatermark("c")
	w.OnEvent(model.NewRowKey(map[string]any{"id": int64(30)}))
	out, _ := w.OnHighWatermark("c")

	got := emittedIDs(out)
	want := []int64{10, 20, 40, 50}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order not preserved: got %v, want %v", got, want)
		}
	}
}

func TestBufferRejectsSecondChunkWhileInFlight(t *testing.T) {
	w := NewWindow()
	_ = w.Buffer("c1", keys(1), rowsFor(1))
	err := w.Buffer("c2", keys(2), rowsFor(2))
	if !errors.Is(err, ErrWindowBusy) {
		t.Fatalf("expected ErrWindowBusy, got %v", err)
	}
}

// Overlapping chunk boundaries would double-apply rows. Better to fail the chunk
// than to let a chunking bug quietly duplicate work.
func TestBufferRejectsDuplicateKeysInChunk(t *testing.T) {
	w := NewWindow()
	err := w.Buffer("c", keys(1, 1), rowsFor(1, 1))
	if err == nil {
		t.Fatal("expected an error for duplicate keys within a chunk")
	}
}

func TestBufferRejectsMismatchedKeyAndRowCounts(t *testing.T) {
	w := NewWindow()
	if err := w.Buffer("c", keys(1, 2), rowsFor(1)); err == nil {
		t.Fatal("expected an error when keys and rows differ in length")
	}
}

func TestAbortDiscardsInFlightChunk(t *testing.T) {
	w := NewWindow()
	_ = w.Buffer("c", keys(1, 2), rowsFor(1, 2))
	w.OnLowWatermark("c")
	w.Abort()

	if w.State() != WindowIdle {
		t.Fatalf("expected idle after abort, got %s", w.State())
	}
	if w.Pending() != 0 {
		t.Fatalf("expected empty buffer after abort, got %d", w.Pending())
	}
	if err := w.Buffer("c-retry", keys(1, 2), rowsFor(1, 2)); err != nil {
		t.Fatalf("window must accept a retry after abort: %v", err)
	}
}

// A chunk in which every row was updated concurrently emits nothing at all —
// which is correct, not a bug, and must not be mistaken for a failure.
func TestFullyEvictedChunkEmitsNothing(t *testing.T) {
	w := NewWindow()
	_ = w.Buffer("c", keys(1, 2, 3), rowsFor(1, 2, 3))
	w.OnLowWatermark("c")
	for _, k := range keys(1, 2, 3) {
		w.OnEvent(k)
	}
	out, ok := w.OnHighWatermark("c")
	if !ok {
		t.Fatal("window should still close")
	}
	if len(out) != 0 {
		t.Fatalf("expected no surviving rows, got %v", emittedIDs(out))
	}
	if s := w.Stats(); s.RowsEvicted != 3 || s.RowsEmitted != 0 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestKeyTypeNormalisationAcrossSnapshotAndStream(t *testing.T) {
	// The snapshot reader yields int64; the JSON-decoded change event yields
	// float64. If these did not normalise to the same key, eviction would never
	// fire and the protocol would silently do nothing.
	w := NewWindow()
	_ = w.Buffer("c", []model.RowKey{model.NewRowKey(map[string]any{"id": int64(7)})},
		[]map[string]any{{"id": int64(7)}})
	w.OnLowWatermark("c")
	if !w.OnEvent(model.NewRowKey(map[string]any{"id": float64(7)})) {
		t.Fatal("float64 change event failed to evict an int64 snapshot row")
	}
}

func TestWindowIsRaceFree(t *testing.T) {
	w := NewWindow()
	ids := make([]int, 500)
	for i := range ids {
		ids[i] = i
	}
	_ = w.Buffer("c", keys(ids...), rowsFor(ids...))
	w.OnLowWatermark("c")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := base; j < 500; j += 16 {
				w.OnEvent(model.NewRowKey(map[string]any{"id": int64(j)}))
				_ = w.Pending()
				_ = w.State()
			}
		}(i)
	}
	wg.Wait()

	out, _ := w.OnHighWatermark("c")
	if len(out) != 0 {
		t.Fatalf("expected every row evicted, %d survived", len(out))
	}
	if s := w.Stats(); s.RowsEvicted != 500 {
		t.Fatalf("expected 500 evictions, got %d", s.RowsEvicted)
	}
}

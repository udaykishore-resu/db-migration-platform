package snapshot

import (
	"errors"
	"fmt"
	"sync"

	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// Window implements watermark-based chunk deduplication.
//
// The problem it solves is the one that quietly breaks most hand-built
// migrations. A snapshot reads a chunk of rows at some instant; the change
// stream is running concurrently. If a row in that chunk is updated while the
// chunk is in flight, two versions exist: the stale snapshot image and the fresh
// change event. Apply them in the wrong order and the fresh value is silently
// overwritten by the stale one. The failure is invisible — no error, no counter,
// no log line — and it surfaces weeks later as "a handful of accounts have old
// balances".
//
// The usual defences are all unsatisfying. Locking the source table is
// unacceptable on a live system. Taking the snapshot at a single consistent
// point and replaying every change since requires the source to hold a snapshot
// open for the duration of a multi-terabyte extract. Doing the snapshot first and
// starting CDC afterwards loses every change in the gap.
//
// The watermark protocol solves it with no source locking and no long-lived
// snapshot, by having the extractor write two markers into a signal table around
// each chunk read:
//
//  1. Write the low watermark for chunk N to the signal table.
//  2. Read chunk N's rows into memory.
//  3. Write the high watermark for chunk N.
//  4. Continue consuming the change stream. Between observing the low and the
//     high watermark, every change event that arrives removes its key from the
//     buffered chunk — the log has a fresher version, so the snapshot's copy is
//     known to be stale and is discarded.
//  5. On observing the high watermark, emit whatever remains in the buffer.
//
// The result is one ordered stream in which each row appears exactly once, in its
// correct position, with no source locking and no boundary to reconcile. The
// technique is Netflix's DBLog, and it is also what Debezium's incremental
// snapshotting implements.
//
// Window is the pure state machine for steps 4 and 5. It is deliberately free of
// I/O so that its behaviour — which is otherwise very hard to observe in
// production — can be exhaustively tested.
type Window struct {
	mu sync.Mutex

	state    WindowState
	chunkID  string
	buffered map[string]bufferedRow
	order    []string

	// Metrics observable by the caller for operational visibility.
	stats WindowStats
}

// WindowState is the phase of the deduplication window.
type WindowState string

// Window states.
const (
	// WindowIdle means no chunk is in flight.
	WindowIdle WindowState = "idle"
	// WindowBuffered means a chunk has been read but its low watermark has not
	// yet been observed in the change stream.
	WindowBuffered WindowState = "buffered"
	// WindowOpen means the low watermark has been observed and change events are
	// actively evicting stale snapshot rows.
	WindowOpen WindowState = "open"
)

type bufferedRow struct {
	key    model.RowKey
	values map[string]any
}

// WindowStats reports what the window has done, for metrics and for the
// per-chunk audit record.
type WindowStats struct {
	// ChunksCompleted is the number of chunks that reached their high watermark.
	ChunksCompleted int64
	// RowsBuffered is the total number of snapshot rows accepted into windows.
	RowsBuffered int64
	// RowsEmitted is the total number of snapshot rows that survived to be
	// applied to the target.
	RowsEmitted int64
	// RowsEvicted is the total number of snapshot rows discarded because the
	// change stream carried a fresher version.
	//
	// This is the single most informative number in the whole protocol. A value
	// of zero on a busy table means the window is not actually catching
	// concurrent writes and the protocol is silently not working; a value that
	// approaches the chunk size means the chunk is too large relative to the
	// write rate and should be reduced.
	RowsEvicted int64
}

// ErrWindowBusy is returned when a chunk is offered while another is in flight.
var ErrWindowBusy = errors.New("snapshot: a chunk window is already in flight")

// NewWindow returns an idle window.
func NewWindow() *Window {
	return &Window{state: WindowIdle, buffered: make(map[string]bufferedRow)}
}

// Buffer accepts the rows read for a chunk. It must be called after the low
// watermark has been written to the source signal table and before the high
// watermark is written, which is the ordering the protocol depends on.
func (w *Window) Buffer(chunkID string, keys []model.RowKey, rows []map[string]any) error {
	if chunkID == "" {
		return errors.New("snapshot: chunk id is required")
	}
	if len(keys) != len(rows) {
		return fmt.Errorf("snapshot: %d keys for %d rows", len(keys), len(rows))
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != WindowIdle {
		return fmt.Errorf("%w: chunk %s is %s", ErrWindowBusy, w.chunkID, w.state)
	}

	w.chunkID = chunkID
	w.buffered = make(map[string]bufferedRow, len(rows))
	w.order = make([]string, 0, len(rows))
	for i, k := range keys {
		ck := k.Canonical()
		if _, dup := w.buffered[ck]; dup {
			// A chunk read that returns the same key twice means the chunking
			// predicate overlaps, which would double-apply rows. Fail loudly.
			return fmt.Errorf("snapshot: chunk %s contains duplicate key %s; chunk boundaries overlap", chunkID, k.Hash())
		}
		w.buffered[ck] = bufferedRow{key: k, values: rows[i]}
		w.order = append(w.order, ck)
	}
	w.state = WindowBuffered
	w.stats.RowsBuffered += int64(len(rows))
	return nil
}

// OnLowWatermark opens the window when the low watermark for the in-flight chunk
// is observed in the change stream. Watermarks for other chunks are ignored,
// which makes the state machine tolerant of replayed change events after a
// restart.
func (w *Window) OnLowWatermark(chunkID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != WindowBuffered || chunkID != w.chunkID {
		return false
	}
	w.state = WindowOpen
	return true
}

// OnEvent offers a change event to the window. If the window is open and the
// event's key is buffered, the buffered snapshot row is evicted because the
// change stream carries a fresher version of that row.
//
// It reports whether an eviction occurred, which the caller records as a metric.
func (w *Window) OnEvent(key model.RowKey) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != WindowOpen {
		return false
	}
	ck := key.Canonical()
	if _, ok := w.buffered[ck]; !ok {
		return false
	}
	delete(w.buffered, ck)
	w.stats.RowsEvicted++
	return true
}

// EmittedRow is a snapshot row that survived the window and must be applied.
type EmittedRow struct {
	Key    model.RowKey
	Values map[string]any
}

// OnHighWatermark closes the window and returns the surviving snapshot rows in
// their original read order. Preserving read order matters because it keeps the
// target's insert pattern sequential in primary key order, which is materially
// kinder to the target's index maintenance than a random scatter.
func (w *Window) OnHighWatermark(chunkID string) ([]EmittedRow, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.state != WindowOpen || chunkID != w.chunkID {
		return nil, false
	}

	out := make([]EmittedRow, 0, len(w.buffered))
	for _, ck := range w.order {
		if row, ok := w.buffered[ck]; ok {
			out = append(out, EmittedRow{Key: row.key, Values: row.values})
		}
	}

	w.state = WindowIdle
	w.chunkID = ""
	w.buffered = make(map[string]bufferedRow)
	w.order = nil
	w.stats.ChunksCompleted++
	w.stats.RowsEmitted += int64(len(out))
	return out, true
}

// Abort discards an in-flight chunk without emitting it, used when a chunk read
// fails partway through and must be retried from the beginning.
func (w *Window) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.state = WindowIdle
	w.chunkID = ""
	w.buffered = make(map[string]bufferedRow)
	w.order = nil
}

// State reports the current phase.
func (w *Window) State() WindowState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state
}

// ChunkID reports the in-flight chunk, if any.
func (w *Window) ChunkID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.chunkID
}

// Pending reports how many buffered rows remain.
func (w *Window) Pending() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buffered)
}

// Stats returns a copy of the window counters.
func (w *Window) Stats() WindowStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// WatermarkKind distinguishes the two markers written to the signal table.
type WatermarkKind string

// Watermark kinds.
const (
	LowWatermark  WatermarkKind = "low"
	HighWatermark WatermarkKind = "high"
)

// Watermark is a marker row written to the source signal table and observed
// coming back through the change stream.
//
// Routing the markers through the source's own transaction log is the subtle
// part: it is what gives them a position in the same total order as the data
// changes. A marker sent out-of-band — over HTTP, or on a side channel — would
// have no defined position relative to the changes, and the protocol would
// degenerate into a race.
type Watermark struct {
	ChunkID string        `json:"chunk_id"`
	Kind    WatermarkKind `json:"kind"`
	Table   string        `json:"table"`
	LSN     uint64        `json:"lsn"`
}

// SignalTableDDL returns the DDL for the source-side signal table. It lives in
// the source database because the marker must travel through the source's
// transaction log to be ordered against the data changes.
func SignalTableDDL(schema string) string {
	if schema == "" {
		schema = "migration_ctl"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.snapshot_signal (
    signal_id   VARCHAR(64)  NOT NULL,
    chunk_id    VARCHAR(128) NOT NULL,
    kind        VARCHAR(8)   NOT NULL,
    table_name  VARCHAR(256) NOT NULL,
    created_at  TIMESTAMP    NOT NULL,
    PRIMARY KEY (signal_id)
)`, schema)
}

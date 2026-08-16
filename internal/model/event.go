// Package model contains the core domain types shared by every service in the
// platform. It deliberately has no dependencies outside the standard library so
// that it can be imported from anywhere without pulling in drivers or clients.
package model

import (
	"fmt"
	"time"
)

// Op is the logical operation carried by a change event. The single-letter
// values match the Debezium `op` field so that decoding is a direct mapping.
type Op string

const (
	// OpCreate is an INSERT captured from the source transaction log.
	OpCreate Op = "c"
	// OpUpdate is an UPDATE captured from the source transaction log.
	OpUpdate Op = "u"
	// OpDelete is a DELETE captured from the source transaction log.
	OpDelete Op = "d"
	// OpRead is a row produced by a snapshot chunk read, not by the log.
	OpRead Op = "r"
	// OpTruncate is a TRUNCATE captured from the source transaction log.
	OpTruncate Op = "t"
)

// Valid reports whether the operation is one the applier knows how to handle.
func (o Op) Valid() bool {
	switch o {
	case OpCreate, OpUpdate, OpDelete, OpRead, OpTruncate:
		return true
	default:
		return false
	}
}

// IsUpsert reports whether the operation results in a row being written.
func (o Op) IsUpsert() bool { return o == OpCreate || o == OpUpdate || o == OpRead }

// SourceMeta carries the provenance of a change event. LSN is the single most
// important field in the whole platform: it is the monotonic change sequence
// number assigned by the source database, and it is what makes the apply path
// idempotent and safe to replay in any order.
type SourceMeta struct {
	// LSN is a monotonically increasing change sequence number from the source
	// transaction log. For DB2 this is the log record sequence number, for
	// Postgres the WAL LSN, for MySQL a (binlog file, position) pair folded
	// into a single ordered integer.
	LSN uint64 `json:"lsn"`
	// TxID identifies the source transaction, used to group events that must
	// commit atomically on the target.
	TxID string `json:"tx_id,omitempty"`
	// CommitTS is when the source transaction committed. Used for lag metrics.
	CommitTS time.Time `json:"commit_ts"`
	// Snapshot marks events produced by the snapshot reader rather than the log.
	Snapshot bool `json:"snapshot"`
	// Connector identifies the CDC connector that produced the event.
	Connector string `json:"connector,omitempty"`
}

// ChangeEvent is the normalised, connector-agnostic representation of a single
// row change. Every component in the platform speaks this type; connector
// specifics are confined to the decoding layer.
type ChangeEvent struct {
	Table  TableRef       `json:"table"`
	Op     Op             `json:"op"`
	Key    RowKey         `json:"key"`
	Before map[string]any `json:"before,omitempty"`
	After  map[string]any `json:"after,omitempty"`
	Source SourceMeta     `json:"source"`

	// Ingested is when this process first observed the event. Combined with
	// Source.CommitTS it yields end-to-end replication lag.
	Ingested time.Time `json:"ingested"`

	// Partition and Offset locate the event in the change stream so that the
	// applier can commit progress transactionally alongside the data write.
	Topic     string `json:"topic,omitempty"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`

	// Raw is the original serialised payload, retained so that a failed event
	// can be dead-lettered byte-for-byte and replayed later without loss.
	Raw []byte `json:"-"`
}

// Values returns the row image that should be written to the target for this
// event. Deletes carry their identifying image in Before.
func (e *ChangeEvent) Values() map[string]any {
	if e.Op == OpDelete {
		if len(e.After) > 0 {
			return e.After
		}
		return e.Before
	}
	if len(e.After) > 0 {
		return e.After
	}
	return e.Before
}

// Lag reports how long the event took to travel from source commit to this
// process. A zero CommitTS yields a zero lag rather than a nonsense value.
func (e *ChangeEvent) Lag() time.Duration {
	if e.Source.CommitTS.IsZero() || e.Ingested.IsZero() {
		return 0
	}
	if d := e.Ingested.Sub(e.Source.CommitTS); d > 0 {
		return d
	}
	return 0
}

// Validate checks the invariants the apply path depends on. Events failing
// validation are dead-lettered immediately rather than retried, because no
// amount of retrying will make them valid.
func (e *ChangeEvent) Validate() error {
	if !e.Op.Valid() {
		return fmt.Errorf("invalid op %q", e.Op)
	}
	if e.Table.Name == "" {
		return fmt.Errorf("event has no table name")
	}
	if e.Op != OpTruncate && e.Key.Len() == 0 {
		return fmt.Errorf("event for %s has no primary key", e.Table)
	}
	if e.Op.IsUpsert() && len(e.Values()) == 0 {
		return fmt.Errorf("%s event for %s has no row image", e.Op, e.Table)
	}
	return nil
}

// String renders a compact, non-PII-bearing description suitable for logs.
// Row values are never included: only the table, operation, key hash and LSN.
func (e *ChangeEvent) String() string {
	return fmt.Sprintf("%s/%s key=%s lsn=%d part=%d off=%d",
		e.Table, e.Op, e.Key.Hash(), e.Source.LSN, e.Partition, e.Offset)
}

// Package cdc decodes change events from the wire format the CDC connector
// produces into the platform's internal representation.
//
// Everything vendor-specific about the change stream lives here. Debezium is the
// default because it is open source, has a DB2 connector, and its envelope is
// well documented — but Qlik Replicate, AWS DMS and a hand-rolled log reader all
// produce something envelope-shaped, and swapping one for another should mean
// writing one Decoder, not touching the apply path.
package cdc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
)

// Decoder turns a raw broker message into a change event.
type Decoder interface {
	Decode(topic string, partition int32, offset int64, key, value []byte) (*model.ChangeEvent, error)
	Name() string
}

// debeziumEnvelope is the subset of the Debezium payload the platform uses.
type debeziumEnvelope struct {
	Before  map[string]any `json:"before"`
	After   map[string]any `json:"after"`
	Op      string         `json:"op"`
	TsMs    int64          `json:"ts_ms"`
	Source  debeziumSource `json:"source"`
	Payload *struct {
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
		Op     string         `json:"op"`
		TsMs   int64          `json:"ts_ms"`
		Source debeziumSource `json:"source"`
	} `json:"payload"`
}

type debeziumSource struct {
	Connector string `json:"connector"`
	Db        string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  any    `json:"snapshot"`
	TxID      any    `json:"txId"`

	// Every connector names its change sequence number differently. Rather than
	// force operators to configure which field to read, all of the known spellings
	// are accepted and the first present one wins.
	LSN       any `json:"lsn"`        // Postgres, DB2
	SCN       any `json:"scn"`        // Oracle
	ChangeLSN any `json:"change_lsn"` // SQL Server
	CommitLSN any `json:"commit_lsn"` // SQL Server
	Pos       any `json:"pos"`        // MySQL binlog position
	File      any `json:"file"`       // MySQL binlog file
	SeqNum    any `json:"sequence"`   // generic
}

// DebeziumDecoder decodes Debezium's JSON envelope, with or without a schema
// wrapper.
type DebeziumDecoder struct {
	// KeyColumns maps a table to its primary key column order. Debezium's key
	// message already carries the key, but a plan-declared order makes the row
	// key canonical and independent of the connector's field ordering.
	KeyColumns map[string][]string
	// SignalTable is the fully-qualified name of the snapshot signal table. Its
	// events are watermarks rather than data and are routed to the snapshot
	// window instead of the applier.
	SignalTable string
}

// Name identifies the decoder.
func (d *DebeziumDecoder) Name() string { return "debezium" }

// Decode parses one message.
//
// Every failure here is classified permanent. A message that does not parse will
// not parse on the next attempt either, and retrying it forever would stall the
// partition behind it — the head-of-line block that turns one malformed record
// into a stopped migration.
func (d *DebeziumDecoder) Decode(topic string, partition int32, offset int64, key, value []byte) (*model.ChangeEvent, error) {
	if len(value) == 0 {
		// A null value is Debezium's tombstone, emitted after a delete so that
		// log compaction can drop the key. The delete itself arrived as its own
		// message, so the tombstone carries no information and is skipped.
		return nil, nil
	}

	var env debeziumEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		return nil, errclass.Permanently(fmt.Errorf("cdc: message at %s/%d/%d is not valid JSON: %w", topic, partition, offset, err))
	}
	// Connectors configured with schemas enabled wrap everything in "payload".
	if env.Payload != nil {
		env.Before, env.After = env.Payload.Before, env.Payload.After
		env.Op, env.TsMs, env.Source = env.Payload.Op, env.Payload.TsMs, env.Payload.Source
	}

	op := model.Op(env.Op)
	if !op.Valid() {
		return nil, errclass.Permanently(fmt.Errorf("cdc: unknown operation %q at %s/%d/%d", env.Op, topic, partition, offset))
	}

	table := model.TableRef{Schema: env.Source.Schema, Name: env.Source.Table}
	if table.Schema == "" {
		table.Schema = env.Source.Db
	}

	ev := &model.ChangeEvent{
		Table:  table,
		Op:     op,
		Before: env.Before,
		After:  env.After,
		Source: model.SourceMeta{
			LSN:       extractLSN(env.Source),
			TxID:      stringOf(env.Source.TxID),
			CommitTS:  msToTime(firstNonZero(env.Source.TsMs, env.TsMs)),
			Snapshot:  isSnapshot(env.Source.Snapshot),
			Connector: env.Source.Connector,
		},
		Ingested:  time.Now().UTC(),
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Raw:       append([]byte(nil), value...),
	}

	rowKey, err := d.rowKey(table, key, ev)
	if err != nil {
		return nil, err
	}
	ev.Key = rowKey

	if err := ev.Validate(); err != nil {
		return nil, errclass.Permanently(fmt.Errorf("cdc: message at %s/%d/%d is malformed: %w", topic, partition, offset, err))
	}
	return ev, nil
}

// rowKey derives the canonical row key, preferring the plan's declared key
// columns over the connector's key message so that the key is stable even if the
// connector's field ordering changes between versions.
func (d *DebeziumDecoder) rowKey(table model.TableRef, key []byte, ev *model.ChangeEvent) (model.RowKey, error) {
	if cols, ok := d.KeyColumns[table.String()]; ok && len(cols) > 0 {
		values := ev.Values()
		missing := make([]string, 0, len(cols))
		for _, c := range cols {
			if _, present := values[c]; !present {
				missing = append(missing, c)
			}
		}
		if len(missing) == 0 {
			return model.NewRowKeyOrdered(cols, values), nil
		}
		// Fall through to the connector's key message: an update whose row image
		// omits a key column is unusual but recoverable if the key message has it.
	}

	if len(key) > 0 {
		var raw map[string]any
		if err := json.Unmarshal(key, &raw); err != nil {
			return model.RowKey{}, errclass.Permanently(fmt.Errorf("cdc: key message is not valid JSON: %w", err))
		}
		if payload, ok := raw["payload"].(map[string]any); ok {
			raw = payload
		}
		if len(raw) > 0 {
			return model.NewRowKey(raw), nil
		}
	}

	return model.RowKey{}, errclass.Permanently(fmt.Errorf("cdc: no primary key could be derived for %s", table))
}

// IsWatermark reports whether an event is a snapshot watermark rather than data,
// and returns the parsed watermark.
func (d *DebeziumDecoder) IsWatermark(ev *model.ChangeEvent) (chunkID, kind string, ok bool) {
	if d.SignalTable == "" || ev == nil || ev.Table.String() != d.SignalTable {
		return "", "", false
	}
	values := ev.Values()
	chunkID = stringOf(values["chunk_id"])
	kind = stringOf(values["kind"])
	return chunkID, kind, chunkID != "" && kind != ""
}

// extractLSN folds whatever the connector calls its change sequence number into
// a single monotonically increasing integer.
//
// This is the value every correctness guarantee in the platform rests on, so it
// is worth being explicit about what "monotonic" requires per source. Postgres
// and DB2 provide a genuine LSN. MySQL provides a (binlog file, position) pair,
// which only orders correctly once the two are combined — a position alone
// resets to zero on every log rotation, so using it raw would make every rotation
// look like the stream jumping backwards in time.
func extractLSN(s debeziumSource) uint64 {
	if v, ok := toUint64(s.LSN); ok {
		return v
	}
	if v, ok := toUint64(s.SCN); ok {
		return v
	}
	if v, ok := toUint64(s.CommitLSN); ok {
		return v
	}
	if v, ok := toUint64(s.ChangeLSN); ok {
		return v
	}
	if v, ok := toUint64(s.SeqNum); ok {
		return v
	}

	// MySQL: combine the binlog file ordinal with the position.
	if pos, ok := toUint64(s.Pos); ok {
		file := binlogOrdinal(stringOf(s.File))
		return file<<32 | (pos & 0xFFFFFFFF)
	}

	// Last resort: commit time in milliseconds. Coarser than a real sequence
	// number — two changes to the same row inside one millisecond become
	// indistinguishable — but still monotonic, so the fence degrades to
	// last-writer-wins within a millisecond rather than failing outright.
	if s.TsMs > 0 {
		return uint64(s.TsMs) //nolint:gosec // positive by the guard above
	}
	return 0
}

// binlogOrdinal extracts the numeric suffix of a binlog file name such as
// "mysql-bin.000042".
func binlogOrdinal(name string) uint64 {
	if name == "" {
		return 0
	}
	i := strings.LastIndex(name, ".")
	if i < 0 || i == len(name)-1 {
		return 0
	}
	n, err := strconv.ParseUint(name[i+1:], 10, 32)
	if err != nil {
		return 0
	}
	return n
}

func toUint64(v any) (uint64, bool) {
	switch t := v.(type) {
	case nil:
		return 0, false
	case float64:
		if t < 0 {
			return 0, false
		}
		return uint64(t), true
	case int64:
		if t < 0 {
			return 0, false
		}
		return uint64(t), true
	case json.Number:
		n, err := t.Int64()
		if err != nil || n < 0 {
			return 0, false
		}
		return uint64(n), true
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		// Postgres reports an LSN as "0/1A2B3C4D"; DB2 as a hex string.
		if i := strings.Index(s, "/"); i >= 0 {
			hi, err1 := strconv.ParseUint(s[:i], 16, 32)
			lo, err2 := strconv.ParseUint(s[i+1:], 16, 32)
			if err1 == nil && err2 == nil {
				return hi<<32 | lo, true
			}
		}
		if n, err := strconv.ParseUint(s, 10, 64); err == nil {
			return n, true
		}
		if n, err := strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64); err == nil {
			return n, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func stringOf(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func isSnapshot(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true" || t == "first" || t == "last" || t == "incremental"
	default:
		return false
	}
}

func msToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func firstNonZero(vals ...int64) int64 {
	for _, v := range vals {
		if v != 0 {
			return v
		}
	}
	return 0
}

// Package dlq is the durable dead-letter store and the retry policy that drains
// it.
//
// The requirement it exists for: when a record fails to apply, it must not be
// lost, must not block the records behind it, and must be retried later without
// an operator having to reconstruct it by hand.
//
// The store is a table in the target database rather than a Kafka topic, and
// that choice is deliberate. A dead letter routed to a topic inherits the topic's
// retention: a record that failed on Friday and is investigated on Monday may
// simply not exist any more. A dead letter in a table survives until somebody
// resolves it, can be queried by table, error class or age without writing a
// consumer, participates in the same transaction as the offset commit — so a
// record is never both dead-lettered and re-consumed — and can be reported on by
// the same cutover gate that reads everything else.
//
// The cost is that the target database now holds a copy of production data with
// a longer retention than the pipeline, which is why payloads are encrypted at
// rest with the same key material that protects the migrated columns.
package dlq

import (
	"strings"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
)

// Status is the lifecycle of a dead-lettered record.
type Status string

// Dead-letter statuses.
const (
	// StatusPending is waiting for its next retry window.
	StatusPending Status = "pending"
	// StatusRetrying has been claimed by a repair worker.
	StatusRetrying Status = "retrying"
	// StatusQuarantined has exhausted its retry budget and needs a human. It is
	// deliberately a terminal state that requires an explicit requeue: a record
	// that keeps failing is a signal, and automatically retrying it forever turns
	// that signal into background noise.
	StatusQuarantined Status = "quarantined"
	// StatusResolved was applied successfully on a later attempt.
	StatusResolved Status = "resolved"
	// StatusDiscarded was dismissed by an operator with a documented reason.
	StatusDiscarded Status = "discarded"
)

// Open reports whether the entry still represents unfinished work. The cutover
// gate refuses to proceed while any entry is open, because cutting over with
// unapplied records means the target is knowingly incomplete.
func (s Status) Open() bool {
	return s == StatusPending || s == StatusRetrying || s == StatusQuarantined
}

// Entry is one dead-lettered record.
type Entry struct {
	ID          int64          `json:"id"`
	MigrationID string         `json:"migration_id"`
	SourceTable model.TableRef `json:"source_table"`
	Op          model.Op       `json:"op"`

	// KeyHash identifies the row for reporting without revealing it.
	KeyHash string `json:"key_hash"`
	// Key is the row key, used by the repair worker to re-read from the source.
	Key model.RowKey `json:"-"`

	// Payload is the original serialised event, stored so the record can be
	// replayed byte-for-byte rather than reconstructed from a partial parse.
	Payload []byte `json:"-"`
	// PayloadEncrypted reports whether Payload is an encrypted envelope.
	PayloadEncrypted bool `json:"payload_encrypted"`

	ErrorClass errclass.Class `json:"error_class"`
	LastError  string         `json:"last_error"`
	Attempts   int            `json:"attempts"`
	Status     Status         `json:"status"`

	FirstSeenAt time.Time `json:"first_seen_at"`
	NextRetryAt time.Time `json:"next_retry_at"`
	ClaimedAt   time.Time `json:"claimed_at,omitempty"`
	ClaimedBy   string    `json:"claimed_by,omitempty"`
	ResolvedAt  time.Time `json:"resolved_at,omitempty"`

	SourceLSN uint64 `json:"source_lsn"`
	Topic     string `json:"topic,omitempty"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// Decision is the outcome of evaluating a failed attempt.
type Decision struct {
	Status      Status
	Attempts    int
	NextRetryAt time.Time
	// Reason explains the decision in operator-facing language, and is written to
	// the entry so that a quarantined record carries its own explanation.
	Reason string
}

// Evaluate decides what happens to an entry after an attempt failed.
//
// It is a pure function on purpose. The alternative — scattering these rules
// through the repair worker's control flow — makes the single most important
// operational behaviour of the platform (what happens when things go wrong)
// impossible to test without a database, and therefore effectively untested.
func Evaluate(e Entry, err error, p retryx.Policy, now time.Time) Decision {
	attempts := e.Attempts + 1
	class := errclass.Classify(err)

	// A permanent error will fail identically forever. Retrying it consumes the
	// budget, delays the operator's discovery of a real data problem, and
	// achieves nothing.
	if class == errclass.Permanent {
		return Decision{
			Status:   StatusQuarantined,
			Attempts: attempts,
			Reason:   "permanent error: " + summarise(err) + " — retrying cannot succeed, this needs a data or schema fix",
		}
	}

	if p.Exhausted(attempts) {
		return Decision{
			Status:   StatusQuarantined,
			Attempts: attempts,
			Reason:   "retry budget of " + itoa(p.MaxAttempts) + " attempts exhausted; last error: " + summarise(err),
		}
	}

	return Decision{
		Status:      StatusPending,
		Attempts:    attempts,
		NextRetryAt: p.NextAt(now, attempts),
		Reason:      string(class) + " error, attempt " + itoa(attempts) + " of " + itoa(p.MaxAttempts),
	}
}

// EvaluateSuccess produces the decision for a successful retry.
func EvaluateSuccess(e Entry, now time.Time) Decision {
	return Decision{
		Status:   StatusResolved,
		Attempts: e.Attempts + 1,
		Reason:   "applied successfully after " + itoa(e.Attempts+1) + " attempts over " + now.Sub(e.FirstSeenAt).Round(time.Second).String(),
	}
}

// NewEntry builds the first dead-letter record for a failed event.
func NewEntry(migrationID string, ev *model.ChangeEvent, err error, p retryx.Policy, now time.Time) Entry {
	class := errclass.Classify(err)
	e := Entry{
		MigrationID: migrationID,
		SourceTable: ev.Table,
		Op:          ev.Op,
		KeyHash:     ev.Key.Hash(),
		Key:         ev.Key,
		Payload:     ev.Raw,
		ErrorClass:  class,
		LastError:   summarise(err),
		Attempts:    1,
		FirstSeenAt: now,
		SourceLSN:   ev.Source.LSN,
		Topic:       ev.Topic,
		Partition:   ev.Partition,
		Offset:      ev.Offset,
	}

	// A permanent failure is quarantined on the first attempt rather than
	// entering a retry loop that cannot succeed.
	if class == errclass.Permanent {
		e.Status = StatusQuarantined
		return e
	}
	e.Status = StatusPending
	e.NextRetryAt = p.NextAt(now, 1)
	return e
}

// Counts summarises the store for the cutover gate and for alerting.
type Counts struct {
	Pending     int64 `json:"pending"`
	Retrying    int64 `json:"retrying"`
	Quarantined int64 `json:"quarantined"`
	Resolved    int64 `json:"resolved"`
	Discarded   int64 `json:"discarded"`
	// OldestOpenAge is how long the oldest unfinished record has been waiting. A
	// growing value means the drain is not keeping up, which a raw count hides.
	OldestOpenAge time.Duration `json:"oldest_open_age"`
}

// Open returns the number of records still representing unfinished work.
func (c Counts) Open() int64 { return c.Pending + c.Retrying + c.Quarantined }

// Clean reports whether the store has nothing outstanding, which is one of the
// conditions the cutover gate requires.
func (c Counts) Clean() bool { return c.Open() == 0 }

// summarise trims an error to something that fits in a database column and does
// not carry a row image into a table that will be read widely.
func summarise(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	msg = strings.ReplaceAll(msg, "\n", " ")
	const limit = 1000
	if len(msg) > limit {
		return msg[:limit] + "… (truncated)"
	}
	return msg
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}

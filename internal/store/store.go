// Package store is the persistence layer for the platform's own bookkeeping:
// stream offsets, dead letters, part and chunk state, and migration status.
//
// All of it lives in the target database rather than in a separate datastore.
// That is what makes the offset commit atomic with the data write, and it means
// an operator investigating a migration has exactly one place to look.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/udaykishore-resu/db-migration-platform/internal/control"
	"github.com/udaykishore-resu/db-migration-platform/internal/dialect"
	"github.com/udaykishore-resu/db-migration-platform/internal/dlq"
	"github.com/udaykishore-resu/db-migration-platform/internal/errclass"
	"github.com/udaykishore-resu/db-migration-platform/internal/model"
	"github.com/udaykishore-resu/db-migration-platform/internal/retryx"
	"github.com/udaykishore-resu/db-migration-platform/internal/sink"
)

// PayloadCipher encrypts dead-lettered payloads at rest. A dead-letter table is a
// durable copy of production data with a longer retention than the pipeline
// itself, so it gets the same protection as the target.
type PayloadCipher interface {
	EncryptPayload(domain string, payload []byte) (string, error)
	DecryptPayload(domain, encoded string) ([]byte, error)
}

// Store persists platform state in the target database.
type Store struct {
	db          *sql.DB
	d           dialect.Dialect
	migrationID string
	cipher      PayloadCipher
}

// New builds a store.
func New(db *sql.DB, d dialect.Dialect, migrationID string, cipher PayloadCipher) *Store {
	return &Store{db: db, d: d, migrationID: migrationID, cipher: cipher}
}

const dlqDomain = "migration_ctl.dead_letter.payload"

// ---------------------------------------------------------------- offsets

// LoadOffsets restores committed stream progress.
//
// The stored offset always wins over any configured start position, because it
// is the one that was written in the same transaction as the data it accounts
// for. Trusting the broker's committed offset instead would reintroduce exactly
// the gap this design exists to close.
func (s *Store) LoadOffsets(ctx context.Context) ([]sink.Position, error) {
	rows, err := s.db.QueryContext(ctx, s.d.SelectOffsets(), s.migrationID)
	if err != nil {
		return nil, fmt.Errorf("store: loading offsets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []sink.Position
	for rows.Next() {
		var p sink.Position
		var lsn int64
		if err := rows.Scan(&p.Topic, &p.Partition, &p.Offset, &lsn); err != nil {
			return nil, fmt.Errorf("store: scanning offset: %w", err)
		}
		p.LastLSN = uint64(lsn) //nolint:gosec // LSNs are written as non-negative
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- dead letters

// PutDeadLetter records a failed event durably.
func (s *Store) PutDeadLetter(ctx context.Context, e dlq.Entry) error {
	payload, encrypted, err := s.encodePayload(e.Payload)
	if err != nil {
		return err
	}
	keyJSON, err := json.Marshal(e.Key.Map())
	if err != nil {
		return fmt.Errorf("store: encoding row key: %w", err)
	}

	q := s.rebind(`INSERT INTO ` + dialect.ControlSchema + `.dead_letter
 (migration_id, source_table, op, row_key_hash, row_key, payload, payload_encrypted,
  error_class, last_error, attempts, status, first_seen_at, next_retry_at,
  source_lsn, topic, ` + s.col("partition") + `, ` + s.col("offset") + `)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)

	_, err = s.db.ExecContext(ctx, q,
		s.migrationID, e.SourceTable.String(), string(e.Op), e.KeyHash, string(keyJSON),
		payload, encrypted, string(e.ErrorClass), e.LastError, e.Attempts, string(e.Status),
		e.FirstSeenAt.UTC(), nullTime(e.NextRetryAt), int64(e.SourceLSN), //nolint:gosec // see above
		e.Topic, e.Partition, e.Offset)
	if err != nil {
		return fmt.Errorf("store: writing dead letter for %s: %w", e.KeyHash, err)
	}
	return nil
}

// PutDeadLetters records a batch of failures.
func (s *Store) PutDeadLetters(ctx context.Context, entries []dlq.Entry) error {
	for _, e := range entries {
		if err := s.PutDeadLetter(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// Claim takes ownership of a batch of due dead letters.
//
// The two-statement shape — a locking SELECT followed by an UPDATE, inside one
// transaction — is used on both engines even though Postgres could express it as
// one statement, so the repair worker has no dialect-specific branches.
func (s *Store) Claim(ctx context.Context, worker string, limit int) ([]dlq.Entry, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: beginning claim: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx, s.d.SelectClaimableDeadLetters(limit))
	if err != nil {
		return nil, fmt.Errorf("store: selecting claimable dead letters: %w", err)
	}

	var entries []dlq.Entry
	var ids []any
	for rows.Next() {
		e, err := s.scanEntry(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		entries = append(entries, e)
		ids = append(ids, e.ID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	if len(entries) == 0 {
		committed = true
		return nil, tx.Commit()
	}

	args := append([]any{worker}, ids...)
	if _, err := tx.ExecContext(ctx, s.d.MarkDeadLettersClaimed(len(ids)), args...); err != nil {
		return nil, fmt.Errorf("store: marking dead letters claimed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: committing claim: %w", err)
	}
	committed = true
	return entries, nil
}

// Resolve marks an entry applied.
func (s *Store) Resolve(ctx context.Context, e dlq.Entry, d dlq.Decision) error {
	q := s.rebind(`UPDATE ` + dialect.ControlSchema + `.dead_letter
 SET status = ?, attempts = ?, resolved_at = ?, last_error = ?, claimed_by = NULL, claimed_at = NULL
 WHERE id = ?`)
	_, err := s.db.ExecContext(ctx, q, string(d.Status), d.Attempts, time.Now().UTC(), d.Reason, e.ID)
	if err != nil {
		return fmt.Errorf("store: resolving dead letter %d: %w", e.ID, err)
	}
	return nil
}

// Reschedule records a failed retry and the next attempt time.
func (s *Store) Reschedule(ctx context.Context, e dlq.Entry, d dlq.Decision, lastErr error) error {
	q := s.rebind(`UPDATE ` + dialect.ControlSchema + `.dead_letter
 SET status = ?, attempts = ?, next_retry_at = ?, last_error = ?, error_class = ?,
     claimed_by = NULL, claimed_at = NULL
 WHERE id = ?`)
	_, err := s.db.ExecContext(ctx, q,
		string(d.Status), d.Attempts, nullTime(d.NextRetryAt), truncate(errText(lastErr), 1000),
		string(errclass.Classify(lastErr)), e.ID)
	if err != nil {
		return fmt.Errorf("store: rescheduling dead letter %d: %w", e.ID, err)
	}
	return nil
}

// Requeue moves quarantined entries back into the retry queue, which is the
// operator action after the underlying cause has been fixed.
func (s *Store) Requeue(ctx context.Context, ids []int64, by string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := ""
	args := []any{time.Now().UTC(), by}
	for i, id := range ids {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += "?"
		args = append(args, id)
	}
	q := s.rebind(`UPDATE ` + dialect.ControlSchema + `.dead_letter
 SET status = 'pending', next_retry_at = ?, attempts = 0, requeued_by = ?
 WHERE id IN (` + placeholders + `) AND status = 'quarantined'`)

	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("store: requeueing dead letters: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// Counts summarises the dead-letter store for the cutover gate and for alerting.
func (s *Store) Counts(ctx context.Context) (dlq.Counts, error) {
	q := s.rebind(`SELECT status, COUNT(*), MIN(first_seen_at)
 FROM ` + dialect.ControlSchema + `.dead_letter WHERE migration_id = ? GROUP BY status`)

	rows, err := s.db.QueryContext(ctx, q, s.migrationID)
	if err != nil {
		return dlq.Counts{}, fmt.Errorf("store: counting dead letters: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var c dlq.Counts
	now := time.Now().UTC()
	for rows.Next() {
		var status string
		var n int64
		var oldest sql.NullTime
		if err := rows.Scan(&status, &n, &oldest); err != nil {
			return c, err
		}
		switch dlq.Status(status) {
		case dlq.StatusPending:
			c.Pending = n
		case dlq.StatusRetrying:
			c.Retrying = n
		case dlq.StatusQuarantined:
			c.Quarantined = n
		case dlq.StatusResolved:
			c.Resolved = n
		case dlq.StatusDiscarded:
			c.Discarded = n
		}
		// A growing age is what a raw count hides: the drain not keeping up looks
		// identical to a steady trickle until you look at how old the oldest is.
		if dlq.Status(status).Open() && oldest.Valid {
			if age := now.Sub(oldest.Time); age > c.OldestOpenAge {
				c.OldestOpenAge = age
			}
		}
	}
	return c, rows.Err()
}

// ---------------------------------------------------------------- migration state

// State is the persisted migration status.
type State struct {
	MigrationID       string
	Phase             control.Phase
	PartsTotal        int
	PartsLoaded       int
	ReconcileRanAt    time.Time
	ReconcileFindings int
	ReconcileComplete bool
	ReverseArmed      bool
	UpdatedAt         time.Time
}

// LoadState reads the migration status row.
func (s *Store) LoadState(ctx context.Context) (State, error) {
	q := s.rebind(`SELECT phase, parts_total, parts_loaded, reconcile_ran_at,
 reconcile_findings, reconcile_complete, reverse_replication_armed, updated_at
 FROM ` + dialect.ControlSchema + `.migration_state WHERE migration_id = ?`)

	var st State
	st.MigrationID = s.migrationID
	var ranAt sql.NullTime
	err := s.db.QueryRowContext(ctx, q, s.migrationID).Scan(
		&st.Phase, &st.PartsTotal, &st.PartsLoaded, &ranAt,
		&st.ReconcileFindings, &st.ReconcileComplete, &st.ReverseArmed, &st.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		st.Phase = control.PhasePlanning
		return st, nil
	}
	if err != nil {
		return st, fmt.Errorf("store: loading migration state: %w", err)
	}
	if ranAt.Valid {
		st.ReconcileRanAt = ranAt.Time
	}
	return st, nil
}

// SetPhase moves the migration to a new phase, refusing illegal transitions.
func (s *Store) SetPhase(ctx context.Context, next control.Phase) error {
	current, err := s.LoadState(ctx)
	if err != nil {
		return err
	}
	if err := control.Transition(current.Phase, next); err != nil {
		return err
	}
	q := s.rebind(`UPDATE ` + dialect.ControlSchema + `.migration_state
 SET phase = ?, updated_at = ? WHERE migration_id = ?`)
	if _, err := s.db.ExecContext(ctx, q, string(next), time.Now().UTC(), s.migrationID); err != nil {
		return fmt.Errorf("store: setting phase: %w", err)
	}
	return nil
}

// RecordReconcileRun stores the outcome of a verification pass.
func (s *Store) RecordReconcileRun(ctx context.Context, findings int, complete bool) error {
	q := s.rebind(`UPDATE ` + dialect.ControlSchema + `.migration_state
 SET reconcile_ran_at = ?, reconcile_findings = ?, reconcile_complete = ?, updated_at = ?
 WHERE migration_id = ?`)
	_, err := s.db.ExecContext(ctx, q, time.Now().UTC(), findings, complete, time.Now().UTC(), s.migrationID)
	if err != nil {
		return fmt.Errorf("store: recording reconciliation run: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------- parts

// MarkPartLoaded records that a part has been applied, so a restart does not
// reload it and a crashed loader resumes where it stopped.
func (s *Store) MarkPartLoaded(ctx context.Context, table model.TableRef, index int, rows int64, digest string) error {
	q := s.rebind(`INSERT INTO ` + dialect.ControlSchema + `.part_state
 (migration_id, source_table, part_index, rows_loaded, sha256, state, loaded_at)
 VALUES (?,?,?,?,?,'loaded',?)`)
	_, err := s.db.ExecContext(ctx, q, s.migrationID, table.String(), index, rows, digest, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("store: marking part %d of %s loaded: %w", index, table, err)
	}
	return nil
}

// PartLoaded reports whether a part has already been applied.
func (s *Store) PartLoaded(ctx context.Context, table model.TableRef, index int) (bool, error) {
	q := s.rebind(`SELECT COUNT(*) FROM ` + dialect.ControlSchema + `.part_state
 WHERE migration_id = ? AND source_table = ? AND part_index = ? AND state = 'loaded'`)
	var n int
	if err := s.db.QueryRowContext(ctx, q, s.migrationID, table.String(), index).Scan(&n); err != nil {
		return false, fmt.Errorf("store: checking part state: %w", err)
	}
	return n > 0, nil
}

// PartCounts reports load progress for the cutover gate.
func (s *Store) PartCounts(ctx context.Context) (total, loaded int, err error) {
	q := s.rebind(`SELECT COUNT(*), SUM(CASE WHEN state = 'loaded' THEN 1 ELSE 0 END)
 FROM ` + dialect.ControlSchema + `.part_state WHERE migration_id = ?`)
	var l sql.NullInt64
	if err := s.db.QueryRowContext(ctx, q, s.migrationID).Scan(&total, &l); err != nil {
		return 0, 0, fmt.Errorf("store: counting parts: %w", err)
	}
	return total, int(l.Int64), nil
}

// ---------------------------------------------------------------- helpers

func (s *Store) scanEntry(rows *sql.Rows) (dlq.Entry, error) {
	var e dlq.Entry
	var table, op, payload, class string
	var encrypted bool
	var lsn int64
	var nextRetry sql.NullTime
	if err := rows.Scan(&e.ID, &e.MigrationID, &table, &op, &e.KeyHash, &payload, &encrypted,
		&class, &e.LastError, &e.Attempts, &e.FirstSeenAt, &nextRetry, &lsn,
		&e.Topic, &e.Partition, &e.Offset); err != nil {
		return e, fmt.Errorf("store: scanning dead letter: %w", err)
	}

	e.SourceTable = model.ParseTableRef(table)
	e.Op = model.Op(op)
	e.ErrorClass = errclass.Class(class)
	e.PayloadEncrypted = encrypted
	e.SourceLSN = uint64(lsn) //nolint:gosec // written as non-negative
	if nextRetry.Valid {
		e.NextRetryAt = nextRetry.Time
	}

	raw, err := s.decodePayload(payload, encrypted)
	if err != nil {
		return e, err
	}
	e.Payload = raw
	return e, nil
}

func (s *Store) encodePayload(raw []byte) (string, bool, error) {
	if s.cipher == nil || len(raw) == 0 {
		return string(raw), false, nil
	}
	ct, err := s.cipher.EncryptPayload(dlqDomain, raw)
	if err != nil {
		return "", false, fmt.Errorf("store: encrypting dead-letter payload: %w", err)
	}
	return ct, true, nil
}

func (s *Store) decodePayload(stored string, encrypted bool) ([]byte, error) {
	if !encrypted {
		return []byte(stored), nil
	}
	if s.cipher == nil {
		return nil, errors.New("store: dead-letter payload is encrypted but no cipher is configured")
	}
	raw, err := s.cipher.DecryptPayload(dlqDomain, stored)
	if err != nil {
		return nil, fmt.Errorf("store: decrypting dead-letter payload: %w", err)
	}
	return raw, nil
}

// rebind converts "?" placeholders to the dialect's convention, so the store's
// statements can be written once in the portable form.
func (s *Store) rebind(q string) string {
	if s.d.Name() != dialect.Postgres {
		return q
	}
	var b []byte
	n := 0
	for i := 0; i < len(q); i++ {
		if q[i] != '?' {
			b = append(b, q[i])
			continue
		}
		n++
		b = append(b, []byte(s.d.Placeholder(n))...)
	}
	return string(b)
}

// col quotes a column name that collides with a reserved word on one engine or
// the other — "offset" and "partition" both do.
func (s *Store) col(name string) string { return s.d.Quote(name) }

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "… (truncated)"
}

// Policies bundles the retry policies the repair worker needs.
type Policies struct {
	Apply retryx.Policy
	DLQ   retryx.Policy
}

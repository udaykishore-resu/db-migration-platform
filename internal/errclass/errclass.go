// Package errclass classifies errors encountered on the apply path.
//
// The distinction it draws is the single most important operational decision in
// the platform. A transient error (deadlock, serialisation failure, connection
// reset, throttling) must be retried, because retrying will succeed. A permanent
// error (constraint violation, type overflow, decode failure) must NOT be
// retried, because retrying will fail identically forever while blocking the
// partition behind it — the classic head-of-line stall that turns a single bad
// row into a stalled migration.
//
// Anything that cannot be confidently classified is treated as transient but
// with a bounded attempt budget, so an unknown failure mode degrades into a
// dead-letter rather than into an infinite loop.
package errclass

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
)

// Class is the retry disposition of an error.
type Class string

const (
	// Transient errors resolve on their own; retry with backoff.
	Transient Class = "transient"
	// Permanent errors will never succeed on retry; dead-letter immediately.
	Permanent Class = "permanent"
	// Unknown errors are retried with a bounded budget, then dead-lettered.
	Unknown Class = "unknown"
)

// Retryable reports whether an error of this class should be retried in place.
func (c Class) Retryable() bool { return c == Transient || c == Unknown }

// permanentError marks an error as never worth retrying.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanently wraps err so that Classify always reports it as Permanent.
// Use this for validation and decode failures raised inside the platform.
func Permanently(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// transientError marks an error as worth retrying regardless of its text.
type transientError struct{ err error }

func (e transientError) Error() string { return e.err.Error() }
func (e transientError) Unwrap() error { return e.err }

// Transiently wraps err so that Classify always reports it as Transient.
func Transiently(err error) error {
	if err == nil {
		return nil
	}
	return transientError{err: err}
}

// sqlStater is satisfied by driver errors that expose a SQLSTATE code.
// Both lib/pq (*pq.Error) and MySQL wrappers can be adapted to it.
type sqlStater interface{ SQLState() string }

// numberedError is satisfied by the MySQL driver's *mysql.MySQLError.
type numberedError interface{ Number() uint16 }

// mysqlNumber extracts a MySQL error number without importing the driver, which
// keeps this package dependency-free and unit-testable in isolation.
func mysqlNumber(err error) (uint16, bool) {
	var ne numberedError
	if errors.As(err, &ne) {
		return ne.Number(), true
	}
	// The driver's error type exposes Number as a struct field, not a method,
	// on older versions; fall back to the textual form in that case.
	return 0, false
}

// Transient MySQL error numbers worth retrying.
var transientMySQL = map[uint16]bool{
	1040: true, // ER_CON_COUNT_ERROR: too many connections
	1053: true, // ER_SERVER_SHUTDOWN
	1205: true, // ER_LOCK_WAIT_TIMEOUT
	1213: true, // ER_LOCK_DEADLOCK
	1290: true, // ER_OPTION_PREVENTS_STATEMENT: read-only replica during failover
	1317: true, // ER_QUERY_INTERRUPTED
	2006: true, // CR_SERVER_GONE_ERROR
	2013: true, // CR_SERVER_LOST
	1927: true, // ER_CONNECTION_KILLED
}

// Permanent MySQL error numbers that must never be retried.
var permanentMySQL = map[uint16]bool{
	1062: true, // ER_DUP_ENTRY
	1264: true, // ER_WARN_DATA_OUT_OF_RANGE
	1265: true, // WARN_DATA_TRUNCATED
	1292: true, // ER_TRUNCATED_WRONG_VALUE
	1366: true, // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
	1406: true, // ER_DATA_TOO_LONG
	1452: true, // ER_NO_REFERENCED_ROW_2 (foreign key)
	1054: true, // ER_BAD_FIELD_ERROR
	1146: true, // ER_NO_SUCH_TABLE
	1364: true, // ER_NO_DEFAULT_FOR_FIELD
}

// Classify determines the retry disposition of an error.
func Classify(err error) Class {
	if err == nil {
		return Transient // caller should not be classifying nil
	}

	// Explicit markers win over any heuristic.
	var pe permanentError
	if errors.As(err, &pe) {
		return Permanent
	}
	var te transientError
	if errors.As(err, &te) {
		return Transient
	}

	// Context cancellation is a shutdown signal, not a data problem. Retrying is
	// correct once the new context is live.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Transient
	}

	// Network-level failures are transient by definition.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return Transient
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return Transient
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return Transient
	}

	// SQLSTATE classification, which covers Postgres precisely.
	var ss sqlStater
	if errors.As(err, &ss) {
		if c := classifySQLState(ss.SQLState()); c != Unknown {
			return c
		}
	}

	if n, ok := mysqlNumber(err); ok {
		if transientMySQL[n] {
			return Transient
		}
		if permanentMySQL[n] {
			return Permanent
		}
	}

	return classifyText(err.Error())
}

// classifySQLState maps a five-character SQLSTATE to a retry disposition.
// Class 40 (transaction rollback) and 08 (connection exception) are retryable;
// class 22 (data exception) and 23 (integrity constraint) never are.
func classifySQLState(state string) Class {
	if len(state) < 2 {
		return Unknown
	}
	switch state {
	case "40001": // serialization_failure
		return Transient
	case "40P01": // deadlock_detected
		return Transient
	case "55P03": // lock_not_available
		return Transient
	case "57P01", "57P02", "57P03": // admin_shutdown, crash_shutdown, cannot_connect_now
		return Transient
	case "53100", "53200", "53300": // disk_full, out_of_memory, too_many_connections
		return Transient
	case "25006": // read_only_sql_transaction, seen mid-failover
		return Transient
	}

	switch state[:2] {
	case "08": // connection exception
		return Transient
	case "40": // transaction rollback
		return Transient
	case "53": // insufficient resources
		return Transient
	case "57": // operator intervention
		return Transient
	case "58": // system error
		return Transient
	case "23": // integrity constraint violation
		return Permanent
	case "22": // data exception: overflow, truncation, invalid format
		return Permanent
	case "42": // syntax error or access rule violation
		return Permanent
	case "3F": // invalid schema name
		return Permanent
	case "0A": // feature not supported
		return Permanent
	}
	return Unknown
}

// Text fragments that reliably indicate a transient condition. Text matching is
// the last resort, used only when no structured code is available.
var transientText = []string{
	"connection reset",
	"connection refused",
	"broken pipe",
	"i/o timeout",
	"no such host",
	"server closed the connection",
	"driver: bad connection",
	"deadlock",
	"lock wait timeout",
	"too many connections",
	"could not serialize",
	"terminating connection",
	"the database system is starting up",
	"in recovery mode",
	"read-only",
	"throttl",
	"try again",
	"temporarily unavailable",
	"leader not available",
	"not enough replicas",
	"request timed out",
	"rebalance in progress",
	"coordinator not available",
}

// Text fragments that reliably indicate a permanent condition.
var permanentText = []string{
	"duplicate key",
	"unique constraint",
	"foreign key constraint",
	"check constraint",
	"violates not-null",
	"value too long",
	"out of range",
	"invalid input syntax",
	"cannot be cast",
	"column does not exist",
	"relation does not exist",
	"unknown column",
	"incorrect",
	"data truncated",
	"unmarshal",
	"invalid character",
	"unsupported",
	"malformed",
	"decode",
}

func classifyText(msg string) Class {
	m := strings.ToLower(msg)
	for _, frag := range transientText {
		if strings.Contains(m, frag) {
			return Transient
		}
	}
	for _, frag := range permanentText {
		if strings.Contains(m, frag) {
			return Permanent
		}
	}
	return Unknown
}

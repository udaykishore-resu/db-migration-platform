package errclass

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
)

type fakePQ struct{ state, msg string }

func (e fakePQ) Error() string    { return e.msg }
func (e fakePQ) SQLState() string { return e.state }

type fakeMySQL struct {
	num uint16
	msg string
}

func (e fakeMySQL) Error() string  { return e.msg }
func (e fakeMySQL) Number() uint16 { return e.num }

func TestClassifyExplicitMarkersWin(t *testing.T) {
	// A duplicate-key message would normally classify Permanent, but an explicit
	// transient marker must override the heuristic.
	err := Transiently(errors.New("duplicate key value violates unique constraint"))
	if got := Classify(err); got != Transient {
		t.Fatalf("explicit transient marker ignored, got %s", got)
	}
	// And the reverse.
	err = Permanently(errors.New("connection reset by peer"))
	if got := Classify(err); got != Permanent {
		t.Fatalf("explicit permanent marker ignored, got %s", got)
	}
}

func TestClassifyMarkersSurviveWrapping(t *testing.T) {
	err := fmt.Errorf("applying batch: %w", Permanently(errors.New("bad row")))
	if got := Classify(err); got != Permanent {
		t.Fatalf("marker lost through fmt.Errorf wrapping, got %s", got)
	}
}

func TestClassifySQLState(t *testing.T) {
	cases := []struct {
		state string
		want  Class
	}{
		{"40001", Transient}, // serialization failure
		{"40P01", Transient}, // deadlock
		{"08006", Transient}, // connection failure
		{"53300", Transient}, // too many connections
		{"57P03", Transient}, // cannot connect now
		{"23505", Permanent}, // unique violation
		{"23503", Permanent}, // foreign key violation
		{"22001", Permanent}, // string data right truncation
		{"22003", Permanent}, // numeric value out of range
		{"42703", Permanent}, // undefined column
		{"0A000", Permanent}, // feature not supported
	}
	for _, tc := range cases {
		got := Classify(fakePQ{state: tc.state, msg: "pq: something"})
		if got != tc.want {
			t.Errorf("SQLSTATE %s: got %s, want %s", tc.state, got, tc.want)
		}
	}
}

func TestClassifyMySQLNumbers(t *testing.T) {
	cases := []struct {
		num  uint16
		want Class
	}{
		{1213, Transient}, // deadlock
		{1205, Transient}, // lock wait timeout
		{1040, Transient}, // too many connections
		{2006, Transient}, // server gone away
		{1062, Permanent}, // duplicate entry
		{1406, Permanent}, // data too long
		{1452, Permanent}, // foreign key
		{1264, Permanent}, // out of range
	}
	for _, tc := range cases {
		got := Classify(fakeMySQL{num: tc.num, msg: "mysql error"})
		if got != tc.want {
			t.Errorf("mysql %d: got %s, want %s", tc.num, got, tc.want)
		}
	}
}

func TestClassifyNetworkErrorsAreTransient(t *testing.T) {
	errs := []error{
		syscall.ECONNRESET,
		syscall.ECONNREFUSED,
		syscall.EPIPE,
		&net.DNSError{Err: "no such host", Name: "aurora.internal"},
		&net.OpError{Op: "dial", Err: syscall.ETIMEDOUT},
	}
	for _, err := range errs {
		if got := Classify(err); got != Transient {
			t.Errorf("%v: got %s, want transient", err, got)
		}
	}
}

func TestClassifyContextErrorsAreTransient(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := Classify(fmt.Errorf("apply: %w", err)); got != Transient {
			t.Errorf("%v: got %s, want transient", err, got)
		}
	}
}

func TestClassifyTextFallback(t *testing.T) {
	cases := []struct {
		msg  string
		want Class
	}{
		{"pq: deadlock detected", Transient},
		{"Error 1213: Deadlock found when trying to get lock", Transient},
		{"kafka: rebalance in progress", Transient},
		{"ERROR: duplicate key value violates unique constraint \"pk_account\"", Permanent},
		{"json: cannot unmarshal string into Go value of type int64", Permanent},
		{"something nobody has ever seen before", Unknown},
	}
	for _, tc := range cases {
		if got := Classify(errors.New(tc.msg)); got != tc.want {
			t.Errorf("%q: got %s, want %s", tc.msg, got, tc.want)
		}
	}
}

// An unknown error must remain retryable so a novel transient failure does not
// dead-letter good data, but the bounded attempt budget stops it looping forever.
func TestUnknownIsRetryableButNotPermanent(t *testing.T) {
	if !Unknown.Retryable() {
		t.Fatal("unknown errors must be retryable with a bounded budget")
	}
	if Permanent.Retryable() {
		t.Fatal("permanent errors must never be retried")
	}
	if !Transient.Retryable() {
		t.Fatal("transient errors must be retryable")
	}
}

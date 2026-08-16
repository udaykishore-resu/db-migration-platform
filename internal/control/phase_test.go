package control

import (
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

func ready() Observed {
	return Observed{
		Phase:                   PhaseVerifying,
		CurrentLag:              2 * time.Second,
		LagUnderThreshold:       30 * time.Minute,
		OpenDeadLetters:         0,
		ReconcileFindings:       0,
		ReconcileRanAt:          testNow.Add(-5 * time.Minute),
		ReconcileComplete:       true,
		PartsTotal:              120,
		PartsLoaded:             120,
		ReverseReplicationArmed: true,
		Now:                     testNow,
	}
}

func codes(r Readiness) []string {
	out := make([]string, len(r.Blockers))
	for i, b := range r.Blockers {
		out[i] = b.Code
	}
	return out
}

func hasCode(r Readiness, code string) bool {
	for _, b := range r.Blockers {
		if b.Code == code {
			return true
		}
	}
	return false
}

func TestGateOpensWhenEverythingIsSatisfied(t *testing.T) {
	r := Evaluate(ready(), DefaultThresholds())
	if !r.Ready {
		t.Fatalf("expected the gate to open, blocked by %v", codes(r))
	}
}

// Extraction and loading genuinely overlap — that pipelining is what halves
// wall-clock time — so the state machine has to permit moving between them.
func TestExtractAndLoadOverlap(t *testing.T) {
	if !PhaseExtracting.CanTransitionTo(PhaseLoading) {
		t.Error("extraction must be able to hand parts to the loader")
	}
	if !PhaseLoading.CanTransitionTo(PhaseExtracting) {
		t.Error("loading must be able to return to extraction while parts are still being produced")
	}
}

// The rollback path has to exist in the state machine, or it will not exist in
// practice when it is needed at three in the morning.
func TestRollbackIsReachableFromCutover(t *testing.T) {
	if !PhaseCutover.CanTransitionTo(PhaseRolledBack) {
		t.Fatal("cutover must be able to roll back")
	}
	if !PhaseRolledBack.CanTransitionTo(PhaseStreaming) {
		t.Fatal("a rolled-back migration must be able to resume streaming and try again")
	}
}

func TestIllegalTransitionsAreRejectedWithGuidance(t *testing.T) {
	err := Transition(PhasePlanning, PhaseCutover)
	if err == nil {
		t.Fatal("planning must not jump straight to cutover")
	}
	// The error should tell the operator what they can do, not only what they
	// cannot.
	if !strings.Contains(err.Error(), "permitted next phases") {
		t.Fatalf("error should list the legal transitions: %v", err)
	}
	if err := Transition(PhaseCompleted, PhaseStreaming); err == nil {
		t.Fatal("a completed migration must not reopen")
	}
	if err := Transition(PhaseStreaming, PhaseStreaming); err != nil {
		t.Fatalf("a no-op transition should be allowed: %v", err)
	}
}

func TestTerminalPhases(t *testing.T) {
	if !PhaseCompleted.Terminal() {
		t.Error("completed should be terminal")
	}
	if PhaseCutover.Terminal() {
		t.Error("cutover is not terminal; rollback and completion both follow it")
	}
}

func TestHighLagBlocksCutover(t *testing.T) {
	o := ready()
	o.CurrentLag = 45 * time.Second
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockLagTooHigh) {
		t.Fatalf("expected a lag blocker, got %v", codes(r))
	}
	// The blocker must state both the observed and the required value.
	for _, b := range r.Blockers {
		if b.Code == BlockLagTooHigh && (!strings.Contains(b.Detail, "45s") || !strings.Contains(b.Detail, "10s")) {
			t.Fatalf("blocker should report observed and threshold: %q", b.Detail)
		}
	}
}

// A momentary dip below the lag threshold proves nothing. The gate has to see
// the target sustain it.
func TestBrieflyLowLagIsNotEnough(t *testing.T) {
	o := ready()
	o.CurrentLag = time.Second
	o.LagUnderThreshold = 30 * time.Second
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockLagNotStable) {
		t.Fatalf("expected a stability blocker, got %v", codes(r))
	}
	// Reporting both "too high" and "not stable long enough" at once would be
	// contradictory.
	if hasCode(r, BlockLagTooHigh) {
		t.Fatal("must not report lag as both too high and merely unstable")
	}
}

// Cutting over with unapplied records means knowingly shipping an incomplete
// target.
func TestOpenDeadLettersBlockCutover(t *testing.T) {
	o := ready()
	o.OpenDeadLetters = 3
	o.QuarantinedLetters = 2
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockOpenDeadLetters) {
		t.Fatalf("expected a dead-letter blocker, got %v", codes(r))
	}
	for _, b := range r.Blockers {
		if b.Code == BlockOpenDeadLetters && !strings.Contains(b.Detail, "quarantined") {
			t.Fatalf("blocker should call out quarantined records needing a decision: %q", b.Detail)
		}
	}
}

// A migration with a documented, accepted set of unmigratable records should be
// able to proceed — deliberately, with the count written down.
func TestDeadLetterToleranceIsConfigurable(t *testing.T) {
	o := ready()
	o.OpenDeadLetters = 3
	th := DefaultThresholds()
	th.MaxOpenDeadLetters = 5
	if r := Evaluate(o, th); !r.Ready {
		t.Fatalf("expected an accepted tolerance to open the gate, blocked by %v", codes(r))
	}
}

func TestReconciliationFindingsBlockCutover(t *testing.T) {
	o := ready()
	o.ReconcileFindings = 1
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockReconcileFindings) {
		t.Fatalf("a single discrepancy must block cutover, got %v", codes(r))
	}
}

// A clean reconciliation from six hours ago says nothing about the last six
// hours.
func TestStaleReconciliationBlocksCutover(t *testing.T) {
	o := ready()
	o.ReconcileRanAt = testNow.Add(-6 * time.Hour)
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockReconcileStale) {
		t.Fatalf("expected a staleness blocker, got %v", codes(r))
	}
}

func TestNeverReconciledBlocksCutover(t *testing.T) {
	o := ready()
	o.ReconcileComplete = false
	o.ReconcileRanAt = time.Time{}
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockReconcileIncomplete) {
		t.Fatalf("expected an incomplete-verification blocker, got %v", codes(r))
	}
}

func TestUnloadedPartsBlockCutover(t *testing.T) {
	o := ready()
	o.PartsLoaded = 119
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockPartsUnloaded) {
		t.Fatalf("expected an unloaded-parts blocker, got %v", codes(r))
	}
	for _, b := range r.Blockers {
		if b.Code == BlockPartsUnloaded && !strings.Contains(b.Detail, "119 of 120") {
			t.Fatalf("blocker should quantify the gap: %q", b.Detail)
		}
	}
}

// Without reverse replication, a post-cutover rollback silently discards every
// write the business made after cutover. That is not a rollback.
func TestMissingRollbackPathBlocksCutover(t *testing.T) {
	o := ready()
	o.ReverseReplicationArmed = false
	r := Evaluate(o, DefaultThresholds())
	if r.Ready || !hasCode(r, BlockNoRollbackPath) {
		t.Fatalf("expected a rollback-path blocker, got %v", codes(r))
	}
	for _, b := range r.Blockers {
		if b.Code == BlockNoRollbackPath && !strings.Contains(b.Detail, "lose every write") {
			t.Fatalf("blocker should explain the consequence: %q", b.Detail)
		}
	}
}

// An operator planning a cutover window needs the whole list at once; returning
// blockers one at a time turns one fix into five separate discoveries.
func TestEveryBlockerIsReportedTogether(t *testing.T) {
	o := Observed{
		Phase: PhasePlanning, CurrentLag: time.Minute, OpenDeadLetters: 7,
		ReconcileFindings: 4, ReconcileComplete: false,
		PartsTotal: 10, PartsLoaded: 2, ReverseReplicationArmed: false, Now: testNow,
	}
	r := Evaluate(o, DefaultThresholds())
	if r.Ready {
		t.Fatal("expected the gate to be closed")
	}
	for _, want := range []string{
		BlockWrongPhase, BlockLagTooHigh, BlockOpenDeadLetters,
		BlockReconcileFindings, BlockReconcileIncomplete, BlockPartsUnloaded, BlockNoRollbackPath,
	} {
		if !hasCode(r, want) {
			t.Errorf("missing blocker %s; got %v", want, codes(r))
		}
	}
}

// Alert rules are written against these codes, so their order must be stable.
func TestBlockerOrderIsDeterministic(t *testing.T) {
	o := Observed{Phase: PhasePlanning, CurrentLag: time.Minute, OpenDeadLetters: 1, Now: testNow}
	first := codes(Evaluate(o, DefaultThresholds()))
	for i := 0; i < 20; i++ {
		got := codes(Evaluate(o, DefaultThresholds()))
		if len(got) != len(first) {
			t.Fatal("blocker count varies between evaluations")
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("blocker order varies: %v vs %v", first, got)
			}
		}
	}
}

// Refusing to provide an override does not stop anyone cutting over; it just
// moves the action somewhere nothing records it.
func TestOverrideRequiresAuthorAndSubstantiveReason(t *testing.T) {
	r := Evaluate(Observed{Phase: PhaseReady, Now: testNow}, DefaultThresholds())

	if err := ValidateOverride(Override{Reason: strings.Repeat("x", 30)}, r); err == nil {
		t.Error("an override without an author must be rejected")
	}
	if err := ValidateOverride(Override{By: "uday", Reason: "ok"}, r); err == nil {
		t.Error("a one-word justification must be rejected")
	}
}

// Acknowledging a stale list of blockers is not acknowledgement.
func TestOverrideMustAcknowledgeEveryActiveBlocker(t *testing.T) {
	o := ready()
	o.OpenDeadLetters = 2
	o.ReverseReplicationArmed = false
	r := Evaluate(o, DefaultThresholds())

	partial := Override{
		By: "uday", Reason: "accepted risk documented in CHG-11482, two records confirmed unmigratable",
		AcknowledgedAt: []string{BlockOpenDeadLetters},
	}
	err := ValidateOverride(partial, r)
	if err == nil {
		t.Fatal("an override acknowledging only some blockers must be rejected")
	}
	if !strings.Contains(err.Error(), BlockNoRollbackPath) {
		t.Fatalf("error should name the unacknowledged blocker: %v", err)
	}

	full := partial
	full.AcknowledgedAt = []string{BlockOpenDeadLetters, BlockNoRollbackPath}
	if err := ValidateOverride(full, r); err != nil {
		t.Fatalf("a fully acknowledged override should be accepted: %v", err)
	}
}

// Package control owns the migration state machine and the cutover gate.
//
// Cutover — repointing the application at the target database — is the one step
// in a migration that is genuinely hard to undo and impossible to do halfway.
// Everything before it can be retried, restarted or abandoned at no cost to the
// business. Everything after it is running on the new database.
//
// So the decision to cut over is not left to a person reading a dashboard at
// two in the morning. It is a predicate over facts the platform already
// measures, evaluated automatically, that either returns "ready" or returns the
// specific list of reasons it is not. An operator can override it — but the
// override is explicit, recorded, and requires them to state that they have read
// the blockers.
package control

import (
	"fmt"
	"sort"
	"time"
)

// Phase is the stage of a migration.
type Phase string

// Migration phases, in the order they occur.
const (
	// PhasePlanning is before anything has been read.
	PhasePlanning Phase = "planning"
	// PhaseExtracting is Phase 1: the source is being unloaded into parts, and
	// the change stream is already running concurrently.
	PhaseExtracting Phase = "extracting"
	// PhaseLoading is applying sealed parts to the target. It overlaps with
	// extraction, because a part becomes loadable the moment it is sealed.
	PhaseLoading Phase = "loading"
	// PhaseStreaming is steady-state change data capture with the bulk load
	// complete.
	PhaseStreaming Phase = "streaming"
	// PhaseVerifying is a full reconciliation pass.
	PhaseVerifying Phase = "verifying"
	// PhaseReady means the gate is satisfied and cutover may proceed.
	PhaseReady Phase = "ready_for_cutover"
	// PhaseCuttingOver is the brief window with source writes frozen.
	PhaseCuttingOver Phase = "cutting_over"
	// PhaseCutover means the application is writing to the target, with reverse
	// replication armed so that a rollback stays possible.
	PhaseCutover Phase = "cutover"
	// PhaseCompleted means reverse replication has been retired and the source
	// decommissioned.
	PhaseCompleted Phase = "completed"
	// PhaseRolledBack means the application was returned to the source.
	PhaseRolledBack Phase = "rolled_back"
	// PhaseFailed means the migration was abandoned.
	PhaseFailed Phase = "failed"
)

// validTransitions encodes the state machine.
//
// Two entries are worth noting. Extracting can move straight to Loading and back
// again, because the two genuinely overlap — that is the pipelining that halves
// wall-clock time. And Cutover can move to RolledBack, because the rollback path
// has to exist in the state machine or it will not exist in practice.
var validTransitions = map[Phase][]Phase{
	PhasePlanning:    {PhaseExtracting, PhaseFailed},
	PhaseExtracting:  {PhaseLoading, PhaseStreaming, PhaseFailed},
	PhaseLoading:     {PhaseExtracting, PhaseStreaming, PhaseFailed},
	PhaseStreaming:   {PhaseVerifying, PhaseLoading, PhaseFailed},
	PhaseVerifying:   {PhaseReady, PhaseStreaming, PhaseFailed},
	PhaseReady:       {PhaseCuttingOver, PhaseStreaming, PhaseVerifying, PhaseFailed},
	PhaseCuttingOver: {PhaseCutover, PhaseRolledBack, PhaseFailed},
	PhaseCutover:     {PhaseCompleted, PhaseRolledBack},
	PhaseCompleted:   {},
	PhaseRolledBack:  {PhaseStreaming, PhaseFailed},
	PhaseFailed:      {PhasePlanning},
}

// Terminal reports whether a phase has no successors.
func (p Phase) Terminal() bool { return len(validTransitions[p]) == 0 }

// CanTransitionTo reports whether a phase change is legal.
func (p Phase) CanTransitionTo(next Phase) bool {
	for _, allowed := range validTransitions[p] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Transition validates a phase change and explains any rejection.
func Transition(from, to Phase) error {
	if from == to {
		return nil
	}
	if _, known := validTransitions[from]; !known {
		return fmt.Errorf("control: unknown phase %q", from)
	}
	if _, known := validTransitions[to]; !known {
		return fmt.Errorf("control: unknown phase %q", to)
	}
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("control: cannot move from %s to %s; permitted next phases are %v", from, to, validTransitions[from])
	}
	return nil
}

// Thresholds are the operator-set conditions the gate enforces.
type Thresholds struct {
	// MaxLag is the replication lag the target must stay under.
	MaxLag time.Duration
	// LagStableFor is how long lag must remain under MaxLag before the gate
	// opens. A momentary dip below the threshold means nothing; the point is to
	// establish that the target is genuinely keeping up, not that it briefly
	// caught its breath.
	LagStableFor time.Duration
	// MaxOpenDeadLetters is normally zero. It exists as a number rather than a
	// boolean so that a migration with a documented, accepted set of unmigratable
	// records can still proceed — deliberately, with the count written down.
	MaxOpenDeadLetters int64
	// MaxReconcileFindings is normally zero, for the same reason.
	MaxReconcileFindings int
	// MaxReconcileAge is how stale a verification result may be. A clean
	// reconciliation from six hours ago says nothing about the last six hours.
	MaxReconcileAge time.Duration
	// RequireAllPartsLoaded fails the gate while any extracted part is unloaded.
	RequireAllPartsLoaded bool
	// RequireReverseReplication fails the gate unless the rollback path is armed.
	RequireReverseReplication bool
}

// DefaultThresholds are deliberately strict. Every one of them can be relaxed
// per migration, but the default position is that cutover requires the target to
// be demonstrably complete and demonstrably keeping up.
func DefaultThresholds() Thresholds {
	return Thresholds{
		MaxLag:                    10 * time.Second,
		LagStableFor:              15 * time.Minute,
		MaxOpenDeadLetters:        0,
		MaxReconcileFindings:      0,
		MaxReconcileAge:           30 * time.Minute,
		RequireAllPartsLoaded:     true,
		RequireReverseReplication: true,
	}
}

// Observed is the measured state the gate evaluates.
type Observed struct {
	Phase Phase

	CurrentLag        time.Duration
	LagUnderThreshold time.Duration

	OpenDeadLetters    int64
	QuarantinedLetters int64

	ReconcileFindings int
	ReconcileRanAt    time.Time
	ReconcileComplete bool

	PartsTotal  int
	PartsLoaded int

	ReverseReplicationArmed bool

	Now time.Time
}

// Blocker is one reason cutover cannot proceed.
type Blocker struct {
	// Code is stable and machine-readable, for alerting rules.
	Code string `json:"code"`
	// Detail is written for the person who has to fix it, and states the observed
	// value and the required one rather than just saying "not ready".
	Detail string `json:"detail"`
}

// Readiness is the gate's verdict.
type Readiness struct {
	Ready     bool      `json:"ready"`
	Blockers  []Blocker `json:"blockers,omitempty"`
	EvaluedAt time.Time `json:"evaluated_at"`
}

// Codes for each blocker, stable across releases so alert rules keep working.
const (
	BlockWrongPhase          = "wrong_phase"
	BlockLagTooHigh          = "lag_above_threshold"
	BlockLagNotStable        = "lag_not_stable_long_enough"
	BlockOpenDeadLetters     = "open_dead_letters"
	BlockReconcileFindings   = "reconciliation_findings"
	BlockReconcileStale      = "reconciliation_stale"
	BlockReconcileIncomplete = "reconciliation_incomplete"
	BlockPartsUnloaded       = "parts_not_loaded"
	BlockNoRollbackPath      = "reverse_replication_not_armed"
)

// Evaluate answers whether cutover may proceed, and if not, exactly why.
//
// It returns every blocker rather than the first, because an operator working
// towards a cutover window needs the whole list to plan against. Returning them
// one at a time turns a fifteen-minute fix into five separate discoveries.
func Evaluate(o Observed, t Thresholds) Readiness {
	if o.Now.IsZero() {
		o.Now = time.Now()
	}
	var blockers []Blocker

	if o.Phase != PhaseReady && o.Phase != PhaseVerifying && o.Phase != PhaseStreaming {
		blockers = append(blockers, Blocker{
			Code:   BlockWrongPhase,
			Detail: fmt.Sprintf("migration is in phase %q; cutover is only evaluated from streaming, verifying or ready", o.Phase),
		})
	}

	if t.MaxLag > 0 && o.CurrentLag > t.MaxLag {
		blockers = append(blockers, Blocker{
			Code:   BlockLagTooHigh,
			Detail: fmt.Sprintf("replication lag is %s, threshold is %s", o.CurrentLag.Round(time.Millisecond), t.MaxLag),
		})
	} else if t.LagStableFor > 0 && o.LagUnderThreshold < t.LagStableFor {
		// Only meaningful once lag is actually under the threshold; reporting
		// both at once would be confusing.
		blockers = append(blockers, Blocker{
			Code: BlockLagNotStable,
			Detail: fmt.Sprintf("lag has only been under %s for %s; %s of stability is required",
				t.MaxLag, o.LagUnderThreshold.Round(time.Second), t.LagStableFor),
		})
	}

	if o.OpenDeadLetters > t.MaxOpenDeadLetters {
		detail := fmt.Sprintf("%d records are still unapplied (limit %d)", o.OpenDeadLetters, t.MaxOpenDeadLetters)
		if o.QuarantinedLetters > 0 {
			detail += fmt.Sprintf("; %d of them are quarantined and need a decision", o.QuarantinedLetters)
		}
		blockers = append(blockers, Blocker{Code: BlockOpenDeadLetters, Detail: detail})
	}

	if o.ReconcileFindings > t.MaxReconcileFindings {
		blockers = append(blockers, Blocker{
			Code:   BlockReconcileFindings,
			Detail: fmt.Sprintf("reconciliation found %d discrepancies (limit %d)", o.ReconcileFindings, t.MaxReconcileFindings),
		})
	}

	if !o.ReconcileComplete {
		blockers = append(blockers, Blocker{
			Code:   BlockReconcileIncomplete,
			Detail: "no complete reconciliation pass has finished for this migration",
		})
	} else if t.MaxReconcileAge > 0 {
		if o.ReconcileRanAt.IsZero() {
			blockers = append(blockers, Blocker{
				Code:   BlockReconcileStale,
				Detail: "reconciliation has never run",
			})
		} else if age := o.Now.Sub(o.ReconcileRanAt); age > t.MaxReconcileAge {
			blockers = append(blockers, Blocker{
				Code: BlockReconcileStale,
				Detail: fmt.Sprintf("the last reconciliation finished %s ago; results older than %s say nothing about the current state",
					age.Round(time.Minute), t.MaxReconcileAge),
			})
		}
	}

	if t.RequireAllPartsLoaded && o.PartsLoaded < o.PartsTotal {
		blockers = append(blockers, Blocker{
			Code:   BlockPartsUnloaded,
			Detail: fmt.Sprintf("%d of %d extracted parts have been loaded", o.PartsLoaded, o.PartsTotal),
		})
	}

	if t.RequireReverseReplication && !o.ReverseReplicationArmed {
		blockers = append(blockers, Blocker{
			Code:   BlockNoRollbackPath,
			Detail: "reverse replication from target back to source is not running, so a post-cutover rollback would lose every write made after cutover",
		})
	}

	sort.SliceStable(blockers, func(i, j int) bool { return blockers[i].Code < blockers[j].Code })
	return Readiness{Ready: len(blockers) == 0, Blockers: blockers, EvaluedAt: o.Now}
}

// Override records a deliberate decision to cut over despite blockers.
//
// It exists because refusing to provide an override does not stop anyone from
// cutting over — it just moves the action outside the platform where nothing
// records it. Making the override a first-class, audited operation is strictly
// safer than pretending it will not happen.
type Override struct {
	By             string    `json:"by"`
	At             time.Time `json:"at"`
	Reason         string    `json:"reason"`
	AcknowledgedAt []string  `json:"acknowledged_blockers"`
}

// ValidateOverride checks that an override is properly justified and that the
// operator acknowledged the blockers that were actually present — not a stale
// list from an earlier evaluation.
func ValidateOverride(o Override, r Readiness) error {
	if o.By == "" {
		return fmt.Errorf("control: an override must record who authorised it")
	}
	if len(o.Reason) < 20 {
		return fmt.Errorf("control: an override must record a substantive reason (got %d characters)", len(o.Reason))
	}

	present := make(map[string]bool, len(r.Blockers))
	for _, b := range r.Blockers {
		present[b.Code] = true
	}
	acknowledged := make(map[string]bool, len(o.AcknowledgedAt))
	for _, c := range o.AcknowledgedAt {
		acknowledged[c] = true
	}
	var missing []string
	for code := range present {
		if !acknowledged[code] {
			missing = append(missing, code)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("control: override does not acknowledge every active blocker: %v", missing)
	}
	return nil
}

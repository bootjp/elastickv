package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	stderrors "errors"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

var (
	ErrTSOPhaseDActivationPending      = errors.New("tso: phase-D activation is pending")
	ErrTSOPhaseDActivationGateRequired = errors.New("tso: phase-D activation gate is required")
)

// TSOPhaseDActivationGate drains unproven prepares after all replicas adopt
// cutover, seals creation before sampling the global floor, then fences each
// data group at the fixed floor. FinishPhaseD releases creation after M7.
// Callbacks must return promptly with ErrTSOPhaseDActivationPending while old
// work remains, never wait for work requiring the allocator mutex, and be
// idempotent across retries, restarts, and leader changes.
type TSOPhaseDActivationGate interface {
	CheckPhaseDProtocol(context.Context) error
	SealPhaseDCreation(context.Context) error
	PreparePhaseD(context.Context, uint64) error
	FinishPhaseD(context.Context, uint64) error
}

// TSOPendingPhaseDFloorSource exposes the durable barrier to cached issuers.
// An already reserved Phase-C window must not bypass a pending activation.
type TSOPendingPhaseDFloorSource interface {
	PendingPhaseDFloor() uint64
}

// DurableLegacyResolutionValidator validates legacy resolution order while
// Phase D is inactive, and additionally the fixed bound once pending exists.
// Callers must prove durable txn evidence in BOTH the pre-pending drain and
// pending stages. Success never authorizes an ordinary write or PREPARE.
type DurableLegacyResolutionValidator interface {
	ValidateLegacyResolutionTimestamps(context.Context, uint64, uint64) error
}

const tsoPendingPhaseDEnvelope = "\x07TSOP\x01"
const tsoSnapshotV5Len = tsoSnapshotV4Len + hlcLeasePayloadLen

const (
	phaseDBootstrapProbeTimeout  = time.Second
	phaseDBootstrapRetryInterval = 5 * time.Second
)

// TSOValidationErrorDomain identifies structured group-0 validation errors.
const TSOValidationErrorDomain = "elastickv.tso"

// TSOPhaseDInactiveReason preserves the inactive sentinel over gRPC without
// confusing it with a not-leader FailedPrecondition.
const TSOPhaseDInactiveReason = "PHASE_D_INACTIVE"

func WithTSOPhaseDActivationGate(gate TSOPhaseDActivationGate) RaftTSOAllocatorOption {
	return func(a *RaftTSOAllocator) { a.phaseDGate = gate }
}

// PendingPhaseDFloor is zero when no activation is waiting for its data gates.
// The backing value remains durable after activation for idempotent WAL replay.
func (f *TSOStateMachine) PendingPhaseDFloor() uint64 {
	if f == nil || f.PhaseDActive() {
		return 0
	}
	return f.pendingPhaseDFloor.Load()
}

// PrepareEvidenceProtocolFloor retains the negotiated barrier even after M7.
// Zero distinguishes historical active snapshots that still need bootstrap.
func (f *TSOStateMachine) PrepareEvidenceProtocolFloor() uint64 {
	if f == nil {
		return 0
	}
	return f.pendingPhaseDFloor.Load()
}

func marshalTSOPendingPhaseD(floor uint64) []byte {
	out := make([]byte, len(tsoPendingPhaseDEnvelope)+hlcLeasePayloadLen)
	copy(out, tsoPendingPhaseDEnvelope)
	binary.BigEndian.PutUint64(out[len(tsoPendingPhaseDEnvelope):], floor)
	return out
}

func (f *TSOStateMachine) applyPendingPhaseDEntry(data []byte) any {
	if len(data) != len(tsoPendingPhaseDEnvelope)+hlcLeasePayloadLen {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "invalid pending phase-D entry length"))
	}
	floor := binary.BigEndian.Uint64(data[len(tsoPendingPhaseDEnvelope):])
	if f == nil || floor == 0 || floor == ^uint64(0) || !f.CutoverActive() {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "pending phase D requires cutover and a usable floor"))
	}
	if old := f.pendingPhaseDFloor.Load(); old != 0 && old != floor {
		return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry, "pending phase-D floor changed"))
	}
	if err := validatePendingPhaseDSnapshot(floor, f.AllocationFloor(), true, f.PhaseDActive(), f.PhaseDFloor()); err != nil {
		return haltErr(err)
	}
	f.pendingPhaseDFloor.Store(floor)
	return nil
}

func isTSOPendingPhaseDEntry(payload []byte) bool {
	return len(payload) == len(tsoPendingPhaseDEnvelope)+hlcLeasePayloadLen &&
		bytes.HasPrefix(payload, []byte(tsoPendingPhaseDEnvelope))
}

func decodePendingPhaseDSnapshot(payload []byte) (uint64, []byte, error) {
	if len(payload) != tsoSnapshotV5Len {
		return 0, payload, nil
	}
	floor := binary.BigEndian.Uint64(payload[tsoSnapshotV4Len:])
	if floor == 0 || floor == ^uint64(0) {
		return 0, nil, errors.Wrap(ErrTSOStateMachineInvalidEntry, "invalid snapshot pending phase-D floor")
	}
	return floor, payload[:tsoSnapshotV4Len], nil
}

func (f *TSOStateMachine) restorePendingPhaseDFloor(pending, allocation uint64, active bool, floor uint64) error {
	old := f.pendingPhaseDFloor.Load()
	if old != 0 && pending != 0 && old != pending {
		return errors.Wrap(ErrTSOStateMachineInvalidEntry, "snapshot changed pending phase-D floor")
	}
	bound := max(old, pending)
	if err := validatePendingPhaseDSnapshot(bound, allocation, true, active, floor); err != nil {
		return err
	}
	if bound != 0 && f.PhaseDActive() && f.PhaseDFloor() != bound {
		return errors.Wrap(ErrTSOStateMachineInvalidEntry, "snapshot conflicts with pending phase-D barrier")
	}
	if pending != 0 {
		f.pendingPhaseDFloor.Store(pending)
	}
	return nil
}

func validatePendingPhaseDSnapshot(pending, allocation uint64, cutover, active bool, floor uint64) error {
	if pending == 0 {
		return nil
	}
	if !cutover || pending == ^uint64(0) {
		return errors.Wrap(ErrTSOStateMachineInvalidEntry, "pending phase D requires cutover and a usable floor")
	}
	if active {
		if floor == pending {
			return nil
		}
		return errors.Wrap(ErrTSOStateMachineInvalidEntry, "phase-D floor disagrees with pending marker")
	}
	if allocation > pending {
		return errors.Wrap(ErrTSOStateMachineInvalidEntry, "allocation exceeds pending phase-D floor")
	}
	return nil
}

func (a *RaftTSOAllocator) preparePhaseDActivation(ctx context.Context, engine raftengine.Engine, term uint64, activate bool) (uint64, error) {
	if a.state.PhaseDActive() {
		return 0, a.finishPhaseDActivation(ctx, engine, term)
	}
	pending := a.state.PendingPhaseDFloor()
	if !activate {
		if pending != 0 {
			return 0, errors.WithStack(ErrTSOPhaseDActivationPending)
		}
		return 0, nil
	}
	if a.phaseDGate == nil {
		return 0, errors.WithStack(ErrTSOPhaseDActivationGateRequired)
	}
	return pending, a.sealPhaseDActivation(ctx, engine, term)
}

func (a *RaftTSOAllocator) sealPhaseDActivation(ctx context.Context, engine raftengine.Engine, term uint64) error {
	// All members must apply cutover before their capability responses can
	// authorize the seal; otherwise a stale runtime could mint legacy values
	// after the data groups are released.
	if !a.state.CutoverActive() {
		if err := a.commitCutover(ctx, engine); err != nil {
			return err
		}
	}
	if err := a.phaseDGate.CheckPhaseDProtocol(ctx); err != nil {
		return errors.Wrap(err, "check phase-D protocol")
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return err
	}
	if err := a.phaseDGate.SealPhaseDCreation(ctx); err != nil {
		return errors.Wrap(err, "seal phase-D creation")
	}
	return verifyTSOLeaderTerm(ctx, engine, term, true)
}

func (a *RaftTSOAllocator) finishPhaseDActivation(ctx context.Context, engine raftengine.Engine, term uint64) error {
	// Require the gate even for historical M7 state: no local compatibility
	// assumption may bypass an unfinished durable seal after restart.
	if a.phaseDGate == nil {
		return errors.WithStack(ErrTSOPhaseDActivationGateRequired)
	}
	if a.phaseDFinishedTerm == term {
		return nil
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return err
	}
	if a.state.PrepareEvidenceProtocolFloor() == 0 {
		started, err := a.tryBootstrapActivePhaseD(ctx, engine, term)
		if err != nil {
			return err
		}
		if !started {
			return nil
		}
	}
	return a.resumeActivePhaseDProtocol(ctx, engine, term)
}

func (a *RaftTSOAllocator) resumeActivePhaseDProtocol(ctx context.Context, engine raftengine.Engine, term uint64) error {
	// Historical M7 bootstrap records its pending marker before sealing, so
	// raw marker presence alone cannot prove these data-side steps completed.
	// Recheck first so a data group added by a restart configuration receives
	// its proof-required marker before the seal is proposed.
	if err := a.phaseDGate.CheckPhaseDProtocol(ctx); err != nil {
		return errors.Wrap(err, "check active phase-D protocol")
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return err
	}
	if err := a.phaseDGate.SealPhaseDCreation(ctx); err != nil {
		return errors.Wrap(err, "resume active phase-D seal")
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return err
	}
	if err := a.phaseDGate.PreparePhaseD(ctx, a.state.PhaseDFloor()); err != nil {
		return errors.Wrap(err, "resume active phase-D data fence")
	}
	return a.releasePhaseDActivation(ctx, engine, term)
}

func (a *RaftTSOAllocator) releasePhaseDActivation(ctx context.Context, engine raftengine.Engine, term uint64) error {
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return err
	}
	if err := a.phaseDGate.FinishPhaseD(ctx, a.state.PhaseDFloor()); err != nil {
		return errors.Wrap(err, "finish phase-D activation")
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return err
	}
	a.phaseDFinishedTerm = term
	return nil
}

func (a *RaftTSOAllocator) tryBootstrapActivePhaseD(ctx context.Context, engine raftengine.Engine, term uint64) (bool, error) {
	if a.state.PrepareEvidenceProtocolFloor() != 0 {
		return true, nil
	}
	if a.phaseDBootstrapTerm == term && time.Now().Before(a.phaseDBootstrapRetryAt) {
		return false, nil
	}
	floor := a.state.PhaseDFloor()
	if floor == 0 || floor == ^uint64(0) {
		return false, errors.Wrap(ErrTSOTimestampInvalid, "historical phase-D floor cannot carry prepare evidence protocol")
	}
	probeCtx, cancel := context.WithTimeout(ctx, phaseDBootstrapProbeTimeout)
	defer cancel()
	err := a.phaseDGate.CheckPhaseDProtocol(probeCtx)
	if fenceErr := verifyTSOLeaderTerm(ctx, engine, term, true); fenceErr != nil {
		return false, fenceErr
	}
	if a.state.PrepareEvidenceProtocolFloor() != 0 {
		return true, nil
	}
	if err != nil {
		// Before the first pending marker, failed capability negotiation or
		// legacy-lock drain must not stop an already active rolling upgrade.
		a.phaseDBootstrapTerm = term
		a.phaseDBootstrapRetryAt = time.Now().Add(phaseDBootstrapRetryInterval)
		return false, nil
	}
	if err := a.commitPendingPhaseD(ctx, engine, term, floor); err != nil {
		return false, err
	}
	return true, nil
}

func (a *RaftTSOAllocator) beginPendingPhaseD(ctx context.Context, engine raftengine.Engine, term uint64, n int, min, floor uint64) (TSOReservation, error) {
	if err := a.commitPendingPhaseD(ctx, engine, term, floor); err != nil {
		return TSOReservation{}, err
	}
	return a.resumePendingPhaseD(ctx, engine, term, n, min, floor)
}

func (a *RaftTSOAllocator) commitPendingPhaseD(ctx context.Context, engine raftengine.Engine, term, floor uint64) error {
	if _, err := a.group.Proposer().Propose(ctx, marshalTSOPendingPhaseD(floor)); err != nil {
		if tsoProposalLostLeadership(engine, err) {
			return errors.Wrap(stderrors.Join(ErrTSONotLeader, err), "commit pending phase D")
		}
		return errors.Wrap(err, "commit pending phase D")
	}
	if a.state.PrepareEvidenceProtocolFloor() != floor {
		return errors.New("pending phase-D proposal committed without applied marker")
	}
	return verifyTSOLeaderTerm(ctx, engine, term, true)
}

func (a *RaftTSOAllocator) resumePendingPhaseD(ctx context.Context, engine raftengine.Engine, term uint64, n int, min, floor uint64) (TSOReservation, error) {
	var empty TSOReservation
	if min > floor {
		return empty, errors.Wrap(ErrTSOTimestampInvalid, "pending phase-D minimum exceeds fixed floor")
	}
	if err := validateTSOMinimumWindow(floor, n); err != nil {
		return empty, err
	}
	if err := a.rejectMinimumWindowBeyondCeiling(floor, n); err != nil {
		return empty, err
	}
	if err := a.phaseDGate.PreparePhaseD(ctx, floor); err != nil {
		return empty, errors.Wrap(err, "prepare data groups for phase D")
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return empty, err
	}
	if err := a.commitPhaseD(ctx, engine, floor); err != nil {
		return empty, err
	}
	if err := a.releasePhaseDActivation(ctx, engine, term); err != nil {
		return empty, err
	}
	end := floor + positiveIntToUint64(n)
	if err := a.commitAllocationFloor(ctx, engine, end); err != nil {
		return empty, err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, false); err != nil {
		return empty, err
	}
	a.initializedTerm = term
	return TSOReservation{Base: floor + 1, Count: n, PreviousAllocationFloor: floor,
		CutoverActive: true, PhaseDActive: true, PhaseDFloor: floor}, nil
}

func (a *RaftTSOAllocator) ValidateLegacyResolutionTimestamps(ctx context.Context, start, commit uint64) error {
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.verifiedLeader(ctx); err != nil {
		return err
	}
	floor := a.state.PendingPhaseDFloor()
	if a.state.PhaseDActive() {
		return errors.WithStack(ErrTSOPhaseDInactive)
	}
	if start == 0 || commit <= start || (floor != 0 && commit > floor) {
		return errors.Wrap(ErrTSOTimestampInvalid, "legacy resolution is outside pending phase-D floor")
	}
	return nil
}

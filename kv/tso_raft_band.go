package kv

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// TSOPhysicalBandReservation is an opt-in, leader-local allocation from a
// whole physical millisecond band whose end was committed by group 0.
// It is not a TSOReservation: its prior floor is not a shadow-migration floor.
type TSOPhysicalBandReservation struct {
	Base             uint64
	Count            int
	CommittedBandEnd uint64
}

type tsoPhysicalBand struct {
	term    uint64
	ceiling int64
	next    uint64
	end     uint64
}

// ReservePhysicalBandAfter serves consecutive logical slots from one committed
// physical band. No production caller uses this method until explicitly wired.
func (a *RaftTSOAllocator) ReservePhysicalBandAfter(
	ctx context.Context, n int, min uint64,
) (TSOPhysicalBandReservation, error) {
	var empty TSOPhysicalBandReservation
	if err := validateTSOMinimumWindow(min, n); err != nil {
		return empty, err
	}
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()

	engine, err := a.verifiedLeader(ctx)
	if err != nil {
		a.band = tsoPhysicalBand{}
		return empty, err
	}
	term := engine.Status().Term
	ceiling := a.clock.PhysicalCeiling()
	if a.band.term != term || a.band.ceiling != ceiling {
		a.band = tsoPhysicalBand{}
	}
	if term == 0 {
		return empty, errors.WithStack(ErrTSONotLeader)
	}
	if err := a.validatePhysicalBandRequest(ceiling, min); err != nil {
		a.band = tsoPhysicalBand{}
		return empty, err
	}
	if reservation, served, err := a.reserveCachedPhysicalBand(ctx, engine, term, ceiling, n, min); served || err != nil {
		return reservation, err
	}
	a.band = tsoPhysicalBand{}
	return a.commitPhysicalBandLocked(ctx, engine, term, ceiling, n, min)
}

func (a *RaftTSOAllocator) validatePhysicalBandRequest(ceiling int64, min uint64) error {
	if a.state.PhaseDActive() || a.state.PendingPhaseDFloor() != 0 {
		return errors.Wrap(ErrTSOTimestampInvalid, "physical bands are unavailable during phase D")
	}
	// Unlike ordinary HLC bootstrap, bands require a committed upper bound.
	if ceiling <= 0 || time.Now().UnixMilli() >= ceiling {
		return errors.WithStack(ErrCeilingExpired)
	}
	if min>>hlcLogicalBits > nonNegativeUint64(ceiling) {
		return errors.Wrapf(ErrTSOTimestampInvalid,
			"physical band minimum %d exceeds committed ceiling %d", min, ceiling)
	}
	return nil
}

func (a *RaftTSOAllocator) physicalBandStateValid(ceiling int64, end uint64) bool {
	return a.clock.PhysicalCeiling() == ceiling && time.Now().UnixMilli() < ceiling &&
		a.state.AllocationFloor() == end && a.clock.Current() <= end &&
		!a.state.PhaseDActive() && a.state.PendingPhaseDFloor() == 0
}

func (a *RaftTSOAllocator) reserveCachedPhysicalBand(
	ctx context.Context, engine raftengine.Engine, term uint64, ceiling int64, n int, min uint64,
) (TSOPhysicalBandReservation, bool, error) {
	var empty TSOPhysicalBandReservation
	if a.band.end == 0 || !a.physicalBandStateValid(ceiling, a.band.end) {
		return empty, false, nil
	}
	base := max(a.band.next, min+1)
	if base > a.band.end || positiveIntToUint64(n)-1 > a.band.end-base {
		return empty, false, nil
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		a.band = tsoPhysicalBand{}
		return empty, false, err
	}
	if !a.physicalBandStateValid(ceiling, a.band.end) {
		a.band = tsoPhysicalBand{}
		return empty, false, errors.WithStack(ErrCeilingExpired)
	}
	a.band.next = base + positiveIntToUint64(n)
	return TSOPhysicalBandReservation{Base: base, Count: n, CommittedBandEnd: a.band.end}, true, nil
}

func (a *RaftTSOAllocator) commitPhysicalBandLocked(
	ctx context.Context, engine raftengine.Engine, term uint64, ceiling int64, n int, min uint64,
) (TSOPhysicalBandReservation, error) {
	var empty TSOPhysicalBandReservation
	floor, err := a.termCommitFloor(ctx, term, false)
	if err != nil {
		return empty, err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return empty, err
	}
	base, end, err := a.selectPhysicalBand(floor, ceiling, n, min)
	if err != nil {
		return empty, err
	}
	// Record the entire attempted band locally even if the proposal outcome
	// is ambiguous. A later attempt must never reuse its logical slots.
	a.clock.Observe(end)
	if err := a.commitAllocationFloor(ctx, engine, end); err != nil {
		return empty, err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return empty, err
	}
	if !a.physicalBandStateValid(ceiling, end) {
		return empty, errors.WithStack(ErrCeilingExpired)
	}
	a.initializedTerm = term
	a.band = tsoPhysicalBand{term: term, ceiling: ceiling, next: base + positiveIntToUint64(n), end: end}
	return TSOPhysicalBandReservation{Base: base, Count: n, CommittedBandEnd: end}, nil
}

func (a *RaftTSOAllocator) selectPhysicalBand(floor uint64, ceiling int64, n int, min uint64) (uint64, uint64, error) {
	// A new or exhausted leader skips the band containing every committed or
	// locally observed timestamp.
	priorPhysical := max(floor, a.state.PhaseDFloor(), a.clock.Current()) >> hlcLogicalBits
	physical := max(nonNegativeUint64(time.Now().UnixMilli()), priorPhysical+1, (min+1)>>hlcLogicalBits)
	if physical > nonNegativeUint64(ceiling) || physical >= (1<<hlcPhysicalBits)-1 {
		return 0, 0, errors.WithStack(ErrCeilingExpired)
	}
	start := physical << hlcLogicalBits
	end := start | hlcLogicalMask
	base := max(start, min+1)
	if base > end || positiveIntToUint64(n)-1 > end-base {
		return 0, 0, errors.WithStack(ErrCeilingExpired)
	}
	return base, end, nil
}

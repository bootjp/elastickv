package kv

import "github.com/cockroachdb/errors"

func pendingPhaseDFloorThrough(allocator any) uint64 {
	state, ok := allocator.(TSOPendingPhaseDFloorSource)
	if !ok {
		return 0
	}
	return state.PendingPhaseDFloor()
}

func (a *RaftTSOAllocator) PendingPhaseDFloor() uint64 {
	if a == nil {
		return 0
	}
	return a.state.PendingPhaseDFloor()
}

func (a *LeaderRoutedTSOAllocator) PendingPhaseDFloor() uint64 {
	return pendingPhaseDFloorThrough(a.local)
}

func (b *BatchAllocator) PendingPhaseDFloor() uint64 {
	return pendingPhaseDFloorThrough(b.tso)
}

func (d *DynamicTimestampAllocator) PendingPhaseDFloor() uint64 {
	return pendingPhaseDFloorThrough(d.currentTimestampAllocator())
}

func (a *ShadowTimestampAllocator) PendingPhaseDFloor() uint64 {
	return pendingPhaseDFloorThrough(a.shadow)
}

func (b *BatchAllocator) rejectPendingPhaseD() error {
	if b.PendingPhaseDFloor() == 0 {
		return nil
	}
	b.Invalidate()
	return errors.WithStack(ErrTSOPhaseDActivationPending)
}

func (b *BatchAllocator) claimCurrentWindow(min uint64) (uint64, bool, error) {
	if b.PendingPhaseDFloor() != 0 {
		// Refill must still reach the underlying allocator: a routed D request
		// can retry the activation, whereas an ordinary reservation is refused.
		b.Invalidate()
		return 0, false, nil
	}
	phaseDAtStart, invalidated := b.ensurePhaseDTransition()
	if invalidated {
		return 0, false, nil
	}
	ts, ok := b.tryWindowAfter(min)
	if !ok {
		return 0, false, nil
	}
	if err := b.rejectPendingPhaseD(); err != nil {
		return 0, false, err
	}
	phaseDAfterClaim, invalidated := b.ensurePhaseDTransition()
	return ts, !invalidated && phaseDAfterClaim == phaseDAtStart, nil
}

package kv

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func newBandTSOFixture(t *testing.T) (*RaftTSOAllocator, *recordingTSOEngine, *HLC) {
	t.Helper()
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{
		state: raftengine.StateLeader, leader: raftengine.LeaderInfo{Address: "self"},
		apply: applyTSOTestFSM(fsm),
	}
	alloc, err := newTestRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock)
	require.NoError(t, err)
	return alloc, engine, clock
}

func TestTSOPhysicalBandCommitsFullEndAndServesDistinctSlots(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	ctx := context.Background()
	before := time.Now().UnixMilli()
	first, err := alloc.ReservePhysicalBandAfter(ctx, 3, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, int64(first.Base>>hlcLogicalBits), before)
	require.LessOrEqual(t, int64(first.Base>>hlcLogicalBits), clock.PhysicalCeiling())
	require.Equal(t, uint64(0), first.Base&hlcLogicalMask)
	require.Equal(t, (first.Base&^hlcLogicalMask)|hlcLogicalMask, first.CommittedBandEnd)
	require.Equal(t, first.CommittedBandEnd, alloc.AllocationFloor())
	payloads := engine.proposedPayloads()
	require.Len(t, payloads, 1)
	require.Equal(t, first.CommittedBandEnd,
		binary.BigEndian.Uint64(payloads[0][len(tsoAllocationFloorEnvelope):]))

	const workers = 32
	results := make(chan TSOPhysicalBandReservation, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservation, reserveErr := alloc.ReservePhysicalBandAfter(ctx, 2, 0)
			results <- reservation
			errs <- reserveErr
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for reserveErr := range errs {
		require.NoError(t, reserveErr)
	}
	seen := make(map[uint64]bool)
	for slot := first.Base; slot < first.Base+3; slot++ {
		seen[slot] = true
	}
	for reservation := range results {
		require.Equal(t, first.CommittedBandEnd, reservation.CommittedBandEnd)
		for slot := reservation.Base; slot < reservation.Base+2; slot++ {
			require.False(t, seen[slot])
			seen[slot] = true
		}
	}
	require.Len(t, seen, 3+workers*2)
	require.Len(t, engine.proposedPayloads(), 1)
}

func TestTSOPhysicalBandRollsOverBeforeCeilingRenewal(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	first, err := alloc.ReservePhysicalBandAfter(context.Background(), maxHLCBatchSize, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(0), first.Base&hlcLogicalMask)
	second, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, second.Base, first.CommittedBandEnd)
	require.Equal(t, first.CommittedBandEnd+1, second.Base)
	require.Less(t, int64(second.Base>>hlcLogicalBits), clock.PhysicalCeiling())
	require.Len(t, engine.proposedPayloads(), 2)
}

func TestTSOPhysicalBandDiscardsOnTermAndCeilingChange(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	first, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	engine.setTerm(2)
	second, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, second.Base, first.CommittedBandEnd)
	require.Len(t, engine.proposedPayloads(), 2)

	clock.SetPhysicalCeiling(clock.PhysicalCeiling() + 1000)
	third, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, third.Base, second.CommittedBandEnd)
	require.LessOrEqual(t, int64(third.Base>>hlcLogicalBits), clock.PhysicalCeiling())
	require.Len(t, engine.proposedPayloads(), 3)
}

func TestTSOPhysicalBandDiscardsCachedSlotsOnCeilingChange(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	first, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	clock.SetPhysicalCeiling(clock.PhysicalCeiling() + 1000)
	second, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, second.Base, first.CommittedBandEnd)
	require.LessOrEqual(t, int64(second.Base>>hlcLogicalBits), clock.PhysicalCeiling())
	require.Len(t, engine.proposedPayloads(), 2)
}

func TestTSOPhysicalBandAmbiguousProposalDiscardsBand(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	engine.setProposeError(errors.New("ambiguous proposal"))
	_, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.Error(t, err)
	attemptedEnd := clock.Current()
	require.Equal(t, hlcLogicalMask, attemptedEnd&hlcLogicalMask)
	engine.setProposeError(nil)
	reservation, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, reservation.Base, attemptedEnd)
	clock.SetPhysicalCeiling(clock.PhysicalCeiling() + 1000)
	reservation, err = alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, reservation.Base, attemptedEnd)
}

func TestTSOPhysicalBandRejectsTermChangeAfterCommit(t *testing.T) {
	alloc, engine, _ := newBandTSOFixture(t)
	engine.afterPropose = func([]byte) { engine.setTerm(2) }
	reservation, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.ErrorIs(t, err, ErrTSONotLeader)
	require.Zero(t, reservation.Base)
	require.Len(t, engine.proposedPayloads(), 1)
}

func TestTSOPhysicalBandRejectsCeilingChangeAfterCommit(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	engine.afterPropose = func([]byte) {
		clock.SetPhysicalCeiling(clock.PhysicalCeiling() + 1000)
	}
	reservation, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.ErrorIs(t, err, ErrCeilingExpired)
	require.Zero(t, reservation.Base)
	committedEnd := alloc.AllocationFloor()
	engine.afterPropose = nil
	next, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, next.Base, committedEnd)
}

func TestTSOPhysicalBandRejectsUntrustedMinimumWithoutPoisoningClock(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	minimum := (uint64(clock.PhysicalCeiling()+1) << hlcLogicalBits)
	_, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, minimum)
	require.ErrorIs(t, err, ErrTSOTimestampInvalid)
	require.Zero(t, clock.Current())
	require.Empty(t, engine.proposedPayloads())
	_, err = alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
}

func TestTSOPhysicalBandDoesNotChangeOrdinaryReservationContract(t *testing.T) {
	alloc, engine, clock := newBandTSOFixture(t)
	band, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	ordinary, err := alloc.ReserveBatchAfter(context.Background(), 1, 0, false, false)
	require.NoError(t, err)
	require.Greater(t, ordinary.Base, band.CommittedBandEnd)
	require.Equal(t, band.CommittedBandEnd, ordinary.PreviousAllocationFloor)
	_, err = alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.ErrorIs(t, err, ErrCeilingExpired)
	clock.SetPhysicalCeiling(clock.PhysicalCeiling() + 1000)
	next, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.NoError(t, err)
	require.Greater(t, next.Base, ordinary.Base)
	require.Len(t, engine.proposedPayloads(), 3)
}

func TestTSOPhysicalBandRejectsActivePhaseD(t *testing.T) {
	alloc, engine, _ := newBandTSOFixture(t)
	state := alloc.state
	require.Nil(t, state.Apply(marshalTSOCutover()))
	require.Nil(t, state.Apply(marshalTSOPhaseD(1)))
	_, err := alloc.ReservePhysicalBandAfter(context.Background(), 1, 0)
	require.ErrorIs(t, err, ErrTSOTimestampInvalid)
	require.Empty(t, engine.proposedPayloads())
}

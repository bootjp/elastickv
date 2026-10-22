package kv

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

type testPhaseDGate struct {
	checks     int
	checkErr   error
	sealErr    error
	prepareErr error
	finishErr  error
	floors     []uint64
	finished   []uint64
	check      func()
	seal       func()
	prepare    func(uint64)
	finish     func(uint64)
}

func (g *testPhaseDGate) CheckPhaseDProtocol(context.Context) error {
	g.checks++
	if g.check != nil {
		g.check()
	}
	return g.checkErr
}

func (g *testPhaseDGate) PreparePhaseD(_ context.Context, floor uint64) error {
	g.floors = append(g.floors, floor)
	if g.prepare != nil {
		g.prepare(floor)
	}
	return g.prepareErr
}

func (g *testPhaseDGate) SealPhaseDCreation(context.Context) error {
	if g.seal != nil {
		g.seal()
	}
	return g.sealErr
}

func (g *testPhaseDGate) FinishPhaseD(_ context.Context, floor uint64) error {
	g.finished = append(g.finished, floor)
	if g.finish != nil {
		g.finish(floor)
	}
	return g.finishErr
}

func pendingTestAllocator(t *testing.T, gate TSOPhaseDActivationGate) (*RaftTSOAllocator, *TSOStateMachine, *recordingTSOEngine, *recordingTSOFloorProvider) {
	t.Helper()
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 1,
		leader: raftengine.LeaderInfo{Address: "self"}, apply: applyTSOTestFSM(fsm)}
	provider := &recordingTSOFloorProvider{}
	a, err := NewRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock,
		WithTSOCutoverFloorProvider(provider), WithTSOPhaseDActivationGate(gate))
	require.NoError(t, err)
	return a, fsm, engine, provider
}

func TestTSOPendingRequiresGateAndCapabilityBeforeMarker(t *testing.T) {
	a, fsm, engine, _ := pendingTestAllocator(t, nil)
	_, err := a.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationGateRequired)
	require.Empty(t, engine.proposedPayloads())
	gate := &testPhaseDGate{checkErr: ErrTSOProtocolUnsupported}
	a.phaseDGate = gate
	_, err = a.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.ErrorIs(t, err, ErrTSOProtocolUnsupported)
	require.Equal(t, [][]byte{marshalTSOCutover()}, engine.proposedPayloads())
	require.Zero(t, fsm.PendingPhaseDFloor())
}

func TestTSOPendingPreBarrierDrainAllowsEvidenceScopedResolutionAndFreshFloor(t *testing.T) {
	gate := &testPhaseDGate{checkErr: ErrTSOPhaseDActivationPending}
	a, fsm, engine, provider := pendingTestAllocator(t, gate)
	_, err := a.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.Equal(t, [][]byte{marshalTSOCutover()}, engine.proposedPayloads())
	require.Equal(t, 0, provider.callCount())
	require.NoError(t, a.ValidateLegacyResolutionTimestamps(context.Background(), 50, 1_000_000))
	require.ErrorIs(t, a.ValidateDurableTimestamp(context.Background(), 1_000_000), ErrTSOPhaseDInactive)
	provider.setFloor(2_000_000)
	gate.checkErr = nil
	result, err := a.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.NoError(t, err)
	require.Greater(t, result.PhaseDFloor, uint64(2_000_000))
	require.Equal(t, 1, provider.callCount())
	require.True(t, fsm.PhaseDActive())
}

func TestTSOPendingFreezesFloorAndAllowsOnlyScopedResolution(t *testing.T) {
	gate := &testPhaseDGate{prepareErr: ErrTSOPhaseDActivationPending}
	a, fsm, engine, provider := pendingTestAllocator(t, gate)
	gate.check = func() { require.Zero(t, fsm.PendingPhaseDFloor()) }
	gate.prepare = func(b uint64) {
		require.Equal(t, b, fsm.PendingPhaseDFloor())
		require.False(t, fsm.PhaseDActive())
		require.Zero(t, fsm.AllocationFloor())
	}
	_, err := a.ReserveBatchAfter(context.Background(), 3, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	b := fsm.PendingPhaseDFloor()
	require.NotZero(t, b)
	require.Len(t, engine.proposedPayloads(), 2)
	_, err = a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.NoError(t, a.ValidateLegacyResolutionTimestamps(context.Background(), 1, b))
	require.ErrorIs(t, a.ValidateDurableTimestamp(context.Background(), b), ErrTSOPhaseDInactive)
	for _, pair := range [][2]uint64{{0, b}, {b, b}, {1, b + 1}} {
		require.ErrorIs(t, a.ValidateLegacyResolutionTimestamps(context.Background(), pair[0], pair[1]), ErrTSOTimestampInvalid)
	}
	gate.check = nil
	gate.prepareErr = nil
	engine.setTerm(2)
	provider.setFloor(b + 1_000_000)
	reservation, err := a.ReserveBatchAfter(context.Background(), 3, 0, true, true)
	require.NoError(t, err)
	require.Equal(t, b, reservation.PhaseDFloor)
	require.Equal(t, b+1, reservation.Base)
	require.Equal(t, []uint64{b, b}, gate.floors)
	require.Equal(t, 1, provider.callCount(), "retry must not move the barrier")
	require.Equal(t, b+3, fsm.AllocationFloor())
	require.ErrorIs(t, a.ValidateLegacyResolutionTimestamps(context.Background(), 1, b), ErrTSOPhaseDInactive)
	require.ErrorIs(t, a.ValidateDurableTimestamp(context.Background(), b), ErrTSOTimestampPrePhaseD)
	require.NoError(t, a.ValidateDurableTimestamp(context.Background(), b+1))
	payloads := engine.proposedPayloads()
	require.Equal(t, marshalTSOPhaseD(b), payloads[2])
	require.Equal(t, marshalTSOAllocationFloor(b+3), payloads[3])
}

func TestTSOPendingSnapshotAndLeaderReplacementKeepBarrier(t *testing.T) {
	gate := &testPhaseDGate{prepareErr: ErrTSOPhaseDActivationPending}
	a, fsm, _, _ := pendingTestAllocator(t, gate)
	_, err := a.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	b := fsm.PendingPhaseDFloor()
	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), tsoSnapshotV5Len)
	restored := NewTSOStateMachine(NewHLC())
	require.NoError(t, restored.Restore(bytes.NewReader(buf.Bytes())))
	require.Equal(t, b, restored.PendingPhaseDFloor())
	engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 2,
		leader: raftengine.LeaderInfo{Address: "new"}, apply: applyTSOTestFSM(restored)}
	gate.prepareErr = nil
	newAlloc, err := NewRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: restored}, restored.hlc,
		WithTSOCutoverFloorProvider(&recordingTSOFloorProvider{floor: b + 100}),
		WithTSOPhaseDActivationGate(gate))
	require.NoError(t, err)
	_, err = newAlloc.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	result, err := newAlloc.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.NoError(t, err)
	require.Equal(t, b, result.PhaseDFloor)
	require.Equal(t, b+1, result.Base)
	activeSnap, err := restored.Snapshot()
	require.NoError(t, err)
	buf.Reset()
	_, err = activeSnap.WriteTo(&buf)
	require.NoError(t, err)
	after := NewTSOStateMachine(NewHLC())
	require.NoError(t, after.Restore(bytes.NewReader(buf.Bytes())))
	require.True(t, after.PhaseDActive())
	require.Zero(t, after.PendingPhaseDFloor())
	require.Nil(t, after.Apply(marshalTSOPendingPhaseD(b)))
}

func TestTSOPendingRejectsDifferentFloorAndAllocationBeforeActivation(t *testing.T) {
	fsm := NewTSOStateMachine(NewHLC())
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPendingPhaseD(100)))
	for _, payload := range [][]byte{marshalTSOPendingPhaseD(101), marshalTSOPhaseD(101), marshalTSOAllocationFloor(101)} {
		require.Error(t, haltApplyOf(fsm.Apply(payload)))
	}
	require.True(t, fsm.IsVolatileOnlyPayload(marshalTSOPendingPhaseD(100)))
	require.False(t, fsm.PhaseDActive())
}

func TestTSOPendingLeaderChangeDuringGateDoesNotActivate(t *testing.T) {
	gate := &testPhaseDGate{}
	a, fsm, engine, _ := pendingTestAllocator(t, gate)
	gate.prepare = func(uint64) { engine.setTerm(2) }
	_, err := a.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.ErrorIs(t, err, ErrTSONotLeader)
	require.False(t, fsm.PhaseDActive())
	require.NotZero(t, fsm.PendingPhaseDFloor())
}

func TestTSOPendingLegacyValidationThroughDecorators(t *testing.T) {
	gate := &testPhaseDGate{prepareErr: ErrTSOPhaseDActivationPending}
	a, fsm, _, _ := pendingTestAllocator(t, gate)
	_, err := a.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	routed, err := NewLeaderRoutedTSOAllocator(a, engineForGroup(a.group))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, routed.Close()) })
	shadow := &ShadowTimestampAllocator{shadow: routed}
	dynamic := NewDynamicTimestampAllocator(shadow)
	batch, err := NewBatchAllocator(routed, 2)
	require.NoError(t, err)
	require.NoError(t, dynamic.ValidateLegacyResolutionTimestamps(context.Background(), 1, fsm.PendingPhaseDFloor()))
	require.NoError(t, batch.ValidateLegacyResolutionTimestamps(context.Background(), 1, fsm.PendingPhaseDFloor()))
	require.ErrorIs(t, batch.ValidateDurableTimestamp(context.Background(), 2), ErrTSOPhaseDInactive)
}

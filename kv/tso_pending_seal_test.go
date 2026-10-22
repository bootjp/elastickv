package kv

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func TestTSOPendingSealPrecedesFreshFloorAndFinishPrecedesAllocation(t *testing.T) {
	gate := &testPhaseDGate{}
	a, fsm, engine, provider := pendingTestAllocator(t, gate)
	var steps []string
	gate.check = func() {
		require.True(t, fsm.CutoverActive())
		steps = append(steps, "check")
	}
	gate.seal = func() {
		require.Zero(t, provider.callCount())
		provider.setFloor(200)
		steps = append(steps, "seal")
	}
	provider.afterRead = func() { steps = append(steps, "floor") }
	gate.prepare = func(floor uint64) {
		require.Greater(t, floor, uint64(200))
		require.Equal(t, floor, fsm.PendingPhaseDFloor())
		steps = append(steps, "prepare")
	}
	gate.finish = func(floor uint64) {
		require.True(t, fsm.PhaseDActive())
		require.Equal(t, floor, fsm.PrepareEvidenceProtocolFloor())
		require.Zero(t, fsm.AllocationFloor())
		steps = append(steps, "finish")
	}
	result, err := a.ReserveBatchAfter(context.Background(), 2, 0, false, true)
	require.NoError(t, err)
	require.Equal(t, []string{"check", "seal", "floor", "prepare", "finish"}, steps)
	require.Equal(t, result.Base+1, fsm.AllocationFloor())
	require.Len(t, engine.proposedPayloads(), 4)
}

func TestTSOPendingSealFailureResumesAfterAllocatorRestart(t *testing.T) {
	gate := &testPhaseDGate{sealErr: ErrTSOPhaseDActivationPending}
	a, fsm, engine, provider := pendingTestAllocator(t, gate)
	_, err := a.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.Zero(t, provider.callCount())
	require.Zero(t, fsm.PrepareEvidenceProtocolFloor())
	require.Equal(t, [][]byte{marshalTSOCutover()}, engine.proposedPayloads())
	gate.sealErr = nil
	engine.setTerm(2)
	restarted, err := NewRaftTSOAllocator(a.group, a.clock,
		WithTSOCutoverFloorProvider(provider), WithTSOPhaseDActivationGate(gate))
	require.NoError(t, err)
	_, err = restarted.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.NoError(t, err)
	require.Equal(t, 1, provider.callCount())
	require.True(t, fsm.PhaseDActive())
}

func TestTSOPendingFinishFailureBlocksNewWindowsUntilRetry(t *testing.T) {
	gate := &testPhaseDGate{finishErr: ErrTSOPhaseDActivationPending}
	a, fsm, engine, _ := pendingTestAllocator(t, gate)
	_, err := a.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.True(t, fsm.PhaseDActive())
	floor := fsm.PrepareEvidenceProtocolFloor()
	require.NotZero(t, floor)
	require.Zero(t, fsm.AllocationFloor())
	_, err = a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.Zero(t, fsm.AllocationFloor())
	gate.finishErr = nil
	ts, err := a.Next(context.Background())
	require.NoError(t, err)
	require.Greater(t, ts, floor)
	require.Equal(t, []uint64{floor, floor, floor}, gate.finished)
	_, err = a.Next(context.Background())
	require.NoError(t, err)
	require.Len(t, gate.finished, 3, "finish is cached only for this leader term")
	engine.setTerm(2)
	gate.finishErr = ErrTSOPhaseDActivationPending
	previous := fsm.AllocationFloor()
	_, err = a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	require.Equal(t, previous, fsm.AllocationFloor())
	gate.finishErr = nil
	_, err = a.Next(context.Background())
	require.NoError(t, err)
	require.Len(t, gate.finished, 5)
}

func TestTSOPendingLeaderChangeDuringFinishDoesNotReturnWindow(t *testing.T) {
	gate := &testPhaseDGate{}
	a, fsm, engine, _ := pendingTestAllocator(t, gate)
	gate.finish = func(uint64) { engine.setTerm(2) }
	_, err := a.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.ErrorIs(t, err, ErrTSONotLeader)
	require.True(t, fsm.PhaseDActive())
	require.Zero(t, fsm.AllocationFloor())
	gate.finish = nil
	_, err = a.Next(context.Background())
	require.NoError(t, err)
	require.Len(t, gate.finished, 2)
}

func TestTSOPendingHistoricalActiveSnapshotBootstrapsProtocol(t *testing.T) {
	gate := &testPhaseDGate{checkErr: ErrTSOPhaseDActivationPending}
	a, fsm, engine, _ := pendingTestAllocator(t, gate)
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(100)))
	require.Nil(t, fsm.Apply(marshalTSOAllocationFloor(120)))
	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), tsoSnapshotV4Len)
	restored := NewTSOStateMachine(a.clock)
	require.NoError(t, restored.Restore(bytes.NewReader(buf.Bytes())))
	engine.apply = applyTSOTestFSM(restored)
	a.state = restored
	a.group.TSOState = restored
	ts, err := a.Next(context.Background())
	require.NoError(t, err, "historical M7 must remain available while old locks drain")
	require.Greater(t, ts, uint64(120))
	require.Zero(t, restored.PrepareEvidenceProtocolFloor())
	gate.checkErr = nil
	a.phaseDBootstrapRetryAt = time.Time{}
	_, err = a.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(100), restored.PrepareEvidenceProtocolFloor())
	require.Zero(t, restored.PendingPhaseDFloor())
	require.Equal(t, []uint64{100}, gate.floors)
	require.Equal(t, []uint64{100}, gate.finished)
	require.Equal(t, marshalTSOPendingPhaseD(100), engine.proposedPayloads()[1])
}

func TestTSOPendingActiveWithoutGateFailsClosed(t *testing.T) {
	a, fsm, engine, _ := pendingTestAllocator(t, nil)
	require.Nil(t, fsm.Apply(marshalTSOCutover()))
	require.Nil(t, fsm.Apply(marshalTSOPhaseD(100)))
	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationGateRequired)
	require.Empty(t, engine.proposedPayloads())
}

func TestTSOPendingActiveSnapshotRetriesDataGatesAfterRestart(t *testing.T) {
	gate := &testPhaseDGate{finishErr: ErrTSOPhaseDActivationPending}
	a, fsm, _, provider := pendingTestAllocator(t, gate)
	_, err := a.ReserveBatchAfter(context.Background(), 2, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	floor := fsm.PrepareEvidenceProtocolFloor()
	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.Len(t, buf.Bytes(), tsoSnapshotV5Len)
	restored := NewTSOStateMachine(NewHLC())
	require.NoError(t, restored.Restore(bytes.NewReader(buf.Bytes())))
	engine := &recordingTSOEngine{state: raftengine.StateLeader, term: 2,
		leader: raftengine.LeaderInfo{Address: "new"}, apply: applyTSOTestFSM(restored)}
	gate.checkErr = ErrTSOProtocolUnsupported
	gate.sealErr = ErrTSOProtocolUnsupported
	gate.finishErr = nil
	restarted, err := NewRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: restored}, restored.hlc,
		WithTSOCutoverFloorProvider(provider), WithTSOPhaseDActivationGate(gate))
	require.NoError(t, err)
	_, err = restarted.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOProtocolUnsupported, "durable pending marker forbids historical fallback")
	require.Zero(t, restored.AllocationFloor())
	gate.checkErr = nil
	gate.sealErr = nil
	ts, err := restarted.Next(context.Background())
	require.NoError(t, err)
	require.Greater(t, ts, floor)
	require.Equal(t, []uint64{floor, floor}, gate.finished)
	require.Equal(t, []uint64{floor, floor}, gate.floors, "retry must reestablish data gates without moving the floor")
}

func TestTSOPendingLeaderChangeAfterSealResamplesBeforeFreezing(t *testing.T) {
	gate := &testPhaseDGate{}
	a, fsm, engine, provider := pendingTestAllocator(t, gate)
	provider.afterRead = func() { engine.setTerm(2) }
	_, err := a.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.ErrorIs(t, err, ErrTSONotLeader)
	require.Zero(t, fsm.PrepareEvidenceProtocolFloor())
	provider.afterRead = nil
	provider.setFloor(1_000)
	result, err := a.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.NoError(t, err)
	require.Greater(t, result.PhaseDFloor, uint64(1_000))
	require.Equal(t, 2, provider.callCount())
}

func TestTSOPendingCachedWindowsStopButRoutedActivationCanResume(t *testing.T) {
	gate := &testPhaseDGate{prepareErr: ErrTSOPhaseDActivationPending}
	a, fsm, engine, _ := pendingTestAllocator(t, gate)
	routed, err := NewLeaderRoutedTSOAllocator(a, engine)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, routed.Close()) })
	batch, err := NewBatchAllocator(routed, 8)
	require.NoError(t, err)
	_, err = batch.Next(context.Background())
	require.NoError(t, err)
	_, err = a.ReserveBatchAfter(context.Background(), 1, 0, true, true)
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	shadow := &ShadowTimestampAllocator{shadow: routed}
	dynamic := NewDynamicTimestampAllocator(batch)
	require.Equal(t, fsm.PendingPhaseDFloor(), shadow.PendingPhaseDFloor())
	require.Equal(t, fsm.PendingPhaseDFloor(), dynamic.PendingPhaseDFloor())
	_, err = batch.Next(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	_, err = batch.FreshAdmissionTimestamp(context.Background())
	require.ErrorIs(t, err, ErrTSOPhaseDActivationPending)
	routed.setActivation(true, true)
	gate.prepareErr = nil
	ts, err := batch.Next(context.Background())
	require.NoError(t, err)
	require.Greater(t, ts, fsm.PhaseDFloor())
	require.True(t, fsm.PhaseDActive())
	require.Equal(t, raftengine.StateLeader, engine.Status().State)
}

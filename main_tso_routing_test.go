package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestConfigureCoordinatorTSORejectsConflictingModes(t *testing.T) {
	setTSOModeFlags(t, true, true)
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(coord, nil)
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestConfigureCoordinatorTSOShadowRequiresDedicatedGroup(t *testing.T) {
	setTSOModeFlags(t, false, true)
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(coord, nil)
	require.ErrorIs(t, err, kv.ErrTSOGroupRequired)
}

func TestConfigureCoordinatorTSOCutoverRoutesThroughDedicatedGroup(t *testing.T) {
	setTSOModeFlags(t, true, false)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.serverAllocator)
	require.NotNil(t, wiring.routedAllocator)
	require.True(t, coord.IsTimestampLeader())

	ts, err := kv.NextTimestampThrough(context.Background(), coord, "test dedicated tso")
	require.NoError(t, err)
	require.NotZero(t, ts)
	require.Equal(t, uint64(2), engine.proposals.Load(), "cutover marker must commit before the first window")
	require.True(t, fsm.CutoverActive())
}

func TestConfigureCoordinatorTSOPhaseDWorksThroughAdapterDecorators(t *testing.T) {
	setTSOModeFlags(t, true, false)
	*tsoPhaseDEnabled = true
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	decorated := kv.WithKeyVizLabel(startupGatedCoordinator{inner: coord}, keyviz.LabelRedis)

	readTS, err := kv.BeginReadTimestampThrough(context.Background(), decorated, 42, "test decorated phase D")
	require.NoError(t, err)
	require.NotZero(t, readTS.Timestamp())
	require.True(t, fsm.CutoverActive())
	require.True(t, fsm.PhaseDActive())
	require.Equal(t, uint64(3), engine.proposals.Load(),
		"cutover and phase-D markers must commit before the timestamp window")
}

func TestConfigureCoordinatorTSOPhaseDRequiresCutoverMode(t *testing.T) {
	setTSOModeFlags(t, false, false)
	*tsoPhaseDEnabled = true
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(coord, nil)
	require.ErrorContains(t, err, "requires --tsoEnabled")
}

func TestConfigureCoordinatorTSOShadowReturnsLegacyTimestamp(t *testing.T) {
	setTSOModeFlags(t, false, true)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	legacyTS, err := kv.NextTimestampThrough(context.Background(), coord, "test shadow tso")
	require.NoError(t, err)
	require.NotZero(t, legacyTS)
	require.Greater(t, clock.Current(), legacyTS)
	require.Equal(t, uint64(1), engine.proposals.Load())
}

func TestConfigureCoordinatorTSOExposesDedicatedServerWithoutCutover(t *testing.T) {
	setTSOModeFlags(t, false, false)
	clock := kv.NewHLC()
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	require.NotNil(t, wiring.serverAllocator)
	require.Nil(t, wiring.routedAllocator)
	require.False(t, coord.IsTimestampLeader(), "timestamp leadership stays on the compatibility bridge until a mode is enabled")
}

func TestConfigureCoordinatorTSORestoresDurableCutoverWithoutFlags(t *testing.T) {
	setTSOModeFlags(t, false, false)
	clock, fsm, engine, groups := newActiveMainTSOCutover(t)
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.serverAllocator)
	require.NotNil(t, wiring.routedAllocator)
	require.True(t, coord.IsTimestampLeader())
	require.True(t, fsm.CutoverActive())

	ts, err := kv.NextTimestampThrough(context.Background(), coord, "test restored tso cutover")
	require.NoError(t, err)
	require.NotZero(t, ts)
	require.Equal(t, uint64(1), engine.proposals.Load(), "restored cutover must only commit the allocation floor")
}

func TestConfigureCoordinatorTSODurableCutoverOverridesShadowFlag(t *testing.T) {
	setTSOModeFlags(t, false, true)
	clock, fsm, engine, groups := newActiveMainTSOCutover(t)
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.routedAllocator)
	require.Equal(t, kv.TSOModeCutover, wiring.runtimeController.CurrentMode(),
		"durable cutover cannot return to legacy shadow issuance")
	require.True(t, coord.IsTimestampLeader())
	require.True(t, fsm.CutoverActive())

	ts, err := kv.NextTimestampThrough(context.Background(), coord, "test shadow flag after cutover")
	require.NoError(t, err)
	require.NotZero(t, ts)
	require.Equal(t, uint64(1), engine.proposals.Load(), "restored cutover must only commit the allocation floor")
}

func TestInternalTimestampOptionsPreservesTSOThroughStartupGate(t *testing.T) {
	t.Parallel()
	allocator := &mainTimestampAllocator{next: 123}
	coord := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(allocator)
	gated := startupGatedCoordinator{inner: coord, gate: &startupPublicKVGate{}}

	got, ok := kv.TimestampAllocatorThrough(gated)
	require.True(t, ok)
	require.Same(t, allocator, got)
	require.Len(t, internalTimestampOptions(gated), 1)

	runtimeAllocator := kv.NewDynamicTimestampAllocator(nil)
	runtimeCoord := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(runtimeAllocator)
	runtimeGated := startupGatedCoordinator{inner: runtimeCoord, gate: &startupPublicKVGate{}}
	got, ok = kv.TimestampAllocatorThrough(runtimeGated)
	require.False(t, ok)
	require.Nil(t, got)
	configured, ok := kv.ConfiguredTimestampAllocatorThrough(runtimeGated)
	require.True(t, ok)
	require.Same(t, runtimeAllocator, configured)
	require.Len(t, internalTimestampOptions(runtimeGated), 1)

	legacy := startupGatedCoordinator{inner: newMainTSOCoordinator(kv.NewHLC(), nil)}
	require.Empty(t, internalTimestampOptions(legacy))
}

func TestConfigureCoordinatorTSOModeFileStartsRuntimeController(t *testing.T) {
	setTSOModeFlags(t, false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("shadow\n"), 0o600))
	*tsoModeFile = path
	*tsoModeReloadInterval = time.Millisecond
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.runtimeController)
	require.Equal(t, kv.TSOModeShadow, wiring.runtimeController.CurrentMode())
	_, configured := kv.TimestampAllocatorThrough(coord)
	require.True(t, configured)
}

func TestConfigureCoordinatorTSOModeFileRequiresDedicatedGroup(t *testing.T) {
	setTSOModeFlags(t, false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	*tsoModeFile = path
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(coord, nil)
	require.ErrorIs(t, err, kv.ErrTSOGroupRequired)
}

func newActiveMainTSOCutover(t *testing.T) (*kv.HLC, *kv.TSOStateMachine, *mainTSOEngine, map[uint64]*kv.ShardGroup) {
	t.Helper()
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	group := &kv.ShardGroup{Engine: engine, TSOState: fsm}
	allocator, err := kv.NewRaftTSOAllocator(group, clock, kv.WithTSOCutoverFloorProvider(mainTSOFloorProvider{}))
	require.NoError(t, err)
	_, err = allocator.ReserveBatchAfter(context.Background(), 1, 0, true, false)
	require.NoError(t, err)
	require.True(t, fsm.CutoverActive())
	engine.proposals.Store(0)
	return clock, fsm, engine, map[uint64]*kv.ShardGroup{dedicatedTSORaftGroupID: group}
}

func setTSOModeFlags(t *testing.T, enabled, shadow bool) {
	t.Helper()
	oldEnabled := *tsoEnabled
	oldShadow := *tsoShadowEnabled
	oldPhaseD := *tsoPhaseDEnabled
	oldBatchSize := *tsoBatchSize
	oldModeFile := *tsoModeFile
	oldReloadInterval := *tsoModeReloadInterval
	*tsoEnabled = enabled
	*tsoShadowEnabled = shadow
	*tsoPhaseDEnabled = false
	*tsoBatchSize = 8
	*tsoModeFile = ""
	*tsoModeReloadInterval = defaultTSOReload
	t.Cleanup(func() {
		*tsoEnabled = oldEnabled
		*tsoShadowEnabled = oldShadow
		*tsoPhaseDEnabled = oldPhaseD
		*tsoBatchSize = oldBatchSize
		*tsoModeFile = oldModeFile
		*tsoModeReloadInterval = oldReloadInterval
	})
}

func newMainTSOCoordinator(clock *kv.HLC, groups map[uint64]*kv.ShardGroup) *kv.ShardedCoordinator {
	return kv.NewShardedCoordinator(distribution.NewEngine(), groups, 1, clock, nil)
}

type mainTSOEngine struct {
	state     raftengine.State
	proposals atomic.Uint64
	tsoState  *kv.TSOStateMachine
}

type mainTSOFloorProvider struct{}

type mainTimestampAllocator struct {
	next uint64
}

func (a *mainTimestampAllocator) Next(context.Context) (uint64, error) {
	return a.next, nil
}

func (mainTSOFloorProvider) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return 0, nil
}

func (e *mainTSOEngine) Propose(_ context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	e.proposals.Add(1)
	if e.tsoState != nil {
		if result := e.tsoState.Apply(payload); result != nil {
			if err, ok := result.(error); ok {
				return nil, err
			}
			return nil, fmt.Errorf("unexpected TSO apply result %T", result)
		}
	}
	return &raftengine.ProposalResult{}, nil
}

func (e *mainTSOEngine) ProposeAdmin(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	return e.Propose(ctx, payload)
}

func (e *mainTSOEngine) State() raftengine.State { return e.state }

func (e *mainTSOEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "self", Address: "127.0.0.1:50051"}
}

func (e *mainTSOEngine) VerifyLeader(context.Context) error {
	if e.state != raftengine.StateLeader {
		return raftengine.ErrNotLeader
	}
	return nil
}

func (e *mainTSOEngine) LinearizableRead(context.Context) (uint64, error) { return 0, nil }

func (e *mainTSOEngine) Status() raftengine.Status {
	return raftengine.Status{State: e.state, Term: 1}
}

func (e *mainTSOEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (e *mainTSOEngine) Close() error { return nil }

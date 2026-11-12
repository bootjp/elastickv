package tsowiring

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
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestConfigureCoordinatorTSORejectsConflictingModes(t *testing.T) {
	cfg := testTSOFlags(true, true)
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(cfg, coord, nil)
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestConfigureCoordinatorTSOShadowRequiresDedicatedGroup(t *testing.T) {
	cfg := testTSOFlags(false, true)
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(cfg, coord, nil)
	require.ErrorIs(t, err, kv.ErrTSOGroupRequired)
}

func TestConfigureCoordinatorTSOCutoverRoutesThroughDedicatedGroup(t *testing.T) {
	cfg := testTSOFlags(true, false)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.ServerAllocator)
	require.NotNil(t, wiring.RoutedAllocator)
	require.True(t, coord.IsTimestampLeader())

	ts, err := kv.NextTimestampThrough(context.Background(), coord, "test dedicated tso")
	require.NoError(t, err)
	require.NotZero(t, ts)
	require.Equal(t, uint64(2), engine.proposals.Load(), "cutover marker must commit before the first window")
	require.True(t, fsm.CutoverActive())
}

func TestConfigureCoordinatorTSOPhaseDRequiresCutoverMode(t *testing.T) {
	cfg := testTSOFlags(false, false)
	cfg.PhaseDEnabled = true
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(cfg, coord, nil)
	require.ErrorContains(t, err, "requires --tsoEnabled")
}

func TestConfigureCoordinatorTSOShadowReturnsLegacyTimestamp(t *testing.T) {
	cfg := testTSOFlags(false, true)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	legacyTS, err := kv.NextTimestampThrough(context.Background(), coord, "test shadow tso")
	require.NoError(t, err)
	require.NotZero(t, legacyTS)
	require.Greater(t, clock.Current(), legacyTS)
	require.Equal(t, uint64(1), engine.proposals.Load())
}

func TestConfigureCoordinatorTSOExposesDedicatedServerWithoutCutover(t *testing.T) {
	cfg := testTSOFlags(false, false)
	clock := kv.NewHLC()
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	require.NotNil(t, wiring.ServerAllocator)
	require.Nil(t, wiring.RoutedAllocator)
	require.False(t, coord.IsTimestampLeader(), "timestamp leadership stays on the compatibility bridge until a mode is enabled")
}

func TestConfigureCoordinatorTSORestoresDurableCutoverWithoutFlags(t *testing.T) {
	cfg := testTSOFlags(false, false)
	clock, fsm, engine, groups := newActiveMainTSOCutover(t)
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.ServerAllocator)
	require.NotNil(t, wiring.RoutedAllocator)
	require.True(t, coord.IsTimestampLeader())
	require.True(t, fsm.CutoverActive())

	ts, err := kv.NextTimestampThrough(context.Background(), coord, "test restored tso cutover")
	require.NoError(t, err)
	require.NotZero(t, ts)
	require.Equal(t, uint64(1), engine.proposals.Load(), "restored cutover must only commit the allocation floor")
}

func TestConfigureCoordinatorTSODurableCutoverOverridesShadowFlag(t *testing.T) {
	cfg := testTSOFlags(false, true)
	clock, fsm, engine, groups := newActiveMainTSOCutover(t)
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.RoutedAllocator)
	require.Equal(t, kv.TSOModeCutover, wiring.RuntimeController.CurrentMode(),
		"durable cutover cannot return to legacy shadow issuance")
	require.True(t, coord.IsTimestampLeader())
	require.True(t, fsm.CutoverActive())

	ts, err := kv.NextTimestampThrough(context.Background(), coord, "test shadow flag after cutover")
	require.NoError(t, err)
	require.NotZero(t, ts)
	require.Equal(t, uint64(1), engine.proposals.Load(), "restored cutover must only commit the allocation floor")
}

func TestConfigureCoordinatorTSOModeFileStartsRuntimeController(t *testing.T) {
	cfg := testTSOFlags(false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("shadow\n"), 0o600))
	cfg.ModeFile = path
	cfg.ModeReloadInterval = time.Millisecond
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.RuntimeController)
	require.Equal(t, kv.TSOModeShadow, wiring.RuntimeController.CurrentMode())
	_, configured := kv.TimestampAllocatorThrough(coord)
	require.True(t, configured)
}

func TestConfigureCoordinatorTSOModeFileRequiresDedicatedGroup(t *testing.T) {
	cfg := testTSOFlags(false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	cfg.ModeFile = path
	coord := newMainTSOCoordinator(kv.NewHLC(), nil)

	_, err := configureCoordinatorTSO(cfg, coord, nil)
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
	return clock, fsm, engine, map[uint64]*kv.ShardGroup{dedicatedTSOGroupID: group}
}

// testTSOFlags returns the registered TSO flag defaults with the given
// legacy stage and a small batch size.
func testTSOFlags(enabled, shadow bool) Flags {
	f := defaultTSOFlags()
	f.Enabled = enabled
	f.ShadowEnabled = shadow
	f.BatchSize = 8
	return f
}

func newMainTSOCoordinator(clock *kv.HLC, groups map[uint64]*kv.ShardGroup) *kv.ShardedCoordinator {
	return kv.NewShardedCoordinator(distribution.NewEngine(), groups, 1, clock, nil)
}

type mainTSOEngine struct {
	state     raftengine.State
	proposals atomic.Uint64
	tsoState  *kv.TSOStateMachine
}

type mainTSOFloorProvider struct {
	floor uint64
}

func (p mainTSOFloorProvider) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return p.floor, nil
}

// These fixtures have no data groups; the tests exercise allocator/decorator
// wiring rather than the production data-group activation gate.
func (mainTSOFloorProvider) CheckPhaseDProtocol(context.Context) error { return nil }

func (mainTSOFloorProvider) SealPhaseDCreation(context.Context) error { return nil }

func (mainTSOFloorProvider) PreparePhaseD(context.Context, uint64) error { return nil }

func (mainTSOFloorProvider) FinishPhaseD(context.Context, uint64) error { return nil }

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

func (e *mainTSOEngine) SnapshotEvery() uint64 { return 0 }

func (e *mainTSOEngine) Close() error { return nil }

// Legacy warm-up must not narrow timestamp leadership, lease renewal, or lease
// recovery to group 0. In legacy mode persistence timestamps still come from
// the data groups' local HLC path, so pinning the timestamp group stops
// renewing those groups: a group-0 quorum loss then expires their ceilings and
// legacy writes fail with ErrCeilingExpired while the data groups are healthy.
// That contradicts the no-path-change warm-up contract.
//
// timestampLeaseRenewalGroupIDs is unexported; IsTimestampLeader is driven by
// the same timestampGroupConfigured flag and is the observable proxy. Group 0
// is a follower here and the data group is the leader, so the pinned and
// unpinned answers differ.
func TestConfigureCoordinatorTSOLegacyDoesNotPinTimestampGroup(t *testing.T) {
	cfg := testTSOFlags(false, false)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {
			Engine:   &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm},
			TSOState: fsm,
		},
		1: {Engine: &mainTSOEngine{state: raftengine.StateLeader}},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	require.Nil(t, wiring.RoutedAllocator,
		"legacy mode must not stand up the routed allocator")
	require.False(t, fsm.CutoverActive())
	require.True(t, coord.IsTimestampLeader(),
		"legacy warm-up must keep answering from the data groups, not group 0 alone")
}

func TestConfigureCoordinatorTSOModeFileLegacyDoesNotPinTimestampGroup(t *testing.T) {
	cfg := testTSOFlags(false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	cfg.ModeFile = path

	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {
			Engine:   &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm},
			TSOState: fsm,
		},
		1: {Engine: &mainTSOEngine{state: raftengine.StateLeader}},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	require.NotNil(t, wiring.RuntimeController,
		"mode-file legacy still needs the runtime controller for later reloads")
	require.Equal(t, kv.TSOModeLegacy, wiring.RuntimeController.CurrentMode())
	require.True(t, coord.IsTimestampLeader(),
		"legacy mode file must not narrow timestamp leadership to group 0")
	_, active := kv.TimestampAllocatorThrough(coord)
	require.False(t, active,
		"legacy mode file should keep persistence writes on the data-group HLC path")
	_, configured := kv.ConfiguredTimestampAllocatorThrough(coord)
	require.True(t, configured,
		"long-lived internal servers must retain the dynamic allocator for later reloads")
}

// Once the dedicated allocator is actually required, the pin must apply.
func TestConfigureCoordinatorTSOCutoverPinsTimestampGroup(t *testing.T) {
	cfg := testTSOFlags(true, false)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {
			Engine:   &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm},
			TSOState: fsm,
		},
		1: {Engine: &mainTSOEngine{state: raftengine.StateLeader}},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	require.False(t, coord.IsTimestampLeader(),
		"with the dedicated allocator active, leadership must come from group 0 only")
}

// The activation gate answers from this node's own runtime mode, so a caller
// cannot drive the group-0 leader past the stage the operator configured.
func TestCoordinatorTSOWiringAuthorizeActivation(t *testing.T) {
	// No dedicated TSO runtime at all: nothing to authorize against.
	var bare Wiring
	require.NoError(t, bare.AuthorizeActivation(false, false))
	require.Error(t, bare.AuthorizeActivation(true, false))
	require.Error(t, bare.AuthorizeActivation(true, true))

	cfg := testTSOFlags(false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	cfg.ModeFile = path

	newWiring := func(t *testing.T, mode kv.TSOMode) Wiring {
		t.Helper()
		clock := kv.NewHLC()
		clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
		fsm := kv.NewTSOStateMachine(clock)
		engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
		groups := map[uint64]*kv.ShardGroup{
			dedicatedTSOGroupID: {Engine: engine, TSOState: fsm},
		}
		coord := newMainTSOCoordinator(clock, groups)
		wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, wiring.Close()) })
		for next := kv.TSOModeShadow; next <= mode; next++ {
			require.NoError(t, wiring.RuntimeController.ApplyMode(next))
		}
		return wiring
	}

	legacy := newWiring(t, kv.TSOModeLegacy)
	require.Error(t, legacy.AuthorizeActivation(true, false), "legacy mode may not activate cutover")
	require.Error(t, legacy.AuthorizeActivation(true, true))

	cutover := newWiring(t, kv.TSOModeCutover)
	require.NoError(t, cutover.AuthorizeActivation(true, false))
	require.Error(t, cutover.AuthorizeActivation(true, true), "cutover mode may not activate phase D")

	phaseD := newWiring(t, kv.TSOModePhaseD)
	require.NoError(t, phaseD.AuthorizeActivation(true, false))
	require.NoError(t, phaseD.AuthorizeActivation(true, true))
}

// The distribution server decides whether a requested activation would change
// anything by probing the allocator it was handed. main.go hands it the
// concrete *kv.RaftTSOAllocator, so if that type stops exposing either marker
// the probe silently reports "still pending" and a node following a marker
// another leader committed answers PermissionDenied -- stalling allocation
// cluster-wide. A stub that happens to implement both cannot catch that.
func TestServerAllocatorExposesDurableMarkerState(t *testing.T) {
	cfg := testTSOFlags(false, false)

	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSOGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)
	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.NotNil(t, wiring.ServerAllocator)

	cutover, ok := any(wiring.ServerAllocator).(interface{ CutoverActive() bool })
	require.True(t, ok, "the server allocator must expose the durable cutover marker")
	phaseD, ok := any(wiring.ServerAllocator).(interface{ PhaseDActive() bool })
	require.True(t, ok, "the server allocator must expose the durable phase-D marker")

	// Nothing is durable yet, so both still read false and a request to activate
	// them is a genuine activation the gate should see.
	require.False(t, cutover.CutoverActive())
	require.False(t, phaseD.PhaseDActive())
}

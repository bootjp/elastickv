package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
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

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{floor: 42})
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
	require.IsType(t, startupGatedCoordinator{}, got)
	_, err := got.Next(context.Background())
	require.Error(t, err)
	require.Len(t, internalTimestampOptions(gated), 2)

	runtimeAllocator := kv.NewDynamicTimestampAllocator(nil)
	runtimeCoord := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(runtimeAllocator)
	runtimeGated := startupGatedCoordinator{inner: runtimeCoord, gate: &startupPublicKVGate{}}
	got, ok = kv.TimestampAllocatorThrough(runtimeGated)
	require.True(t, ok)
	require.IsType(t, startupGatedCoordinator{}, got)
	configured, ok := kv.ConfiguredTimestampAllocatorThrough(runtimeGated)
	require.True(t, ok)
	require.Same(t, runtimeAllocator, configured)
	require.Len(t, internalTimestampOptions(runtimeGated), 2)

	legacy := startupGatedCoordinator{inner: newMainTSOCoordinator(kv.NewHLC(), nil)}
	require.Len(t, internalTimestampOptions(legacy), 1)
}

func TestInternalTimestampOptionsPreservesForwardObserverThroughStartupGate(t *testing.T) {
	t.Parallel()
	allocator := &mainTimestampAllocator{next: 123}
	inner := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(allocator)
	observing := &mainForwardObservingCoordinator{ShardedCoordinator: inner}
	gated := startupGatedCoordinator{inner: observing}
	opts := internalTimestampOptions(gated)
	require.Len(t, opts, 2)

	txn := &mainForwardTxn{}
	internal := adapter.NewInternalWithEngine(txn, &mainTSOEngine{state: raftengine.StateLeader}, nil, nil, opts...)
	reqs := []*pb.Request{{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
	}}
	resp, err := internal.Forward(context.Background(), &pb.ForwardRequest{Requests: reqs})
	require.NoError(t, err)
	require.True(t, resp.GetSuccess())
	require.Equal(t, uint64(1), observing.observed.Load())
}

func TestInternalForwardUsesRuntimeAllocatorAfterModeReload(t *testing.T) {
	t.Parallel()

	setTSOModeFlags(t, false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	*tsoModeFile = path

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
	require.Equal(t, kv.TSOModeLegacy, wiring.runtimeController.CurrentMode())

	gated := startupGatedCoordinator{inner: coord, gate: &startupPublicKVGate{}}
	opts := internalTimestampOptions(gated)
	require.Len(t, opts, 2)

	txn := &mainForwardTxn{}
	internal := adapter.NewInternalWithEngine(txn, engine, nil, nil, opts...)

	require.NoError(t, wiring.runtimeController.ApplyMode(kv.TSOModeShadow))
	require.NoError(t, wiring.runtimeController.ApplyMode(kv.TSOModeCutover))

	reqs := []*pb.Request{{
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")}},
	}}
	resp, err := internal.Forward(context.Background(), &pb.ForwardRequest{Requests: reqs})
	require.NoError(t, err)
	require.True(t, resp.GetSuccess())
	require.Len(t, txn.requests, 1)
	require.Greater(t, txn.requests[0].GetTs(), uint64(1),
		"timestamp 1 would mean the long-lived Internal server lost the runtime allocator and used its nil-clock fallback")
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

// setTSOModeFlags overwrites process-global flag values for the duration of one
// test.
//
// A caller must therefore NOT be parallel. Go runs sequential tests to
// completion before the parallel ones resume, so a single parallel caller is
// safe on its own -- but the moment a second one exists the two overwrite each
// other's flags and -race reports it, which is what happened when
// TestCoordinatorTSOWiringAuthorizeActivation was first written with
// t.Parallel(). Every other caller in this file is sequential.
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

type mainTSOFloorProvider struct {
	floor uint64
}

type mainTimestampAllocator struct {
	next uint64
}

func (a *mainTimestampAllocator) Next(context.Context) (uint64, error) {
	return a.next, nil
}

type mainForwardTxn struct {
	requests []*pb.Request
}

type mainForwardObservingCoordinator struct {
	*kv.ShardedCoordinator
	observed atomic.Uint64
}

func (c *mainForwardObservingCoordinator) ObserveForwardedRequests(reqs []*pb.Request) {
	for range reqs {
		c.observed.Add(1)
	}
}

func (t *mainForwardTxn) Commit(_ context.Context, reqs []*pb.Request) (*kv.TransactionResponse, error) {
	t.requests = reqs
	return &kv.TransactionResponse{CommitIndex: 1}, nil
}

func (t *mainForwardTxn) Abort(context.Context, []*pb.Request) (*kv.TransactionResponse, error) {
	return &kv.TransactionResponse{}, nil
}

func (p mainTSOFloorProvider) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return p.floor, nil
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
	setTSOModeFlags(t, false, false)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {
			Engine:   &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm},
			TSOState: fsm,
		},
		1: {Engine: &mainTSOEngine{state: raftengine.StateLeader}},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	require.Nil(t, wiring.routedAllocator,
		"legacy mode must not stand up the routed allocator")
	require.False(t, fsm.CutoverActive())
	require.True(t, coord.IsTimestampLeader(),
		"legacy warm-up must keep answering from the data groups, not group 0 alone")
}

func TestConfigureCoordinatorTSOModeFileLegacyDoesNotPinTimestampGroup(t *testing.T) {
	setTSOModeFlags(t, false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	*tsoModeFile = path

	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {
			Engine:   &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm},
			TSOState: fsm,
		},
		1: {Engine: &mainTSOEngine{state: raftengine.StateLeader}},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	require.NotNil(t, wiring.runtimeController,
		"mode-file legacy still needs the runtime controller for later reloads")
	require.Equal(t, kv.TSOModeLegacy, wiring.runtimeController.CurrentMode())
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
	setTSOModeFlags(t, true, false)
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	groups := map[uint64]*kv.ShardGroup{
		dedicatedTSORaftGroupID: {
			Engine:   &mainTSOEngine{state: raftengine.StateFollower, tsoState: fsm},
			TSOState: fsm,
		},
		1: {Engine: &mainTSOEngine{state: raftengine.StateLeader}},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })

	require.False(t, coord.IsTimestampLeader(),
		"with the dedicated allocator active, leadership must come from group 0 only")
}

// phaseDMainAllocator carries the Phase-D surfaces the forwarded-timestamp
// validation depends on.
type phaseDMainAllocator struct {
	floor uint64
	next  atomic.Uint64
}

func (a *phaseDMainAllocator) Next(context.Context) (uint64, error) {
	return a.floor + a.next.Add(1), nil
}

func (a *phaseDMainAllocator) ValidateDurableTimestamp(_ context.Context, timestamp uint64) error {
	if timestamp <= a.floor {
		return errors.Join(kv.ErrTSOTimestampInvalid, kv.ErrTSOTimestampPrePhaseD)
	}
	return nil
}

func (a *phaseDMainAllocator) PhaseDActive() bool   { return true }
func (a *phaseDMainAllocator) PhaseDRequired() bool { return true }

// Internal.Forward validates a pre-stamped persistence timestamp through the
// allocator internalTimestampOptions installs. main.go hands the adapters a
// startupGatedCoordinator, so if that wrapper stopped exposing the inner
// allocator, the installed allocator would carry no Phase-D surfaces at all,
// ValidateDurablePersistenceTimestamp would find nothing required, and the
// receiver-side check would quietly pass everything in production while unit
// tests that use an unwrapped allocator stayed green.
func TestInternalTimestampOptionsPreservesPhaseDValidatorThroughStartupGate(t *testing.T) {
	t.Parallel()

	allocator := &phaseDMainAllocator{floor: 100}
	inner := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(allocator)
	gated := startupGatedCoordinator{inner: inner, gate: &startupPublicKVGate{}}

	installed, ok := kv.ConfiguredTimestampAllocatorThrough(gated)
	require.True(t, ok)
	require.Same(t, allocator, installed,
		"the wrapper must expose the inner allocator, not itself")

	_, isValidator := installed.(kv.DurableTimestampValidator)
	require.True(t, isValidator, "installed allocator must carry the durable validator")
	phaseD, isPhaseD := installed.(kv.TSOPhaseDState)
	require.True(t, isPhaseD)
	require.True(t, phaseD.PhaseDRequired())

	// End to end through the wiring the server actually builds.
	require.ErrorIs(t,
		kv.ValidateDurablePersistenceTimestamp(context.Background(), installed, 50, "test"),
		kv.ErrTSOTimestampPrePhaseD,
	)
	require.NoError(t,
		kv.ValidateDurablePersistenceTimestamp(context.Background(), installed, 101, "test"))
}

// The activation gate answers from this node's own runtime mode, so a caller
// cannot drive the group-0 leader past the stage the operator configured.
// Not parallel: setTSOModeFlags mutates process-global flags.
func TestCoordinatorTSOWiringAuthorizeActivation(t *testing.T) {
	// No dedicated TSO runtime at all: nothing to authorize against.
	var bare coordinatorTSOWiring
	require.NoError(t, bare.authorizeActivation(false, false))
	require.Error(t, bare.authorizeActivation(true, false))
	require.Error(t, bare.authorizeActivation(true, true))

	newWiring := func(t *testing.T, mode kv.TSOMode) coordinatorTSOWiring {
		t.Helper()
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
		for next := kv.TSOModeShadow; next <= mode; next++ {
			require.NoError(t, wiring.runtimeController.ApplyMode(next))
		}
		return wiring
	}

	setTSOModeFlags(t, false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	*tsoModeFile = path

	legacy := newWiring(t, kv.TSOModeLegacy)
	require.Error(t, legacy.authorizeActivation(true, false), "legacy mode may not activate cutover")
	require.Error(t, legacy.authorizeActivation(true, true))

	cutover := newWiring(t, kv.TSOModeCutover)
	require.NoError(t, cutover.authorizeActivation(true, false))
	require.Error(t, cutover.authorizeActivation(true, true), "cutover mode may not activate phase D")

	phaseD := newWiring(t, kv.TSOModePhaseD)
	require.NoError(t, phaseD.authorizeActivation(true, false))
	require.NoError(t, phaseD.authorizeActivation(true, true))
}

// The distribution server decides whether a requested activation would change
// anything by probing the allocator it was handed. main.go hands it the
// concrete *kv.RaftTSOAllocator, so if that type stops exposing either marker
// the probe silently reports "still pending" and a node following a marker
// another leader committed answers PermissionDenied -- stalling allocation
// cluster-wide. A stub that happens to implement both cannot catch that.
func TestServerAllocatorExposesDurableMarkerState(t *testing.T) {
	setTSOModeFlags(t, false, false)

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

	cutover, ok := any(wiring.serverAllocator).(interface{ CutoverActive() bool })
	require.True(t, ok, "the server allocator must expose the durable cutover marker")
	phaseD, ok := any(wiring.serverAllocator).(interface{ PhaseDActive() bool })
	require.True(t, ok, "the server allocator must expose the durable phase-D marker")

	// Nothing is durable yet, so both still read false and a request to activate
	// them is a genuine activation the gate should see.
	require.False(t, cutover.CutoverActive())
	require.False(t, phaseD.PhaseDActive())
}

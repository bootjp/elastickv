package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestConfigureCoordinatorTSOPhaseDWorksThroughAdapterDecorators(t *testing.T) {
	cfg := testTSOFlags(true, false)
	cfg.PhaseDEnabled = true
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		noderuntime.DedicatedTSORaftGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)

	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{floor: 42})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	decorated := kv.WithKeyVizLabel(clusterbootstrap.NewStartupGatedCoordinator(coord, nil), keyviz.LabelRedis)

	readTS, err := kv.BeginReadTimestampThrough(context.Background(), decorated, 42, "test decorated phase D")
	require.NoError(t, err)
	require.NotZero(t, readTS.Timestamp())
	require.True(t, fsm.CutoverActive())
	require.True(t, fsm.PhaseDActive())
	require.Equal(t, uint64(4), engine.proposals.Load(),
		"cutover, pending, and phase-D markers must commit before the timestamp window")
}

func TestInternalTimestampOptionsPreservesTSOThroughStartupGate(t *testing.T) {
	t.Parallel()
	allocator := &mainTimestampAllocator{next: 123}
	coord := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(allocator)
	gated := clusterbootstrap.NewStartupGatedCoordinator(coord, &clusterbootstrap.StartupPublicKVGate{})

	got, ok := kv.TimestampAllocatorThrough(gated)
	require.True(t, ok)
	require.IsType(t, clusterbootstrap.StartupGatedCoordinator{}, got)
	_, err := got.Next(context.Background())
	require.Error(t, err)
	// startupGatedCoordinator forwards the write gate and the forwarded-write
	// observer as well as the allocator, so it satisfies all three optional
	// interfaces and the option count is structural rather than a signal about
	// which allocator survived. The behavioural checks below are what pin that.
	require.Len(t, internalTimestampOptions(gated), 3)

	runtimeAllocator := kv.NewDynamicTimestampAllocator(nil)
	runtimeCoord := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(runtimeAllocator)
	runtimeGated := clusterbootstrap.NewStartupGatedCoordinator(runtimeCoord, &clusterbootstrap.StartupPublicKVGate{})
	got, ok = kv.TimestampAllocatorThrough(runtimeGated)
	require.True(t, ok)
	require.IsType(t, clusterbootstrap.StartupGatedCoordinator{}, got)
	configured, ok := kv.ConfiguredTimestampAllocatorThrough(runtimeGated)
	require.True(t, ok)
	require.Same(t, runtimeAllocator, configured)
	require.Len(t, internalTimestampOptions(runtimeGated), 3)

	legacy := clusterbootstrap.NewStartupGatedCoordinator(newMainTSOCoordinator(kv.NewHLC(), nil), nil)
	// No TSO allocator underneath: only the write gate and the forwarded-write
	// observer, which the gate wrapper satisfies unconditionally.
	require.Len(t, internalTimestampOptions(legacy), 2)
}

func TestInternalTimestampOptionsPreservesForwardObserverThroughStartupGate(t *testing.T) {
	t.Parallel()
	allocator := &mainTimestampAllocator{next: 123}
	inner := newMainTSOCoordinator(kv.NewHLC(), nil).WithTSOAllocator(allocator)
	observing := &mainForwardObservingCoordinator{ShardedCoordinator: inner}
	gated := clusterbootstrap.NewStartupGatedCoordinator(observing, nil)
	opts := internalTimestampOptions(gated)
	require.Len(t, opts, 3)

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

	cfg := testTSOFlags(false, false)
	path := filepath.Join(t.TempDir(), "tso-mode")
	require.NoError(t, os.WriteFile(path, []byte("legacy\n"), 0o600))
	cfg.ModeFile = path

	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	groups := map[uint64]*kv.ShardGroup{
		noderuntime.DedicatedTSORaftGroupID: {Engine: engine, TSOState: fsm},
	}
	coord := newMainTSOCoordinator(clock, groups)
	wiring, err := configureCoordinatorTSO(cfg, coord, groups, mainTSOFloorProvider{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wiring.Close()) })
	require.Equal(t, kv.TSOModeLegacy, wiring.RuntimeController.CurrentMode())

	gated := clusterbootstrap.NewStartupGatedCoordinator(coord, &clusterbootstrap.StartupPublicKVGate{})
	opts := internalTimestampOptions(gated)
	require.Len(t, opts, 3)

	txn := &mainForwardTxn{}
	internal := adapter.NewInternalWithEngine(txn, engine, nil, nil, opts...)

	require.NoError(t, wiring.RuntimeController.ApplyMode(kv.TSOModeShadow))
	require.NoError(t, wiring.RuntimeController.ApplyMode(kv.TSOModeCutover))

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
	gated := clusterbootstrap.NewStartupGatedCoordinator(inner, &clusterbootstrap.StartupPublicKVGate{})

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

package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newBootstrapExternalLifecycleFixture(t *testing.T) *rotationRuntimeFixture {
	t.Helper()
	f := newRotationRuntimeFixture(t, raftengine.StateLeader)
	require.NoError(t, f.runtime.step(t.Context()))
	require.True(t, f.a.LocalPermissionReady())
	f.runtime.config.planExternal = func(ctx context.Context, request *pb.RotateDEKRequest) (*bootstrapRotationRecipe, error) {
		return newBootstrapExternalRotationRecipe(ctx, f.io.cfg, request)
	}
	return f
}

func bootstrapExternalLifecycleWorker(t *testing.T, f *rotationRuntimeFixture, release func()) (func(), context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	start := sync.OnceFunc(func() { go func() { done <- f.runtime.Run(ctx) }() })
	t.Cleanup(func() {
		cancel()
		release()
		start()
		select {
		case <-f.runtime.done:
		case <-time.After(5 * time.Second):
			t.Error("external supervisor leaked ownership during cleanup")
		}
	})
	return start, cancel, done
}

// Take the already captured call before starting the worker so queue/active
// boundaries are deterministic, without sleeping or renewing its original H.
func bootstrapExternalLifecycleEnqueue(t *testing.T, ctx context.Context, f *rotationRuntimeFixture) (bootstrapExternalRotationCall, <-chan bootstrapExternalRotationResult) {
	t.Helper()
	result := make(chan bootstrapExternalRotationResult, 1)
	request := externalRotationTestRequest(f.io)
	go func() {
		response, err := f.runtime.RotateDEK(ctx, request)
		result <- bootstrapExternalRotationResult{response: response, err: err}
	}()
	call := awaitBootstrapGateResult(t, ctx, f.runtime.requests, "external request did not enter its bounded queue")
	return call, result
}

func TestBootstrapExternalRotationLifecycleShutdownActiveJoins(t *testing.T) {
	f := newBootstrapExternalLifecycleFixture(t)
	prior := len(f.io.calls)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	entered, cancelled := make(chan context.Context, 1), make(chan struct{})
	blocked := make(chan struct{})
	release := sync.OnceFunc(func() { close(blocked) })
	f.io.admin = func(ctx context.Context, _ *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		entered <- ctx
		<-ctx.Done()
		close(cancelled)
		<-blocked
		return nil, ctx.Err()
	}
	start, stopSupervisor, supervisor := bootstrapExternalLifecycleWorker(t, f, release)
	call, rpc := bootstrapExternalLifecycleEnqueue(t, ctx, f)
	shutdownCancelled := make(chan struct{})
	originalCancel := call.cancel
	call.cancel = func() { originalCancel(); close(shutdownCancelled) }
	f.runtime.requests <- call
	start()
	old := awaitBootstrapGateResult(t, ctx, entered, "external proposal not entered")
	stopSupervisor()
	awaitBootstrapGateResult(t, ctx, shutdownCancelled, "shutdown did not cancel the original bounded caller")
	awaitBootstrapGateResult(t, ctx, cancelled, "shutdown did not cancel the active native child")
	require.NoError(t, ctx.Err(), "the caller itself is still live")
	state, err := f.a.RotationStatus()
	require.NoError(t, err)
	require.True(t, state.Running, "active owner remains retained until the child joins")
	require.False(t, f.a.LocalPermissionReady())
	select {
	case <-rpc:
		t.Fatal("RPC retired active ownership before the child joined")
	case <-supervisor:
		t.Fatal("supervisor exited before its active child joined")
	default:
	}
	release()
	require.NoError(t, awaitBootstrapGateResult(t, ctx, supervisor, "supervisor failed to join external proposal"))
	result := awaitBootstrapGateResult(t, ctx, rpc, "RPC failed to retire after shutdown join")
	require.Nil(t, result.response)
	require.ErrorIs(t, result.err, kv.ErrTransactionOutcomeUnknown, "an attempted rotation cannot become a definitive rejection")
	require.ErrorIs(t, f.a.ValidateResult(old), raftadmission.ErrStale)
	state, err = f.a.RotationStatus()
	require.NoError(t, err)
	require.False(t, state.Running)
	require.True(t, state.Responsibility, "shutdown does not fabricate read-only reconciliation")
	require.Len(t, f.io.calls, prior+1, "shutdown cannot reissue the attempted external write")
}

func TestBootstrapExternalRotationLifecycleShutdownQueuedDoesNotPlan(t *testing.T) {
	f := newBootstrapExternalLifecycleFixture(t)
	prior := len(f.io.calls)
	previous := f.runtime.recipe
	var plans atomic.Int64
	f.runtime.config.planExternal = func(context.Context, *pb.RotateDEKRequest) (*bootstrapRotationRecipe, error) {
		plans.Add(1)
		return nil, raftadmission.ErrClosed
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start, stopSupervisor, supervisor := bootstrapExternalLifecycleWorker(t, f, func() {})
	call, rpc := bootstrapExternalLifecycleEnqueue(t, ctx, f)
	require.NoError(t, f.a.ValidateResult(call.intent.Context()))
	f.runtime.requests <- call
	stopSupervisor()
	start()
	require.NoError(t, awaitBootstrapGateResult(t, ctx, supervisor, "queued shutdown did not finish supervisor"))
	result := awaitBootstrapGateResult(t, ctx, rpc, "queued shutdown did not retire RPC intent")
	require.Nil(t, result.response)
	require.ErrorIs(t, result.err, raftadmission.ErrClosed)
	require.NoError(t, ctx.Err(), "shutdown must not depend on the caller deadline")
	require.ErrorIs(t, f.a.ValidateResult(call.intent.Context()), raftadmission.ErrStale)
	require.Error(t, call.intent.Context().Err())
	require.Zero(t, plans.Load(), "queued shutdown must never plan under a fresh owner")
	require.Same(t, previous, f.runtime.recipe)
	require.Len(t, f.io.calls, prior, "queued shutdown must not submit a proposal")
	state, err := f.a.RotationStatus()
	require.NoError(t, err)
	require.False(t, state.Running)
	require.False(t, state.Responsibility)
}

func TestBootstrapExternalRotationLifecyclePanicJoinsOwner(t *testing.T) {
	f := newBootstrapExternalLifecycleFixture(t)
	prior := len(f.io.calls)
	var old context.Context
	f.io.admin = func(ctx context.Context, _ *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		old = ctx
		panic("external proposal panic")
	}
	intent, err := f.a.CaptureRotationRequest(t.Context())
	require.NoError(t, err)
	defer intent.CancelAndJoin()
	call := bootstrapExternalRotationCall{intent: intent, request: externalRotationTestRequest(f.io)}
	require.Panics(t, func() { _, _ = f.runtime.executeExternal(call) })
	require.NotNil(t, old)
	require.Error(t, old.Err())
	require.ErrorIs(t, f.a.ValidateResult(old), raftadmission.ErrStale)
	state, err := f.a.RotationStatus()
	require.NoError(t, err)
	require.False(t, state.Running, "panic must finish the transferred Rotation owner")
	require.True(t, state.Responsibility, "panic cannot claim the proposal absent or complete")
	require.False(t, f.a.LocalPermissionReady())
	require.Len(t, f.io.calls, prior+1)
	closed := make(chan struct{})
	go func() { f.a.Close(); close(closed) }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	awaitBootstrapGateResult(t, ctx, closed, "panic leaked ownership and blocked arbiter Close")
}

func TestBootstrapExternalRotationLifecycleOnlyVerifiedCorruptionLatchesFatal(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "untrusted invalid request", true: "verified corruption through unknown wrapper"}[corrupt], func(t *testing.T) {
			f := newBootstrapExternalLifecycleFixture(t)
			prior := len(f.io.calls)
			f.runtime.config.planExternal = func(context.Context, *pb.RotateDEKRequest) (*bootstrapRotationRecipe, error) {
				if corrupt {
					return nil, bootstrapExternalRotationUnknown(errBootstrapRotationInvalidEvidence)
				}
				return nil, errBootstrapExternalRotationRequest
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			intent, err := f.a.CaptureRotationRequest(ctx)
			require.NoError(t, err)
			defer intent.CancelAndJoin()
			call := bootstrapExternalRotationCall{intent: intent, request: externalRotationTestRequest(f.io),
				result: make(chan bootstrapExternalRotationResult, 1), cancel: cancel}
			f.runtime.handleExternal(t.Context(), call)
			result := <-call.result
			require.Nil(t, result.response)
			require.Error(t, result.err)
			_, stateErr := f.a.RotationStatus()
			if corrupt {
				require.ErrorIs(t, result.err, errBootstrapRotationInvalidEvidence)
				require.ErrorIs(t, result.err, kv.ErrTransactionOutcomeUnknown)
				require.ErrorIs(t, stateErr, raftadmission.ErrClosed)
				require.ErrorIs(t, stateErr, errBootstrapRotationInvalidEvidence)
				require.False(t, f.a.LocalPermissionReady())
			} else {
				require.Equal(t, codes.InvalidArgument, status.Code(result.err))
				require.NoError(t, stateErr)
				require.True(t, f.a.LocalPermissionReady())
			}
			require.Len(t, f.io.calls, prior, "neither rejected evidence path may submit a rotation")
		})
	}
}

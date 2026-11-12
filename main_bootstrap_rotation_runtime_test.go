package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

// Unit boundary: real Arbiter/owners and immutable recipe, controlled admin
// I/O. Actual native recovery is covered separately by rotation E2E fixtures.
type rotationRuntimeFixture struct {
	io               *rotationRecipeFixture
	a                *raftadmission.Arbiter
	runtime          *bootstrapRotationRuntime
	policy           raftbootstrap.PolicyRecord
	session          uint64
	plans, refreshes atomic.Int64
	wrappers         atomic.Bool
}

func newRotationRuntimeFixture(t *testing.T, role raftengine.State) *rotationRuntimeFixture {
	t.Helper()
	f := &rotationRuntimeFixture{io: newRotationRecipeFixture(t)}
	configuration := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{91}}, Members: []raftbootstrap.Member{{ID: 91, Address: "local:9000", PublicKeyDigest: sha256.Sum256([]byte("key"))}}}
	digest, err := raftbootstrap.ConfigurationDigest(configuration)
	require.NoError(t, err)
	f.policy = raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion, Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1},
		BaselineDigest: digest, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 1, Generation: 1, LastConfigurationIndex: 1, Configuration: configuration}
	f.a, err = raftadmission.New(raftadmission.Config{Cluster: f.policy.Identity.Cluster, Groups: []uint64{1}, DefaultGroup: 1, EncryptionConfigured: true, RotationRequested: true, ValidatePayload: bootstrapPayloadValidator(f.policy.Identity.Cluster)})
	require.NoError(t, err)
	t.Cleanup(f.a.Close)
	f.role(t, 1, role)
	f.wrappers.Store(true)
	install := f.io.cfg.InstallAndVerify
	f.io.cfg.InstallAndVerify = func(ctx context.Context, id uint32, key encryption.SidecarKey) error {
		if err := install(ctx, id, key); err != nil {
			return err
		}
		f.wrappers.Store(true)
		return nil
	}
	f.io.cfg.Validate = func(ctx context.Context) error { return errors.Join(ctx.Err(), f.a.ValidateResult(ctx)) }
	f.io.cfg.IsLeader = func() bool {
		status, err := f.a.RotationStatus()
		return err == nil && status.Native.Role == raftengine.StateLeader
	}
	f.runtime, err = newBootstrapRotationRuntime(bootstrapRotationRuntimeConfig{
		admission: f.a, group: 1, requested: true,
		barrier: func(ctx context.Context) (uint64, error) { return f.io.index, f.a.ValidateResult(ctx) },
		observe: f.observe,
		plan: func(ctx context.Context) (*bootstrapRotationRecipe, error) {
			f.plans.Add(1)
			return newBootstrapRotationRecipe(ctx, f.io.cfg)
		},
		authorize: func(*bootstrapRotationRecipe) error { return nil },
		refresh:   func() error { f.refreshes.Add(1); return nil },
	})
	require.NoError(t, err)
	return f
}

func (f *rotationRuntimeFixture) role(t *testing.T, term uint64, role raftengine.State) {
	t.Helper()
	require.NoError(t, f.a.WithNative(t.Context(), 1, func(scope raftengine.NativeScope) error {
		scope.Observe(raftengine.NativeState{Term: term, Role: role, LeaderID: 91, ConfigurationIndex: 1})
		return nil
	}))
	ticket, err := f.a.Ticket(1)
	require.NoError(t, err)
	f.session++
	require.NoError(t, f.a.PublishRecovery(ticket, f.policy, f.session, raftadmission.RegistrationUnknown))
	ticket, err = f.a.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, f.a.MarkCatalogLoaded(ticket))
}

func (f *rotationRuntimeFixture) observe(ctx context.Context) (encryptionwiring.BootstrapEncryptionProjection, error) {
	if !f.wrappers.Load() {
		return encryptionwiring.BootstrapEncryptionProjection{}, encryptionwiring.ErrBootstrapEncryptionProjectionPending
	}
	sc, err := f.io.cfg.ReadSidecar()
	if err != nil {
		return encryptionwiring.BootstrapEncryptionProjection{}, err
	}
	p := encryptionwiring.BootstrapEncryptionProjection{Kind: encryptionwiring.BootstrapEncryptionObserved, Rotation: encryptionwiring.BootstrapRotationRequestedUnproven, FullNodeID: 91, AppliedIndex: f.io.index, InstalledEvidence: true}
	p.Storage = f.purpose(sc, sc.Active.Storage)
	p.Raft = f.purpose(sc, sc.Active.Raft)
	return p, f.a.ValidateResult(ctx)
}

func (f *rotationRuntimeFixture) purpose(sc *encryption.Sidecar, id uint32) encryptionwiring.BootstrapEncryptionPurposeProjection {
	key := sc.Keys[strconv.FormatUint(uint64(id), 10)]
	return encryptionwiring.BootstrapEncryptionPurposeProjection{DekID: id, Epoch: key.LocalEpoch, Registered: true, WrapperInstalled: true, KeyDigest: sha256.Sum256(key.Wrapped),
		Registry: encryption.RegistryValue{FullNodeID: 91, FirstSeenLocalEpoch: key.LocalEpoch, LastSeenLocalEpoch: key.LocalEpoch}}
}

func TestBootstrapRotationRuntimeLeaderResumesEachInvalidatedApply(t *testing.T) {
	t.Parallel()
	f := newRotationRuntimeFixture(t, raftengine.StateLeader)
	f.io.admin = func(_ context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		f.io.apply(req)
		f.a.InvalidateEncryption()
		return nil, raftadmission.ErrStale
	}
	for i := range 2 {
		require.ErrorIs(t, f.runtime.step(t.Context()), raftadmission.ErrStale)
		state, err := f.a.RotationStatus()
		require.NoError(t, err)
		require.True(t, state.Responsibility)
		require.False(t, state.Running)
		require.False(t, f.a.LocalPermissionReady())
		require.Len(t, f.io.calls, i+1)
	}
	require.NoError(t, f.runtime.step(t.Context()))
	require.True(t, f.a.LocalPermissionReady())
	require.Equal(t, int64(1), f.plans.Load(), "apply invalidation cannot replace the original plan")
	require.Len(t, f.io.calls, 2)
}

func TestBootstrapRotationRuntimeFollowerPermissionWithoutLeaderCompletion(t *testing.T) {
	t.Parallel()
	f := newRotationRuntimeFixture(t, raftengine.StateFollower)
	require.NoError(t, f.runtime.step(t.Context()))
	require.True(t, f.a.LocalPermissionReady())
	require.Zero(t, f.plans.Load())
	require.Empty(t, f.io.calls)
	f.role(t, 2, raftengine.StateLeader)
	require.False(t, f.a.LocalPermissionReady(), "F cannot become leader completion")
	require.NoError(t, f.runtime.step(t.Context()))
	require.True(t, f.a.LocalPermissionReady())
	require.Len(t, f.io.calls, 2)
}

func TestBootstrapRotationRuntimeRepairsBeforeObservingProjection(t *testing.T) {
	t.Parallel()
	f := newRotationRuntimeFixture(t, raftengine.StateLeader)
	f.wrappers.Store(false)
	install := f.io.cfg.InstallAndVerify
	f.io.cfg.InstallAndVerify = func(ctx context.Context, id uint32, key encryption.SidecarKey) error {
		if !f.wrappers.Load() {
			f.wrappers.Store(true)
			f.a.InvalidateEncryption()
		}
		return install(ctx, id, key)
	}
	require.Error(t, f.runtime.step(t.Context()), "repair revokes the old observation owner")
	require.Empty(t, f.io.calls)
	require.NoError(t, f.runtime.step(t.Context()))
	require.True(t, f.a.LocalPermissionReady(), "new observation must reach the repaired wrapper rather than loop on pending")
}

func TestBootstrapRotationRuntimeStepdownJoinsAdminBeforeFollowerPermission(t *testing.T) {
	t.Parallel()
	f := newRotationRuntimeFixture(t, raftengine.StateLeader)
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.io.admin = func(ctx context.Context, _ *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- f.runtime.step(t.Context()) }()
	<-entered
	require.NoError(t, f.a.WithNative(t.Context(), 1, func(scope raftengine.NativeScope) error {
		scope.Observe(raftengine.NativeState{Term: 2, Role: raftengine.StateFollower, LeaderID: 92, ConfigurationIndex: 1})
		return nil
	}))
	<-cancelled
	select {
	case <-done:
		t.Fatal("supervisor retired an admin call before it joined")
	default:
	}
	require.False(t, f.a.LocalPermissionReady())
	close(release)
	require.Error(t, <-done)
	f.role(t, 2, raftengine.StateFollower)
	require.NoError(t, f.runtime.step(t.Context()))
	require.True(t, f.a.LocalPermissionReady())
	state, err := f.a.RotationStatus()
	require.NoError(t, err)
	require.False(t, state.Responsibility)
	require.False(t, state.Running)
	require.Len(t, f.io.calls, 1, "follower reconciliation must not rotate the unresolved purpose")
}

func TestBootstrapRotationRuntimeRunAndWaitLifetime(t *testing.T) {
	t.Parallel()
	f := newRotationRuntimeFixture(t, raftengine.StateFollower)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- f.runtime.Run(ctx) }()
	wait, waitCancel := context.WithTimeout(t.Context(), time.Second)
	defer waitCancel()
	require.NoError(t, f.runtime.Wait(wait))
	cancel()
	require.NoError(t, <-done)
	require.Empty(t, f.io.calls)
	f.a.LatchFatal(errors.New("terminal storage failure"))
	require.ErrorContains(t, f.runtime.Wait(t.Context()), "terminal storage failure")
}

func TestBootstrapRotationRuntimePanicClosesRecoveryOwner(t *testing.T) {
	t.Parallel()
	f := newRotationRuntimeFixture(t, raftengine.StateFollower)
	var captured context.Context
	f.runtime.config.observe = func(ctx context.Context) (encryptionwiring.BootstrapEncryptionProjection, error) {
		captured = ctx
		panic("projection panic")
	}
	require.Panics(t, func() { _ = f.runtime.step(t.Context()) })
	require.NotNil(t, captured)
	require.Error(t, captured.Err())
	done := make(chan struct{})
	go func() { f.a.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("panic leaked Recovery ownership")
	}
}

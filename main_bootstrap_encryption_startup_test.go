package main

import (
	"context"
	"testing"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type bootstrapStartupRecoveryEngine struct {
	raftengine.Engine
	read func(context.Context) (uint64, error)
}

func TestBootstrapEncryptionStartupInactiveStorageRejectsBeforeWorkers(t *testing.T) {
	t.Parallel()
	f := newEncryptionRuntimeIOFixture(t)
	require.False(t, f.cfg.Wiring.cache.StorageEnvelopeActive())
	var workers errgroup.Group
	var cleanup internalutil.CleanupStack
	_, supervisor, err := configureBootstrapEncryptionStartup(t.Context(), bootstrapEncryptionStartupConfig{
		runtimes: []*raftGroupRuntime{f.cfg.DefaultRuntime}, groups: map[uint64]*kv.ShardGroup{1: f.cfg.Group}, defaultGroup: 1,
		network: &bootstrapNetwork{}, wiring: f.cfg.Wiring, sidecarPath: f.cfg.SidecarPath,
		wrapper: f.cfg.Wrapper, nodeID: f.cfg.NodeID, requested: true, eg: &workers, cleanup: &cleanup,
	})
	require.ErrorIs(t, err, errBootstrapEncryptionUnsupported)
	require.Nil(t, supervisor)
	require.NoError(t, workers.Wait(), "unsupported profile must not leave background work")
	cleanup.Run()
}

func (e *bootstrapStartupRecoveryEngine) RecoveryLinearizableRead(ctx context.Context) (uint64, error) {
	return e.read(ctx)
}

func TestBootstrapEncryptionStartupPlaintextAndMissingIO(t *testing.T) {
	t.Parallel()
	ctx, supervisor, err := configureBootstrapEncryptionStartup(t.Context(), bootstrapEncryptionStartupConfig{})
	require.NoError(t, err)
	require.Equal(t, t.Context(), ctx)
	require.Nil(t, supervisor)
	domain := bootstrapIngressFixture(t).domain
	runtimes := []*raftGroupRuntime{{bootstrapDomain: domain}}
	for _, cfg := range []bootstrapEncryptionStartupConfig{
		{runtimes: runtimes, requested: true},
		{runtimes: runtimes, sidecarPath: "configured-but-no-cipher"},
	} {
		_, supervisor, err := configureBootstrapEncryptionStartup(t.Context(), cfg)
		require.Error(t, err)
		require.Nil(t, supervisor)
	}
}

func bootstrapStartupRotationAdmission(t *testing.T) *bootstrapDomain {
	t.Helper()
	cluster := raftbootstrap.UUID{1}
	a, err := raftadmission.New(raftadmission.Config{Cluster: cluster, DefaultGroup: 1, Groups: []uint64{1},
		RotationRequested: true, ValidatePayload: bootstrapPayloadValidator(cluster)})
	require.NoError(t, err)
	t.Cleanup(a.Close)
	bootstrapIngressObserve(t, a, 1)
	c := raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{1}},
		Members: []raftbootstrap.Member{{ID: 1, Address: "localhost:9000", PublicKeyDigest: raftbootstrap.Digest{1}}}}
	digest, err := raftbootstrap.ConfigurationDigest(c)
	require.NoError(t, err)
	ticket, err := a.Ticket(1)
	require.NoError(t, err)
	require.NoError(t, a.PublishRecovery(ticket, raftbootstrap.PolicyRecord{Version: raftbootstrap.PolicyVersion,
		Identity: raftbootstrap.Identity{Cluster: cluster, Group: 1}, BaselineDigest: digest,
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, ActivationIndex: 1, Generation: 1,
		LastConfigurationIndex: 1, Configuration: c}, 1, raftadmission.RegistrationNotConfigured))
	enc, err := a.EncryptionTicket()
	require.NoError(t, err)
	require.NoError(t, a.PublishEncryption(enc, raftbootstrap.Digest{2}))
	return &bootstrapDomain{admission: a}
}

func TestBootstrapEncryptionRecoveryBarrierOwnsRecoveryChildOfRotation(t *testing.T) {
	t.Parallel()
	domain := bootstrapStartupRotationAdmission(t)
	engine := &bootstrapStartupRecoveryEngine{}
	engine.read = func(ctx context.Context) (uint64, error) {
		err := domain.admission.WithNative(ctx, 1, func(scope raftengine.NativeScope) error {
			return scope.Check(ctx, raftengine.NativeRequest{Operation: raftengine.NativeRecoveryReadIndex})
		})
		return 17, err
	}
	rt := &raftGroupRuntime{spec: noderuntime.GroupSpec{ID: 1}, engine: engine}
	barrier, err := bootstrapEncryptionRecoveryBarrier(domain, rt)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	run, err := domain.admission.BeginRotation(ctx, raftbootstrap.Digest{3})
	require.NoError(t, err)
	defer func() { _ = run.Finish(raftadmission.ErrClosed) }()
	_, err = engine.RecoveryLinearizableRead(run.Context())
	require.Error(t, err, "Rotation itself is not Recovery read authority")
	index, err := barrier(run.Context())
	require.NoError(t, err, "barrier must create the owned Recovery child")
	require.EqualValues(t, 17, index)
	require.NoError(t, domain.admission.ValidateResult(run.Context()))
	_, finish, err := domain.admission.Enter(ctx, raftadmission.Recovery, 1)
	require.NoError(t, err)
	require.NoError(t, finish(nil))
}

func TestBootstrapEncryptionRecoveryBarrierRejectsRetiredEngineAndCancellation(t *testing.T) {
	t.Parallel()
	domain := bootstrapIngressFixture(t).domain
	engine := &bootstrapStartupRecoveryEngine{}
	rt := &raftGroupRuntime{spec: noderuntime.GroupSpec{ID: 1}, engine: engine}
	engine.read = func(context.Context) (uint64, error) {
		rt.engineMu.Lock()
		rt.engine = nil
		rt.engineMu.Unlock()
		return 17, nil
	}
	barrier, err := bootstrapEncryptionRecoveryBarrier(domain, rt)
	require.NoError(t, err)
	index, err := barrier(t.Context())
	require.Error(t, err)
	require.Zero(t, index)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = barrier(ctx)
	require.Error(t, err)
	_, err = bootstrapEncryptionRecoveryBarrier(domain, rt)
	require.Error(t, err, "no legacy native read fallback")
}

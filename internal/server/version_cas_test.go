package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestVersionCASStartupRequiresCommitTSFence(t *testing.T) {
	flags := defaultServerFlags()
	require.False(t, flags.consistency.versionCAS)
	flags.consistency.versionCAS = true
	_, _, _, _, err := resolveValidatedRuntimeInputs(flags)
	require.ErrorContains(t, err, "versionCAS requires --commitTSFence")
}

func TestVersionCASRejectsExternalRestoreOrigin(t *testing.T) {
	dir := t.TempDir()
	marker := []byte(`{"version":1,"index":3,"term":2,"completed":true,"fsm_payload_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "external-restore-origin.json"), marker, 0o600))
	rt := &clusterbootstrap.GroupRuntime{
		BootstrapNetwork:    &clusterbootstrap.BootstrapNetwork{},
		BootstrapPeerConfig: &clusterbootstrap.BootstrapPeerConfig{},
	}

	conns := kv.NewGRPCConnCache(nil)
	defer func() { require.NoError(t, conns.Close()) }()
	err := configureVersionCASGroup(rt, nil, dir, "member", conns)
	require.ErrorIs(t, err, kv.ErrVersionCASUnavailable)
	require.ErrorContains(t, err, "external snapshot restore")
	require.False(t, rt.VersionCASCapable)

	require.NoError(t, configureVersionCASGroup(rt, nil, dir, "member", nil))
	require.False(t, rt.VersionCASCapable)
	require.Nil(t, rt.VersionCASAdmission)
}

type versionCASGateEngine struct {
	raftengine.Engine
	status raftengine.Status
	config raftengine.Configuration
}

func (e versionCASGateEngine) Status() raftengine.Status { return e.status }

func (e versionCASGateEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return e.config, nil
}

func TestVersionCASGateRequiresFenceAndEveryReplica(t *testing.T) {
	engine := &versionCASGateEngine{
		status: raftengine.Status{
			State: raftengine.StateLeader, Term: 12, ConfigurationIndex: 7,
			Leader: raftengine.LeaderInfo{ID: "leader"},
		},
		config: raftengine.Configuration{Servers: []raftengine.Server{
			{ID: "leader", Address: "leader", Suffrage: "voter"},
			{ID: "voter", Address: "voter", Suffrage: "voter"},
			{ID: "learner", Address: "learner", Suffrage: "learner"},
		}},
	}
	active := false
	g := versionCASGate{
		engine: engine, localID: "leader",
		fence:               func(context.Context) (bool, error) { return active, nil },
		identity:            func(context.Context) (bool, error) { return true, nil },
		stableConfiguration: func(uint64) bool { return true },
		checkPeer: func(_ context.Context, peer raftengine.Server, index uint64) error {
			if peer.ID == "learner" {
				return errors.New("learner uses an older binary")
			}
			return nil
		},
	}
	_, err := g.admit(t.Context())
	require.ErrorContains(t, err, "commit-ts fence is inactive")
	active = true
	_, err = g.admit(t.Context())
	require.ErrorIs(t, err, kv.ErrVersionCASUnavailable)
	require.ErrorContains(t, err, "older binary")
	checked := make(chan string, 2)
	g.checkPeer = func(_ context.Context, peer raftengine.Server, index uint64) error {
		if index != 7 {
			return errors.New("wrong configuration index")
		}
		checked <- peer.ID
		return nil
	}
	ctx, err := g.admit(t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"voter", "learner"}, []string{<-checked, <-checked})
	proof, ok := raftengine.VersionCASProposalProof(ctx)
	require.True(t, ok)
	require.Equal(t, raftengine.VersionCASProof{Term: 12, ConfigurationIndex: 7}, proof)
	engine.status.PendingConfChange = true
	_, err = g.admit(t.Context())
	require.ErrorIs(t, err, kv.ErrVersionCASUnavailable)
	engine.status.PendingConfChange = false
	g.stableConfiguration = func(uint64) bool { return false }
	_, err = g.admit(t.Context())
	require.ErrorIs(t, err, kv.ErrVersionCASUnavailable)
}

func multiEnablementVersionCAS(t *testing.T, processes []*multiEnablementProcess, leader *multiEnablementProcess) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), bootstrapE2EWaitTimeout)
	defer cancel()
	const key = "protected-key"
	oldVersion, found, err := leader.runtime.Store.LatestCommitTS(ctx, []byte(key))
	require.NoError(t, err)
	require.True(t, found)
	attempt := func(process *multiEnablementProcess) (*kv.CoordinateResponse, error) {
		return process.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: []*kv.Elem[kv.OP]{
			{Op: kv.CompareAndSwap, Key: []byte(key), Value: []byte("cas-on-three-replicas"), ExpectedCommitTS: oldVersion},
		}})
	}
	_, err = attempt(leader)
	require.ErrorIs(t, err, kv.ErrVersionCASUnavailable, "configured feature is still off until fence apply")
	owned, op, err := leader.domain.Admission.Begin(ctx, raftadmission.Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	result, err := leader.shard.Proposer().Propose(owned, kv.EncodeCommitTSFenceActivation())
	require.NoError(t, clusterbootstrap.FinishBootstrapIngress(op, err))
	require.Nil(t, result.Response)

	var follower *multiEnablementProcess
	for _, process := range processes {
		require.Eventually(t, func() bool {
			active, readErr := kv.CommitTSFenceEnabled(ctx, process.runtime.Store)
			return readErr == nil && active && process.runtime.PublicReady()
		}, bootstrapE2EWaitTimeout, clusterbootstrap.BootstrapReadinessRetryInterval)
		if process != leader {
			follower = process
		}
	}
	require.NotNil(t, follower)
	conns := clusterbootstrap.NewBootstrapRuntimeConnCache([]*clusterbootstrap.GroupRuntime{follower.runtime})
	defer func() { require.NoError(t, conns.Close()) }()
	conn, err := conns.ConnFor(leader.runtime.Spec.Address)
	require.NoError(t, err)
	value, err := pb.NewTransactionalKVClient(conn).Get(ctx, &pb.GetRequest{Key: []byte(key), NeedCommitTs: true})
	require.NoError(t, err, "the first token read activates migration identity protection")
	require.Equal(t, oldVersion, value.GetCommitTs())
	require.Equal(t, []byte("replicated-protected"), value.GetValue())
	for _, process := range processes {
		require.Eventually(t, func() bool {
			active, readErr := kv.MigrationVersionIdentityEnabled(ctx, process.runtime.Store)
			return readErr == nil && active && process.runtime.PublicReady()
		}, bootstrapE2EWaitTimeout, clusterbootstrap.BootstrapReadinessRetryInterval)
	}

	leaderConns := clusterbootstrap.NewBootstrapRuntimeConnCache([]*clusterbootstrap.GroupRuntime{leader.runtime})
	defer func() { require.NoError(t, leaderConns.Close()) }()
	followerConn, err := leaderConns.ConnFor(follower.runtime.Spec.Address)
	require.NoError(t, err)
	res, err := pb.NewTransactionalKVClient(followerConn).CompareAndSwap(ctx, &pb.CompareAndSwapRequest{
		Key: []byte(key), ExpectedCommitTs: value.GetCommitTs(), Value: []byte("cas-on-three-replicas"),
	})
	require.NoError(t, err, "public follower ingress reaches the admitted leader over pinned TLS")
	require.Greater(t, res.GetNewCommitTs(), oldVersion)

	for _, process := range processes {
		multiEnablementReplicatedRead(t, process, key, "cas-on-three-replicas")
	}
	_, err = attempt(leader)
	require.True(t, kv.IsCertifiedCASMismatch(err), "old token returns a certified mismatch: %v", err)
}

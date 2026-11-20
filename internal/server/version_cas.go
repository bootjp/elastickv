package server

import (
	"context"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const versionCASProbeTimeout = 2 * time.Second

func configureProtectedShardNetwork(ctx context.Context, flags *Flags, cfg noderuntime.RuntimeConfig, runtimes []*clusterbootstrap.GroupRuntime, shards *kv.ShardStore, groups map[uint64]*kv.ShardGroup, cleanup *internalutil.CleanupStack) (*clusterbootstrap.BootstrapNetwork, error) {
	network, err := clusterbootstrap.ConfigureBootstrapNetwork(ctx, runtimes, shards, groups, cleanup)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := configureVersionCAS(flags, cfg, runtimes, groups, cleanup); err != nil {
		return nil, err
	}
	return network, nil
}

func configureVersionCAS(flags *Flags, cfg noderuntime.RuntimeConfig, runtimes []*clusterbootstrap.GroupRuntime, groups map[uint64]*kv.ShardGroup, cleanup *internalutil.CleanupStack) error {
	enabled := flags.consistency.versionCAS
	multi := noderuntime.EffectiveMultiDataDirs(cfg.Groups, cfg.Multi)
	var conns *kv.GRPCConnCache
	if enabled {
		conns = clusterbootstrap.NewBootstrapRuntimeConnCache(runtimes)
		cleanup.Add(func() { _ = conns.Close() })
	}
	for _, rt := range runtimes {
		if rt.Spec.ID == noderuntime.DedicatedTSORaftGroupID {
			continue
		}
		dir := noderuntime.GroupDataDir(flags.raft.dataDir, flags.raft.id, rt.Spec.ID, multi)
		if err := configureVersionCASGroup(rt, groups[rt.Spec.ID], dir, flags.raft.id, conns); err != nil {
			return errors.Wrapf(err, "configure version CAS for group %d", rt.Spec.ID)
		}
	}
	return nil
}

func unsupportedVersionCASGroup(enabled bool, reason string) error {
	if enabled {
		return errors.Wrap(kv.ErrVersionCASUnavailable, reason)
	}
	return nil
}

func configureVersionCASGroup(rt *clusterbootstrap.GroupRuntime, group *kv.ShardGroup, dir, localID string, conns *kv.GRPCConnCache) error {
	// A connection cache is installed only with --versionCAS.
	enabled := conns != nil
	if rt.BootstrapNetwork == nil || rt.BootstrapPeerConfig == nil {
		return unsupportedVersionCASGroup(enabled, "no protected runtime network")
	}
	rewound, err := etcdraftengine.HasExternalRestoreOrigin(dir)
	if err != nil {
		return errors.Wrap(err, "check restore origin")
	}
	if rewound {
		return unsupportedVersionCASGroup(enabled, "group descends from an external snapshot restore")
	}
	// Capability attests replay support for both reserved frames even
	// while the replica's own proposal admission is disabled.
	_, canApply := rt.Store.(store.RaftEntryApplier)
	_, hasMetadata := rt.Store.(store.RaftControlMetadataStore)
	if !canApply || !hasMetadata {
		return unsupportedVersionCASGroup(enabled, "group lacks replicated metadata storage")
	}
	rt.VersionCASCapable = true
	if !enabled {
		return nil
	}
	return installVersionCASAdmission(rt, group, localID, conns)
}

func installVersionCASAdmission(rt *clusterbootstrap.GroupRuntime, group *kv.ShardGroup, localID string, conns *kv.GRPCConnCache) error {
	if group == nil {
		return errors.Wrap(kv.ErrVersionCASUnavailable, "data group is not configured")
	}
	proxy, ok := group.Txn.(*kv.LeaderProxy)
	if !ok {
		return errors.Wrap(kv.ErrVersionCASUnavailable, "data group has no leader proxy")
	}
	gate := versionCASGate{
		engine:     group.Engine,
		proposer:   group.Proposer(),
		fence:      func(ctx context.Context) (bool, error) { return kv.CommitTSFenceEnabled(ctx, rt.Store) },
		identity:   func(ctx context.Context) (bool, error) { return kv.MigrationVersionIdentityEnabled(ctx, rt.Store) },
		activation: make(chan struct{}, 1),
		localID:    localID,
		checkPeer:  versionCASPeerCheck(rt, conns),
		stableConfiguration: func(index uint64) bool {
			return rt.BootstrapNetwork.HasStableConfiguration(rt.Spec.ID, index)
		},
	}
	rt.VersionCASAdmission = gate.admit
	proxy.ConfigureVersionCASAdmission(gate.admit)
	group.ConfigureVersionReadAdmission(gate.admitRead)
	return nil
}

type versionCASGate struct {
	engine              raftengine.Engine
	proposer            raftengine.Proposer
	fence               func(context.Context) (bool, error)
	identity            func(context.Context) (bool, error)
	activation          chan struct{}
	localID             string
	checkPeer           func(context.Context, raftengine.Server, uint64) error
	stableConfiguration func(uint64) bool
}

func (g versionCASGate) admit(ctx context.Context) (context.Context, error) {
	proof, err := g.prove(ctx)
	if err != nil {
		return nil, err
	}
	if err := g.activate(proof); err != nil {
		return nil, err
	}
	return proof, nil
}

func (g versionCASGate) admitRead(ctx context.Context) error {
	admitted, err := g.admit(ctx)
	if err != nil {
		return err
	}
	proof, _ := raftengine.VersionCASProposalProof(admitted)
	st := g.engine.Status()
	if !g.configurationReady(st) || st.Term != proof.Term || st.ConfigurationIndex != proof.ConfigurationIndex {
		return errors.Wrap(kv.ErrVersionCASUnavailable, "membership or leadership changed during token admission")
	}
	return nil
}

// The only maintenance proposal is the first operation's sticky marker.
// Recheck under the per-group slot to avoid a burst of duplicate proposals.
func (g versionCASGate) activate(ctx context.Context) error {
	active, err := g.identity(ctx)
	if err != nil {
		return errors.Join(kv.ErrVersionCASUnavailable, err)
	}
	if active {
		return nil
	}
	select {
	case g.activation <- struct{}{}:
		defer func() { <-g.activation }()
	case <-ctx.Done():
		return errors.Join(kv.ErrVersionCASUnavailable, ctx.Err())
	}
	return g.proposeActivationIfNeeded(ctx)
}

func (g versionCASGate) proposeActivationIfNeeded(ctx context.Context) error {
	active, err := g.identity(ctx)
	if err != nil {
		return errors.Join(kv.ErrVersionCASUnavailable, err)
	}
	if active {
		return nil
	}
	result, err := g.proposer.Propose(ctx, kv.EncodeMigrationVersionIdentityActivation())
	if err != nil {
		return errors.Wrap(errors.Join(kv.ErrVersionCASUnavailable, err), "activate migration version identity")
	}
	if result == nil || result.CommitIndex == 0 || result.Response != nil {
		return errors.Wrap(kv.ErrVersionCASUnavailable, "migration version identity activation did not apply")
	}
	active, err = g.identity(ctx)
	if err != nil || !active {
		return errors.Wrap(errors.Join(kv.ErrVersionCASUnavailable, err), "migration version identity activation is absent")
	}
	return nil
}

func (g versionCASGate) requireCommitFence(ctx context.Context) error {
	active, err := g.fence(ctx)
	if err != nil || !active {
		return errors.Wrap(errors.Join(kv.ErrVersionCASUnavailable, err), "commit-ts fence is inactive")
	}
	return nil
}

func (g versionCASGate) prove(ctx context.Context) (context.Context, error) {
	st := g.engine.Status()
	if !g.configurationReady(st) {
		return nil, errors.Wrap(kv.ErrVersionCASUnavailable, "membership or leadership is unsettled")
	}
	if err := g.requireCommitFence(ctx); err != nil {
		return nil, err
	}
	cfg, err := g.engine.Configuration(ctx)
	if err != nil {
		return nil, errors.Join(kv.ErrVersionCASUnavailable, err)
	}
	probes, cancel := context.WithTimeout(ctx, versionCASProbeTimeout)
	defer cancel()
	if err := g.checkConfiguration(probes, cfg, st.ConfigurationIndex); err != nil {
		return nil, err
	}
	return raftengine.WithVersionCASProof(ctx, raftengine.VersionCASProof{
		Term: st.Term, ConfigurationIndex: st.ConfigurationIndex,
	}), nil
}

func (g versionCASGate) configurationReady(st raftengine.Status) bool {
	return st.State == raftengine.StateLeader && st.Leader.ID == g.localID && st.Term != 0 &&
		st.ConfigurationIndex != 0 && !st.PendingConfChange && g.stableConfiguration(st.ConfigurationIndex)
}

func (g versionCASGate) checkConfiguration(ctx context.Context, cfg raftengine.Configuration, index uint64) error {
	probes, ctx := errgroup.WithContext(ctx)
	localVoter := false
	for _, peer := range cfg.Servers {
		if peer.ID == g.localID && peer.Suffrage == etcdraftengine.SuffrageVoter && peer.Address != "" {
			localVoter = true
			continue
		}
		probes.Go(func() error { return g.checkPeer(ctx, peer, index) })
	}
	err := probes.Wait()
	if !localVoter || err != nil {
		return errors.Wrap(errors.Join(kv.ErrVersionCASUnavailable, err), "replica capability check failed")
	}
	return nil
}

func versionCASPeerCheck(rt *clusterbootstrap.GroupRuntime, conns *kv.GRPCConnCache) func(context.Context, raftengine.Server, uint64) error {
	return func(ctx context.Context, peer raftengine.Server, index uint64) error {
		if !versionCASPeerProvisioned(rt, peer) {
			return errors.Newf("replica %q at %q lacks a protected identity", peer.ID, peer.Address)
		}
		conn, err := conns.ConnFor(peer.Address)
		if err != nil {
			return errors.Wrapf(err, "connect to replica %s", peer.ID)
		}
		status, err := pb.NewRaftAdminClient(conn).Status(ctx, &pb.RaftAdminStatusRequest{}, grpc.WaitForReady(false))
		if err != nil {
			return errors.Wrapf(err, "check replica %s", peer.ID)
		}
		if !status.GetSupportsVersionCas() || status.GetPendingConfChange() || status.GetConfigurationIndex() != index {
			return errors.Newf("replica %s lacks version CAS capability at configuration %d", peer.ID, index)
		}
		return nil
	}
}

func versionCASPeerProvisioned(rt *clusterbootstrap.GroupRuntime, peer raftengine.Server) bool {
	for _, provision := range rt.BootstrapPeerConfig.Provisions {
		if provision.Node.Member != etcdraftengine.DeriveNodeID(peer.ID) || provision.Node.Address != peer.Address {
			continue
		}
		for _, group := range provision.Groups {
			if group == rt.Spec.ID {
				return true
			}
		}
	}
	return false
}

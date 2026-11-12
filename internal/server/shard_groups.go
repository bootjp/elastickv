package server

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func newRaftFactory(engineType noderuntime.RaftEngineType, coldStartObs raftengine.ColdStartObserver) (raftengine.Factory, error) {
	switch engineType {
	case noderuntime.RaftEngineEtcd:
		return etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
			TickInterval:      etcdTickInterval,
			HeartbeatTick:     durationToTicks(heartbeatTimeout, etcdTickInterval, etcdHeartbeatMinTicks),
			ElectionTick:      durationToTicks(electionTimeout, etcdTickInterval, etcdElectionMinTicks),
			MaxSizePerMsg:     etcdMaxSizePerMsg,
			MaxInflightMsg:    etcdMaxInflightMsg,
			ColdStartObserver: coldStartObs,
		}), nil
	default:
		return nil, errors.Wrapf(noderuntime.ErrUnsupportedRaftEngine, "%q", engineType)
	}
}

func durationToTicks(timeout time.Duration, tick time.Duration, min int) int {
	if tick <= 0 {
		return min
	}
	ticks := int(timeout / tick)
	if timeout%tick != 0 {
		ticks++
	}
	if ticks < min {
		return min
	}
	return ticks
}

func startRaftEngineLifecycleWatchers(ctx context.Context, eg *errgroup.Group, runtimes []*clusterbootstrap.GroupRuntime) {
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		engine := clusterbootstrap.RuntimeCore(rt).SnapshotEngine()
		lifecycle, ok := engine.(raftengine.Lifecycle)
		if !ok {
			continue
		}
		done := lifecycle.Done()
		if done == nil {
			continue
		}
		groupID := rt.Spec.ID
		eg.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			case <-done:
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				if err := lifecycle.Err(); err != nil {
					return errors.Wrapf(err, "raft group %d engine stopped", groupID)
				}
				return nil
			}
		})
	}
}

func buildShardGroups(
	raftID string,
	raftDir string,
	groups []noderuntime.GroupSpec,
	multi bool,
	bootstrap bool,
	bootstrapCfg noderuntime.BootstrapConfig,
	factory raftengine.Factory,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	clock *kv.HLC,
	readTracker *kv.ActiveTimestampTracker,
	kekWrapper kek.Wrapper,
	keystore *encryption.Keystore,
	sidecarPath string,
	encWiring encryptionwiring.WriteWiring,
	routeEngine *distribution.Engine,
	applyObservers ...kv.ApplyObserver,
) ([]*clusterbootstrap.GroupRuntime, map[uint64]*kv.ShardGroup, error) {
	// Defend the "cache is always non-nil" contract for callers that
	// pass a zero-value encryptionwiring.WriteWiring{} (the encryption-off
	// test harnesses). WithStateCache(nil) would otherwise make
	// NewApplier install a private per-applier cache, silently
	// breaking the shared-cache invariant 6D-6c-1 relies on.
	encWiring = encWiring.WithDefaultedCache()
	multi = noderuntime.EffectiveMultiDataDirs(groups, multi)
	bootstrapSpecs, bootstrapDomain, err := clusterbootstrap.LoadProtectedBootstrapDomain(raftID, raftDir, groups, multi, bootstrapCfg, encWiring)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	builder := shardGroupBuilder{
		raftID:                   raftID,
		raftDir:                  raftDir,
		multi:                    multi,
		bootstrap:                bootstrap,
		bootstrapCfg:             bootstrapCfg,
		factory:                  factory,
		proposalObserverForGroup: proposalObserverForGroup,
		clock:                    clock,
		readTracker:              readTracker,
		kekWrapper:               kekWrapper,
		keystore:                 keystore,
		sidecarPath:              sidecarPath,
		encWiring:                encWiring,
		routeEngine:              routeEngine,
		applyObservers:           applyObservers,
		bootstrapSpecs:           bootstrapSpecs,
	}
	runtimes := make([]*clusterbootstrap.GroupRuntime, 0, len(groups))
	shardGroups := make(map[uint64]*kv.ShardGroup, len(groups))
	for _, g := range groups {
		runtime, sg, err := builder.build(g)
		if err != nil {
			closeRaftGroupRuntimes(runtimes)
			if bootstrapDomain != nil {
				bootstrapDomain.Admission.Close()
			}
			return nil, nil, err
		}
		runtimes = append(runtimes, runtime)
		shardGroups[g.ID] = sg
		encWiring.AttachRaftEnvelopeGroup(g.ID, sg)
		if bootstrapDomain != nil {
			if err := bootstrapDomain.AttachEncryptionGroup(g.ID, sg); err != nil {
				closeRaftGroupRuntimes(runtimes)
				bootstrapDomain.Admission.Close()
				return nil, nil, errors.WithStack(err)
			}
		}
	}
	return runtimes, shardGroups, nil
}

type shardGroupBuilder struct {
	raftID                   string
	raftDir                  string
	multi                    bool
	bootstrap                bool
	bootstrapCfg             noderuntime.BootstrapConfig
	factory                  raftengine.Factory
	proposalObserverForGroup func(uint64) kv.ProposalObserver
	clock                    *kv.HLC
	readTracker              *kv.ActiveTimestampTracker
	kekWrapper               kek.Wrapper
	keystore                 *encryption.Keystore
	sidecarPath              string
	encWiring                encryptionwiring.WriteWiring
	routeEngine              *distribution.Engine
	applyObservers           []kv.ApplyObserver
	bootstrapSpecs           map[uint64]*clusterbootstrap.GroupBootstrapSpec
}

func (b shardGroupBuilder) build(group noderuntime.GroupSpec) (*clusterbootstrap.GroupRuntime, *kv.ShardGroup, error) {
	groupBootstrap, groupBootstrapServers, groupBootstrapSeed := noderuntime.BootstrapSettingsForGroup(b.bootstrapCfg, group.ID, b.bootstrap)
	observer := observerForGroup(b.proposalObserverForGroup, group.ID)
	if group.ID == noderuntime.DedicatedTSORaftGroupID {
		if spec := b.bootstrapSpecs[group.ID]; spec != nil {
			runtime, sg, err := clusterbootstrap.BuildProtectedDedicatedTSOGroup(b.raftID, group, b.raftDir, b.multi, groupBootstrap, b.bootstrapCfg.JoinAsLearner,
				groupBootstrapServers, groupBootstrapSeed, b.factory, b.clock, observer,
				spec, spec.Domain, clusterbootstrap.BootstrapPublicGRPCMethods(), b.encWiring.PebbleOptions()...)
			return runtime, sg, errors.WithStack(err)
		}
		if b.bootstrapCfg.PeerConfigDir != "" {
			return nil, nil, errors.New("protected timestamp group missing bootstrap domain configuration")
		}
		runtime, sg, err := buildDedicatedTSOGroup(
			b.raftID, group, b.raftDir, b.multi, groupBootstrap, b.bootstrapCfg.JoinAsLearner,
			groupBootstrapServers, groupBootstrapSeed,
			b.factory, b.clock, observer,
		)
		return runtime, sg, errors.Wrap(err, "failed to start dedicated TSO group")
	}
	return b.buildDataGroup(group, groupBootstrap, groupBootstrapServers, groupBootstrapSeed, observer)
}

func (b shardGroupBuilder) buildDataGroup(
	group noderuntime.GroupSpec,
	bootstrap bool,
	bootstrapServers []raftengine.Server,
	bootstrapSeed []raftengine.Server,
	proposalObserver kv.ProposalObserver,
) (*clusterbootstrap.GroupRuntime, *kv.ShardGroup, error) {
	dir := noderuntime.GroupDataDir(b.raftDir, b.raftID, group.ID, b.multi)
	bootstrapSpec, err := b.bootstrapSpecForGroup(group.ID)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create fsm store dir for group %d", group.ID)
	}
	st, err := store.NewPebbleStore(filepath.Join(dir, "fsm.db"), b.encWiring.PebbleOptions()...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open pebble fsm store for group %d", group.ID)
	}
	factory, err := clusterbootstrap.BootstrapFactoryForGroup(b.factory, bootstrapSpec, st)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.WithStack(err)
	}
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to construct writer registry for group %d", group.ID)
	}
	applierOpts := encryptionwiring.ApplierOptionsFor(b.kekWrapper, b.keystore, b.sidecarPath, b.encWiring)
	applier, err := encryption.NewApplier(reg, applierOpts...)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to construct encryption applier for group %d", group.ID)
	}
	sm := b.newDataGroupFSM(st, applier, group.ID, bootstrapSpec)
	inner, err := noderuntime.BuildRuntimeForGroup(
		b.raftID, group, b.raftDir, b.multi, bootstrap,
		bootstrapServers, bootstrapSeed,
		st, sm, factory, b.bootstrapCfg.JoinAsLearner,
	)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to start raft group %d", group.ID)
	}
	runtime := &clusterbootstrap.GroupRuntime{GroupRuntime: inner}
	// Every group goes through the wrap-aware proposer so HLC renewals and
	// transactional proposals observe raft-envelope cutovers uniformly.
	sg := &kv.ShardGroup{Engine: runtime.Engine, Store: st}
	if bootstrapSpec != nil {
		runtime.BootstrapDomain = bootstrapSpec.Domain
		runtime.BootstrapPeerConfig = bootstrapSpec.Config
	}
	sg.Txn = kv.NewLeaderProxyForShardGroup(sg, kv.WithProposalObserver(proposalObserver))
	return runtime, sg, nil
}

func (b shardGroupBuilder) newDataGroupFSM(st store.MVCCStore, applier *encryption.Applier, groupID uint64, bootstrapSpec *clusterbootstrap.GroupBootstrapSpec) raftengine.StateMachine {
	opts := fsmOptionsForGroup(applier, b.routeEngine, groupID, b.encWiring, b.applyObservers...)
	if bootstrapSpec != nil {
		opts = append(opts, kv.WithRaftBootstrapIdentity(bootstrapSpec.Config.Local.Cluster, groupID))
	}
	return kv.NewKvFSMWithHLCAndTracker(st, b.clock, b.readTracker, opts...)
}

func closeRaftGroupRuntimes(runtimes []*clusterbootstrap.GroupRuntime) {
	for _, runtime := range runtimes {
		runtime.Close()
	}
}

// buildDedicatedTSOGroup constructs group 0 with the minimal TSO state machine.
// It intentionally does not open an MVCC store: shard routing validation keeps
// user data out of group 0, while the state machine persists its ceiling,
// allocation floor, and one-way cutover marker in Raft snapshots. The
// ShardGroup still receives the normal proposer wrapper so lease renewals
// participate in raft-envelope cutovers exactly like data-group proposals.
func buildDedicatedTSOGroup(
	raftID string,
	group noderuntime.GroupSpec,
	baseDir string,
	multi bool,
	bootstrap bool,
	joinAsLearner bool,
	bootstrapServers []raftengine.Server,
	bootstrapSeed []raftengine.Server,
	factory raftengine.Factory,
	clock *kv.HLC,
	proposalObserver kv.ProposalObserver,
) (*clusterbootstrap.GroupRuntime, *kv.ShardGroup, error) {
	sm := kv.NewTSOStateMachine(clock)
	inner, err := noderuntime.BuildRuntimeForGroup(
		raftID, group, baseDir, multi, bootstrap,
		bootstrapServers, bootstrapSeed,
		nil, sm, factory, joinAsLearner,
	)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	runtime := &clusterbootstrap.GroupRuntime{GroupRuntime: inner}
	sg := &kv.ShardGroup{Engine: runtime.Engine, TSOState: sm}
	sg.Txn = kv.NewLeaderProxyForShardGroup(sg, kv.WithProposalObserver(proposalObserver))
	return runtime, sg, nil
}

func fsmOptionsForGroup(applier *encryption.Applier, routeEngine *distribution.Engine, groupID uint64, encWiring encryptionwiring.WriteWiring, applyObservers ...kv.ApplyObserver) []kv.FSMOption {
	opts := []kv.FSMOption{
		kv.WithEncryption(applier),
		kv.WithRouteHistory(kv.WrapDistributionEngine(routeEngine), groupID),
	}
	if encWiring.RaftEnvelope != nil {
		opts = append(opts, kv.WithCutoverSource(encWiring.RaftEnvelope))
	}
	for _, observer := range applyObservers {
		opts = append(opts, kv.WithApplyObserver(observer))
	}
	return opts
}

func observerForGroup(factory func(uint64) kv.ProposalObserver, groupID uint64) kv.ProposalObserver {
	if factory == nil {
		return nil
	}
	return factory(groupID)
}

// proposerForGroup returns the wrap-aware proposer for rt's shard
// group, falling back to the raw engine when shardGroups does not
// have an entry (test fixtures that construct a partial map).
// Centralises the Stage 6E-2c forwarded-write wiring so the
// Internal.Forward receive side and any future wrap-aware
// construction site share the same lookup shape: the leader's
// local LeaderProxy.Commit path is already wrap-aware via
// NewLeaderProxyForShardGroup; this helper extends the same
// guarantee to the Internal.Forward path that follower-forwarded
// writes ride (codex P1 round-1 r2).
func proposerForGroup(rt *clusterbootstrap.GroupRuntime, shardGroups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	return noderuntime.ProposerForGroup(rt.GroupRuntime, shardGroups)
}

func postCutoverProposerForRuntime(rt *clusterbootstrap.GroupRuntime, shardGroups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	return noderuntime.PostCutoverProposer(clusterbootstrap.RuntimeCore(rt), shardGroups)
}

// buildShardGroupsWithEncryptionWiring assembles the storage-envelope
// write wiring from the process flags and then constructs the shard
// groups with it. The Stage 6C-3 membership guards run here before
// encryptionwiring.BuildWriteWiring can bump sidecar local_epoch and before
// buildShardGroups can replay committed Raft entries into encrypted
// stores. The helper keeps run() to a single startup-fault branch.
func buildShardGroupsWithEncryptionWiring(
	raftID string,
	raftDir string,
	groups []noderuntime.GroupSpec,
	defaultGroup uint64,
	multi bool,
	bootstrap bool,
	bootstrapCfg noderuntime.BootstrapConfig,
	factory raftengine.Factory,
	proposalObserverForGroup func(uint64) kv.ProposalObserver,
	clock *kv.HLC,
	kekWrapper kek.Wrapper,
	keystore *encryption.Keystore,
	sidecarPath string,
	encryptionEnabled bool,
	readTracker *kv.ActiveTimestampTracker,
	routeEngine *distribution.Engine,
	encryptionObserver store.EncryptionObserver,
	applyObservers ...kv.ApplyObserver,
) ([]*clusterbootstrap.GroupRuntime, map[uint64]*kv.ShardGroup, encryptionwiring.WriteWiring, error) {
	if guardErr := encryptionwiring.CheckMembershipStartupGuardsBeforeEngine(encryptionwiring.MembershipStartupGuardInput{
		RaftID:            raftID,
		RaftDir:           raftDir,
		Groups:            groups,
		Multi:             multi,
		DefaultGroup:      defaultGroup,
		SidecarPath:       sidecarPath,
		EncryptionEnabled: encryptionEnabled,
		BootstrapServers:  bootstrapCfg.AdminSeed(defaultGroup),
	}); guardErr != nil {
		return nil, nil, encryptionwiring.WriteWiring{}, errors.WithStack(guardErr)
	}
	encWiring, err := encryptionwiring.BuildWriteWiring(encryptionEnabled, raftID, sidecarPath, kekWrapper, keystore, groups, encryptionObserver)
	if err != nil {
		return nil, nil, encryptionwiring.WriteWiring{}, errors.WithStack(err)
	}
	encryptionwiring.ConfigureRaftEnvelopeFactory(factory, encWiring)
	runtimes, shardGroups, err := buildShardGroups(raftID, raftDir, groups, multi, bootstrap, bootstrapCfg,
		factory, proposalObserverForGroup, clock, readTracker, kekWrapper, keystore, sidecarPath, encWiring,
		routeEngine, applyObservers...)
	if err != nil {
		return runtimes, shardGroups, encWiring, err
	}
	if guardErr := encryptionwiring.CheckLocalEpochRollbackAfterReplay(raftID, clusterbootstrap.RuntimeCores(runtimes), defaultGroup, sidecarPath, encryptionEnabled); guardErr != nil {
		for _, rt := range runtimes {
			if rt != nil {
				rt.Close()
			}
		}
		return nil, nil, encryptionwiring.WriteWiring{}, errors.WithStack(guardErr)
	}
	// Return the wiring (cache + bumped epoch) so run() can drive the
	// Stage 7a process-start registration after the shard stores open.
	return runtimes, shardGroups, encWiring, nil
}

func (b shardGroupBuilder) bootstrapSpecForGroup(id uint64) (*clusterbootstrap.GroupBootstrapSpec, error) {
	spec := b.bootstrapSpecs[id]
	if b.bootstrapCfg.PeerConfigDir != "" && (spec == nil || spec.Domain == nil) {
		return nil, errors.New("protected group missing the process-wide bootstrap domain")
	}
	return spec, nil
}

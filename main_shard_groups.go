package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func newRaftFactory(engineType raftEngineType, coldStartObs raftengine.ColdStartObserver) (raftengine.Factory, error) {
	switch engineType {
	case raftEngineEtcd:
		return etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
			TickInterval:      etcdTickInterval,
			HeartbeatTick:     durationToTicks(heartbeatTimeout, etcdTickInterval, etcdHeartbeatMinTicks),
			ElectionTick:      durationToTicks(electionTimeout, etcdTickInterval, etcdElectionMinTicks),
			MaxSizePerMsg:     etcdMaxSizePerMsg,
			MaxInflightMsg:    etcdMaxInflightMsg,
			ColdStartObserver: coldStartObs,
		}), nil
	default:
		return nil, errors.Wrapf(ErrUnsupportedRaftEngine, "%q", engineType)
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

func startRaftEngineLifecycleWatchers(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime) {
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		engine := rt.snapshotEngine()
		lifecycle, ok := engine.(raftengine.Lifecycle)
		if !ok {
			continue
		}
		done := lifecycle.Done()
		if done == nil {
			continue
		}
		groupID := rt.spec.ID
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
	encWiring encryptionWriteWiring,
	routeEngine *distribution.Engine,
	applyObservers ...kv.ApplyObserver,
) ([]*raftGroupRuntime, map[uint64]*kv.ShardGroup, error) {
	// Defend the "cache is always non-nil" contract for callers that
	// pass a zero-value encryptionWriteWiring{} (the encryption-off
	// test harnesses). WithStateCache(nil) would otherwise make
	// NewApplier install a private per-applier cache, silently
	// breaking the shared-cache invariant 6D-6c-1 relies on.
	encWiring = encWiring.withDefaultedCache()
	multi = noderuntime.EffectiveMultiDataDirs(groups, multi)
	bootstrapSpecs, bootstrapDomain, err := loadProtectedBootstrapDomain(raftID, raftDir, groups, multi, bootstrapCfg, encWiring)
	if err != nil {
		return nil, nil, err
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
	runtimes := make([]*raftGroupRuntime, 0, len(groups))
	shardGroups := make(map[uint64]*kv.ShardGroup, len(groups))
	for _, g := range groups {
		runtime, sg, err := builder.build(g)
		if err != nil {
			closeRaftGroupRuntimes(runtimes)
			if bootstrapDomain != nil {
				bootstrapDomain.admission.Close()
			}
			return nil, nil, err
		}
		runtimes = append(runtimes, runtime)
		shardGroups[g.ID] = sg
		encWiring.attachRaftEnvelopeGroup(g.ID, sg)
		if bootstrapDomain != nil {
			if err := bootstrapDomain.attachEncryptionGroup(g.ID, sg); err != nil {
				closeRaftGroupRuntimes(runtimes)
				bootstrapDomain.admission.Close()
				return nil, nil, err
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
	encWiring                encryptionWriteWiring
	routeEngine              *distribution.Engine
	applyObservers           []kv.ApplyObserver
	bootstrapSpecs           map[uint64]*groupBootstrapSpec
}

func (b shardGroupBuilder) build(group noderuntime.GroupSpec) (*raftGroupRuntime, *kv.ShardGroup, error) {
	groupBootstrap, groupBootstrapServers, groupBootstrapSeed := noderuntime.BootstrapSettingsForGroup(b.bootstrapCfg, group.ID, b.bootstrap)
	observer := observerForGroup(b.proposalObserverForGroup, group.ID)
	if group.ID == noderuntime.DedicatedTSORaftGroupID {
		if spec := b.bootstrapSpecs[group.ID]; spec != nil {
			return buildProtectedDedicatedTSOGroup(b.raftID, group, b.raftDir, b.multi, groupBootstrap, b.bootstrapCfg.JoinAsLearner,
				groupBootstrapServers, groupBootstrapSeed, b.factory, b.clock, observer,
				spec, spec.domain, bootstrapPublicGRPCMethods(), b.encWiring.pebbleOptions()...)
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
) (*raftGroupRuntime, *kv.ShardGroup, error) {
	dir := groupDataDir(b.raftDir, b.raftID, group.ID, b.multi)
	bootstrapSpec, err := b.bootstrapSpecForGroup(group.ID)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create fsm store dir for group %d", group.ID)
	}
	st, err := store.NewPebbleStore(filepath.Join(dir, "fsm.db"), b.encWiring.pebbleOptions()...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open pebble fsm store for group %d", group.ID)
	}
	factory, err := bootstrapFactoryForGroup(b.factory, bootstrapSpec, st)
	if err != nil {
		_ = st.Close()
		return nil, nil, err
	}
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to construct writer registry for group %d", group.ID)
	}
	applierOpts := encryptionApplierOptionsFor(b.kekWrapper, b.keystore, b.sidecarPath, b.encWiring)
	applier, err := encryption.NewApplier(reg, applierOpts...)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to construct encryption applier for group %d", group.ID)
	}
	sm := b.newDataGroupFSM(st, applier, group.ID, bootstrapSpec)
	runtime, err := buildRuntimeForGroup(
		b.raftID, group, b.raftDir, b.multi, bootstrap,
		bootstrapServers, bootstrapSeed,
		st, sm, factory, b.bootstrapCfg.JoinAsLearner,
	)
	if err != nil {
		_ = st.Close()
		return nil, nil, errors.Wrapf(err, "failed to start raft group %d", group.ID)
	}
	// Every group goes through the wrap-aware proposer so HLC renewals and
	// transactional proposals observe raft-envelope cutovers uniformly.
	sg := &kv.ShardGroup{Engine: runtime.engine, Store: st}
	if bootstrapSpec != nil {
		runtime.bootstrapDomain = bootstrapSpec.domain
		runtime.bootstrapPeerConfig = bootstrapSpec.config
	}
	sg.Txn = kv.NewLeaderProxyForShardGroup(sg, kv.WithProposalObserver(proposalObserver))
	return runtime, sg, nil
}

func (b shardGroupBuilder) newDataGroupFSM(st store.MVCCStore, applier *encryption.Applier, groupID uint64, bootstrapSpec *groupBootstrapSpec) raftengine.StateMachine {
	opts := fsmOptionsForGroup(applier, b.routeEngine, groupID, b.encWiring, b.applyObservers...)
	if bootstrapSpec != nil {
		opts = append(opts, kv.WithRaftBootstrapIdentity(bootstrapSpec.config.local.Cluster, groupID))
	}
	return kv.NewKvFSMWithHLCAndTracker(st, b.clock, b.readTracker, opts...)
}

func closeRaftGroupRuntimes(runtimes []*raftGroupRuntime) {
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
) (*raftGroupRuntime, *kv.ShardGroup, error) {
	sm := kv.NewTSOStateMachine(clock)
	runtime, err := buildRuntimeForGroup(
		raftID, group, baseDir, multi, bootstrap,
		bootstrapServers, bootstrapSeed,
		nil, sm, factory, joinAsLearner,
	)
	if err != nil {
		return nil, nil, err
	}
	sg := &kv.ShardGroup{Engine: runtime.engine, TSOState: sm}
	sg.Txn = kv.NewLeaderProxyForShardGroup(sg, kv.WithProposalObserver(proposalObserver))
	return runtime, sg, nil
}

func fsmOptionsForGroup(applier *encryption.Applier, routeEngine *distribution.Engine, groupID uint64, encWiring encryptionWriteWiring, applyObservers ...kv.ApplyObserver) []kv.FSMOption {
	opts := []kv.FSMOption{
		kv.WithEncryption(applier),
		kv.WithRouteHistory(kv.WrapDistributionEngine(routeEngine), groupID),
	}
	if encWiring.raftEnvelope != nil {
		opts = append(opts, kv.WithCutoverSource(encWiring.raftEnvelope))
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
func proposerForGroup(rt *raftGroupRuntime, shardGroups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	if sg, ok := shardGroups[rt.spec.ID]; ok && sg != nil {
		return sg.Proposer()
	}
	return rt.engine
}

func postCutoverProposerForRuntime(rt *raftGroupRuntime, shardGroups map[uint64]*kv.ShardGroup) raftengine.Proposer {
	if rt == nil {
		return nil
	}
	return proposerForGroup(rt, shardGroups)
}

func appliedIndexForEngine(engine raftengine.Engine) func() uint64 {
	applied, ok := engine.(interface{ AppliedIndex() uint64 })
	if !ok {
		return nil
	}
	return applied.AppliedIndex
}

package main

import (
	"context"
	"crypto"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type tsoEnablementNode struct {
	runtimes []*raftGroupRuntime
	groups   map[uint64]*kv.ShardGroup
	domain   *bootstrapDomain
	cluster  raftbootstrap.UUID
	data     *enablementProtectedNode
	wiring   coordinatorTSOWiring
	close    func()
}

type tsoEnablementState struct {
	allocation, pending, phaseDFloor uint64
	cutover, phaseD                  bool
	policies                         map[uint64]raftbootstrap.PolicyRecord
}

func tsoEnablementLegacyProfile(t *testing.T, base string, groups []noderuntime.GroupSpec) {
	t.Helper()
	clock := kv.NewHLC()
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	builder := shardGroupBuilder{raftID: "n1", raftDir: base, multi: true, bootstrap: true,
		factory: enablementFactory(), clock: clock, readTracker: kv.NewActiveTimestampTracker(), routeEngine: routes}
	data, dataShard, err := builder.build(groups[1])
	require.NoError(t, err)
	t.Cleanup(data.Close)
	tso, shard, err := builder.build(groups[0])
	require.NoError(t, err)
	t.Cleanup(tso.Close)
	for _, runtime := range []*raftGroupRuntime{tso, data} {
		require.Eventually(t, func() bool { return runtime.Engine.State() == raftengine.StateLeader }, enablementE2EWait, 10*time.Millisecond)
	}
	enablementLegacyPut(t, data.Engine, clock.Next(), "legacy-key", "original-value")
	catalog, err := setupDistributionCatalog(t.Context(), []*raftGroupRuntime{tso, data}, routes)
	require.NoError(t, err)
	image, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, image.Routes)
	shards := kv.NewShardStore(routes, map[uint64]*kv.ShardGroup{0: shard, 1: dataShard})
	allocator, err := kv.NewRaftTSOAllocator(shard, clock, kv.WithTSOCutoverFloorProvider(shards))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	reservation, err := allocator.ReserveBatchAfter(ctx, 3, 0, true, false)
	require.NoError(t, err)
	require.True(t, reservation.CutoverActive)
	require.NotZero(t, reservation.Base)
	enablementLegacyPut(t, data.Engine, clock.Next(), "cut-key", "quiesced")
	for _, runtime := range []*raftGroupRuntime{tso, data} {
		cut := runtime.Engine.Status().AppliedIndex
		require.Eventually(t, func() bool { return runtime.Engine.Status().LastSnapshotIndex >= cut }, enablementE2EWait, 10*time.Millisecond)
		runtime.Close()
	}
	tsoDir := noderuntime.GroupDataDir(base, "n1", 0, true)
	require.NoDirExists(t, filepath.Join(tsoDir, dedicatedTSOControlStoreName))
	require.NoDirExists(t, filepath.Join(tsoDir, "fsm.db"))
}

func tsoEnablementSignedBaseline(t *testing.T, dir string, cfg *bootstrapPeerConfig) raftbootstrap.BaselineManifest {
	t.Helper()
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, ownership.Close()) }()
	state, proof, err := probeOfflineTSOBaseline(t.Context(), dir, ownership)
	require.NoError(t, err)
	source, err := etcd.ReadOfflineBaselineWALSource(t.Context(), dir, ownership)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Snapshot.Close()) }()
	c := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
		Identity: raftbootstrap.Identity{Cluster: cfg.local.Cluster, Group: noderuntime.DedicatedTSORaftGroupID}, OperationID: raftbootstrap.OperationID{72},
		Configuration: raftbootstrap.Configuration{Index: source.ConfigurationIndex,
			State: raftbootstrap.ConfState{Voters: source.Configuration.GetVoters(), VotersOutgoing: source.Configuration.GetVotersOutgoing(),
				Learners: source.Configuration.GetLearners(), LearnersNext: source.Configuration.GetLearnersNext(), AutoLeave: source.Configuration.GetAutoLeave()},
			Members: []raftbootstrap.Member{{ID: cfg.local.Member, Address: cfg.local.Address, PublicKeyDigest: cfg.local.Key}}},
		Minimum:     raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		CommitIndex: state.AppliedIndex, CommitTerm: source.CommitTerm, Watermark: state.Watermark, DataDigest: state.DataDigest}
	inspection, err := etcd.InspectOfflineBaseline(t.Context(), etcd.OfflineBaselineOptions{Ownership: ownership, DataDir: dir,
		MemberID: cfg.local.Member, Expected: c, ProbeStore: func(context.Context) (etcd.BaselineStoreProof, error) { return proof, nil }})
	require.NoError(t, err)
	signer, ok := cfg.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c, Evidence: inspection.Evidence}, signer)
	require.NoError(t, err)
	return raftbootstrap.BaselineManifest{BaselineContext: c, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
}

func tsoEnablementStart(t *testing.T, base, peerConfigDir string, profile []noderuntime.GroupSpec) *tsoEnablementNode {
	t.Helper()
	bootstrapCfg := noderuntime.BootstrapConfig{PeerConfigDir: peerConfigDir}
	specs, domain, err := loadProtectedBootstrapDomain("n1", base, profile, true, bootstrapCfg)
	require.NoError(t, err)
	t.Cleanup(domain.admission.Close)
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	clock := kv.NewHLC()
	builder := shardGroupBuilder{raftID: "n1", raftDir: base, multi: true, factory: enablementFactory(), bootstrapCfg: bootstrapCfg,
		clock: clock, readTracker: kv.NewActiveTimestampTracker(), routeEngine: routes, bootstrapSpecs: specs}
	node := &tsoEnablementNode{domain: domain, cluster: specs[0].intent.Identity.Cluster, groups: make(map[uint64]*kv.ShardGroup)}
	for _, group := range profile {
		runtime, shard, err := builder.build(group)
		require.NoError(t, err, "group %d protected Open must use real migrated native proof", group.ID)
		t.Cleanup(runtime.Close)
		node.runtimes = append(node.runtimes, runtime)
		node.groups[group.ID] = shard
	}
	_, premature, err := domain.admission.Begin(t.Context(), raftadmission.Data)
	if premature != nil {
		premature.CancelAndJoin()
	}
	require.Error(t, err, "both groups and catalog must be ready before any user operation")
	shards := kv.NewShardStore(routes, node.groups)
	shards.SetOperationAdmission(domain.operationAdmission)
	coordinate := kv.NewShardedCoordinator(routes, node.groups, 1, clock, shards).WithOperationAdmission(domain.operationAdmission)
	node.wiring, err = configureCoordinatorTSO(defaultFlags[tsoFlags](), coordinate, node.groups, shards)
	require.NoError(t, err)
	node.data = &enablementProtectedNode{runtime: node.runtimes[1], domain: domain, coordinate: coordinate, shards: shards}
	ctx, cancel := context.WithCancel(t.Context())
	eg, runCtx := errgroup.WithContext(ctx)
	startBootstrapActivationWorkers(runCtx, eg, node.runtimes, node.groups)
	catalog := distributionCatalogStoreForGroup(node.runtimes, 1)
	startBootstrapReadinessReconciler(runCtx, eg, node.runtimes, catalog, routes)
	var once sync.Once
	node.close = func() {
		once.Do(func() {
			cancel()
			require.NoError(t, eg.Wait())
			require.NoError(t, node.wiring.Close())
			require.NoError(t, coordinate.Close())
			domain.admission.Close()
			for _, runtime := range node.runtimes {
				runtime.Close()
			}
		})
	}
	t.Cleanup(node.close)
	require.Eventually(t, func() bool {
		return node.runtimes[0].PublicReady() && node.runtimes[1].PublicReady()
	}, enablementE2EWait, 10*time.Millisecond, "both protected groups must reach DataReady without synthetic recovery publication")
	for _, group := range profile {
		require.Equal(t, peersession.DataReady, domain.admission.Stage(group.ID))
	}
	return node
}

func tsoEnablementCapture(t *testing.T, node *tsoEnablementNode) tsoEnablementState {
	t.Helper()
	state := node.groups[0].TSOState
	image := tsoEnablementState{allocation: state.AllocationFloor(), pending: state.PendingPhaseDFloor(),
		cutover: state.CutoverActive(), phaseD: state.PhaseDActive(), phaseDFloor: state.PhaseDFloor(),
		policies: make(map[uint64]raftbootstrap.PolicyRecord)}
	for _, runtime := range node.runtimes {
		policy, err := kv.LoadAppliedBootstrapPolicy(t.Context(), runtime.Store,
			raftbootstrap.Identity{Cluster: node.cluster, Group: runtime.Spec.ID}, runtime.Engine.Status().AppliedIndex)
		require.NoError(t, err)
		image.policies[runtime.Spec.ID] = policy
	}
	return image
}

func tsoEnablementAllocate(t *testing.T, node *tsoEnablementNode, min uint64, activatePhaseD bool) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	owned, finish, err := node.domain.operationAdmission(ctx, 0, kv.AdmissionAllocator)
	require.NoError(t, err)
	allocator, ok := node.wiring.serverAllocator.(kv.TSOReservationAllocator)
	require.True(t, ok)
	reservation, err := allocator.ReserveBatchAfter(owned, 1, min, false, activatePhaseD)
	require.NoError(t, finish(err))
	ts := reservation.Base
	require.Greater(t, ts, min)
	return ts
}

// prepareTSOEnablementProfile returns the data base dir, the group profile,
// the TSO baseline manifest and the prepared peer config dir.
func prepareTSOEnablementProfile(t *testing.T) (string, []noderuntime.GroupSpec, raftbootstrap.BaselineManifest, string) {
	t.Helper()
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	base := t.TempDir()
	raw, _ := enablementPeerConfig(t)
	raw.Members[0].Groups = []uint64{0, 1}
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	cfg, err := loadBootstrapFixture(t, encoded)
	require.NoError(t, err)
	profile := []noderuntime.GroupSpec{{ID: 0, Address: cfg.local.Address}, {ID: 1, Address: cfg.local.Address}}
	tsoEnablementLegacyProfile(t, base, profile)
	t.Log("legacy profile quiesced with populated data and snapshot-only dedicated TSO")
	tsoManifest := tsoEnablementSignedBaseline(t, noderuntime.GroupDataDir(base, "n1", 0, true), cfg)
	dataManifest := enablementSignedBaseline(t, noderuntime.GroupDataDir(base, "n1", 1, true), cfg, profile[1])
	configDir := t.TempDir()
	for _, manifest := range []raftbootstrap.BaselineManifest{tsoManifest, dataManifest} {
		dir := noderuntime.GroupDataDir(base, "n1", manifest.Identity.Group, true)
		payload := enablementMigrateAndCertify(t, dir, cfg, manifest)
		t.Logf("group %d migrated to bound with required marker, native v2 and signed final readiness", manifest.Identity.Group)
		local := raw
		local.ActivationFile = filepath.Join(configDir, fmt.Sprintf("activation-%d.bin", manifest.Identity.Group))
		require.NoError(t, os.WriteFile(local.ActivationFile, payload, 0o600))
		encoded, err := json.Marshal(local)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(configDir, fmt.Sprintf("group-%d.json", manifest.Identity.Group)), encoded, 0o600))
	}
	return base, profile, tsoManifest, configDir
}

func TestBootstrapEnablementLegacyTSOAndDataProfile(t *testing.T) {
	base, profile, tsoManifest, configDir := prepareTSOEnablementProfile(t)
	// Keep the original native legacy snapshot across protected writes/restart:
	// success must come from exact suffix proof, not a convenient new snapshot.
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "100000")
	node := tsoEnablementStart(t, base, configDir, profile)
	enablementRead(t, node.data, "legacy-key", "original-value")
	ts := tsoEnablementAllocate(t, node, tsoManifest.Watermark, false)
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	_, err := node.data.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put,
		Key: []byte("protected-key"), Value: []byte("durable-value")}}})
	require.NoError(t, err)
	enablementRead(t, node.data, "protected-key", "durable-value")
	// Drive pending/seal/prepare/finish through the actual data-group gate,
	// rather than installing scalar markers or a successful gate double.
	ts = tsoEnablementAllocate(t, node, ts, true)
	before := tsoEnablementCapture(t, node)
	require.GreaterOrEqual(t, before.allocation, ts)
	require.True(t, before.cutover)
	require.True(t, before.phaseD)
	require.NotZero(t, before.phaseDFloor)
	require.Zero(t, before.pending, "completed real Phase-D protocol must clear pending before restart")
	node.close()
	assertTSOMissingReceiptRefusesRecovery(t, base, configDir, profile)
	restarted := tsoEnablementStart(t, base, configDir, profile)
	after := tsoEnablementCapture(t, restarted)
	require.GreaterOrEqual(t, after.allocation, before.allocation)
	require.Equal(t, before.cutover, after.cutover)
	require.Equal(t, before.pending, after.pending)
	require.Equal(t, before.phaseD, after.phaseD)
	require.Equal(t, before.phaseDFloor, after.phaseDFloor)
	require.Equal(t, before.policies, after.policies)
	enablementRead(t, restarted.data, "legacy-key", "original-value")
	enablementRead(t, restarted.data, "protected-key", "durable-value")
	tsoEnablementAllocate(t, restarted, before.allocation, false)
}

package main

import (
	"context"
	"crypto"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

const enablementE2EWait = 15 * time.Second

func enablementFactory() *etcd.Factory {
	return etcd.NewFactory(etcd.FactoryConfig{TickInterval: 20 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10})
}

func enablementPeerConfig(t *testing.T) (bootstrapPeerJSON, *bootstrapPeerConfig) {
	t.Helper()
	raw := bootstrapConfigFixture(t)
	raw.Local = etcd.DeriveNodeID("n1")
	raw.Members[0].ID = raw.Local
	raw.StartupTokenFile = filepath.Join(t.TempDir(), "startup-token")
	require.NoError(t, os.WriteFile(raw.StartupTokenFile, []byte("enablement-e2e-startup-token"), 0o600))
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	cfg, err := loadBootstrapFixture(t, encoded)
	require.NoError(t, err)
	return raw, cfg
}

func enablementLegacyCut(t *testing.T, base string, group groupSpec) string {
	t.Helper()
	clock := kv.NewHLC()
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, group.id)
	builder := shardGroupBuilder{raftID: "n1", raftDir: base, bootstrap: true, factory: enablementFactory(),
		clock: clock, readTracker: kv.NewActiveTimestampTracker(), routeEngine: routes}
	runtime, _, err := builder.build(group)
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	require.Eventually(t, func() bool { return runtime.engine.State() == raftengine.StateLeader }, enablementE2EWait, 10*time.Millisecond)
	enablementLegacyPut(t, runtime.engine, clock.Next(), "legacy-key", "original-value")
	catalog, err := setupDistributionCatalog(t.Context(), []*raftGroupRuntime{runtime}, routes)
	require.NoError(t, err)
	snapshot, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.NotZero(t, snapshot.Version)
	require.Len(t, snapshot.Routes, 1)
	// A real proposal after legacy catalog initialization snapshots the complete
	// populated store. No fabricated applied index or native recovery image.
	enablementLegacyPut(t, runtime.engine, clock.Next(), "cut-key", "quiesced")
	cut := runtime.engine.Status().AppliedIndex
	require.Eventually(t, func() bool { return runtime.engine.Status().LastSnapshotIndex >= cut }, enablementE2EWait, 10*time.Millisecond)
	runtime.Close()
	return groupDataDir(base, "n1", group.id, false)
}

func enablementLegacyPut(t *testing.T, engine raftengine.Engine, ts uint64, key, value string) {
	t.Helper()
	payload, err := proto.Marshal(&pb.Request{Ts: ts, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(key), Value: []byte(value)}}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	_, err = engine.Propose(ctx, payload)
	require.NoError(t, err)
}

func enablementSignedBaseline(t *testing.T, dir string, cfg *bootstrapPeerConfig, group groupSpec) raftbootstrap.BaselineManifest {
	t.Helper()
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, ownership.Close()) }()
	source, err := etcd.ReadOfflineBaselineWALSource(t.Context(), dir, ownership)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Snapshot.Close()) }()
	proof, err := bootstrapOfflineDataStoreProbe(t.Context(), dir)
	require.NoError(t, err)
	require.Equal(t, source.CommitIndex, proof.AppliedIndex)
	require.Equal(t, source.CommitIndex, source.LastLogIndex, "all members must remain quiesced")
	configuration := raftbootstrap.Configuration{Index: source.ConfigurationIndex,
		State: raftbootstrap.ConfState{Voters: source.Configuration.GetVoters(), VotersOutgoing: source.Configuration.GetVotersOutgoing(),
			Learners: source.Configuration.GetLearners(), LearnersNext: source.Configuration.GetLearnersNext(), AutoLeave: source.Configuration.GetAutoLeave()},
		Members: []raftbootstrap.Member{{ID: cfg.local.Member, Address: cfg.local.Address, PublicKeyDigest: cfg.local.Key}}}
	cut := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
		Identity: raftbootstrap.Identity{Cluster: cfg.local.Cluster, Group: group.id}, OperationID: raftbootstrap.OperationID{71},
		Configuration: configuration, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		CommitIndex: source.CommitIndex, CommitTerm: source.CommitTerm, Watermark: proof.Watermark, DataDigest: proof.DataDigest}
	inspection, err := etcd.InspectOfflineBaseline(t.Context(), etcd.OfflineBaselineOptions{Ownership: ownership, DataDir: dir,
		MemberID: cfg.local.Member, Expected: cut, ProbeStore: func(ctx context.Context) (etcd.BaselineStoreProof, error) {
			return bootstrapOfflineDataStoreProbe(ctx, dir)
		}})
	require.NoError(t, err)
	signer, ok := cfg.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: cut, Evidence: inspection.Evidence}, signer)
	require.NoError(t, err)
	return raftbootstrap.BaselineManifest{BaselineContext: cut, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
}

func enablementMigrateAndCertify(t *testing.T, dir string, cfg *bootstrapPeerConfig, manifest raftbootstrap.BaselineManifest) []byte {
	t.Helper()
	before := offlineDataFiles(t, dir)
	request := bootstrapBaselineCLIRequest{Command: "prepare", RaftDataDir: dir, MemberID: cfg.local.Member, OperationID: manifest.OperationID}
	prepared, err := executeBootstrapMigration(t.Context(), request, manifest.BaselineContext, manifest)
	require.NoError(t, err)
	require.Equal(t, "prepared", prepared.Phase)
	require.Equal(t, before, offlineDataFiles(t, prepared.BackupDir), "backup must preserve every original file exactly")
	backup := offlineDataFiles(t, prepared.BackupDir)
	request.Command = "resume"
	bound, err := executeBootstrapMigration(t.Context(), request, manifest.BaselineContext, manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", bound.Phase)
	again, err := executeBootstrapMigration(t.Context(), request, manifest.BaselineContext, manifest)
	require.NoError(t, err)
	require.Equal(t, bound, again)
	require.Equal(t, backup, offlineDataFiles(t, bound.BackupDir))
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, ownership.Close()) }()
	options, err := bootstrapOfflineOptions(request, manifest.BaselineContext, manifest, ownership)
	require.NoError(t, err)
	evidence, err := etcd.VerifyOfflineBaselinePrepared(t.Context(), options, manifest)
	require.NoError(t, err, "ready ACK must follow final native/marker/store verification")
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	require.Equal(t, digest, evidence.Identity.PreparationDigest, "exact WAL-v2 identity binds local preparation")
	require.NotZero(t, evidence.DurableProofDigest)
	inspection, err := raftbootstrap.NewStore(dir).Inspect(raftbootstrap.Evidence{Identity: &manifest.Identity})
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.MarkerRequired, inspection.Marker)
	require.NotNil(t, inspection.Intent)
	require.Equal(t, raftbootstrap.Floor{}, inspection.Intent.Floor)
	ack, err := signBootstrapPreparedAck(cfg, manifest, cfg.local.Member, evidence.DurableProofDigest)
	require.NoError(t, err)
	encoded, err := marshalBootstrapPreparedAck(ack, manifest)
	require.NoError(t, err)
	payload, err := assembleBootstrapActivation(manifest, manifest.BaselineContext, [][]byte{encoded})
	require.NoError(t, err)
	return payload
}

type enablementProtectedNode struct {
	runtime    *raftGroupRuntime
	domain     *bootstrapDomain
	coordinate *kv.ShardedCoordinator
	shards     *kv.ShardStore
	close      func()
}

func enablementStartProtected(t *testing.T, base string, group groupSpec) *enablementProtectedNode {
	t.Helper()
	specs, domain, err := loadProtectedBootstrapDomain("n1", base, []groupSpec{group}, false, raftBootstrapConfig{})
	require.NoError(t, err)
	t.Cleanup(domain.admission.Close)
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, group.id)
	clock := kv.NewHLC()
	builder := shardGroupBuilder{raftID: "n1", raftDir: base, factory: enablementFactory(),
		clock: clock, readTracker: kv.NewActiveTimestampTracker(), routeEngine: routes, bootstrapSpecs: specs}
	runtime, shard, err := builder.build(group)
	require.NoError(t, err, "protected Open must recover from the real prepared native image")
	t.Cleanup(runtime.Close)
	// No data root may enter before the catalog-loaded publication. This also
	// applies on restart with retained P; native identity alone is insufficient.
	for _, kind := range []raftadmission.Kind{raftadmission.Data, raftadmission.Maintenance} {
		_, op, err := domain.admission.Begin(t.Context(), kind)
		if op != nil {
			op.CancelAndJoin()
		}
		require.Error(t, err, "process-wide DataReady is required before target routing")
	}
	groups := map[uint64]*kv.ShardGroup{group.id: shard}
	shards := kv.NewShardStore(routes, groups)
	shards.SetOperationAdmission(domain.operationAdmission)
	coordinate := kv.NewShardedCoordinator(routes, groups, group.id, clock, shards).WithOperationAdmission(domain.operationAdmission)
	ctx, cancel := context.WithCancel(t.Context())
	eg, runCtx := errgroup.WithContext(ctx)
	startBootstrapActivationWorkers(runCtx, eg, []*raftGroupRuntime{runtime}, groups)
	catalog := distributionCatalogStoreForGroup([]*raftGroupRuntime{runtime}, group.id)
	startBootstrapReadinessReconciler(runCtx, eg, []*raftGroupRuntime{runtime}, catalog, routes)
	var once sync.Once
	node := &enablementProtectedNode{runtime: runtime, domain: domain, coordinate: coordinate, shards: shards}
	node.close = func() {
		once.Do(func() {
			cancel()
			require.NoError(t, eg.Wait())
			require.NoError(t, coordinate.Close())
			domain.admission.Close()
			runtime.Close()
		})
	}
	t.Cleanup(node.close)
	require.Eventually(t, func() bool { return runtime.publicReady() }, enablementE2EWait, 10*time.Millisecond,
		"activation/recovery/catalog must reach DataReady without synthetic publication")
	require.Equal(t, peersession.DataReady, domain.admission.Stage(group.id))
	snapshot, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.NotZero(t, snapshot.Version)
	require.Len(t, snapshot.Routes, 1)
	return node
}

func enablementRead(t *testing.T, node *enablementProtectedNode, key, value string) {
	t.Helper()
	// Match adapter direct reads: root ownership precedes target routing and
	// depends on ALL local protected groups. Never refresh a stale owner here.
	ctx, op, err := node.domain.admission.Begin(t.Context(), raftadmission.Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	got, err := node.shards.GetAt(ctx, []byte(key), ^uint64(0))
	err = finishBootstrapIngress(op, err)
	require.NoError(t, err)
	require.Equal(t, value, string(got))
}

func TestBootstrapEnablementLegacyDataToProtectedDataReady(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	require.Empty(t, *raftBootstrapPeerConfigDir)
	base := t.TempDir()
	raw, cfg := enablementPeerConfig(t)
	group := groupSpec{id: 1, address: cfg.local.Address}
	dir := enablementLegacyCut(t, base, group)
	manifest := enablementSignedBaseline(t, dir, cfg, group)
	payload := enablementMigrateAndCertify(t, dir, cfg, manifest)
	configDir := t.TempDir()
	raw.ActivationFile = filepath.Join(configDir, "activation.bin")
	require.NoError(t, os.WriteFile(raw.ActivationFile, payload, 0o600))
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "group-1.json"), encoded, 0o600))
	old := *raftBootstrapPeerConfigDir
	*raftBootstrapPeerConfigDir = configDir
	t.Cleanup(func() { *raftBootstrapPeerConfigDir = old })
	node := enablementStartProtected(t, base, group)
	enablementRead(t, node, "legacy-key", "original-value")
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	_, err = node.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("protected-key"), Value: []byte("durable-value")}}})
	require.NoError(t, err)
	enablementRead(t, node, "protected-key", "durable-value")
	policy, err := kv.LoadAppliedBootstrapPolicy(t.Context(), node.runtime.store, manifest.Identity, node.runtime.engine.Status().AppliedIndex)
	require.NoError(t, err)
	require.Greater(t, policy.ActivationIndex, manifest.CommitIndex)
	require.Equal(t, uint64(1), policy.Generation)
	node.close()
	restarted := enablementStartProtected(t, base, group)
	enablementRead(t, restarted, "legacy-key", "original-value")
	enablementRead(t, restarted, "protected-key", "durable-value")
	recovered, err := kv.LoadAppliedBootstrapPolicy(t.Context(), restarted.runtime.store, manifest.Identity, restarted.runtime.engine.Status().AppliedIndex)
	require.NoError(t, err)
	require.Equal(t, policy.ActivationIndex, recovered.ActivationIndex, "restart must not reactivate or downgrade P")
	require.Equal(t, policy.Generation, recovered.Generation)
}

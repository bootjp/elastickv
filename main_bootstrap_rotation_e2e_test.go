package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// This fixture is a real prepared plaintext cut with durable keys/registry but
// inactive envelopes. Protected startup must consume the ordinary domain,
// projection and supervisor wiring; tests must not synthesize admission state.
type bootstrapRotationE2EFixture struct {
	base, dir, configDir, sidecarPath string
	group                             noderuntime.GroupSpec
	config                            *bootstrapPeerConfig
	manifest                          raftbootstrap.BaselineManifest
	activation                        []byte
	wrapper                           *kek.FileWrapper
	storageWrapped, raftWrapped       []byte
	activeEnvelopes                   bool
	listener                          net.Listener
}

func newBootstrapRotationE2EFixture(t *testing.T, active ...bool) *bootstrapRotationE2EFixture {
	t.Helper()
	raw, _ := enablementPeerConfig(t)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	raw.Members[0].Address = net.JoinHostPort("localhost", port)
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	cfg, err := loadBootstrapFixture(t, encoded)
	require.NoError(t, err)
	f := &bootstrapRotationE2EFixture{base: t.TempDir(), configDir: t.TempDir(), config: cfg,
		group: noderuntime.GroupSpec{ID: 1, Address: cfg.local.Address}, listener: listener, activeEnvelopes: len(active) != 0 && active[0]}
	f.dir = noderuntime.GroupDataDir(f.base, "n1", f.group.ID, false)
	f.sidecarPath = filepath.Join(f.dir, "encryption", "keys.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(f.sidecarPath), 0o700))
	kekPath := filepath.Join(t.TempDir(), "kek")
	key := make([]byte, encryption.KeySize)
	_, err = rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(kekPath, key, 0o600))
	zeroBytes(key)
	f.wrapper, err = kek.NewFileWrapper(kekPath)
	require.NoError(t, err)
	f.prepareLegacyCut(t)
	f.manifest = enablementSignedBaseline(t, f.dir, f.config, f.group)
	f.activation = enablementMigrateAndCertify(t, f.dir, f.config, f.manifest)
	raw.ActivationFile = filepath.Join(f.configDir, "activation.bin")
	require.NoError(t, os.WriteFile(raw.ActivationFile, f.activation, 0o600))
	encoded, err = json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(f.configDir, "group-1.json"), encoded, 0o600))
	return f
}

func (f *bootstrapRotationE2EFixture) prepareLegacyCut(t *testing.T) {
	t.Helper()
	clock := kv.NewHLC()
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, f.group.ID)
	keystore := encryption.NewKeystore()
	wiring, err := buildEncryptionWriteWiring(true, "n1", f.sidecarPath, f.wrapper, keystore, []noderuntime.GroupSpec{f.group}, nil)
	require.NoError(t, err)
	factory := enablementFactory()
	configureRaftEnvelopeFactory(factory, wiring)
	builder := shardGroupBuilder{raftID: "n1", raftDir: f.base, bootstrap: true, factory: factory,
		clock: clock, readTracker: kv.NewActiveTimestampTracker(), routeEngine: routes,
		kekWrapper: f.wrapper, keystore: keystore, sidecarPath: f.sidecarPath, encWiring: wiring}
	runtime, shard, err := builder.build(f.group)
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	wiring.attachRaftEnvelopeGroup(f.group.ID, shard)
	require.Eventually(t, func() bool { return runtime.Engine.State() == raftengine.StateLeader }, enablementE2EWait, 10*time.Millisecond)
	enablementLegacyPut(t, runtime.Engine, clock.Next(), "legacy-key", "original-value")
	catalog, err := setupDistributionCatalog(t.Context(), []*raftGroupRuntime{runtime}, routes)
	require.NoError(t, err)
	snapshot, err := catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.NotZero(t, snapshot.Version)
	f.bootstrapKeys(t, runtime, shard, wiring, keystore)
	// A real user proposal snapshots the committed registry/checkpoint alongside
	// plaintext data. No applier calls or fabricated indices seed this fixture.
	if f.activeEnvelopes {
		f.enableEnvelopes(t, runtime, shard, wiring)
	} else {
		enablementLegacyPut(t, runtime.Engine, clock.Next(), "cut-key", "quiesced")
	}
	cut := runtime.Engine.Status().AppliedIndex
	require.Eventually(t, func() bool { return runtime.Engine.Status().LastSnapshotIndex >= cut }, enablementE2EWait, 10*time.Millisecond)
	runtime.Close()
	_ = f.listener.Close()
}

func (f *bootstrapRotationE2EFixture) bootstrapKeys(t *testing.T, runtime *raftGroupRuntime, shard *kv.ShardGroup, wiring encryptionWriteWiring, ks *encryption.Keystore) {
	t.Helper()
	registry, err := store.WriterRegistryFor(runtime.Store)
	require.NoError(t, err)
	server := adapter.NewEncryptionAdminServer(
		adapter.WithEncryptionAdminSidecarPath(f.sidecarPath),
		adapter.WithEncryptionAdminFullNodeID(f.config.local.Member),
		adapter.WithEncryptionAdminProposer(runtime.Engine),
		adapter.WithEncryptionAdminPostCutoverProposer(shard.Proposer()),
		adapter.WithEncryptionAdminLeaderView(runtime.Engine))
	require.NoError(t, server.Validate())
	f.storageWrapped, err = wrapFreshStartupDEK(f.wrapper)
	require.NoError(t, err)
	f.raftWrapped, err = wrapFreshStartupDEK(f.wrapper)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	response, err := server.BootstrapEncryption(ctx, &pb.BootstrapEncryptionRequest{
		StorageDekId: e2eStorageDEKID, RaftDekId: e2eRaftDEKID,
		WrappedStorageDek: f.storageWrapped, WrappedRaftDek: f.raftWrapped,
		WriterBatch: []*pb.WriterRegistryEntry{{FullNodeId: f.config.local.Member, LocalEpoch: uint32(wiring.epoch)}},
	})
	require.NoError(t, err)
	require.NotZero(t, response.AppliedIndex)
	fence, err := runtime.Engine.LinearizableRead(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, fence, response.AppliedIndex)
	require.Eventually(t, func() bool { return runtime.Engine.Status().AppliedIndex >= response.AppliedIndex }, enablementE2EWait, time.Millisecond)
	for _, id := range []uint32{e2eStorageDEKID, e2eRaftDEKID} {
		raw, present, err := registry.GetRegistryRow(encryption.RegistryKey(id, encryption.NodeID16(f.config.local.Member)))
		require.NoError(t, err)
		require.True(t, present)
		row, err := encryption.DecodeRegistryValue(raw)
		require.NoError(t, err)
		require.Equal(t, f.config.local.Member, row.FullNodeID)
		require.Zero(t, row.LastSeenLocalEpoch)
		dek, loaded := ks.DEK(id)
		require.True(t, loaded, "actual committed apply must hydrate the live keystore")
		wrapped := f.storageWrapped
		if id == e2eRaftDEKID {
			wrapped = f.raftWrapped
		}
		expected, err := f.wrapper.Unwrap(wrapped)
		require.NoError(t, err)
		require.Equal(t, expected, dek[:])
		zeroBytes(expected)
	}
	f.assertInactiveKeys(t)
	sc, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	expected, err := encryption.SidecarCheckpoint(sc)
	require.NoError(t, err)
	checkpoints, ok := runtime.Store.(bootstrapEncryptionProjectionStore)
	require.True(t, ok)
	checkpoint, present, err := checkpoints.LoadEncryptionSnapshotCheckpoint()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, expected, checkpoint, "committed sidecar and durable Pebble checkpoint must agree")
}

func (f *bootstrapRotationE2EFixture) enableEnvelopes(t *testing.T, runtime *raftGroupRuntime, shard *kv.ShardGroup, wiring encryptionWriteWiring) {
	t.Helper()
	installRaftRegistrationVerifier(shard, wiring, "n1")
	cache := kv.NewGRPCConnCache(nil)
	defer func() { require.NoError(t, cache.Close()) }()
	fanout := buildCapabilityFanoutFn(runtimeCores([]*raftGroupRuntime{runtime}), cache, capabilityFanoutTimeout)
	server := adapter.NewEncryptionAdminServer(
		adapter.WithEncryptionAdminSidecarPath(f.sidecarPath), adapter.WithEncryptionAdminFullNodeID(f.config.local.Member),
		adapter.WithEncryptionAdminProposer(runtime.Engine), adapter.WithEncryptionAdminPostCutoverProposer(shard.Proposer()),
		adapter.WithEncryptionAdminLeaderView(runtime.Engine), adapter.WithEncryptionAdminCapabilityFanout(fanout),
		adapter.WithEncryptionAdminLatestAppliedIndex(func() uint64 { return runtime.Engine.Status().AppliedIndex }),
		adapter.WithEncryptionAdminCutoverBarrier(wiring.raftEnvelope.barrier()))
	require.NoError(t, server.Validate())
	grpcServer := grpc.NewServer()
	runtime.registerGRPC(grpcServer)
	pb.RegisterEncryptionAdminServer(grpcServer, server)
	served := make(chan error, 1)
	go func() { served <- grpcServer.Serve(f.listener) }()
	defer func() { grpcServer.Stop(); require.NoError(t, <-served) }()
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	_, err := server.EnableStorageEnvelope(ctx, &pb.EnableStorageEnvelopeRequest{ProposerNodeId: f.config.local.Member, ProposerLocalEpoch: uint32(wiring.epoch)})
	require.NoError(t, err)
	_, err = server.EnableRaftEnvelope(ctx, &pb.EnableRaftEnvelopeRequest{ProposerNodeId: f.config.local.Member, ProposerLocalEpoch: uint32(wiring.raftEpoch)})
	require.NoError(t, err)
	sc, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	require.True(t, sc.StorageEnvelopeActive)
	require.NotZero(t, sc.RaftEnvelopeCutoverIndex)
	require.NotNil(t, shard.RaftPayloadWrap())
}

func (f *bootstrapRotationE2EFixture) assertInactiveKeys(t *testing.T) {
	t.Helper()
	sc, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	require.Equal(t, encryption.ActiveKeys{Storage: e2eStorageDEKID, Raft: e2eRaftDEKID}, sc.Active)
	require.False(t, sc.StorageEnvelopeActive)
	require.Zero(t, sc.RaftEnvelopeCutoverIndex)
	require.True(t, bytes.Equal(f.storageWrapped, sc.Keys["7"].Wrapped))
	require.True(t, bytes.Equal(f.raftWrapped, sc.Keys["8"].Wrapped))
	require.NotZero(t, sc.RaftAppliedIndex)
}

func TestBootstrapRotationE2EPreparedInactiveKeys(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t)
	f.assertInactiveKeys(t)
	command, matched, err := raftbootstrap.ParseActivationCommand(f.activation)
	require.NoError(t, err)
	require.True(t, matched)
	require.NoError(t, raftbootstrap.VerifyPreparedCertificate(command.Readiness, f.manifest))
	multiEnablementNoPolicy(t, &multiEnablementIdentity{dir: f.dir}, f.manifest.Identity)
	plain, err := store.NewPebbleStore(filepath.Join(f.dir, "fsm.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, plain.Close()) })
	value, err := plain.GetAt(t.Context(), []byte("legacy-key"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, "original-value", string(value), "baseline is genuinely plaintext despite committed inactive DEKs")
}

func TestBootstrapRotationE2EPreparedActiveEnvelopes(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	sc, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	require.True(t, sc.StorageEnvelopeActive)
	require.NotZero(t, sc.RaftEnvelopeCutoverIndex)
	command, matched, err := raftbootstrap.ParseActivationCommand(f.activation)
	require.NoError(t, err)
	require.True(t, matched)
	require.NoError(t, raftbootstrap.VerifyPreparedCertificate(command.Readiness, f.manifest))
	multiEnablementNoPolicy(t, &multiEnablementIdentity{dir: f.dir}, f.manifest.Identity)
}

type bootstrapRotationE2ENode struct {
	runtime         *raftGroupRuntime
	domain          *bootstrapDomain
	coordinate      *kv.ShardedCoordinator
	shards          *kv.ShardStore
	wiring          encryptionWriteWiring
	registrationCtx context.Context
	group           *kv.ShardGroup
	stopReadiness   func()
	supervisor      *bootstrapRotationRuntime
	close           func()
}

func (f *bootstrapRotationE2EFixture) protected(t *testing.T) *bootstrapRotationE2ENode {
	t.Helper()
	clock, routes := kv.NewHLC(), distribution.NewEngine()
	routes.UpdateRoute(nil, nil, f.group.ID)
	runtimes, groups, wiring, err := buildShardGroupsWithEncryptionWiring("n1", f.base, []noderuntime.GroupSpec{f.group}, 1, false, false,
		noderuntime.BootstrapConfig{PeerConfigDir: f.configDir, Encryption: noderuntime.BootstrapEncryptionRequest{Configured: true, RotateOnStartup: true}},
		enablementFactory(), nil, clock, f.wrapper, encryption.NewKeystore(), f.sidecarPath, true,
		kv.NewActiveTimestampTracker(), routes, nil)
	require.NoError(t, err)
	runtime := runtimes[0]
	domain := runtime.bootstrapDomain
	t.Cleanup(runtime.Close)
	ctx, cancel := context.WithCancel(t.Context())
	eg, ctx := errgroup.WithContext(ctx)
	var cleanup internalutil.CleanupStack
	shards := kv.NewShardStore(routes, groups)
	shards.SetOperationAdmission(domain.operationAdmission)
	coordinate := kv.NewShardedCoordinator(routes, groups, 1, clock, shards).WithOperationAdmission(domain.operationAdmission)
	node := &bootstrapRotationE2ENode{runtime: runtime, domain: domain, coordinate: coordinate, shards: shards, wiring: wiring}
	var once sync.Once
	var server *grpc.Server
	var served chan error
	node.close = func() {
		once.Do(func() {
			cancel()
			if server != nil {
				server.Stop()
				require.NoError(t, <-served)
			}
			cleanup.Run()
			require.NoError(t, eg.Wait())
			require.NoError(t, coordinate.Close())
			domain.admission.Close()
			runtime.Close()
		})
	}
	t.Cleanup(node.close)
	network, err := configureBootstrapNetwork(ctx, runtimes, shards, groups, &cleanup)
	require.NoError(t, err)
	registered, supervisor, err := configureBootstrapEncryptionStartup(ctx, bootstrapEncryptionStartupConfig{
		runtimes: runtimes, groups: groups, defaultGroup: 1, network: network, wiring: wiring, sidecarPath: f.sidecarPath,
		wrapper: f.wrapper, nodeID: f.config.local.Member, requested: true, eg: eg, cleanup: &cleanup})
	require.NoError(t, err)
	require.NotNil(t, supervisor)
	node.supervisor = supervisor
	node.registrationCtx, node.group = registered, groups[1]
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", f.group.Address)
	require.NoError(t, err)
	server = grpc.NewServer(runtime.TransportServerOptions...)
	runtime.registerGRPC(server)
	served = make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	startBootstrapActivationWorkers(registered, eg, runtimes, groups)
	catalog, err := setupDistributionAndRegistration(registered, eg, runtimes, routes, coordinate, groups[1], wiring, "n1", f.sidecarPath)
	require.NoError(t, err)
	readinessCtx, stopReadiness := context.WithCancel(registered)
	var readiness errgroup.Group
	startBootstrapReadinessReconciler(readinessCtx, &readiness, runtimes, catalog, routes)
	eg.Go(readiness.Wait)
	node.stopReadiness = func() { stopReadiness(); require.NoError(t, readiness.Wait()) }
	wait, stop := context.WithTimeout(t.Context(), 30*time.Second)
	defer stop()
	require.NoError(t, supervisor.Wait(wait), "actual protected encryption supervisor must publish owned completion")
	require.Eventually(t, runtime.PublicReady, enablementE2EWait, 10*time.Millisecond)
	return node
}

func (n *bootstrapRotationE2ENode) read(t *testing.T, key, expected string) {
	t.Helper()
	ctx, op, err := n.domain.admission.Begin(t.Context(), raftadmission.Data)
	require.NoError(t, err)
	defer op.CancelAndJoin()
	value, err := n.shards.GetAt(ctx, []byte(key), ^uint64(0))
	require.NoError(t, finishBootstrapIngress(op, err))
	require.Equal(t, expected, string(value))
}

func TestBootstrapRotationE2EActiveEnvelopesBusinessRestart(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f := newBootstrapRotationE2EFixture(t, true)
	node := f.protected(t)
	sc, err := encryption.ReadSidecar(f.sidecarPath)
	require.NoError(t, err)
	require.True(t, sc.StorageEnvelopeActive)
	require.NotZero(t, sc.RaftEnvelopeCutoverIndex)
	require.Greater(t, sc.Active.Storage, e2eRaftDEKID)
	require.Greater(t, sc.Active.Raft, e2eRaftDEKID)
	policy, err := kv.LoadAppliedBootstrapPolicy(t.Context(), node.runtime.Store, f.manifest.Identity, node.runtime.Engine.Status().AppliedIndex)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	_, err = node.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("rotated-business"), Value: []byte("encrypted-durable")}}})
	require.NoError(t, err)
	node.read(t, "rotated-business", "encrypted-durable")
	node.read(t, "legacy-key", "original-value")
	node.close()
	plain, err := store.NewPebbleStore(filepath.Join(f.dir, "fsm.db"))
	require.NoError(t, err)
	_, err = plain.GetAt(t.Context(), []byte("rotated-business"), ^uint64(0))
	require.ErrorContains(t, err, "no cipher configured", "business data must actually be encrypted at rest")
	require.NoError(t, plain.Close())
	restarted := f.protected(t)
	restarted.read(t, "rotated-business", "encrypted-durable")
	restarted.read(t, "legacy-key", "original-value")
	recovered, err := kv.LoadAppliedBootstrapPolicy(t.Context(), restarted.runtime.Store, f.manifest.Identity, restarted.runtime.Engine.Status().AppliedIndex)
	require.NoError(t, err)
	require.Equal(t, policy.ActivationIndex, recovered.ActivationIndex)
	require.Equal(t, policy.Generation, recovered.Generation)
}

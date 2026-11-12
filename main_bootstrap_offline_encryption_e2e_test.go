package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newOfflineEncryptedE2EFixture(t *testing.T) (*bootstrapRotationE2EFixture, bootstrapOfflineEncryptionCLI) {
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
	f := &bootstrapRotationE2EFixture{base: t.TempDir(), configDir: t.TempDir(), config: cfg, group: noderuntime.GroupSpec{ID: 1, Address: cfg.local.Address}, listener: listener, activeEnvelopes: true}
	f.dir = noderuntime.GroupDataDir(f.base, "n1", 1, false)
	f.sidecarPath = filepath.Join(f.dir, "encryption", "keys.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(f.sidecarPath), 0o700))
	assets := newOfflineEncryptionAssets(t)
	wrapper, ok := assets.wrapper.(*kek.FileWrapper)
	require.True(t, ok)
	f.wrapper = wrapper
	prepareOfflineEncryptedLegacy(t, f)
	encryptionOptions := bootstrapOfflineEncryptionCLI{Sidecar: f.sidecarPath, KEKFile: assets.file}
	pins := raftbootstrap.BaselineContext{Version: 1, Identity: raftbootstrap.Identity{Cluster: cfg.local.Cluster, Group: 1}, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, Configuration: raftbootstrap.Configuration{State: raftbootstrap.ConfState{Voters: []uint64{cfg.local.Member}}, Members: []raftbootstrap.Member{{ID: cfg.local.Member, Address: cfg.local.Address, PublicKeyDigest: cfg.local.Key}}}}
	probe, err := observeBootstrapOriginal(t.Context(), bootstrapObservationCommand{DataDir: f.dir, MemberID: cfg.local.Member, OperationID: raftbootstrap.OperationID{71}, Encryption: encryptionOptions}, pins, cfg)
	require.NoError(t, err)
	f.manifest = raftbootstrap.BaselineManifest{BaselineContext: probe.BaselineContext, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
	f.activation = migrateOfflineEncryptedE2E(t, f, encryptionOptions)
	raw.ActivationFile = filepath.Join(f.configDir, "activation.bin")
	require.NoError(t, os.WriteFile(raw.ActivationFile, f.activation, 0o600))
	encoded, err = json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(f.configDir, "group-1.json"), encoded, 0o600))
	return f, encryptionOptions
}

func prepareOfflineEncryptedLegacy(t *testing.T, f *bootstrapRotationE2EFixture) {
	t.Helper()
	clock, routes := kv.NewHLC(), distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	keys := encryption.NewKeystore()
	w, err := encryptionwiring.BuildWriteWiring(true, "n1", f.sidecarPath, f.wrapper, keys, []noderuntime.GroupSpec{f.group}, nil)
	require.NoError(t, err)
	factory := enablementFactory()
	encryptionwiring.ConfigureRaftEnvelopeFactory(factory, w)
	builder := shardGroupBuilder{raftID: "n1", raftDir: f.base, bootstrap: true, factory: factory, clock: clock, readTracker: kv.NewActiveTimestampTracker(), routeEngine: routes, kekWrapper: f.wrapper, keystore: keys, sidecarPath: f.sidecarPath, encWiring: w}
	rt, group, err := builder.build(f.group)
	require.NoError(t, err)
	t.Cleanup(rt.Close)
	w.AttachRaftEnvelopeGroup(1, group)
	require.Eventually(t, func() bool { return rt.Engine.State() == raftengine.StateLeader }, enablementE2EWait, 10*time.Millisecond)
	enablementLegacyPut(t, rt.Engine, clock.Next(), "legacy-key", "original-value")
	_, err = setupDistributionCatalog(t.Context(), []*raftGroupRuntime{rt}, routes)
	require.NoError(t, err)
	f.bootstrapKeys(t, rt, group, w, keys)
	f.enableEnvelopes(t, rt, group, w)
	for _, value := range []string{"old-history", "encrypted-original"} {
		payload, err := proto.Marshal(&pb.Request{Ts: clock.Next(), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("encrypted-before-migration"), Value: []byte(value)}}})
		require.NoError(t, err)
		_, err = group.Proposer().Propose(t.Context(), payload)
		require.NoError(t, err)
	}
	cut := rt.Engine.Status().AppliedIndex
	require.Eventually(t, func() bool { return rt.Engine.Status().LastSnapshotIndex >= cut }, enablementE2EWait, 10*time.Millisecond)
	rt.Close()
	_ = f.listener.Close()
	plain, err := store.NewPebbleStore(filepath.Join(f.dir, "fsm.db"))
	require.NoError(t, err)
	_, err = plain.GetAt(t.Context(), []byte("encrypted-before-migration"), ^uint64(0))
	require.ErrorContains(t, err, "no cipher configured")
	require.NoError(t, plain.Close())
}

func migrateOfflineEncryptedE2E(t *testing.T, f *bootstrapRotationE2EFixture, enc bootstrapOfflineEncryptionCLI) []byte {
	t.Helper()
	before := offlineDataFiles(t, f.dir)
	r := bootstrapBaselineCLIRequest{Command: "prepare", RaftDataDir: f.dir, MemberID: f.config.local.Member, OperationID: f.manifest.OperationID, Encryption: enc}
	prepared, err := executeBootstrapMigration(t.Context(), r, f.manifest.BaselineContext, f.manifest)
	require.NoError(t, err)
	require.Equal(t, before, offlineDataFiles(t, prepared.BackupDir))
	r.Command = "resume"
	bound, err := executeBootstrapMigration(t.Context(), r, f.manifest.BaselineContext, f.manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", bound.Phase)
	again, err := executeBootstrapMigration(t.Context(), r, f.manifest.BaselineContext, f.manifest)
	require.NoError(t, err)
	require.Equal(t, bound, again)
	require.Equal(t, before, offlineDataFiles(t, bound.BackupDir))
	ownership, err := etcd.AcquireOfflineBaselineOwnership(f.dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, ownership.Close()) }()
	options, err := bootstrapOfflineOptions(r, f.manifest.BaselineContext, f.manifest, ownership)
	require.NoError(t, err)
	evidence, err := etcd.VerifyOfflineBaselinePrepared(t.Context(), options, f.manifest)
	require.NoError(t, err)
	ack, err := signBootstrapPreparedAck(f.config, f.manifest, f.config.local.Member, evidence.DurableProofDigest)
	require.NoError(t, err)
	encoded, err := marshalBootstrapPreparedAck(ack, f.manifest)
	require.NoError(t, err)
	activation, err := assembleBootstrapActivation(f.manifest, f.manifest.BaselineContext, [][]byte{encoded})
	require.NoError(t, err)
	return activation
}

func TestBootstrapOfflineEncryptionLegacyCiphertextToProtectedRestart(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	f, _ := newOfflineEncryptedE2EFixture(t)
	node := f.protected(t)
	node.read(t, "encrypted-before-migration", "encrypted-original")
	policy, err := kv.LoadAppliedBootstrapPolicy(t.Context(), node.runtime.Store, f.manifest.Identity, node.runtime.Engine.Status().AppliedIndex)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), enablementE2EWait)
	defer cancel()
	_, err = node.coordinate.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("post-encrypted-migration"), Value: []byte("protected")}}})
	require.NoError(t, err)
	node.read(t, "post-encrypted-migration", "protected")
	node.close()
	restarted := f.protected(t)
	restarted.read(t, "encrypted-before-migration", "encrypted-original")
	restarted.read(t, "post-encrypted-migration", "protected")
	again, err := kv.LoadAppliedBootstrapPolicy(t.Context(), restarted.runtime.Store, f.manifest.Identity, restarted.runtime.Engine.Status().AppliedIndex)
	require.NoError(t, err)
	require.Equal(t, policy.ActivationIndex, again.ActivationIndex)
	require.Equal(t, policy.Generation, again.Generation)
}

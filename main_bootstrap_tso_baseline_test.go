package main

import (
	"bytes"
	"context"
	"crypto"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestOfflineTSOBaselineFromVerifiedBootstrapGenesis(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	base := t.TempDir()
	group := groupSpec{id: dedicatedTSORaftGroupID, address: "127.0.0.1:50051"}
	runtime, _, err := buildDedicatedTSOGroup("n1", group, base, true, true, false, nil, nil, etcd.NewFactory(etcd.FactoryConfig{}), kv.NewHLC(), nil)
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = runtime.engine.Propose(ctx, binary.BigEndian.AppendUint64([]byte("\x07TSOF\x01"), 42))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return runtime.engine.Status().LastSnapshotIndex != 0 }, 5*time.Second, 10*time.Millisecond)
	runtime.Close()
	dir := groupDataDir(base, "n1", dedicatedTSORaftGroupID, true)
	path := filepath.Join(dir, dedicatedTSOControlStoreName)
	require.NoDirExists(t, path)
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownership.Close()) })
	state, proof, err := probeOfflineTSOBaseline(t.Context(), dir, ownership)
	require.NoError(t, err)
	require.Equal(t, uint64(42), state.Watermark)
	require.Equal(t, uint64(1), proof.ConfigurationIndex, "native proves the retained genesis epoch, not the data snapshot index")
	require.Greater(t, proof.AppliedIndex, proof.ConfigurationIndex)
	require.NoDirExists(t, path, "offline probe must not silently initialize a control DB")
}

func TestOfflineTSOBaselineFromNativeWithoutControlDB(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "snap"), tsoBaselineDirectoryPerm))
	change, err := proto.Marshal(&raftpb.ConfChange{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: proto.Uint64(7)})
	require.NoError(t, err)
	w, err := wal.Create(zap.NewNop(), filepath.Join(dir, "wal"), []byte("legacy metadata"))
	require.NoError(t, err)
	require.NoError(t, w.Save(&raftpb.HardState{Term: proto.Uint64(4), Vote: proto.Uint64(7), Commit: proto.Uint64(3)}, []*raftpb.Entry{
		{Index: proto.Uint64(1), Term: proto.Uint64(3), Type: raftpb.EntryConfChange.Enum(), Data: change},
		{Index: proto.Uint64(2), Term: proto.Uint64(3), Type: raftpb.EntryNormal.Enum()},
		{Index: proto.Uint64(3), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum()},
	}))
	conf := &raftpb.ConfState{Voters: []uint64{7}}
	require.NoError(t, w.SaveSnapshot(&walpb.Snapshot{Index: proto.Uint64(3), Term: proto.Uint64(4), ConfState: conf}))
	require.NoError(t, w.Close())
	state := kv.NewTSOStateMachine(nil)
	require.Nil(t, state.Apply(binary.BigEndian.AppendUint64([]byte("\x07TSOF\x01"), 42)))
	snapshot, err := state.Snapshot()
	require.NoError(t, err)
	var image bytes.Buffer
	_, err = snapshot.WriteTo(&image)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	require.NoError(t, snap.New(zap.NewNop(), filepath.Join(dir, "snap")).SaveSnap(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(3), Term: proto.Uint64(4), ConfState: conf}, Data: image.Bytes()}))
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownership.Close()) })
	baseline, proof, err := probeOfflineTSOBaseline(t.Context(), dir, ownership)
	require.NoError(t, err)
	require.Equal(t, uint64(42), baseline.Watermark)
	require.Equal(t, uint64(3), proof.AppliedIndex)
	require.Equal(t, uint64(1), proof.ConfigurationIndex, "C epoch comes from retained ConfChange, not snapshot index")
	require.Equal(t, []uint64{7}, proof.ConfigurationMembers)
	require.NoDirExists(t, filepath.Join(dir, dedicatedTSOControlStoreName))
}

func preparedTSOMigrationFixture(t *testing.T) (kv.TSOBaselineState, kv.PreparedBootstrapActivation) {
	t.Helper()
	spec, _, st, old := preparedTSOFixture(t, 7)
	backing, ok := st.(kv.TSODurableBacking)
	require.True(t, ok)
	fsm, err := kv.NewDurableTSOStateMachine(nil, backing)
	require.NoError(t, err)
	state, err := fsm.ScalarBaseline(7, 1)
	require.NoError(t, err)
	c := old.Probe.BaselineContext
	c.DataDigest = state.DataDigest
	evidence := old.Probe.Evidence
	evidence.DataDigest = state.DataDigest
	evidence.PublicKeyDER = nil
	evidence.Signature = nil
	signer, ok := spec.config.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c, Evidence: evidence}, signer)
	require.NoError(t, err)
	manifest := raftbootstrap.BaselineManifest{BaselineContext: c, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	return state, kv.PreparedBootstrapActivation{Probe: probe, ManifestDigest: digest}
}

func TestExplicitTSOBaselineMigrationAndExactRetry(t *testing.T) {
	t.Parallel()
	state, prepared := preparedTSOMigrationFixture(t)
	dir := t.TempDir()
	digest, err := initializePreparedTSOBaseline(t.Context(), dir, state, prepared)
	require.NoError(t, err)
	require.NotEqual(t, state.DataDigest, digest, "scalar cut and complete prepared DB are distinct fingerprints")
	again, err := initializePreparedTSOBaseline(t.Context(), dir, state, prepared)
	require.NoError(t, err)
	require.Equal(t, digest, again)
	st, err := store.NewPebbleStore(filepath.Join(dir, dedicatedTSOControlStoreName))
	require.NoError(t, err)
	fsm, err := newProtectedTSOStateMachine(st, nil, prepared.Probe.Identity)
	require.NoError(t, err)
	require.Equal(t, state.Watermark, fsm.AllocationFloor())
	_, err = fsm.policyLoader()(t.Context(), prepared.Probe.Identity, state.AppliedIndex)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.NoError(t, st.PutAt(t.Context(), []byte("unexpected"), []byte("preserve"), 99, 0))
	require.NoError(t, st.Close())
	_, err = initializePreparedTSOBaseline(t.Context(), dir, state, prepared)
	require.Error(t, err, "arbitrary extra data is not an idempotent migration prefix")
	st, err = store.NewPebbleStore(filepath.Join(dir, dedicatedTSOControlStoreName))
	require.NoError(t, err)
	value, err := st.GetAt(t.Context(), []byte("unexpected"), 99)
	require.NoError(t, err)
	require.Equal(t, []byte("preserve"), value)
	require.NoError(t, st.Close())
}

func TestExplicitTSOBaselineMigrationRejectsChangedDigestBeforeCreatingDB(t *testing.T) {
	t.Parallel()
	state, prepared := preparedTSOMigrationFixture(t)
	state.DataDigest[0]++
	dir := t.TempDir()
	_, err := initializePreparedTSOBaseline(t.Context(), dir, state, prepared)
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(dir, dedicatedTSOControlStoreName))
}

func TestExplicitTSOBaselineMigrationResumesEmptyDB(t *testing.T) {
	t.Parallel()
	state, prepared := preparedTSOMigrationFixture(t)
	dir := t.TempDir()
	st, err := store.NewPebbleStore(filepath.Join(dir, dedicatedTSOControlStoreName))
	require.NoError(t, err)
	require.NoError(t, st.Close())
	digest, err := initializePreparedTSOBaseline(t.Context(), dir, state, prepared)
	require.NoError(t, err)
	verified, err := verifyPreparedTSOBaseline(t.Context(), filepath.Join(dir, dedicatedTSOControlStoreName), state, prepared)
	require.NoError(t, err)
	require.Equal(t, digest, verified)
}

func tsoPreparationCallbackFixture(t *testing.T) (*etcd.BaselinePreparationCallbacks, etcd.BaselinePreparationContext, etcd.BaselineStoreProof, *groupBootstrapSpec) {
	t.Helper()
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	spec, _, _ := protectedTSOFixture(t, t.TempDir())
	base := t.TempDir()
	group := groupSpec{id: dedicatedTSORaftGroupID, address: spec.config.local.Address}
	runtime, _, err := buildDedicatedTSOGroup("n1", group, base, true, true, false, nil, nil, etcd.NewFactory(etcd.FactoryConfig{}), kv.NewHLC(), nil)
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = runtime.engine.Propose(ctx, binary.BigEndian.AppendUint64([]byte("\x07TSOF\x01"), 42))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return runtime.engine.Status().LastSnapshotIndex != 0 }, 5*time.Second, 10*time.Millisecond)
	runtime.Close()
	backup := groupDataDir(base, "n1", dedicatedTSORaftGroupID, true)
	ownership, err := etcd.AcquireOfflineBaselineOwnership(backup)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownership.Close()) })
	state, proof, err := probeOfflineTSOBaseline(ctx, backup, ownership)
	require.NoError(t, err)
	source, err := etcd.ReadOfflineBaselineWALSource(ctx, backup, ownership)
	require.NoError(t, err)
	term := source.CommitTerm
	require.NoError(t, source.Snapshot.Close())
	c := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion, Identity: spec.intent.Identity,
		OperationID: raftbootstrap.OperationID{9}, Configuration: spec.intent.Baseline, Minimum: spec.intent.Minimum,
		CommitIndex: state.AppliedIndex, CommitTerm: term, Watermark: state.Watermark, DataDigest: state.DataDigest}
	inspection, err := etcd.InspectOfflineBaseline(ctx, etcd.OfflineBaselineOptions{Ownership: ownership, DataDir: backup,
		MemberID: spec.config.local.Member, Expected: c, ProbeStore: func(context.Context) (etcd.BaselineStoreProof, error) { return proof, nil }})
	require.NoError(t, err)
	signer, ok := spec.config.clientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: c, Evidence: inspection.Evidence}, signer)
	require.NoError(t, err)
	manifest := raftbootstrap.BaselineManifest{BaselineContext: c, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	callbacks, err := bootstrapOfflineTSOPreparation(kv.PreparedBootstrapActivation{Probe: probe, ManifestDigest: digest})
	require.NoError(t, err)
	require.NoError(t, ownership.Close())
	return callbacks, etcd.BaselinePreparationContext{DataDir: t.TempDir(), BackupDir: backup, Manifest: manifest}, proof, spec
}

func TestOfflineTSOPreparationCallbacksUseOriginalBackup(t *testing.T) {
	callbacks, c, original, _ := tsoPreparationCallbackFixture(t)
	require.Equal(t, []string{dedicatedTSOControlStoreName}, callbacks.MutablePaths)
	// No target WAL exists: reading target as a legacy source would fail.
	require.NoFileExists(t, filepath.Join(c.DataDir, "wal"))
	require.NoError(t, callbacks.StagePreparation(t.Context(), c))
	verified, err := callbacks.VerifyPreparedStore(t.Context(), c)
	require.NoError(t, err)
	require.Equal(t, original, verified.Baseline)
	require.NotEqual(t, original.DataDigest, verified.PreparedDataDigest)
	require.NoError(t, callbacks.StagePreparation(t.Context(), c), "resume must use backup, not the now-prepared target")
	again, err := callbacks.VerifyPreparedStore(t.Context(), c)
	require.NoError(t, err)
	require.Equal(t, verified, again)
	c.BackupDir = c.DataDir
	require.Error(t, callbacks.StagePreparation(t.Context(), c), "target cannot substitute for original backup")
}

func TestOfflineTSOPreparationRejectsChangedManifestAndSource(t *testing.T) {
	callbacks, c, _, _ := tsoPreparationCallbackFixture(t)
	changed := c
	changed.Manifest.OperationID[0]++
	require.Error(t, callbacks.StagePreparation(t.Context(), changed))
	require.NoDirExists(t, filepath.Join(c.DataDir, dedicatedTSOControlStoreName))
	require.NoError(t, callbacks.StagePreparation(t.Context(), c))
	backup := c.BackupDir
	// An existing control DB marks this as a transformed image, not a legacy
	// source, regardless of whether its scalar values happen to agree.
	require.NoError(t, os.Mkdir(filepath.Join(backup, dedicatedTSOControlStoreName), tsoBaselineDirectoryPerm))
	_, err := callbacks.VerifyPreparedStore(t.Context(), c)
	require.Error(t, err)
}

func TestOfflineTSOPreparationNativeBound(t *testing.T) {
	callbacks, c, _, spec := tsoPreparationCallbackFixture(t)
	dir := c.BackupDir // The fixture's original legacy source, not a prepared target.
	ownership, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ownership.Close()) })
	options := etcd.OfflineBaselineOptions{DataDir: dir, Ownership: ownership, MemberID: spec.config.local.Member,
		Expected: c.Manifest.BaselineContext, Preparation: bootstrapOfflineFormatPreparation(callbacks),
		ProbeStore: func(ctx context.Context) (etcd.BaselineStoreProof, error) {
			_, proof, err := probeOfflineTSOBaseline(ctx, dir, ownership)
			return proof, err
		}}
	migration, err := etcd.PrepareOfflineBaseline(t.Context(), options, c.Manifest)
	require.NoError(t, err)
	require.Equal(t, "prepared", migration.Phase)
	for range 4 {
		migration, err = etcd.ResumeOfflineBaseline(t.Context(), options, c.Manifest)
		require.NoError(t, err)
		if migration.Phase == "bound" {
			break
		}
	}
	require.Equal(t, "bound", migration.Phase)
	_, err = etcd.VerifyBoundOfflineBaseline(t.Context(), options, c.Manifest)
	require.NoError(t, err)
	require.NoError(t, ownership.Close())
	verifyBoundTSOControl(t, dir, spec, c.Manifest)
}

func verifyBoundTSOControl(t *testing.T, dir string, spec *groupBootstrapSpec, manifest raftbootstrap.BaselineManifest) {
	t.Helper()
	st, err := store.NewPebbleStore(filepath.Join(dir, dedicatedTSOControlStoreName))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	sm, err := newProtectedTSOStateMachine(st, nil, spec.intent.Identity)
	require.NoError(t, err)
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	prepared, err := kv.ReadPreparedBootstrapActivation(t.Context(), metadata, spec.intent.Identity)
	require.NoError(t, err)
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	require.Equal(t, digest, prepared.ManifestDigest)
	require.Equal(t, manifest.Watermark, sm.AllocationFloor())
	_, err = sm.policyLoader()(t.Context(), spec.intent.Identity, manifest.CommitIndex)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "bound/prepared recovery does not activate P")
}

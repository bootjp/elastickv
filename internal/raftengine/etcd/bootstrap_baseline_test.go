package etcd

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func offlineBaselineFixture(t *testing.T, retained bool) (OfflineBaselineOptions, ed25519.PrivateKey) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "node")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, snapDirName), 0o700))
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(pub)
	require.NoError(t, err)
	peer := Peer{NodeID: 7, ID: "n7", Address: "127.0.0.1:7007"}
	contextBytes, err := encodeConfChangeContext(11, peer)
	require.NoError(t, err)
	change, err := proto.Marshal(&raftpb.ConfChange{Type: raftpb.ConfChangeAddNode.Enum(), NodeId: proto.Uint64(7), Context: contextBytes})
	require.NoError(t, err)
	w, err := wal.Create(zap.NewNop(), filepath.Join(dir, walDirName), []byte("original legacy metadata"))
	require.NoError(t, err)
	entries := []*raftpb.Entry{
		{Index: proto.Uint64(1), Term: proto.Uint64(3), Type: raftpb.EntryConfChange.Enum(), Data: change},
		{Index: proto.Uint64(2), Term: proto.Uint64(3), Type: raftpb.EntryNormal.Enum(), Data: []byte("retained old value")},
		{Index: proto.Uint64(3), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum(), Data: []byte("latest value")},
	}
	if !retained {
		entries = nil
	}
	require.NoError(t, w.Save(&raftpb.HardState{Term: proto.Uint64(9), Vote: proto.Uint64(7), Commit: proto.Uint64(3)}, entries))
	conf := &raftpb.ConfState{Voters: []uint64{7}}
	require.NoError(t, w.SaveSnapshot(&walpb.Snapshot{Index: proto.Uint64(3), Term: proto.Uint64(4), ConfState: conf}))
	require.NoError(t, w.Close())
	require.NoError(t, snap.New(zap.NewNop(), filepath.Join(dir, snapDirName)).SaveSnap(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(3), Term: proto.Uint64(4), ConfState: conf}, Data: []byte("FSM snapshot")}))
	require.NoError(t, savePersistedPeers(dir, 3, []Peer{peer}))
	data := raftbootstrap.Digest(sha256.Sum256([]byte("all replicated history and metadata")))
	c := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion, Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1},
		OperationID: raftbootstrap.OperationID{1}, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		Configuration: raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{7}},
			Members: []raftbootstrap.Member{{ID: 7, Address: peer.Address, PublicKeyDigest: sha256.Sum256(der)}}},
		CommitIndex: 3, CommitTerm: 4, Watermark: 123, DataDigest: data}
	o := OfflineBaselineOptions{DataDir: dir, MemberID: 7, Expected: c, ProbeStore: func(context.Context) (BaselineStoreProof, error) {
		return BaselineStoreProof{AppliedIndex: 3, ConfigurationIndex: 1, ConfigurationMembers: []uint64{7}, Watermark: 123, DataDigest: data}, nil
	}}
	o.Preparation = &BaselinePreparationCallbacks{MutablePaths: []string{raftbootstrap.IntentFile, raftbootstrap.MarkerFile}, StagePreparation: func(_ context.Context, input BaselinePreparationContext) error {
		return stageOfflineTestRequiredIntent(input)
	},
		VerifyPreparedStore: func(ctx context.Context, input BaselinePreparationContext) (BaselinePreparedStoreProof, error) {
			digest, err := raftbootstrap.BaselineManifestDigest(input.Manifest)
			if err != nil {
				return BaselinePreparedStoreProof{}, err
			}
			identity, err := baselineDestinationIdentity(o, digest)
			if err != nil {
				return BaselinePreparedStoreProof{}, err
			}
			if _, err := verifyBaselineRequiredIntent(o, identity); err != nil {
				return BaselinePreparedStoreProof{}, err
			}
			proof, err := o.ProbeStore(ctx)
			return BaselinePreparedStoreProof{Baseline: proof, PreparedDataDigest: proof.DataDigest}, err
		}}
	o.Ownership, err = AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Ownership.Close()) })
	return o, key
}

func signedOfflineBaseline(t *testing.T, o OfflineBaselineOptions, key ed25519.PrivateKey) raftbootstrap.BaselineManifest {
	t.Helper()
	inspection, err := InspectOfflineBaseline(t.Context(), o)
	require.NoError(t, err)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: o.Expected, Evidence: inspection.Evidence}, key)
	require.NoError(t, err)
	return raftbootstrap.BaselineManifest{BaselineContext: o.Expected, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
}

func TestOfflineBaselineInspectPreservesFilesAndActualConfiguration(t *testing.T) {
	o, _ := offlineBaselineFixture(t, true)
	before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	inspection, err := InspectOfflineBaseline(t.Context(), o)
	require.NoError(t, err)
	require.Equal(t, uint64(1), inspection.ConfigurationIndex)
	require.Equal(t, uint64(3), inspection.Snapshot.GetMetadata().GetIndex())
	require.Equal(t, uint64(3), inspection.RetainedEntries)
	require.Equal(t, uint64(9), inspection.HardState.GetTerm())
	require.Equal(t, uint64(7), inspection.HardState.GetVote())
	after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestOfflineBaselineRejectsUnverifiableConfigurationAndPendingState(t *testing.T) {
	for _, kind := range []string{"missing proof", "wrong index", "prepared", "unapplied", "data mismatch", "peers ahead"} {
		t.Run(kind, func(t *testing.T) {
			o, _ := offlineBaselineFixture(t, false)
			original := o.ProbeStore
			o.ProbeStore = func(ctx context.Context) (BaselineStoreProof, error) {
				proof, err := original(ctx)
				switch kind {
				case "missing proof":
					proof.ConfigurationIndex = 0
				case "wrong index":
					proof.ConfigurationIndex = 3
				case "prepared":
					proof.PreparedTransactions = 1
				case "unapplied":
					proof.AppliedIndex = 2
				case "data mismatch":
					proof.DataDigest = raftbootstrap.Digest{99}
				}
				return proof, err
			}
			if kind == "peers ahead" {
				require.NoError(t, savePersistedPeers(o.DataDir, 4, []Peer{{NodeID: 7, ID: "n7", Address: "127.0.0.1:7007"}}))
			}
			before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
			require.NoError(t, err)
			_, err = InspectOfflineBaseline(t.Context(), o)
			require.Error(t, err)
			after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestOfflineBaselineMissingRowRequiresCompleteNativeHistory(t *testing.T) {
	o, _ := offlineBaselineFixture(t, true)
	probe := o.ProbeStore
	o.ProbeStore = func(ctx context.Context) (BaselineStoreProof, error) {
		proof, err := probe(ctx)
		proof.ConfigurationIndex, proof.ConfigurationMembers = 0, nil
		return proof, err
	}
	inspection, err := InspectOfflineBaseline(t.Context(), o)
	require.NoError(t, err)
	require.Equal(t, uint64(1), inspection.ConfigurationIndex)
}

func TestOfflineBaselineMigrationPreservesTermVoteAndAllRetainedEntries(t *testing.T) {
	o, key := offlineBaselineFixture(t, true)
	manifest := signedOfflineBaseline(t, o, key)
	original, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	paths, err := PrepareOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "prepared", paths.Phase)
	stillOld, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	require.Equal(t, original.fileDigest, stillOld.fileDigest)
	paths, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", paths.Phase)
	live, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	require.True(t, original.equivalent(live))
	require.Len(t, live.entries, 3, "entries at/before selected data snapshot are retained")
	require.Equal(t, uint64(9), live.hardState.GetTerm())
	require.Equal(t, uint64(7), live.hardState.GetVote())
	manifestDigest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	identity, err := baselineDestinationIdentity(o, manifestDigest)
	require.NoError(t, err)
	require.NoError(t, verifyWALIdentity(live.metadata, identity))
	require.Equal(t, manifestDigest, identity.PreparationDigest)
	otherCut := *identity
	otherCut.PreparationDigest[0] ^= 1
	require.ErrorIs(t, verifyWALIdentity(live.metadata, &otherCut), ErrWALIdentity)
	v1 := *identity
	v1.PreparationDigest = raftbootstrap.Digest{}
	require.ErrorIs(t, verifyWALIdentity(live.metadata, &v1), ErrWALIdentity)
	old, err := readBaselineWAL(paths.PreparedWALDir)
	require.NoError(t, err)
	require.Equal(t, original.fileDigest, old.fileDigest)
	_, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err, "bound resume is idempotent")
}

func TestOfflineBaselineResumeAfterExchangeBeforeJournal(t *testing.T) {
	o, key := offlineBaselineFixture(t, true)
	manifest := signedOfflineBaseline(t, o, key)
	paths, err := PrepareOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.NoError(t, baselineAtomicExchange(filepath.Join(o.DataDir, walDirName), paths.PreparedWALDir))
	require.NoError(t, o.Ownership.Close())
	o.Ownership, err = AcquireOfflineBaselineOwnership(o.DataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Ownership.Close() })
	paths, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", paths.Phase)
}

func TestOfflineBaselineResumeInterruptedBackupAndRejectChangedSource(t *testing.T) {
	o, key := offlineBaselineFixture(t, true)
	manifest := signedOfflineBaseline(t, o, key)
	paths := baselineMigrationPaths(o)
	inspection, err := InspectOfflineBaseline(t.Context(), o)
	require.NoError(t, err)
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(paths.Directory, 0o700))
	transformation, err := baselineTransformationDigest(o)
	require.NoError(t, err)
	require.NoError(t, writeBaselineJournal(paths.Directory, baselineMigrationJournal{phase: baselinePreparing, manifest: digest, source: inspection.Evidence.NativeStateDigest, transformation: transformation}))
	paths, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "prepared", paths.Phase)
	require.NoError(t, os.WriteFile(filepath.Join(o.DataDir, "unexpected-mutation"), []byte("changed"), 0o600))
	_, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.Error(t, err)
	original, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	require.Equal(t, []byte("original legacy metadata"), original.metadata)
}

func TestOfflineBaselineRejectsZeroStateWithoutBootstrap(t *testing.T) {
	o, _ := offlineBaselineFixture(t, true)
	o.DataDir = t.TempDir()
	_, err := InspectOfflineBaseline(t.Context(), o)
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(o.DataDir, walDirName))
}

func TestOfflineBaselinePreparationProofRejectsV1AndForeignCut(t *testing.T) {
	legacy := identityFixture()
	state := RecoveredState{identity: legacy}
	digest := raftbootstrap.Digest{5}
	require.ErrorIs(t, state.VerifyPreparation(digest), ErrWALIdentity)
	legacy.PreparationDigest = digest
	newDir := t.TempDir()
	require.ErrorIs(t, validateBaselineStartupSource(OpenConfig{DataDir: newDir, Bootstrap: true, NativeIdentity: legacy}), ErrWALIdentity)
	require.NoDirExists(t, filepath.Join(newDir, walDirName))
	encoded, err := encodeWALIdentity(legacy)
	require.NoError(t, err)
	require.Equal(t, "EKVWID02", string(encoded[:8]))
	require.Len(t, encoded, walPreparationIdentitySize)
	require.NoError(t, verifyWALIdentity(encoded, legacy))
	require.NoError(t, state.VerifyPreparation(digest))
	require.ErrorIs(t, state.VerifyPreparation(raftbootstrap.Digest{}), ErrWALIdentity)
	require.ErrorIs(t, state.VerifyPreparation(raftbootstrap.Digest{6}), ErrWALIdentity)
	require.ErrorIs(t, verifyWALIdentity(encoded, nil), ErrWALIdentity)
	encoded[len(encoded)-1] ^= 1
	require.ErrorIs(t, verifyWALIdentity(encoded, legacy), ErrWALIdentity)
}

func TestOfflineBaselineOwnershipRejectsRunningAndReplacedSources(t *testing.T) {
	o, _ := offlineBaselineFixture(t, true)
	before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	_, err = Open(t.Context(), OpenConfig{NodeID: 7, LocalID: "n7", LocalAddress: "127.0.0.1:7007", DataDir: o.DataDir, StateMachine: &testStateMachine{}})
	require.Error(t, err, "current startup must honor the stable directory guard")
	after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, o.Ownership.Close())
	legacy, err := wal.Open(zap.NewNop(), filepath.Join(o.DataDir, walDirName), &walpb.Snapshot{Index: proto.Uint64(3), Term: proto.Uint64(4)})
	require.NoError(t, err)
	_, err = AcquireOfflineBaselineOwnership(o.DataDir)
	require.Error(t, err, "old servers that do not honor the directory guard still hold segment locks")
	require.NoError(t, legacy.Close())
	o.Ownership, err = AcquireOfflineBaselineOwnership(o.DataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Ownership.Close() })
	require.NoError(t, os.Rename(filepath.Join(o.DataDir, walDirName), filepath.Join(o.DataDir, "old-wal")))
	require.NoError(t, copyBaselineTree(t.Context(), filepath.Join(o.DataDir, "old-wal"), filepath.Join(o.DataDir, walDirName)))
	_, err = InspectOfflineBaseline(t.Context(), o)
	require.ErrorContains(t, err, "WAL directory replaced")
}

func TestOfflineBaselinePreparedTransformationAndRestart(t *testing.T) {
	o, key := offlineBaselineFixture(t, true)
	baselineProbe := o.ProbeStore
	metadataPath := "prepared-metadata"
	preparedValue := []byte("exact manifest-bound activation metadata")
	prepared := false
	o.Preparation = &BaselinePreparationCallbacks{MutablePaths: []string{metadataPath, raftbootstrap.IntentFile, raftbootstrap.MarkerFile},
		StagePreparation: func(_ context.Context, input BaselinePreparationContext) error {
			if value, err := os.ReadFile(filepath.Join(input.DataDir, metadataPath)); err == nil {
				if !bytes.Equal(value, preparedValue) {
					return errors.New("foreign prepared metadata")
				}
				return stageOfflineTestRequiredIntent(input)
			} else if !os.IsNotExist(err) {
				return err
			}
			prepared = true
			if err := os.WriteFile(filepath.Join(input.DataDir, metadataPath), preparedValue, 0o600); err != nil {
				return err
			}
			return stageOfflineTestRequiredIntent(input)
		},
		VerifyPreparedStore: func(ctx context.Context, input BaselinePreparationContext) (BaselinePreparedStoreProof, error) {
			return verifyOfflineTestPreparedStore(ctx, o, input, baselineProbe, metadataPath, preparedValue)
		}}
	o.ProbeStore = func(ctx context.Context) (BaselineStoreProof, error) {
		proof, err := baselineProbe(ctx)
		if prepared {
			proof.DataDigest = sha256.Sum256(preparedValue)
		}
		return proof, err
	}
	manifest := signedOfflineBaseline(t, o, key)
	paths, err := PrepareOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "prepared", paths.Phase)
	_, err = VerifyBoundOfflineBaseline(t.Context(), o, manifest)
	require.Error(t, err, "prepared-before-exchange cannot produce PreparedAck")
	_, err = InspectOfflineBaseline(t.Context(), o)
	require.Error(t, err, "the original probe must not silently exclude new preparation metadata")
	paths, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", paths.Phase)
	ackDigest, err := VerifyBoundOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.NotEqual(t, raftbootstrap.Digest{}, ackDigest)
	_, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
}

func verifyOfflineTestPreparedStore(ctx context.Context, o OfflineBaselineOptions, input BaselinePreparationContext, probe BaselineStoreProbe, path string, expected []byte) (BaselinePreparedStoreProof, error) {
	value, err := os.ReadFile(filepath.Join(input.DataDir, path))
	if err != nil {
		return BaselinePreparedStoreProof{}, err
	}
	if !bytes.Equal(value, expected) {
		return BaselinePreparedStoreProof{}, errors.New("prepared transform differs")
	}
	if _, err := os.Stat(filepath.Join(input.BackupDir, path)); !os.IsNotExist(err) {
		return BaselinePreparedStoreProof{}, errors.New("original backup already contains preparation")
	}
	proof, err := probe(ctx)
	if err != nil {
		return BaselinePreparedStoreProof{}, err
	}
	digest, err := raftbootstrap.BaselineManifestDigest(input.Manifest)
	if err != nil {
		return BaselinePreparedStoreProof{}, err
	}
	identity, err := baselineDestinationIdentity(o, digest)
	if err != nil {
		return BaselinePreparedStoreProof{}, err
	}
	if _, err := verifyBaselineRequiredIntent(o, identity); err != nil {
		return BaselinePreparedStoreProof{}, err
	}
	return BaselinePreparedStoreProof{Baseline: proof, PreparedDataDigest: sha256.Sum256(value)}, nil
}

func stageOfflineTestRequiredIntent(input BaselinePreparationContext) error {
	digest, err := raftbootstrap.ConfigurationDigest(input.Manifest.Configuration)
	if err != nil {
		return err
	}
	identity := input.Manifest.Identity
	return raftbootstrap.NewStore(input.DataDir).Publish(raftbootstrap.Intent{Identity: identity, BaselineKind: raftbootstrap.Accepted, Baseline: input.Manifest.Configuration, Minimum: input.Manifest.Minimum},
		raftbootstrap.Evidence{Identity: &identity, BaselineDigest: &digest, RaftState: raftbootstrap.Intact})
}

func TestOfflineBaselineNoMarkerCannotExchangeV2(t *testing.T) {
	o, key := offlineBaselineFixture(t, true)
	o.Preparation = nil
	manifest := signedOfflineBaseline(t, o, key)
	_, err := PrepareOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	_, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.ErrorContains(t, err, "requires exact preparation")
	image, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	require.Equal(t, []byte("original legacy metadata"), image.metadata)
}

func TestOfflineBaselineRemovedFormatBarrierRejectsSwapAndAck(t *testing.T) {
	o, key := offlineBaselineFixture(t, true)
	manifest := signedOfflineBaseline(t, o, key)
	_, err := PrepareOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(o.DataDir, raftbootstrap.MarkerFile)))
	_, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.Error(t, err)
	_, err = VerifyBoundOfflineBaseline(t.Context(), o, manifest)
	require.Error(t, err)
	image, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	require.Equal(t, []byte("original legacy metadata"), image.metadata)
}

func TestOfflineBaselineCorruptTailRefusesWithoutRepair(t *testing.T) {
	o, _ := offlineBaselineFixture(t, true)
	names, err := baselineWALNames(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	file, err := os.OpenFile(filepath.Join(o.DataDir, walDirName, names[len(names)-1]), os.O_RDWR, 0)
	require.NoError(t, err)
	info, err := file.Stat()
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{1}, info.Size()-1)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	_, err = InspectOfflineBaseline(t.Context(), o)
	require.Error(t, err)
	after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestOfflineBaselineProvesRetainedNativeGenesisNotSnapshotDataIndex(t *testing.T) {
	t.Setenv(snapshotEveryEnvVar, "1")
	dir := t.TempDir()
	e, err := Open(t.Context(), OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:7001", DataDir: dir, Bootstrap: true, StateMachine: &testStateMachine{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	_, err = e.Propose(t.Context(), []byte("latest"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return e.Status().LastSnapshotIndex > 1 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, e.Close())
	ownership, err := AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	defer ownership.Close()
	source, err := ReadOfflineBaselineWALSource(t.Context(), dir, ownership)
	require.NoError(t, err)
	defer source.Snapshot.Close()
	require.Greater(t, source.SnapshotIndex, uint64(1))
	require.Equal(t, uint64(1), source.ConfigurationIndex)
	image, err := readBaselineWAL(filepath.Join(dir, walDirName))
	require.NoError(t, err)
	snapshot, err := selectBaselineSnapshot(dir, image)
	require.NoError(t, err)
	image.genesis = nil
	_, err = baselineFullPrefixConfiguration(image, snapshot)
	require.Error(t, err, "data snapshot alone cannot replace a missing genesis/configuration proof")
}

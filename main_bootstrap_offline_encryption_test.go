package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

type offlineEncryptionTestAssets struct {
	wrapper kek.Wrapper
	file    string
	wrapped map[string][]byte
}

func newOfflineEncryptionAssets(t *testing.T) *offlineEncryptionTestAssets {
	t.Helper()
	path := filepath.Join(t.TempDir(), "external.kek")
	key := make([]byte, encryption.KeySize)
	_, err := rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, key, 0o600))
	clear(key)
	wrapper, err := kek.NewFileWrapper(path)
	require.NoError(t, err)
	a := &offlineEncryptionTestAssets{wrapper: wrapper, file: path, wrapped: make(map[string][]byte)}
	for _, id := range []byte{7, 8} {
		wrapped, err := wrapper.Wrap(bytes.Repeat([]byte{id}, encryption.KeySize))
		require.NoError(t, err)
		a.wrapped[string('0'+id)] = wrapped
	}
	return a
}

func offlineEncryptedStore(t *testing.T, member uint64, a *offlineEncryptionTestAssets) (string, string, *clusterbootstrap.BootstrapOfflineEncryption) {
	t.Helper()
	root, _, _ := offlineDataStoreFixture(t, true)
	sepoch, repoch := uint16(member+2), uint16(member+4)
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(7, bytes.Repeat([]byte{7}, encryption.KeySize)))
	require.NoError(t, keys.Set(8, bytes.Repeat([]byte{8}, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	st, err := store.NewPebbleStore(filepath.Join(root, clusterbootstrap.BootstrapOfflineDataPath), store.WithEncryption(cipher, encryption.NewDeterministicNonceFactory(uint16(member), sepoch), func() (uint32, bool) { return 7, true }), store.WithStorageEnvelopeGate(func() bool { return true }))
	require.NoError(t, err)
	require.NoError(t, st.PutAt(t.Context(), []byte("encrypted-history"), []byte("first"), 24, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("encrypted-history"), []byte("second"), 25, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("expired-encrypted"), []byte("not ignored"), 26, 1))
	reg, err := store.WriterRegistryFor(st)
	require.NoError(t, err)
	for _, node := range []uint64{1, 2} {
		for _, id := range []uint32{7, 8} {
			epoch := uint16(node + 2)
			if id == 8 {
				epoch += 2
			}
			require.NoError(t, reg.SetRegistryRow(encryption.RegistryKey(id, uint16(node)), encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: node, FirstSeenLocalEpoch: epoch, LastSeenLocalEpoch: epoch})))
		}
	}
	sc := &encryption.Sidecar{Version: encryption.SidecarVersion, RaftAppliedIndex: 9, Active: encryption.ActiveKeys{Storage: 7, Raft: 8}, StorageEnvelopeActive: true, StorageEnvelopeCutoverIndex: 4, RaftEnvelopeCutoverIndex: 6, Keys: map[string]encryption.SidecarKey{
		"7": {Purpose: encryption.SidecarPurposeStorage, Wrapped: a.wrapped["7"], LocalEpoch: sepoch}, "8": {Purpose: encryption.SidecarPurposeRaft, Wrapped: a.wrapped["8"], LocalEpoch: repoch}}}
	path := filepath.Join(t.TempDir(), "sidecar.json")
	require.NoError(t, encryption.WriteSidecar(path, sc))
	cp, err := encryption.SidecarCheckpoint(sc)
	require.NoError(t, err)
	sink, ok := st.(interface {
		SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint) error
	})
	require.True(t, ok)
	require.NoError(t, sink.SaveEncryptionSnapshotCheckpoint(cp))
	require.NoError(t, st.Close())
	enc, err := clusterbootstrap.NewBootstrapOfflineEncryption(clusterbootstrap.BootstrapOfflineEncryptionConfig{SidecarPath: path, Wrapper: a.wrapper, MemberID: member})
	require.NoError(t, err)
	return root, path, enc
}

func signedOfflineEncryptedPreparation(t *testing.T, proof etcd.BaselineStoreProof) (raftbootstrap.BaselineManifest, kv.PreparedBootstrapActivation) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(public)
	require.NoError(t, err)
	cut := raftbootstrap.BaselineContext{Version: 1, Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 7}, OperationID: raftbootstrap.OperationID{2}, Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, CommitIndex: proof.AppliedIndex, CommitTerm: 3, DataDigest: proof.DataDigest, Watermark: proof.Watermark,
		Configuration: raftbootstrap.Configuration{Index: 8, State: raftbootstrap.ConfState{Voters: []uint64{1}}, Members: []raftbootstrap.Member{{ID: 1, Address: "local:9000", PublicKeyDigest: sha256.Sum256(der)}}}}
	config, err := raftbootstrap.ConfigurationDigest(cut.Configuration)
	require.NoError(t, err)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: cut, Evidence: raftbootstrap.BaselineMemberEvidence{MemberID: 1, CommitIndex: cut.CommitIndex, CommitTerm: 3, AppliedIndex: cut.CommitIndex, LastLogIndex: cut.CommitIndex, ConfigurationDigest: config, NativeStateDigest: raftbootstrap.Digest{3}, DataDigest: proof.DataDigest, Watermark: proof.Watermark}}, private)
	require.NoError(t, err)
	m := raftbootstrap.BaselineManifest{BaselineContext: cut, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
	digest, err := raftbootstrap.BaselineManifestDigest(m)
	require.NoError(t, err)
	return m, kv.PreparedBootstrapActivation{Probe: probe, ManifestDigest: digest}
}

func TestBootstrapOfflineEncryptionTwoMembersLogicalEqual(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	one, _, e1 := offlineEncryptedStore(t, 1, a)
	two, _, e2 := offlineEncryptedStore(t, 2, a)
	before := offlineDataFiles(t, one)
	p1, err := clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), one, e1)
	require.NoError(t, err)
	p2, err := clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), two, e2)
	require.NoError(t, err)
	require.Equal(t, p1.DataDigest, p2.DataDigest, "local epoch differs but checkpoint excludes it; complete common registry remains included")
	require.True(t, p1.EncryptionContextRequired)
	require.Equal(t, uint64(6), p1.RaftEnvelopeCutoverIndex)
	require.Equal(t, before, offlineDataFiles(t, one))
	_, err = clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), one)
	require.Error(t, err, "no automatic encrypted-prefix mode selection")
}

func TestBootstrapOfflineEncryptionPrepareRestartPreservesCiphertext(t *testing.T) {
	// Not parallel: store.VerifyBootstrapBaselinePhysicalEquality calls Error() on a closed pebble iterator, whose pooled allocation another test's pebble DB may be reusing.
	a := newOfflineEncryptionAssets(t)
	root, path, enc := offlineEncryptedStore(t, 1, a)
	proof, err := clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), root, enc)
	require.NoError(t, err)
	m, prepared := signedOfflineEncryptedPreparation(t, proof)
	backup := offlineDataBackup(t, root)
	before := offlineDataFiles(t, backup)
	sidecarBefore, err := os.ReadFile(path)
	require.NoError(t, err)
	callbacks, err := clusterbootstrap.BootstrapOfflineDataPreparation(prepared, enc)
	require.NoError(t, err)
	c := etcd.BaselinePreparationContext{DataDir: root, BackupDir: backup, Manifest: m}
	require.NoError(t, callbacks.StagePreparation(t.Context(), c))
	staged, err := callbacks.VerifyPreparedStore(t.Context(), c)
	require.NoError(t, err)
	require.Equal(t, proof.DataDigest, staged.Baseline.DataDigest)
	frozen := offlineDataFiles(t, root)
	restartedContext, err := clusterbootstrap.NewBootstrapOfflineEncryption(clusterbootstrap.BootstrapOfflineEncryptionConfig{SidecarPath: path, Wrapper: a.wrapper, MemberID: 1})
	require.NoError(t, err)
	restarted, err := clusterbootstrap.BootstrapOfflineDataPreparation(prepared, restartedContext)
	require.NoError(t, err)
	require.NoError(t, restarted.StagePreparation(t.Context(), c))
	again, err := restarted.VerifyPreparedStore(t.Context(), c)
	require.NoError(t, err)
	require.Equal(t, staged, again)
	require.Equal(t, frozen, offlineDataFiles(t, root))
	require.Equal(t, before, offlineDataFiles(t, backup))
	sidecarAfter, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, sidecarBefore, sidecarAfter, "epochs/wrapped material stay unchanged")
}

func TestBootstrapOfflineEncryptionWrongKEKMissingKeyAndEpochDrift(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	root, path, enc := offlineEncryptedStore(t, 1, a)
	before := offlineDataFiles(t, root)
	wrong := newOfflineEncryptionAssets(t)
	_, err := clusterbootstrap.NewBootstrapOfflineEncryption(clusterbootstrap.BootstrapOfflineEncryptionConfig{SidecarPath: path, Wrapper: wrong.wrapper, MemberID: 1})
	require.Error(t, err)
	sc, err := encryption.ReadSidecar(path)
	require.NoError(t, err)
	key := sc.Keys["7"]
	key.LocalEpoch++
	sc.Keys["7"] = key
	require.NoError(t, encryption.WriteSidecar(path, sc))
	_, err = clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), root, enc)
	require.Error(t, err, "frozen context detects sidecar changes even when checkpoint intentionally excludes epochs")
	changed, err := clusterbootstrap.NewBootstrapOfflineEncryption(clusterbootstrap.BootstrapOfflineEncryptionConfig{SidecarPath: path, Wrapper: a.wrapper, MemberID: 1})
	require.NoError(t, err)
	_, err = clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), root, changed)
	require.Error(t, err, "actual registry rejects an unregistered local epoch")
	delete(sc.Keys, "8")
	// The production writer correctly refuses this invalid fixture; write its
	// malformed on-disk JSON directly to exercise the read-only rejection.
	data, err := json.Marshal(sc)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	_, err = clusterbootstrap.NewBootstrapOfflineEncryption(clusterbootstrap.BootstrapOfflineEncryptionConfig{SidecarPath: path, Wrapper: a.wrapper, MemberID: 1})
	require.Error(t, err)
	require.Equal(t, before, offlineDataFiles(t, root))
}

func TestBootstrapOfflineEncryptionTamperedExpiredValueFailsReadOnly(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	root, _, enc := offlineEncryptedStore(t, 1, a)
	require.NoError(t, clusterbootstrap.BootstrapOfflineWithData(t.Context(), root, false, func(db *pebble.DB) error {
		it, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			if bytes.HasPrefix(it.Key(), []byte("expired-encrypted")) {
				value := bytes.Clone(it.Value())
				value[len(value)-1] ^= 1
				return db.Set(it.Key(), value, pebble.Sync)
			}
		}
		return context.Canceled
	}))
	before := offlineDataFiles(t, root)
	_, err := clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), root, enc)
	require.Error(t, err)
	require.Equal(t, before, offlineDataFiles(t, root))
}

func TestBootstrapOfflineEncryptionNativeSnapshotValidation(t *testing.T) {
	t.Parallel()
	a := newOfflineEncryptionAssets(t)
	root, _, enc := offlineEncryptedStore(t, 1, a)
	var stream bytes.Buffer
	stream.WriteString("EKVTHLC2")
	stream.Write(binary.BigEndian.AppendUint16(nil, 64))
	stream.Write(binary.BigEndian.AppendUint64(nil, 100))
	stream.Write(binary.BigEndian.AppendUint64(nil, enc.Cutover))
	stream.WriteString("EKVENCP1")
	stream.Write(binary.BigEndian.AppendUint64(nil, enc.Checkpoint.Index))
	stream.Write(enc.Checkpoint.State[:])
	p, err := clusterbootstrap.BootstrapOfflineDataStoreProbe(t.Context(), root, enc)
	require.NoError(t, err)
	stream.WriteString("EKVPBBL1")
	stream.Write(binary.LittleEndian.AppendUint64(nil, p.Watermark))
	require.NoError(t, clusterbootstrap.BootstrapOfflineWithData(t.Context(), root, true, func(db *pebble.DB) error {
		it, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			stream.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(it.Key()))))
			stream.Write(it.Key())
			stream.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(it.Value()))))
			stream.Write(it.Value())
		}
		return it.Error()
	}))
	before := offlineDataFiles(t, root)
	require.NoError(t, enc.NativeCrypto().ValidateSnapshot(t.Context(), 10, bytes.NewReader(stream.Bytes())))
	bad := bytes.Clone(stream.Bytes())
	bad[42] ^= 1
	require.Error(t, enc.NativeCrypto().ValidateSnapshot(t.Context(), 10, bytes.NewReader(bad)))
	// A self-consistent header AND raw checkpoint is not sufficient at the
	// same encryption-entry index: it must match the independently pinned state.
	rawCheckpoint := binary.BigEndian.AppendUint64(nil, enc.Checkpoint.Index)
	rawCheckpoint = append(rawCheckpoint, enc.Checkpoint.State[:]...)
	consistentTamper := bytes.Clone(stream.Bytes())
	occurrences := 0
	for start := 0; start < len(consistentTamper); {
		at := bytes.Index(consistentTamper[start:], rawCheckpoint)
		if at < 0 {
			break
		}
		at += start
		consistentTamper[at+8] ^= 1
		occurrences++
		start = at + len(rawCheckpoint)
	}
	require.Equal(t, 2, occurrences, "fixture includes the carried and raw checkpoints")
	require.ErrorContains(t, enc.NativeCrypto().ValidateSnapshot(t.Context(), 10, bytes.NewReader(consistentTamper)), "same-index")
	require.Error(t, enc.NativeCrypto().ValidateSnapshot(t.Context(), 8, bytes.NewReader(stream.Bytes())))
	require.Error(t, enc.NativeCrypto().ValidateSnapshot(t.Context(), 10, bytes.NewReader(stream.Bytes()[:stream.Len()-1])))
	require.Equal(t, before, offlineDataFiles(t, root))
}

func TestBootstrapOfflineEncryptionSnapshotHeaderBoundaries(t *testing.T) {
	t.Parallel()
	e := &clusterbootstrap.BootstrapOfflineEncryption{Checkpoint: encryption.SnapshotCheckpoint{Index: 10, State: [32]byte{1}}, Cutover: 6}
	for _, tc := range []struct {
		name           string
		index, cutover uint64
		checkpoint     encryption.SnapshotCheckpoint
		valid          bool
	}{
		{"current matching", 10, 6, e.Checkpoint, true},
		{"current state mismatch", 10, 6, encryption.SnapshotCheckpoint{Index: 10, State: [32]byte{2}}, false},
		{"historical checkpoint allowed", 8, 6, encryption.SnapshotCheckpoint{Index: 7, State: [32]byte{2}}, true},
		{"before cutover remains plaintext", 5, 0, encryption.SnapshotCheckpoint{Index: 4, State: [32]byte{2}}, true},
		{"future cutover beyond snapshot", 5, 6, encryption.SnapshotCheckpoint{Index: 4}, false},
		{"cutover beyond checkpoint", 8, 6, encryption.SnapshotCheckpoint{Index: 5}, false},
		{"missing cutover at boundary", 6, 0, encryption.SnapshotCheckpoint{Index: 6}, false},
		{"checkpoint beyond snapshot", 8, 6, e.Checkpoint, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := e.ValidateSnapshotHeader(tc.index, tc.cutover, tc.checkpoint)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

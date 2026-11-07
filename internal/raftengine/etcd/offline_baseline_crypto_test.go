package etcd

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func offlineCryptoCipher(t *testing.T, historical bool, wrong bool) *encryption.Cipher {
	t.Helper()
	keys := encryption.NewKeystore()
	for _, id := range []uint32{1, 2} {
		if id == 1 && !historical {
			continue
		}
		value := byte(id)
		if wrong {
			value += 10
		}
		require.NoError(t, keys.Set(id, bytes.Repeat([]byte{value}, encryption.KeySize)))
	}
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	return cipher
}

func offlineCryptoSeal(t *testing.T, cipher *encryption.Cipher, id uint32, data []byte) []byte {
	t.Helper()
	nonce := make([]byte, encryption.NonceSize)
	_, err := rand.Read(nonce)
	require.NoError(t, err)
	sealed, err := encryption.WrapRaftPayload(cipher, id, nonce, data)
	require.NoError(t, err)
	return sealed
}

// The callback is a native boundary fixture, not the production storage codec.
// Main/store integration separately supplies the actual FSM snapshot verifier.
func offlineCryptoFixture(t *testing.T, tamper bool) (OfflineBaselineOptions, ed25519.PrivateKey) {
	t.Helper()
	o, signer := offlineBaselineFixture(t, false)
	cipher := offlineCryptoCipher(t, true, false)
	require.NoError(t, o.Ownership.Close())
	w, err := wal.Open(zap.NewNop(), filepath.Join(o.DataDir, walDirName), &walpb.Snapshot{Index: proto.Uint64(3), Term: proto.Uint64(4)})
	require.NoError(t, err)
	_, _, _, err = w.ReadAll()
	require.NoError(t, err)
	covered := offlineCryptoSeal(t, cipher, 1, []byte("covered historical payload"))
	if tamper {
		covered[len(covered)-1] ^= 1
	}
	entries := []*raftpb.Entry{
		{Index: proto.Uint64(2), Term: proto.Uint64(3), Type: raftpb.EntryNormal.Enum(), Data: encodeProposalEnvelope(2, []byte("plaintext cutover entry"))},
		{Index: proto.Uint64(3), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum(), Data: encodeProposalEnvelope(3, covered)},
		{Index: proto.Uint64(4), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum(), Data: encodeProposalEnvelope(4, offlineCryptoSeal(t, cipher, 2, []byte("decrypted tail")))},
		{Index: proto.Uint64(5), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum()},
	}
	require.NoError(t, w.Save(&raftpb.HardState{Term: proto.Uint64(9), Vote: proto.Uint64(7), Commit: proto.Uint64(5)}, entries))
	require.NoError(t, w.Close())
	sealedSnapshot := offlineCryptoSeal(t, cipher, 1, []byte("snapshot logical cut"))
	require.NoError(t, snap.New(zap.NewNop(), filepath.Join(o.DataDir, snapDirName)).SaveSnap(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(3), Term: proto.Uint64(4), ConfState: &raftpb.ConfState{Voters: []uint64{7}}}, Data: sealedSnapshot}))
	o.Ownership, err = AcquireOfflineBaselineOwnership(o.DataDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Ownership.Close()) })
	o.Expected.CommitIndex = 5
	base := o.ProbeStore
	o.ProbeStore = func(ctx context.Context) (BaselineStoreProof, error) {
		proof, err := base(ctx)
		proof.AppliedIndex, proof.RaftEnvelopeCutoverIndex = 5, 2
		proof.EncryptionContextRequired = true
		return proof, err
	}
	o.Crypto = &OfflineBaselineCrypto{RaftCipher: cipher, RaftEnvelopeCutoverIndex: 2,
		ValidateSnapshot: func(ctx context.Context, index uint64, reader io.Reader) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			require.Equal(t, uint64(3), index)
			data, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			plain, err := encryption.UnwrapRaftPayload(cipher, data)
			if err == nil && !bytes.Equal(plain, []byte("snapshot logical cut")) {
				err = ErrOfflineBaseline
			}
			clear(plain)
			return err
		}}
	return o, signer
}

func TestOfflineBaselineCryptoAuthenticatesRetainedAndSourceReplay(t *testing.T) {
	t.Parallel()
	o, _ := offlineCryptoFixture(t, false)
	before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	inspection, err := InspectOfflineBaseline(t.Context(), o)
	require.NoError(t, err)
	require.Equal(t, uint64(9), inspection.HardState.GetTerm())
	require.Equal(t, uint64(7), inspection.HardState.GetVote())
	proof, err := o.ProbeStore(t.Context())
	require.NoError(t, err)
	source, err := ReadOfflineBaselineWALSourceWithOptions(t.Context(), OfflineBaselineSourceOptions{
		DataDir: o.DataDir, Ownership: o.Ownership, Crypto: o.Crypto, StoreProof: &proof})
	require.NoError(t, err)
	require.Equal(t, uint64(1), source.ConfigurationIndex)
	require.Equal(t, []byte("decrypted tail"), source.Replay[0].Payload)
	require.Empty(t, source.Replay[1].Payload)
	_, err = io.Copy(io.Discard, source.Snapshot)
	require.NoError(t, err)
	require.NoError(t, source.Snapshot.Close())
	after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	require.Equal(t, before, after, "authentication must not rewrite source ciphertext")
}

func TestOfflineBaselineCryptoRejectsInvalidContextOrHistory(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"missing context", "wrong key", "missing historical key", "wrong cutover", "missing snapshot verifier", "partial snapshot", "snapshot authentication", "covered tamper"} {
		t.Run(kind, func(t *testing.T) {
			o, _ := offlineCryptoFixture(t, kind == "covered tamper")
			switch kind {
			case "missing context":
				o.Crypto = nil
			case "wrong key":
				o.Crypto.RaftCipher = offlineCryptoCipher(t, true, true)
			case "missing historical key":
				o.Crypto.RaftCipher = offlineCryptoCipher(t, false, false)
			case "wrong cutover":
				o.Crypto.RaftEnvelopeCutoverIndex = 3
			case "missing snapshot verifier":
				o.Crypto.ValidateSnapshot = nil
			case "partial snapshot":
				o.Crypto.ValidateSnapshot = func(context.Context, uint64, io.Reader) error { return nil }
			case "snapshot authentication":
				o.Crypto.ValidateSnapshot = func(context.Context, uint64, io.Reader) error { return encryption.ErrIntegrity }
			}
			_, err := InspectOfflineBaseline(t.Context(), o)
			require.Error(t, err)
		})
	}
}

func TestOfflineBaselineCryptoStorageOnlyRequiresExplicitMode(t *testing.T) {
	t.Parallel()
	crypto := &OfflineBaselineCrypto{ValidateSnapshot: func(_ context.Context, _ uint64, reader io.Reader) error {
		_, err := io.Copy(io.Discard, reader)
		return err
	}}
	proof := BaselineStoreProof{EncryptionContextRequired: true}
	require.NoError(t, validateOfflineBaselineCrypto(crypto))
	require.NoError(t, validateOfflineBaselineCryptoCut(crypto, proof, 3))
	require.Error(t, validateOfflineBaselineCryptoCut(nil, proof, 3))
	require.Error(t, validateOfflineBaselineCryptoCut(crypto, BaselineStoreProof{}, 3))
}

func TestOfflineBaselineCryptoPostCutoverHasNoPlaintextFallback(t *testing.T) {
	t.Parallel()
	crypto := &OfflineBaselineCrypto{RaftCipher: offlineCryptoCipher(t, true, false), RaftEnvelopeCutoverIndex: 2}
	for _, payload := range [][]byte{nil, []byte("plaintext must not be accepted")} {
		entry := &raftpb.Entry{Index: proto.Uint64(3), Type: raftpb.EntryNormal.Enum(), Data: encodeProposalEnvelope(1, payload)}
		_, err := offlineBaselineReplayPayload(entry, crypto)
		require.Error(t, err, "only a genuinely empty native no-op bypasses envelope authentication")
	}
	noOp := &raftpb.Entry{Index: proto.Uint64(3), Type: raftpb.EntryNormal.Enum()}
	payload, err := offlineBaselineReplayPayload(noOp, crypto)
	require.NoError(t, err)
	require.Empty(t, payload)
}

func TestOfflineBaselineCryptoSourceRejectsInconsistentStoreProof(t *testing.T) {
	t.Parallel()
	o, _ := offlineCryptoFixture(t, false)
	for _, kind := range []string{"unapplied", "pending", "namespace"} {
		t.Run(kind, func(t *testing.T) {
			proof, err := o.ProbeStore(t.Context())
			require.NoError(t, err)
			switch kind {
			case "unapplied":
				proof.AppliedIndex--
			case "pending":
				proof.PreparedTransactions = 1
			case "namespace":
				proof.ConfigurationMembers = []uint64{99}
			}
			_, err = ReadOfflineBaselineWALSourceWithOptions(t.Context(), OfflineBaselineSourceOptions{
				DataDir: o.DataDir, Ownership: o.Ownership, Crypto: o.Crypto, StoreProof: &proof})
			require.Error(t, err)
		})
	}
}

func TestOfflineBaselineCryptoLogicalCutIsIndependentOfCiphertext(t *testing.T) {
	t.Parallel()
	first, _ := offlineCryptoFixture(t, false)
	second, _ := offlineCryptoFixture(t, false)
	second.Expected = first.Expected
	a, err := InspectOfflineBaseline(t.Context(), first)
	require.NoError(t, err)
	b, err := InspectOfflineBaseline(t.Context(), second)
	require.NoError(t, err)
	require.Equal(t, a.Evidence.DataDigest, b.Evidence.DataDigest,
		"the independent logical store proof, not ciphertext bytes, supplies the shared cut")
	require.NotEqual(t, a.Evidence.NativeStateDigest, b.Evidence.NativeStateDigest,
		"member-local native evidence must still bind its exact original ciphertext")
}

func TestOfflineBaselineCryptoAuthenticatesSupersededRecords(t *testing.T) {
	t.Parallel()
	o, _ := offlineCryptoFixture(t, true)
	require.NoError(t, o.Ownership.Close())
	w, err := wal.Open(zap.NewNop(), filepath.Join(o.DataDir, walDirName), &walpb.Snapshot{Index: proto.Uint64(3), Term: proto.Uint64(4)})
	require.NoError(t, err)
	_, _, _, err = w.ReadAll()
	require.NoError(t, err)
	valid := offlineCryptoSeal(t, o.Crypto.RaftCipher, 1, []byte("valid replacement"))
	require.NoError(t, w.Save(&raftpb.HardState{}, []*raftpb.Entry{
		{Index: proto.Uint64(3), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum(), Data: encodeProposalEnvelope(3, valid)},
		{Index: proto.Uint64(4), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum(), Data: encodeProposalEnvelope(4, offlineCryptoSeal(t, o.Crypto.RaftCipher, 2, []byte("decrypted tail")))},
		{Index: proto.Uint64(5), Term: proto.Uint64(4), Type: raftpb.EntryNormal.Enum()},
	}))
	require.NoError(t, w.Close())
	o.Ownership, err = AcquireOfflineBaselineOwnership(o.DataDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, o.Ownership.Close()) })
	image, err := readBaselineWALPayloads(filepath.Join(o.DataDir, walDirName), true)
	require.NoError(t, err)
	_, err = offlineBaselineReplayPayload(image.entries[3], o.Crypto)
	require.NoError(t, err, "final selected entry is authentic")
	_, err = InspectOfflineBaseline(t.Context(), o)
	require.ErrorIs(t, err, encryption.ErrIntegrity, "superseding an entry cannot hide retained invalid ciphertext")
}

func TestOfflineBaselineCryptoObservationAndMigrationPreserveCiphertext(t *testing.T) {
	t.Parallel()
	o, signer := offlineCryptoFixture(t, false)
	probe, err := ObserveOfflineBaseline(t.Context(), OfflineBaselineObservationOptions{
		DataDir: o.DataDir, Ownership: o.Ownership, MemberID: o.MemberID, Crypto: o.Crypto, ProbeStore: o.ProbeStore,
		Identity: o.Expected.Identity, OperationID: o.Expected.OperationID, Minimum: o.Expected.Minimum, Members: o.Expected.Configuration.Members})
	require.NoError(t, err)
	require.Equal(t, o.Expected, probe.BaselineContext)
	manifest := signedOfflineBaseline(t, o, signer)
	// The generic fixture's verifier captures its original plaintext cut; this
	// migration test supplies the actual independently checked encrypted cut.
	verify := o.Preparation.VerifyPreparedStore
	o.Preparation.VerifyPreparedStore = func(ctx context.Context, input BaselinePreparationContext) (BaselinePreparedStoreProof, error) {
		prepared, err := verify(ctx, input)
		if err != nil {
			return prepared, err
		}
		prepared.Baseline, err = o.ProbeStore(ctx)
		return prepared, err
	}
	original, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	paths, err := PrepareOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	backup, err := readBaselineWAL(filepath.Join(paths.BackupDir, walDirName))
	require.NoError(t, err)
	require.True(t, original.equivalent(backup))
	paths, err = ResumeOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	require.Equal(t, "bound", paths.Phase)
	bound, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
	require.NoError(t, err)
	require.True(t, original.equivalent(bound))
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	identity, err := baselineDestinationIdentity(o, digest)
	require.NoError(t, err)
	require.NoError(t, verifyWALIdentity(bound.metadata, identity))
	_, err = VerifyBoundOfflineBaseline(t.Context(), o, manifest)
	require.NoError(t, err)
	withoutCrypto := o
	withoutCrypto.Crypto = nil
	_, err = ResumeOfflineBaseline(t.Context(), withoutCrypto, manifest)
	require.Error(t, err, "resume must preserve explicit authenticated mode")
}

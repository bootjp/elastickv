package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func encryptedBaselineStore(t *testing.T, node, epoch uint16) (*pebbleStore, *OfflineBaselineDecoder) {
	t.Helper()
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(7, bytes.Repeat([]byte{7}, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	st, err := NewPebbleStore(t.TempDir(), WithEncryption(cipher, encryption.NewDeterministicNonceFactory(node, epoch), func() (uint32, bool) { return 7, true }), WithStorageEnvelopeGate(func() bool { return true }))
	require.NoError(t, err)
	s, ok := st.(*pebbleStore)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	for _, v := range []struct {
		key, value string
		ts, ttl    uint64
	}{{"history", "old", 10, 0}, {"history", "latest", 20, 0}, {"expired", "old-but-authenticated", 21, 1}} {
		require.NoError(t, s.PutAt(t.Context(), []byte(v.key), []byte(v.value), v.ts, v.ttl))
	}
	require.NoError(t, s.DeleteAt(t.Context(), []byte("deleted"), 22))
	require.NoError(t, s.PutAt(t.Context(), MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 8), 23, 0))
	require.NoError(t, s.SetDurableAppliedIndex(100))
	control := []byte(RaftControlMetadataPrefix + "R/test")
	body, state, err := s.encryptForKey(control, []byte("replicated control"), 0, false)
	require.NoError(t, err)
	baselineProbeRaw(t, s, control, encodeValue(body, false, 0, state))
	for _, id := range []uint64{1, 2} {
		baselineProbeRaw(t, s, encryption.RegistryKey(7, uint16(id)), encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: id, FirstSeenLocalEpoch: uint16(id + 2), LastSeenLocalEpoch: uint16(id + 2)}))
	}
	decoder, err := NewOfflineBaselineDecoder(cipher)
	require.NoError(t, err)
	return s, decoder
}

func encryptedBaselineProof(t *testing.T, s *pebbleStore, d *OfflineBaselineDecoder) BootstrapBaselineProbe {
	t.Helper()
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	p, err := ProbeBootstrapBaselineSnapshotWithDecoder(t.Context(), snapshot, d)
	require.NoError(t, err)
	return p
}

func TestEncryptedBaselineLogicalEqualityIncludesAllReplicatedRows(t *testing.T) {
	t.Parallel()
	a, da := encryptedBaselineStore(t, 1, 3)
	b, db := encryptedBaselineStore(t, 2, 4)
	pa, pb := encryptedBaselineProof(t, a, da), encryptedBaselineProof(t, b, db)
	require.Equal(t, pa.DataDigest, pb.DataDigest, "different per-node ciphertext/nonces/epochs must yield one logical image")
	as, bs := a.db.NewSnapshot(), b.db.NewSnapshot()
	defer as.Close()
	defer bs.Close()
	require.Error(t, VerifyBootstrapBaselinePhysicalEquality(t.Context(), as, bs), "logical equivalence does not authorize ciphertext replacement")
	baselineProbeRaw(t, b, encryption.RegistryKey(7, 1), encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 1, FirstSeenLocalEpoch: 3, LastSeenLocalEpoch: 4}))
	changed := encryptedBaselineProof(t, b, db)
	require.NotEqual(t, pa.DataDigest, changed.DataDigest, "registry differences remain replicated differences, never excluded")
	var cp [40]byte
	binary.BigEndian.PutUint64(cp[:8], 50)
	cp[8] = 9
	baselineProbeRaw(t, b, encryptionCheckpointKey, cp[:])
	changedCP := encryptedBaselineProof(t, b, db)
	require.NotEqual(t, changed.DataDigest, changedCP.DataDigest)
}

func TestEncryptedBaselineWrongCipherMissingKeyTamperAndLocks(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"wrong cipher", "missing key", "tamper", "rebadged", "pending lock"} {
		t.Run(name, func(t *testing.T) {
			s, d := encryptedBaselineStore(t, 1, 3)
			if name == "wrong cipher" || name == "missing key" {
				keys := encryption.NewKeystore()
				id := uint32(7)
				if name == "missing key" {
					id = 8
				}
				require.NoError(t, keys.Set(id, make([]byte, encryption.KeySize)))
				cipher, err := encryption.NewCipher(keys)
				require.NoError(t, err)
				d, err = NewOfflineBaselineDecoder(cipher)
				require.NoError(t, err)
			}
			if name == "tamper" || name == "rebadged" {
				key := encodeKey([]byte("expired"), 21)
				raw, close, err := s.db.Get(key)
				require.NoError(t, err)
				copyRaw := bytes.Clone(raw)
				require.NoError(t, close.Close())
				if name == "tamper" {
					copyRaw[len(copyRaw)-1] ^= 1
				} else {
					copyRaw[0] = 0
				}
				baselineProbeRaw(t, s, key, copyRaw)
			}
			if name == "pending lock" {
				require.NoError(t, s.PutAt(t.Context(), []byte("!txn|lock|x"), []byte("pending"), 24, 1))
			}
			snapshot := s.db.NewSnapshot()
			defer snapshot.Close()
			_, err := ProbeBootstrapBaselineSnapshotWithDecoder(t.Context(), snapshot, d)
			require.Error(t, err)
		})
	}
}

func TestEncryptedBaselineSnapshotStreamAndExactTransform(t *testing.T) {
	t.Parallel()
	s, d := encryptedBaselineStore(t, 1, 3)
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	watermark, err := readPebbleSnapshotLastCommitTS(snapshot)
	require.NoError(t, err)
	var stream bytes.Buffer
	_, err = newPebbleSnapshot(snapshot, watermark).WriteTo(&stream)
	require.NoError(t, err)
	proof, err := ProbeBootstrapBaselineStreamWithDecoder(t.Context(), bytes.NewReader(stream.Bytes()), d)
	require.NoError(t, err)
	require.Equal(t, encryptedBaselineProof(t, s, d).DataDigest, proof.DataDigest)
	for _, raw := range [][]byte{stream.Bytes()[:len(stream.Bytes())-1], append(bytes.Clone(stream.Bytes()), 1)} {
		_, err := ProbeBootstrapBaselineStreamWithDecoder(t.Context(), bytes.NewReader(raw), d)
		require.Error(t, err)
	}
	// Copy physical rows, then add exactly the trusted physical plain J value.
	target := newBaselineProbeStore(t)
	it, err := snapshot.NewIter(nil)
	require.NoError(t, err)
	for it.First(); it.Valid(); it.Next() {
		require.NoError(t, target.db.Set(it.Key(), it.Value(), pebble.Sync))
	}
	require.NoError(t, it.Close())
	key := baselineTransformKey(7)
	value, err := EncodeOfflinePlainControlMetadata([]byte("trusted prepared"))
	require.NoError(t, err)
	baselineProbeRaw(t, target, []byte(key), value)
	dst := target.db.NewSnapshot()
	defer dst.Close()
	require.NoError(t, VerifyBootstrapBaselinePreparedTransformWithDecoder(t.Context(), snapshot, dst, map[string][]byte{key: value}, d))
}

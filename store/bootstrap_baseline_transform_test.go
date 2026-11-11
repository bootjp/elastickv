package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func baselineTransformKey(group uint64) string {
	cluster := raftbootstrap.UUID{1}
	key := append([]byte(baselinePreparedRawPrefix), cluster[:]...)
	return string(binary.BigEndian.AppendUint64(key, group))
}

func baselineTransformPair(t *testing.T) (*pebbleStore, *pebbleStore) {
	t.Helper()
	source, target := newBaselineProbeStore(t), newBaselineProbeStore(t)
	for _, s := range []*pebbleStore{source, target} {
		baselineProbeRaw(t, s, encodeKey([]byte("key"), 1), encodeValue([]byte("old history"), false, 0, encStateCleartext))
		baselineProbeRaw(t, s, encodeKey([]byte("key"), 8), encodeValue([]byte("latest"), false, 0, encStateCleartext))
		baselineProbeRaw(t, s, encodeKey([]byte("gone"), 2), encodeValue(nil, true, 0, encStateCleartext))
		baselineProbeRaw(t, s, encodeKey([]byte("expired"), 3), encodeValue([]byte("expired"), false, 1, encStateCleartext))
		baselineProbeRaw(t, s, metaLastCommitTSBytes, binary.LittleEndian.AppendUint64(nil, 8))
		baselineProbeRaw(t, s, encodeKey(MigrationConfigurationIndexKey(), 1), encodeValue(binary.BigEndian.AppendUint64(nil, 9), false, 0, encStateCleartext))
		baselineProbeRaw(t, s, []byte(RaftControlMetadataPrefix+"A/before"), []byte("before"))
		baselineProbeRaw(t, s, []byte(RaftControlMetadataPrefix+"Z/after"), []byte("after"))
		baselineProbeRaw(t, s, encryption.RegistryKey(2, 3), encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 3}))
		require.NoError(t, s.SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint{Index: 3, State: [32]byte{1}}))
	}
	require.Equal(t, baselineProbeImage(t, source), baselineProbeImage(t, target))
	return source, target
}

func verifyBaselineTransform(t *testing.T, source, target *pebbleStore, allowed map[string][]byte) error {
	t.Helper()
	src, dst := source.db.NewSnapshot(), target.db.NewSnapshot()
	defer src.Close()
	defer dst.Close()
	return VerifyBootstrapBaselinePreparedTransform(t.Context(), src, dst, allowed)
}

func TestBootstrapBaselinePreparedTransformExactAddition(t *testing.T) {
	t.Parallel()
	source, target := baselineTransformPair(t)
	key, expected := baselineTransformKey(1), []byte("independently trusted prepared bytes")
	beforeSource, beforeTarget := baselineProbeImage(t, source), baselineProbeImage(t, target)
	baselineProbeRaw(t, target, []byte(key), expected)
	preparedTarget := baselineProbeImage(t, target)
	require.NotEqual(t, beforeTarget.DataDigest, preparedTarget.DataDigest, "base digest must still include prepared metadata")
	require.NoError(t, verifyBaselineTransform(t, source, target, map[string][]byte{key: expected}))
	require.Equal(t, beforeSource, baselineProbeImage(t, source))
	require.Equal(t, preparedTarget, baselineProbeImage(t, target), "comparison must not modify either image")
}

func TestBootstrapBaselinePreparedTransformRejectsAllOtherDeltas(t *testing.T) {
	t.Parallel()
	key, expected := baselineTransformKey(1), []byte("trusted bytes")
	cases := []struct {
		name   string
		mutate func(*testing.T, *pebbleStore, *pebbleStore)
	}{
		{"missing addition", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			require.NoError(t, target.db.Delete([]byte(key), pebble.Sync))
		}},
		{"wrong prepared bytes", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, []byte(key), []byte("different bytes"))
		}},
		{"already existed identically", func(t *testing.T, source, _ *pebbleStore) {
			baselineProbeRaw(t, source, []byte(key), expected)
		}},
		{"replaced prepared record", func(t *testing.T, source, _ *pebbleStore) {
			baselineProbeRaw(t, source, []byte(key), []byte("old preparation"))
		}},
		{"other journal addition", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, []byte(baselineTransformKey(2)), expected)
		}},
		{"new MVCC row", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, encodeKey([]byte("new"), 5), encodeValue([]byte("unapproved"), false, 0, encStateCleartext))
		}},
		{"deleted old MVCC history", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			require.NoError(t, target.db.Delete(encodeKey([]byte("key"), 1), pebble.Sync))
		}},
		{"replaced old MVCC history", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, encodeKey([]byte("key"), 1), encodeValue([]byte("changed old value"), false, 0, encStateCleartext))
		}},
		{"deleted tombstone", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			require.NoError(t, target.db.Delete(encodeKey([]byte("gone"), 2), pebble.Sync))
		}},
		{"replaced expired history", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, encodeKey([]byte("expired"), 3), encodeValue([]byte("expired"), false, 2, encStateCleartext))
		}},
		{"watermark change", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, metaLastCommitTSBytes, binary.LittleEndian.AppendUint64(nil, 9))
		}},
		{"applied index change", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			require.NoError(t, target.SetDurableAppliedIndex(101))
		}},
		{"configuration change", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, encodeKey(MigrationConfigurationIndexKey(), 1), encodeValue(binary.BigEndian.AppendUint64(nil, 10), false, 0, encStateCleartext))
		}},
		{"deleted control record", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			require.NoError(t, target.db.Delete([]byte(RaftControlMetadataPrefix+"A/before"), pebble.Sync))
		}},
		{"checkpoint change", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			require.NoError(t, target.SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint{Index: 3, State: [32]byte{2}}))
		}},
		{"registry epoch change", func(t *testing.T, _ *pebbleStore, target *pebbleStore) {
			baselineProbeRaw(t, target, encryption.RegistryKey(2, 3), encryption.EncodeRegistryValue(encryption.RegistryValue{FullNodeID: 3, LastSeenLocalEpoch: 1}))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source, target := baselineTransformPair(t)
			baselineProbeRaw(t, target, []byte(key), expected)
			tc.mutate(t, source, target)
			require.ErrorIs(t, verifyBaselineTransform(t, source, target, map[string][]byte{key: expected}), ErrBootstrapBaselineTransform)
		})
	}
}

func TestBootstrapBaselinePreparedTransformBoundsAndScope(t *testing.T) {
	t.Parallel()
	key := baselineTransformKey(1)
	zeroCluster := append([]byte(baselinePreparedRawPrefix), make([]byte, len(raftbootstrap.UUID{})+timestampSize)...)
	for _, allowed := range []map[string][]byte{
		nil,
		{},
		{key: []byte("one"), baselineTransformKey(2): []byte("two")},
		{"ordinary": []byte("bytes")},
		{RaftControlMetadataPrefix + "P": []byte("policy replacement")},
		{RaftControlMetadataPrefix + "J/other": []byte("other journal")},
		{string(zeroCluster): []byte("bytes")},
		{key[:len(key)-1]: []byte("bytes")},
		{key: nil},
		{key: bytes.Repeat([]byte{1}, raftbootstrap.MaxFileSize+1)},
	} {
		_, _, err := baselinePreparedAddition(allowed)
		require.ErrorIs(t, err, ErrBootstrapBaselineTransform)
	}
	value := []byte("owned bytes")
	_, copied, err := baselinePreparedAddition(map[string][]byte{key: value})
	require.NoError(t, err)
	value[0]++
	require.Equal(t, []byte("owned bytes"), copied)
}

func TestBootstrapBaselinePreparedTransformPreservesBaseRefusals(t *testing.T) {
	t.Parallel()
	for _, mutateSource := range []bool{true, false} {
		for _, encrypted := range []bool{true, false} {
			source, target := baselineTransformPair(t)
			altered := target
			if mutateSource {
				altered = source
			}
			key, state := []byte("!txn|lock|pending"), encStateCleartext
			wanted := ErrBootstrapBaselinePending
			if encrypted {
				key, state = []byte("encrypted"), encStateEncrypted
				wanted = ErrBootstrapBaselineUnsupported
			}
			baselineProbeRaw(t, altered, encodeKey(key, 1), encodeValue([]byte("unresolved"), false, 0, state))
			preparedKey, value := baselineTransformKey(1), []byte("trusted")
			baselineProbeRaw(t, target, []byte(preparedKey), value)
			require.ErrorIs(t, verifyBaselineTransform(t, source, target, map[string][]byte{preparedKey: value}), wanted)
		}
	}
}

func TestBootstrapBaselinePreparedTransformCancellationAndMissingSnapshot(t *testing.T) {
	t.Parallel()
	source, target := baselineTransformPair(t)
	src, dst := source.db.NewSnapshot(), target.db.NewSnapshot()
	defer src.Close()
	defer dst.Close()
	allowed := map[string][]byte{baselineTransformKey(1): []byte("trusted")}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, VerifyBootstrapBaselinePreparedTransform(ctx, src, dst, allowed), context.Canceled)
	require.ErrorIs(t, VerifyBootstrapBaselinePreparedTransform(t.Context(), nil, dst, allowed), ErrBootstrapBaselineTransform)
	require.ErrorIs(t, VerifyBootstrapBaselinePreparedTransform(t.Context(), src, nil, allowed), ErrBootstrapBaselineTransform)
}

func TestBootstrapBaselinePreparedTransformReadOnlyImagesDoNotMutateDisk(t *testing.T) {
	t.Parallel()
	key, value := baselineTransformKey(1), []byte("independently trusted")
	var dirs []string
	for _, target := range []bool{false, true} {
		dir := t.TempDir()
		dirs = append(dirs, dir)
		st, err := NewPebbleStore(dir)
		require.NoError(t, err)
		s, ok := st.(*pebbleStore)
		require.True(t, ok)
		require.NoError(t, s.PutAt(t.Context(), []byte("k"), []byte("preserved"), 1, 0))
		require.NoError(t, s.SetDurableAppliedIndex(100))
		if target {
			baselineProbeRaw(t, s, []byte(key), value)
		}
		require.NoError(t, s.Close())
	}
	sourceBefore, targetBefore := baselineProbeFiles(t, dirs[0]), baselineProbeFiles(t, dirs[1])
	sourceDB, err := pebble.Open(dirs[0], &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	targetDB, err := pebble.Open(dirs[1], &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	source, target := sourceDB.NewSnapshot(), targetDB.NewSnapshot()
	verifyErr := VerifyBootstrapBaselinePreparedTransform(t.Context(), source, target, map[string][]byte{key: value})
	require.NoError(t, source.Close())
	require.NoError(t, target.Close())
	require.NoError(t, sourceDB.Close())
	require.NoError(t, targetDB.Close())
	require.NoError(t, verifyErr)
	require.Equal(t, sourceBefore, baselineProbeFiles(t, dirs[0]))
	require.Equal(t, targetBefore, baselineProbeFiles(t, dirs[1]))
}

func TestBootstrapBaselinePreparedTransformPropagatesReadFailures(t *testing.T) {
	t.Parallel()
	failure := errors.New("injected transform I/O failure")
	good := baselineProbeErrorIterator{}
	broken := baselineProbeErrorIterator{iterErr: failure}
	require.ErrorIs(t, compareBaselinePreparedRows(t.Context(), broken, good, []byte("allowed"), []byte("value")), failure)
	require.ErrorIs(t, compareBaselinePreparedRows(t.Context(), good, broken, []byte("allowed"), []byte("value")), failure)
	broken = baselineProbeErrorIterator{valueErr: failure}
	require.ErrorIs(t, compareBaselinePreparedRows(t.Context(), broken, broken, []byte("allowed"), []byte("value")), failure)
}

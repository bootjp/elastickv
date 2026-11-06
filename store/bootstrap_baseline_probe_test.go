package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func newBaselineProbeStore(t *testing.T) *pebbleStore {
	t.Helper()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	s, ok := st.(*pebbleStore)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.SetDurableAppliedIndex(100))
	return s
}

func baselineProbeRaw(t *testing.T, s *pebbleStore, key, value []byte) {
	t.Helper()
	require.NoError(t, s.db.Set(key, value, pebble.Sync))
}

func baselineProbeImage(t *testing.T, s *pebbleStore) BootstrapBaselineProbe {
	t.Helper()
	got, err := ProbeBootstrapBaseline(t.Context(), s)
	require.NoError(t, err)
	return got
}

func TestBootstrapBaselineProbeFullHistoryNotLatestRows(t *testing.T) {
	t.Parallel()
	a, b := newBaselineProbeStore(t), newBaselineProbeStore(t)
	for i, s := range []*pebbleStore{a, b} {
		old := []byte("first history")
		if i != 0 {
			old = []byte("different history")
		}
		require.NoError(t, s.PutAt(t.Context(), []byte("key"), old, 10, 0))
		require.NoError(t, s.PutAt(t.Context(), []byte("key"), []byte("same latest"), 20, 0))
	}
	x, y := baselineProbeImage(t, a), baselineProbeImage(t, b)
	require.Equal(t, uint64(2), x.MVCCVersions)
	require.Equal(t, uint64(20), x.Watermark)
	require.True(t, x.WatermarkPresent)
	require.Equal(t, uint64(100), x.AppliedIndex)
	require.NotEqual(t, x.DataDigest, y.DataDigest)
}

func TestBootstrapBaselineProbeCanonicalAcrossWriteOrderAndSSTLayout(t *testing.T) {
	t.Parallel()
	a, b := newBaselineProbeStore(t), newBaselineProbeStore(t)
	rows := []struct {
		key   string
		ts    uint64
		value string
	}{
		{"a", 1, "old"}, {"a\x00", 3, "prefix"}, {"a", 4, "new"}, {"z", 2, "last"},
	}
	for _, row := range rows {
		baselineProbeRaw(t, a, encodeKey([]byte(row.key), row.ts), encodeValue([]byte(row.value), false, 0, encStateCleartext))
	}
	for i := len(rows) - 1; i >= 0; i-- {
		row := rows[i]
		baselineProbeRaw(t, b, encodeKey([]byte(row.key), row.ts), encodeValue([]byte(row.value), false, 0, encStateCleartext))
		require.NoError(t, b.db.Flush())
	}
	baselineProbeRaw(t, a, metaLastCommitTSBytes, binary.LittleEndian.AppendUint64(nil, 4))
	baselineProbeRaw(t, b, metaLastCommitTSBytes, binary.LittleEndian.AppendUint64(nil, 4))
	require.Equal(t, baselineProbeImage(t, a), baselineProbeImage(t, b))
}

func TestBootstrapBaselineProbeIncludesEveryVersionAndRawMetadata(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		alter func(*testing.T, *pebbleStore)
	}{
		{"tombstone history", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, encodeKey([]byte("gone"), 2), encodeValue(nil, true, 0, encStateCleartext))
		}},
		{"expired history", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, encodeKey([]byte("expired"), 2), encodeValue([]byte("still hashed"), false, 1, encStateCleartext))
		}},
		{"watermark only", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, metaLastCommitTSBytes, binary.LittleEndian.AppendUint64(nil, 90))
		}},
		{"retention boundary", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, metaMinRetainedTSBytes, binary.LittleEndian.AppendUint64(nil, 2))
		}},
		{"applied proof", func(t *testing.T, s *pebbleStore) { require.NoError(t, s.SetDurableAppliedIndex(101)) }},
		{"protected policy", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, []byte(RaftControlMetadataPrefix+"P"), []byte("applied policy"))
		}},
		{"protected receipt", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, []byte(RaftControlMetadataPrefix+"R/op"), []byte("receipt"))
		}},
		{"replicated encryption checkpoint", func(t *testing.T, s *pebbleStore) {
			require.NoError(t, s.SaveEncryptionSnapshotCheckpoint(encryption.SnapshotCheckpoint{Index: 3, State: [32]byte{1}}))
		}},
		{"replicated writer registry", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, encryption.RegistryKey(2, 3), encryption.EncodeRegistryValue(encryption.RegistryValue{
				FullNodeID: 3, LastSeenLocalEpoch: 7,
			}))
		}},
		{"local auxiliary not silently excluded", func(t *testing.T, s *pebbleStore) {
			baselineProbeRaw(t, s, encodeKey([]byte("!s3|chunkblob|local"), 2), encodeValue([]byte("blob"), false, 0, encStateCleartext))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newBaselineProbeStore(t)
			before := baselineProbeImage(t, s)
			tc.alter(t, s)
			after := baselineProbeImage(t, s)
			require.NotEqual(t, before.DataDigest, after.DataDigest)
		})
	}
}

func TestBootstrapBaselineProbeConfigurationProof(t *testing.T) {
	t.Parallel()
	s := newBaselineProbeStore(t)
	before := baselineProbeImage(t, s)
	require.False(t, before.ConfigurationIndexPresent)
	require.False(t, before.WatermarkPresent)
	for _, index := range []uint64{8, 9} {
		baselineProbeRaw(t, s, encodeKey(MigrationConfigurationIndexKey(), index),
			encodeValue(binary.BigEndian.AppendUint64(nil, index), false, 0, encStateCleartext))
	}
	got := baselineProbeImage(t, s)
	require.Equal(t, uint64(9), got.ConfigurationIndex)
	require.True(t, got.ConfigurationIndexPresent)
	require.Equal(t, uint64(2), got.MVCCVersions)
	require.NotEqual(t, before.DataDigest, got.DataDigest)
}

func TestBootstrapBaselineProbeRejectsLivePreparedArtifacts(t *testing.T) {
	t.Parallel()
	for _, prefix := range []string{"!txn|lock|", "!txn|int|", "!dist|migstage|", "!migstage|"} {
		t.Run(prefix, func(t *testing.T) {
			s := newBaselineProbeStore(t)
			key := []byte(prefix + "key")
			// Even an expired lock, or one claiming committed evidence, must be
			// resolved by its real protocol, never by this read-only probe.
			baselineProbeRaw(t, s, encodeKey(key, 2), encodeValue([]byte("prepared"), false, 1, encStateCleartext))
			got, err := ProbeBootstrapBaseline(t.Context(), s)
			require.ErrorIs(t, err, ErrBootstrapBaselinePending)
			require.Equal(t, BootstrapBaselineProbe{}, got)
			baselineProbeRaw(t, s, encodeKey(key, 3), encodeValue(nil, true, 0, encStateCleartext))
			settled := baselineProbeImage(t, s)
			require.Equal(t, uint64(2), settled.MVCCVersions, "resolved history must remain in digest")
			baselineProbeRaw(t, s, encodeKey(key, 4), encodeValue([]byte("new prepare"), false, 0, encStateCleartext))
			_, err = ProbeBootstrapBaseline(t.Context(), s)
			require.ErrorIs(t, err, ErrBootstrapBaselinePending)
		})
	}
}

func TestBootstrapBaselineProbeRejectsIncompleteOrUnsupportedImage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  []byte
		data []byte
	}{
		{"short MVCC key", []byte("x"), []byte("v")},
		{"short value", encodeKey([]byte("k"), 1), []byte("v")},
		{"encrypted value", encodeKey([]byte("k"), 1), encodeValue([]byte("ciphertext"), false, 0, encStateEncrypted)},
		{"reserved flags", encodeKey([]byte("k"), 1), append([]byte{0xff}, make([]byte, 8)...)},
		{"bad applied index", metaAppliedIndexBytes, []byte{1}},
		{"zero applied index", metaAppliedIndexBytes, make([]byte, 8)},
		{"bad watermark", metaLastCommitTSBytes, []byte{1}},
		{"sentinel watermark", metaLastCommitTSBytes, binary.LittleEndian.AppendUint64(nil, ^uint64(0))},
		{"pending retention", metaPendingMinRetainedTSBytes, binary.LittleEndian.AppendUint64(nil, 5)},
		{"future txn namespace", encodeKey([]byte("!txn|future|k"), 1), encodeValue([]byte("?"), false, 0, encStateCleartext)},
		{"persisted wire-only meta", encodeKey([]byte("!txn|meta|"), 1), encodeValue([]byte("?"), false, 0, encStateCleartext)},
		{"bad configuration", encodeKey(MigrationConfigurationIndexKey(), 1), encodeValue([]byte{1}, false, 0, encStateCleartext)},
		{"configuration beyond cut", encodeKey(MigrationConfigurationIndexKey(), 1), encodeValue(binary.BigEndian.AppendUint64(nil, 101), false, 0, encStateCleartext)},
		{"deleted configuration", encodeKey(MigrationConfigurationIndexKey(), 1), encodeValue(nil, true, 0, encStateCleartext)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newBaselineProbeStore(t)
			baselineProbeRaw(t, s, tc.key, tc.data)
			got, err := ProbeBootstrapBaseline(t.Context(), s)
			require.Error(t, err)
			require.Equal(t, BootstrapBaselineProbe{}, got)
		})
	}
	s := newBaselineProbeStore(t)
	require.NoError(t, s.db.Delete(metaAppliedIndexBytes, pebble.Sync))
	_, err := ProbeBootstrapBaseline(t.Context(), s)
	require.ErrorIs(t, err, ErrBootstrapBaselineInvalid)
}

func TestBootstrapBaselineProbeCancellationAndUnsupportedStore(t *testing.T) {
	t.Parallel()
	s := newBaselineProbeStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err := ProbeBootstrapBaseline(ctx, s)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, BootstrapBaselineProbe{}, got)
	for _, st := range []MVCCStore{nil, (*pebbleStore)(nil), NewMVCCStore()} {
		got, err = ProbeBootstrapBaseline(t.Context(), st)
		require.ErrorIs(t, err, ErrBootstrapBaselineUnsupported)
		require.Equal(t, BootstrapBaselineProbe{}, got)
	}
}

func TestBootstrapBaselineProbePinnedSnapshot(t *testing.T) {
	t.Parallel()
	s := newBaselineProbeStore(t)
	require.NoError(t, s.PutAt(t.Context(), []byte("k"), []byte("before"), 1, 0))
	before := baselineProbeImage(t, s)
	snapshot := s.db.NewSnapshot()
	defer snapshot.Close()
	require.NoError(t, s.PutAt(t.Context(), []byte("k"), []byte("after"), 2, 0))
	require.NoError(t, s.SetDurableAppliedIndex(101))
	pinned, err := ProbeBootstrapBaselineSnapshot(t.Context(), snapshot)
	require.NoError(t, err)
	require.Equal(t, before, pinned)
	require.NotEqual(t, before.DataDigest, baselineProbeImage(t, s).DataDigest)
	_, err = ProbeBootstrapBaselineSnapshot(t.Context(), nil)
	require.ErrorIs(t, err, ErrBootstrapBaselineInvalid)
}

func baselineProbeFiles(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	files := make(map[string][32]byte)
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files[path] = sha256.Sum256(data)
		}
		return nil
	}))
	return files
}

func TestBootstrapBaselineProbeReadOnlyReopenDoesNotMutateDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	s, ok := st.(*pebbleStore)
	require.True(t, ok)
	require.NoError(t, s.PutAt(t.Context(), []byte("k"), []byte("history"), 1, 0))
	require.NoError(t, s.SetDurableAppliedIndex(100))
	want := baselineProbeImage(t, s)
	require.NoError(t, s.Close())
	before := baselineProbeFiles(t, dir)
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	readOnly := &pebbleStore{db: db}
	got, probeErr := ProbeBootstrapBaseline(t.Context(), readOnly)
	require.NoError(t, db.Close())
	require.NoError(t, probeErr)
	require.Equal(t, want, got)
	require.Equal(t, before, baselineProbeFiles(t, dir))
}

type baselineProbeErrorIterator struct {
	valueErr error
	iterErr  error
}

func (i baselineProbeErrorIterator) First() bool { return i.valueErr != nil }
func (i baselineProbeErrorIterator) Next() bool  { return false }
func (i baselineProbeErrorIterator) Key() []byte { return nil }
func (i baselineProbeErrorIterator) ValueAndErr() ([]byte, error) {
	return nil, i.valueErr
}
func (i baselineProbeErrorIterator) Error() error { return i.iterErr }

func TestBootstrapBaselineProbePropagatesIteratorIOFailures(t *testing.T) {
	t.Parallel()
	failure := errors.New("injected iterator I/O failure")
	for _, iter := range []baselineProbeErrorIterator{{valueErr: failure}, {iterErr: failure}} {
		reader := baselineProbeReader{}
		require.ErrorIs(t, reader.scan(t.Context(), iter), failure)
	}
}

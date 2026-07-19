package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/stretchr/testify/require"
)

type shortWriteBuffer struct {
	bytes.Buffer
}

func (w *shortWriteBuffer) Write(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return w.Buffer.Write(p)
}

func openSSTSnapshotTestStore(t *testing.T, dir string, opts ...PebbleStoreOption) *pebbleStore {
	t.Helper()
	mvcc, err := NewPebbleStore(dir, opts...)
	require.NoError(t, err)
	store, ok := mvcc.(*pebbleStore)
	require.True(t, ok)
	t.Cleanup(func() {
		if store.db != nil {
			require.NoError(t, store.Close())
		}
	})
	return store
}

func withSSTIngestTargetFileBytesForTest(target uint64) PebbleStoreOption {
	return func(store *pebbleStore) {
		store.sstIngestTargetFileBytes = target
	}
}

func writeSSTSnapshotTestData(t *testing.T, store *pebbleStore) {
	t.Helper()
	ctx := context.Background()
	timestamps := []uint64{10, 11, 12, 13}
	for i, key := range []string{"alpha", "bravo", "charlie", "delta"} {
		value := strings.Repeat(key, 32)
		require.NoError(t, store.PutAt(ctx, []byte(key), []byte(value), timestamps[i], 0))
	}
	require.NoError(t, store.SetDurableAppliedIndex(77))
}

func snapshotBytesForTest(t *testing.T, store *pebbleStore) ([]byte, Snapshot) {
	t.Helper()
	snapshot, err := store.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	return raw.Bytes(), snapshot
}

func TestPebbleStoreSSTIngestSnapshotRoundTrip(t *testing.T) {
	setPebbleCacheBytesForTest(t, 8<<20)
	src := openSSTSnapshotTestStore(
		t,
		filepath.Join(t.TempDir(), "src"),
		WithSSTIngestSnapshots(true),
		withSSTIngestTargetFileBytesForTest(128),
	)
	writeSSTSnapshotTestData(t, src)

	snapshot, err := src.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, snapshot.Close()) })
	sstSnapshot, ok := snapshot.(*pebbleSSTIngestSnapshot)
	require.True(t, ok)
	checkpointDir := sstSnapshot.checkpointDir
	require.DirExists(t, checkpointDir)

	// This write happens after the checkpoint and must not leak into the image.
	require.NoError(t, src.PutAt(context.Background(), []byte("late"), []byte("not-in-snapshot"), 99, 0))
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.Equal(t, pebbleSSTIngestSnapshotMagic[:], raw.Bytes()[:len(pebbleSSTIngestSnapshotMagic)])

	reader := bytes.NewReader(raw.Bytes())
	manifest, err := readPebbleSSTIngestManifest(reader)
	require.NoError(t, err)
	require.Greater(t, len(manifest.Files), 1)
	require.GreaterOrEqual(t, manifest.EntryCount, uint64(4))
	require.NotNil(t, manifest.AppliedIndex)
	require.Equal(t, uint64(77), *manifest.AppliedIndex)

	dst := openSSTSnapshotTestStore(t, filepath.Join(t.TempDir(), "dst"))
	require.NoError(t, dst.PutAt(context.Background(), []byte("old"), []byte("remove-me"), 1, 0))
	require.NoError(t, dst.Restore(iotest.OneByteReader(bytes.NewReader(raw.Bytes()))))

	for _, key := range []string{"alpha", "bravo", "charlie", "delta"} {
		value, getErr := dst.GetAt(context.Background(), []byte(key), 100)
		require.NoError(t, getErr)
		require.Equal(t, strings.Repeat(key, 32), string(value))
	}
	_, err = dst.GetAt(context.Background(), []byte("late"), 100)
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = dst.GetAt(context.Background(), []byte("old"), 100)
	require.ErrorIs(t, err, ErrKeyNotFound)
	applied, present, err := dst.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(77), applied)

	require.NoError(t, snapshot.Close())
	require.NoDirExists(t, checkpointDir)
}

func TestPebbleStoreSSTIngestSnapshotCorruptionPreservesDestination(t *testing.T) {
	setPebbleCacheBytesForTest(t, 8<<20)
	src := openSSTSnapshotTestStore(t, filepath.Join(t.TempDir(), "src"), WithSSTIngestSnapshots(true))
	writeSSTSnapshotTestData(t, src)
	raw, snapshot := snapshotBytesForTest(t, src)
	require.NoError(t, snapshot.Close())

	manifestLen, err := internalutil.Uint64ToInt(binary.BigEndian.Uint64(raw[len(pebbleSSTIngestSnapshotMagic):]))
	require.NoError(t, err)
	dataOffset := len(pebbleSSTIngestSnapshotMagic) + 8 + sstIngestSnapshotDigestBytes + manifestLen
	require.Less(t, dataOffset, len(raw))

	tests := []struct {
		name    string
		corrupt func([]byte) []byte
	}{
		{"manifest digest", func(corrupt []byte) []byte {
			manifestOffset := len(pebbleSSTIngestSnapshotMagic) + 8 + sstIngestSnapshotDigestBytes
			corrupt[manifestOffset] ^= 0x01
			return corrupt
		}},
		{"file digest", func(corrupt []byte) []byte {
			corrupt[dataOffset] ^= 0x01
			return corrupt
		}},
		{"truncated file", func(corrupt []byte) []byte {
			return corrupt[:len(corrupt)-1]
		}},
		{"trailing bytes", func(corrupt []byte) []byte {
			return append(corrupt, 0x01)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := append([]byte(nil), raw...)
			corrupt = test.corrupt(corrupt)
			dstDir := filepath.Join(t.TempDir(), "dst")
			dst := openSSTSnapshotTestStore(t, dstDir)
			require.NoError(t, dst.PutAt(context.Background(), []byte("sentinel"), []byte("keep"), 5, 0))
			require.Error(t, dst.Restore(bytes.NewReader(corrupt)))
			value, err := dst.GetAt(context.Background(), []byte("sentinel"), 10)
			require.NoError(t, err)
			require.Equal(t, []byte("keep"), value)
			matches, err := filepath.Glob(dstDir + "-sst-*")
			require.NoError(t, err)
			require.Empty(t, matches)
		})
	}
}

func TestPebbleStoreSSTIngestSnapshotFallsBackBeforeStreaming(t *testing.T) {
	setPebbleCacheBytesForTest(t, 8<<20)
	src := openSSTSnapshotTestStore(t, filepath.Join(t.TempDir(), "src"), WithSSTIngestSnapshots(true))
	writeSSTSnapshotTestData(t, src)
	snapshot, err := src.Snapshot()
	require.NoError(t, err)
	sstSnapshot, ok := snapshot.(*pebbleSSTIngestSnapshot)
	require.True(t, ok)
	require.NoError(t, os.RemoveAll(sstSnapshot.checkpointDir))

	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.Equal(t, pebbleSnapshotMagic[:], raw.Bytes()[:len(pebbleSnapshotMagic)])

	dst := openSSTSnapshotTestStore(t, filepath.Join(t.TempDir(), "dst"))
	require.NoError(t, dst.Restore(bytes.NewReader(raw.Bytes())))
	value, err := dst.GetAt(context.Background(), []byte("alpha"), 100)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("alpha", 32), string(value))
	require.NoError(t, snapshot.Close())
}

func TestPebbleStoreCleansStaleSSTSnapshotArtifacts(t *testing.T) {
	setPebbleCacheBytesForTest(t, 8<<20)
	t.Run("recovers interrupted swap", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "fsm.db")
		original := openSSTSnapshotTestStore(t, dir)
		require.NoError(t, original.PutAt(context.Background(), []byte("sentinel"), []byte("keep"), 5, 0))
		require.NoError(t, original.Close())
		original.db = nil

		backup := dir + "-restore-backup-interrupted"
		require.NoError(t, os.Rename(dir, backup))
		restored := openSSTSnapshotTestStore(t, dir)
		value, err := restored.GetAt(context.Background(), []byte("sentinel"), 10)
		require.NoError(t, err)
		require.Equal(t, []byte("keep"), value)
		require.NoDirExists(t, backup)
	})

	t.Run("removes stale artifacts around live store", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "fsm[1].db")
		original := openSSTSnapshotTestStore(t, dir)
		require.NoError(t, original.PutAt(context.Background(), []byte("sentinel"), []byte("keep"), 5, 0))
		require.NoError(t, original.Close())
		original.db = nil

		stale := []string{
			dir + "-sst-checkpoint-stale",
			dir + "-sst-ingest-stale",
			dir + "-sst-receive-stale",
			dir + "-restore-backup-stale",
		}
		for _, path := range stale {
			require.NoError(t, os.MkdirAll(path, 0755))
		}
		restored := openSSTSnapshotTestStore(t, dir)
		value, err := restored.GetAt(context.Background(), []byte("sentinel"), 10)
		require.NoError(t, err)
		require.Equal(t, []byte("keep"), value)
		for _, path := range stale {
			require.NoDirExists(t, path)
		}
	})

	t.Run("preserves ambiguous restore backups", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "fsm.db")
		backups := []string{
			dir + "-restore-backup-first",
			dir + "-restore-backup-second",
		}
		for _, backup := range backups {
			require.NoError(t, os.MkdirAll(backup, 0755))
		}
		require.Error(t, cleanupPebbleSnapshotArtifacts(dir))
		for _, backup := range backups {
			require.DirExists(t, backup)
		}
		require.NoDirExists(t, dir)
	})
}

func TestSwapInTempDBRollsBackOnMetadataFailure(t *testing.T) {
	setPebbleCacheBytesForTest(t, 8<<20)
	parent := t.TempDir()
	dstDir := filepath.Join(parent, "dst")
	dst := openSSTSnapshotTestStore(t, dstDir)
	require.NoError(t, dst.PutAt(context.Background(), []byte("sentinel"), []byte("keep"), 5, 0))

	tmpDir, err := makeSiblingTempDir(dstDir, "test-replacement")
	require.NoError(t, err)
	replacement, err := NewPebbleStore(tmpDir)
	require.NoError(t, err)
	require.NoError(t, replacement.PutAt(context.Background(), []byte("replacement"), []byte("discard"), 9, 0))
	require.NoError(t, replacement.Close())

	dst.maintenanceMu.Lock()
	dst.dbMu.Lock()
	dst.mtx.Lock()
	err = dst.swapInTempDBWithMetadata(tmpDir, &pebbleSnapshotMetadata{LastCommitTS: 999})
	dst.mtx.Unlock()
	dst.dbMu.Unlock()
	dst.maintenanceMu.Unlock()
	require.Error(t, err)

	value, err := dst.GetAt(context.Background(), []byte("sentinel"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), value)
	_, err = dst.GetAt(context.Background(), []byte("replacement"), 10)
	require.ErrorIs(t, err, ErrKeyNotFound)
	matches, err := filepath.Glob(dstDir + "-restore-backup-*")
	require.NoError(t, err)
	require.Empty(t, matches)
}

func TestSSTIngestSnapshotConfigurationAndManifestValidation(t *testing.T) {
	require.False(t, resolveSSTIngestSnapshots(""))
	require.False(t, resolveSSTIngestSnapshots("invalid"))
	require.True(t, resolveSSTIngestSnapshots(" true "))
	require.True(t, resolveSSTIngestSnapshots("1"))

	manifest := pebbleSSTIngestManifest{
		Version:        sstIngestSnapshotVersion,
		TotalFileBytes: 1,
		Files: []pebbleSSTIngestFileMeta{{
			Name:     "../escape.sst",
			Size:     1,
			SHA256:   strings.Repeat("0", 64),
			Smallest: []byte("a"),
			Largest:  []byte("b"),
		}},
	}
	require.Error(t, validateSSTIngestManifest(manifest))
}

func TestSSTIngestManifestRejectsNegativeLength(t *testing.T) {
	t.Parallel()

	var stream bytes.Buffer
	stream.Write(pebbleSSTIngestSnapshotMagic[:])
	require.NoError(t, binary.Write(&stream, binary.BigEndian, int64(-1)))

	_, err := readPebbleSSTIngestManifest(&stream)
	require.ErrorContains(t, err, "invalid SST ingest manifest length")
}

func TestCountingWriterRejectsShortWrites(t *testing.T) {
	var dst shortWriteBuffer
	writer := &countingWriter{w: &dst}
	n, err := writer.Write([]byte("transport"))
	require.Equal(t, 1, n)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

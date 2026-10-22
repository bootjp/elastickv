package etcd

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func restartOriginStore(t *testing.T, format string) store.MVCCStore {
	t.Helper()
	if format == "mvcc" {
		return store.NewMVCCStore()
	}
	st, err := store.NewPebbleStore(t.TempDir(), store.WithSSTIngestSnapshots(format == "sst"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	return st
}

func prepareRestartOrigin(t *testing.T, format string) string {
	t.Helper()
	st := restartOriginStore(t, format)
	require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 11), 10, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("ordinary"), []byte("value"), 20, 0))
	snapshot, err := kv.NewKvFSMWithHLC(st, kv.NewHLC()).Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	input := filepath.Join(t.TempDir(), "original")
	require.NoError(t, os.WriteFile(input, raw.Bytes(), 0o600))
	dir := filepath.Join(t.TempDir(), "raft")
	_, err = PreparePhysicalSnapshotRestore(PhysicalSnapshotRestoreOptions{
		InputFSMPath: input, DataDir: dir, Index: 42, Term: 7,
		Peers: []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.NoError(t, err)
	return dir
}

func assertRestartOriginEpochAbsent(t *testing.T, st store.MVCCStore) {
	t.Helper()
	_, err := st.GetAt(t.Context(), store.MigrationConfigurationIndexKey(), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound, "a full fallback must not reintroduce the imported epoch")
	value, err := st.GetAt(t.Context(), []byte("ordinary"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestExternalOriginCrashBeforeAppliedCheckpointRevalidates(t *testing.T) {
	for _, format := range []string{"mvcc", "native", "sst"} {
		t.Run(format, func(t *testing.T) {
			dir := prepareRestartOrigin(t, format)
			original, err := readExternalRestoreOrigin(dir)
			require.NoError(t, err)
			for range 2 {
				// A fresh destination has no durable applied index: force the
				// full-restore fallback after the first successful validation.
				st := restartOriginStore(t, format)
				disk, err := loadOriginFixture(dir, kv.NewKvFSMWithHLC(st, kv.NewHLC()))
				require.NoError(t, err)
				require.NoError(t, closePersist(disk.Persist))
				assertRestartOriginEpochAbsent(t, st)
				require.NoError(t, completeExternalRestoreOrigin(dir, disk.ExternalRestoreValidated))
			}
			require.FileExists(t, filepath.Join(dir, externalRestoreOriginFile))
			completed, err := readExternalRestoreOrigin(dir)
			require.NoError(t, err)
			require.True(t, completed.Completed)
			completed.Completed = false
			require.Equal(t, original, completed, "completion cannot change the original identity")
		})
	}
}

func publishRestartNative(t *testing.T, dir string, disk *diskState, st store.MVCCStore) {
	t.Helper()
	require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 66), 30, 0))
	writer, ok := st.(store.MigrationTargetReadinessWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadiness(t.Context(), store.TargetStagedReadinessState{
		JobID: 7, MigrationJobID: 7, Armed: true, MinWriteTSExclusive: 100,
		RouteStart: []byte("a"), RouteEnd: []byte("z"),
	}))
	entry := raftpb.Entry{Index: proto.Uint64(43), Term: proto.Uint64(8), Type: raftpb.EntryNormal.Enum()}
	require.NoError(t, disk.Persist.Save(&raftpb.HardState{Term: proto.Uint64(8), Commit: proto.Uint64(43)}, []*raftpb.Entry{&entry}))
	require.NoError(t, disk.Storage.Append([]*raftpb.Entry{&entry}))
	fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC())
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	engine := &Engine{dataDir: dir, fsmSnapDir: filepath.Join(dir, fsmSnapDirName),
		storage: disk.Storage, persist: disk.Persist, fsm: fsm}
	require.NoError(t, engine.persistLocalSnapshot(snapshotRequest{index: 43, snapshot: snapshot}))
}

func TestExternalOriginNativePublicationAndOriginalFallback(t *testing.T) {
	for _, format := range []string{"mvcc", "native", "sst"} {
		t.Run(format, func(t *testing.T) {
			dir := prepareRestartOrigin(t, format)
			st := restartOriginStore(t, format)
			disk, err := loadOriginFixture(dir, kv.NewKvFSMWithHLC(st, kv.NewHLC()))
			require.NoError(t, err)
			require.NoError(t, completeExternalRestoreOrigin(dir, disk.ExternalRestoreValidated))
			publishRestartNative(t, dir, disk, st)
			require.NoError(t, closePersist(disk.Persist))
			fresh := restartOriginStore(t, format)
			disk, err = loadOriginFixture(dir, kv.NewKvFSMWithHLC(fresh, kv.NewHLC()))
			require.NoError(t, err, "a WAL-published native snapshot must not be externally sanitized")
			require.Equal(t, uint64(43), disk.LocalSnap.GetMetadata().GetIndex())
			require.NoError(t, closePersist(disk.Persist))
			value, err := fresh.GetAt(t.Context(), store.MigrationConfigurationIndexKey(), ^uint64(0))
			require.NoError(t, err)
			require.Equal(t, binary.BigEndian.AppendUint64(nil, 66), value)
			reader, ok := fresh.(store.MigrationTargetReadinessReader)
			require.True(t, ok)
			states, err := reader.MigrationTargetReadinessStates(t.Context())
			require.NoError(t, err)
			require.Len(t, states, 1)
			newest := "0000000000000008-000000000000002b.snap"
			require.NoError(t, os.Remove(filepath.Join(dir, snapDirName, newest)))
			fallback := restartOriginStore(t, format)
			disk, err = loadOriginFixture(dir, kv.NewKvFSMWithHLC(fallback, kv.NewHLC()))
			require.NoError(t, err)
			require.Equal(t, uint64(42), disk.LocalSnap.GetMetadata().GetIndex())
			require.NoError(t, closePersist(disk.Persist))
			assertRestartOriginEpochAbsent(t, fallback)
		})
	}
}

type originOwnedSnapshot struct {
	wrote  bool
	closed bool
}

func (s *originOwnedSnapshot) WriteTo(w io.Writer) (int64, error) {
	s.wrote = true
	return bytes.NewReader([]byte("must not overwrite the original snapshot")).WriteTo(w)
}

func (s *originOwnedSnapshot) Close() error { s.closed = true; return nil }

func TestExternalOriginDuplicateCaptureDoesNotOverwrite(t *testing.T) {
	dir, result := prepareOriginFixture(t)
	disk, err := loadOriginFixture(dir, &originRecordingFSM{})
	require.NoError(t, err)
	defer func() { require.NoError(t, closePersist(disk.Persist)) }()
	before, err := os.ReadFile(result.FSMPath)
	require.NoError(t, err)
	owned := &originOwnedSnapshot{}
	engine := &Engine{dataDir: dir, fsmSnapDir: filepath.Join(dir, fsmSnapDirName), storage: disk.Storage, persist: disk.Persist}
	require.NoError(t, engine.persistLocalSnapshot(snapshotRequest{index: 42, snapshot: owned}))
	require.True(t, owned.closed)
	require.False(t, owned.wrote, "duplicate must be rejected before cleanup or serialization")
	after, err := os.ReadFile(result.FSMPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
	engine.fsm = &originRecordingFSM{}
	engine.snapshotMu.Lock()
	_, err = engine.snapshotPayloadLocked(42)
	engine.snapshotMu.Unlock()
	require.ErrorIs(t, err, raftengine.ErrSnapshotDeferred, "config capture must not overwrite the same retained file")
	after, err = os.ReadFile(result.FSMPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestExternalOriginNativeRequiresCompletion(t *testing.T) {
	dir := prepareRestartOrigin(t, "mvcc")
	st := restartOriginStore(t, "mvcc")
	disk, err := loadOriginFixture(dir, kv.NewKvFSMWithHLC(st, kv.NewHLC()))
	require.NoError(t, err)
	// Deliberately manufacture publication before Open's durable completion.
	// Production cannot do this: completion precedes transport and the run loop.
	publishRestartNative(t, dir, disk, st)
	require.NoError(t, closePersist(disk.Persist))
	probe := &originRecordingFSM{}
	_, err = loadOriginFixture(dir, probe)
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreOrigin)
	require.Empty(t, probe.origins)
	require.Empty(t, probe.bodyBytes)
}

func TestExternalOriginNativeRequiresWALSelection(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	disk, err := loadOriginFixture(dir, &originRecordingFSM{})
	require.NoError(t, err)
	require.NoError(t, completeExternalRestoreOrigin(dir, disk.ExternalRestoreValidated))
	require.NoError(t, closePersist(disk.Persist))
	crc, _ := writeFSMFileForTest(t, filepath.Join(dir, fsmSnapDirName), 43, []byte("not published in WAL"))
	unpublished := raftpb.Snapshot{Data: encodeSnapshotToken(43, crc), Metadata: testSnapshotMetadata(43, 8, []uint64{1})}
	require.NoError(t, snap.New(zap.NewNop(), filepath.Join(dir, snapDirName)).SaveSnap(&unpublished))
	probe := &originRecordingFSM{}
	disk, err = loadOriginFixture(dir, probe)
	require.NoError(t, err)
	require.NoError(t, closePersist(disk.Persist))
	require.Equal(t, uint64(42), disk.LocalSnap.GetMetadata().GetIndex())
	require.Equal(t, []raftengine.SnapshotRestoreOrigin{raftengine.SnapshotRestoreExternalRebase}, probe.origins)
	_, _, err = restoreSnapshotWithOrigin(&originRecordingFSM{}, nil, 0, filepath.Join(dir, fsmSnapDirName), nil, nil)
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreOrigin)
}

func TestExternalOriginNativeValidationBeforeSkip(t *testing.T) {
	for _, corruption := range []string{"token-index", "token-crc", "payload", "unsupported-legacy"} {
		t.Run(corruption, func(t *testing.T) {
			dir := prepareRestartOrigin(t, "mvcc")
			st := restartOriginStore(t, "mvcc")
			disk, err := loadOriginFixture(dir, kv.NewKvFSMWithHLC(st, kv.NewHLC()))
			require.NoError(t, err)
			require.NoError(t, completeExternalRestoreOrigin(dir, disk.ExternalRestoreValidated))
			publishRestartNative(t, dir, disk, st)
			selected, err := disk.Storage.Snapshot()
			require.NoError(t, err)
			require.NoError(t, closePersist(disk.Persist))
			token, err := decodeSnapshotToken(selected.Data)
			require.NoError(t, err)
			switch corruption {
			case "token-index":
				selected.Data = encodeSnapshotToken(44, token.CRC32C)
			case "token-crc":
				selected.Data = encodeSnapshotToken(43, token.CRC32C^1)
			case "payload":
				path := fsmSnapPath(filepath.Join(dir, fsmSnapDirName), 43)
				data, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				data[0] ^= 1
				root, rootErr := os.OpenRoot(dir)
				require.NoError(t, rootErr)
				require.NoError(t, root.WriteFile("fsm-snap/000000000000002b.fsm", data, 0o600))
				require.NoError(t, root.Close())
			case "unsupported-legacy":
				selected.Data = []byte("unrecognized embedded legacy snapshot")
			}
			require.NoError(t, snap.New(zap.NewNop(), filepath.Join(dir, snapDirName)).SaveSnap(selected))
			probe := &originRecordingFSM{skipGateFSM: skipGateFSM{applied: 100, appliedPresent: true}}
			_, err = loadOriginFixture(dir, probe)
			require.Error(t, err)
			require.Empty(t, probe.origins)
			require.Empty(t, probe.bodyBytes)
			require.False(t, probe.restoredHeader, "validation must precede even the header-only skip")
		})
	}
}

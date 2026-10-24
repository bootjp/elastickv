package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type originRecordingFSM struct {
	skipGateFSM
	origins []raftengine.SnapshotRestoreOrigin
	reject  error
}

func (f *originRecordingFSM) RestoreWithContext(_ context.Context, r io.Reader, opts raftengine.SnapshotRestoreOptions) error {
	f.origins = append(f.origins, opts.Origin)
	if f.reject != nil {
		return f.reject
	}
	return f.Restore(r)
}

func prepareOriginFixture(t *testing.T) (string, *ExternalSnapshotRestoreResult) {
	t.Helper()
	root := t.TempDir()
	input := filepath.Join(root, "payload")
	require.NoError(t, os.WriteFile(input, []byte("EKVTHLC1proof-free-payload"), 0o600))
	dir := filepath.Join(root, "raft")
	result, err := PreparePhysicalSnapshotRestore(PhysicalSnapshotRestoreOptions{
		InputFSMPath: input, DataDir: dir, Index: 42, Term: 7,
		Peers: []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.NoError(t, err)
	return dir, result
}

func loadOriginFixture(dir string, fsm StateMachine) (*diskState, error) {
	return loadWalState(zap.NewNop(), filepath.Join(dir, walDirName), filepath.Join(dir, snapDirName),
		filepath.Join(dir, fsmSnapDirName), fsm, nil)
}

func TestExternalOriginForcesValidatedFullRestore(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	fsm := &originRecordingFSM{skipGateFSM: skipGateFSM{applied: 100, appliedPresent: true}}
	disk, err := loadOriginFixture(dir, fsm)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closePersist(disk.Persist)) })
	require.Equal(t, []raftengine.SnapshotRestoreOrigin{raftengine.SnapshotRestoreExternalRebase}, fsm.origins)
	require.Equal(t, uint64(42), disk.EffectiveApplied)
	require.Equal(t, []byte("EKVTHLC1proof-free-payload"), fsm.bodyBytes)
	require.False(t, fsm.restoredHeader, "external restore must not use the header-only skip")
	require.FileExists(t, filepath.Join(dir, externalRestoreOriginFile), "full restore alone cannot consume the marker")
}

func TestExternalOriginRejectsUnsupportedFSM(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	fsm := &skipGateFSM{}
	disk, err := loadOriginFixture(dir, fsm)
	if disk != nil {
		t.Cleanup(func() { require.NoError(t, closePersist(disk.Persist)) })
	}
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreUnsupported)
	require.Empty(t, fsm.bodyBytes)
}

func TestExternalOriginBindsWrappedHeaderAndPreservesInputHash(t *testing.T) {
	root := t.TempDir()
	payload := []byte("EKVPBBL1ordinary-kv")
	input := filepath.Join(root, "input")
	require.NoError(t, os.WriteFile(input, payload, 0o600))
	dir := filepath.Join(root, "raft")
	result, err := PrepareExternalSnapshotRestore(ExternalSnapshotRestoreOptions{
		InputFSMPath: input, DataDir: dir, Index: 42, Term: 7, SnapshotCeilingMs: 123,
		Peers: []Peer{{NodeID: 1, ID: "n1", Address: "127.0.0.1:12001"}},
	})
	require.NoError(t, err)
	marker, err := readExternalRestoreOrigin(dir)
	require.NoError(t, err)
	require.NotNil(t, marker)
	bytes, err := os.ReadFile(result.FSMPath)
	require.NoError(t, err)
	whole := sha256.Sum256(bytes[:len(bytes)-fsmFooterSize])
	original := sha256.Sum256(payload)
	require.Equal(t, hex.EncodeToString(whole[:]), marker.PayloadSHA256)
	require.Equal(t, hex.EncodeToString(original[:]), result.PayloadSHA256)
	require.NotEqual(t, result.PayloadSHA256, marker.PayloadSHA256)
}

func TestExternalOriginRejectsMarkerMismatchBeforeFSM(t *testing.T) {
	for _, completed := range []bool{false, true} {
		for _, field := range []string{"version", "index", "term", "crc", "sha", "truncated", "oversized"} {
			t.Run(field+map[bool]string{false: "/pending", true: "/completed"}[completed], func(t *testing.T) {
				dir, _ := prepareOriginFixture(t)
				marker, err := readExternalRestoreOrigin(dir)
				require.NoError(t, err)
				marker.Completed = completed
				switch field {
				case "version":
					marker.Version++
				case "index":
					marker.Index++
				case "term":
					marker.Term++
				case "crc":
					marker.CRC32C++
				case "sha":
					marker.PayloadSHA256 = hex.EncodeToString(make([]byte, sha256.Size))
				}
				data, err := json.Marshal(marker)
				require.NoError(t, err)
				if field == "truncated" {
					data = []byte("{")
				}
				if field == "oversized" {
					data = make([]byte, externalRestoreOriginMaxBytes+1)
				}
				require.NoError(t, os.WriteFile(filepath.Join(dir, externalRestoreOriginFile), data, 0o600))
				fsm := &originRecordingFSM{}
				_, err = loadOriginFixture(dir, fsm)
				require.ErrorIs(t, err, ErrExternalSnapshotRestoreOrigin)
				require.Empty(t, fsm.origins)
				require.FileExists(t, filepath.Join(dir, externalRestoreOriginFile))
			})
		}
	}
}

func TestExternalOriginVerifiesCRCAndSHAOnSamePayload(t *testing.T) {
	for _, repairCRC := range []bool{false, true} {
		t.Run(map[bool]string{false: "crc", true: "sha"}[repairCRC], func(t *testing.T) {
			dir, result := prepareOriginFixture(t)
			marker, err := readExternalRestoreOrigin(dir)
			require.NoError(t, err)
			data, err := os.ReadFile(result.FSMPath)
			require.NoError(t, err)
			data[0] ^= 1
			if repairCRC {
				marker.CRC32C = crc32.Checksum(data[:len(data)-fsmFooterSize], crc32cTable)
				binary.BigEndian.PutUint32(data[len(data)-fsmFooterSize:], marker.CRC32C)
			}
			root, err := os.OpenRoot(dir)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, root.Close()) })
			require.NoError(t, root.WriteFile("fsm-snap/000000000000002a.fsm", data, 0o600))
			snapshot := raftpb.Snapshot{Data: encodeSnapshotToken(marker.Index, marker.CRC32C),
				Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(marker.Index), Term: proto.Uint64(marker.Term)}}
			fsm := &originRecordingFSM{}
			_, err = restoreWithExternalOrigin(fsm, snapshot, filepath.Join(dir, fsmSnapDirName), marker)
			if repairCRC {
				require.ErrorIs(t, err, ErrExternalSnapshotRestoreOrigin)
			} else {
				require.ErrorIs(t, err, ErrFSMSnapshotFileCRC)
			}
			require.Empty(t, fsm.origins)
		})
	}
}

func TestExternalOriginValidationFailureSurvivesRetry(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	rejected := errors.New("decoded migration-bearing restore unsupported")
	for range 2 {
		fsm := &originRecordingFSM{skipGateFSM: skipGateFSM{applied: 100, appliedPresent: true}, reject: rejected}
		_, err := loadOriginFixture(dir, fsm)
		require.ErrorIs(t, err, rejected)
		require.Len(t, fsm.origins, 1)
		require.FileExists(t, filepath.Join(dir, externalRestoreOriginFile))
	}
}

func TestExternalOriginMarkerlessUsesOrdinarySkip(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	require.NoError(t, os.Remove(filepath.Join(dir, externalRestoreOriginFile)))
	fsm := &originRecordingFSM{skipGateFSM: skipGateFSM{applied: 100, appliedPresent: true}}
	disk, err := loadOriginFixture(dir, fsm)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closePersist(disk.Persist)) })
	require.Empty(t, fsm.origins)
	require.True(t, fsm.restoredHeader)
	require.Equal(t, uint64(100), disk.EffectiveApplied)
}

// Embedding the old interface strips the optional capability. Do not unwrap it
// behind a wrapper's back or silently call the legacy Restore method.
type originStrippingFSM struct{ StateMachine }

func TestExternalOriginWrapperMustForwardCapability(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	inner := &originRecordingFSM{}
	_, err := loadOriginFixture(dir, &originStrippingFSM{StateMachine: inner})
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreUnsupported)
	require.Empty(t, inner.origins)
}

func TestExternalOriginSuccessfulOpenRetainsCompletedBinding(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fsm := &originRecordingFSM{}
	engine, err := Open(ctx, OpenConfig{NodeID: 1, LocalID: "n1", LocalAddress: "127.0.0.1:12001", DataDir: dir,
		StateMachine: fsm, TickInterval: 10 * time.Millisecond, ElectionTick: 5, HeartbeatTick: 1})
	require.NoError(t, err)
	marker, err := readExternalRestoreOrigin(dir)
	require.NoError(t, err)
	require.NotNil(t, marker)
	require.True(t, marker.Completed)
	require.NoError(t, engine.Close())
	require.Equal(t, []raftengine.SnapshotRestoreOrigin{raftengine.SnapshotRestoreExternalRebase}, fsm.origins)
	// Completion never lets the original bytes bypass origin validation.
	ordinary := &skipGateFSM{applied: 42, appliedPresent: true}
	_, err = loadOriginFixture(dir, ordinary)
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreUnsupported)
}

func TestExternalOriginMissingWALCannotBootstrap(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	require.NoError(t, os.RemoveAll(filepath.Join(dir, walDirName)))
	_, err := openDiskState(OpenConfig{DataDir: dir, StateMachine: &originRecordingFSM{}, Bootstrap: true}, nil)
	require.ErrorIs(t, err, ErrExternalSnapshotRestoreOrigin)
}

func TestExternalOriginCompletionRetriesDirectoryFlush(t *testing.T) {
	dir, _ := prepareOriginFixture(t)
	failure := errors.New("completion directory flush failed after rename")
	flushes, writes := 0, 0
	flush := func(path string) error {
		require.Equal(t, dir, path)
		flushes++
		if flushes < 3 {
			return failure
		}
		return syncDir(path)
	}
	write := func(path string, data []byte) error {
		writes++
		// Publish real, valid completed bytes atomically, then inject the
		// post-rename error returned by the completion writer's contract.
		require.NoError(t, writeAndSyncFile(path, data))
		return flush(filepath.Dir(path))
	}
	for attempt := range 3 {
		err := completeExternalRestoreOriginWithPersistence(dir, true, write, flush)
		if attempt < 2 {
			require.ErrorIs(t, err, failure, "Open must remain gated while completion durability is uncertain")
		} else {
			require.NoError(t, err)
		}
		marker, readErr := readExternalRestoreOrigin(dir)
		require.NoError(t, readErr)
		require.True(t, marker.Completed, "a visible completed flag is not proof that its directory flush succeeded")
	}
	require.Equal(t, 1, writes, "completed retries must not rewrite the immutable binding")
	require.Equal(t, 3, flushes, "every retry must re-establish the completion durability barrier")
}

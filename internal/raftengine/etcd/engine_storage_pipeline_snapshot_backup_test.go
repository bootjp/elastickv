package etcd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type destructiveNormalizationStorage struct {
	etcdstorage.Storage
	path string
	err  error
}

func (s *destructiveNormalizationStorage) SaveSnap(_ *raftpb.Snapshot) error {
	if err := os.WriteFile(s.path, []byte("partial snapshot"), 0o600); err != nil {
		return err
	}
	if err := os.Remove(s.path); err != nil {
		return err
	}
	return s.err
}

func TestPipelineRecoveredSnapshotRestoresPriorFileAfterDestructiveSaveFailure(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	hs := &raftpb.HardState{Term: uint64Ptr(2), Commit: uint64Ptr(3)}
	entries := []*raftpb.Entry{{Index: uint64Ptr(2), Term: uint64Ptr(2)}, {Index: uint64Ptr(3), Term: uint64Ptr(2)}}
	require.NoError(t, e.persist.Save(hs, entries))
	source := raftTestSnapshot(3, 2, []uint64{1, 2}, []byte{0, 0, 0, 0})
	require.NoError(t, e.persist.SaveSnap(&source))
	require.NoError(t, e.persist.Sync())
	path := filepath.Join(e.dataDir, snapDirName, "0000000000000002-0000000000000003.snap")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	replacement := proto.CloneOf(&source)
	replacement.Data = encodeSnapshotToken(3, 1)
	failure := errors.New("partial native SaveSnap")
	broken := &destructiveNormalizationStorage{Storage: e.persist, path: path, err: failure}
	require.ErrorIs(t, persistPipelineRecoveredSnapshot(broken,
		&pipelineSnapshotNormalization{path: path, snapshot: replacement}), failure)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(path + pipelineSnapshotBackupSuffix)
	require.NoError(t, err)
	require.NoError(t, closePersist(e.persist))
	reopened, err := openDiskState(OpenConfig{DataDir: e.dataDir, StateMachine: &testStateMachine{}}, nil)
	require.NoError(t, err)
	e.persist = reopened.Persist
	require.Equal(t, source.Data, reopened.LocalSnap.Data)
	require.Equal(t, source.Metadata.GetIndex(), reopened.LocalSnap.Metadata.GetIndex())
	require.Equal(t, source.Metadata.GetTerm(), reopened.LocalSnap.Metadata.GetTerm())
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = os.Stat(path + pipelineSnapshotBackupSuffix)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPipelineRecoveredSnapshotRollsBackCrashBeforeBackupRetirement(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	hs := &raftpb.HardState{Term: uint64Ptr(2), Commit: uint64Ptr(3)}
	entries := []*raftpb.Entry{{Index: uint64Ptr(2), Term: uint64Ptr(2)}, {Index: uint64Ptr(3), Term: uint64Ptr(2)}}
	require.NoError(t, e.persist.Save(hs, entries))
	source := raftTestSnapshot(3, 2, []uint64{1, 2}, []byte{0, 0, 0, 0})
	require.NoError(t, e.persist.SaveSnap(&source))
	require.NoError(t, e.persist.Sync())
	path := filepath.Join(e.dataDir, snapDirName, "0000000000000002-0000000000000003.snap")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.Rename(path, path+pipelineSnapshotBackupSuffix))
	require.NoError(t, syncDir(filepath.Dir(path)))
	replacement := proto.CloneOf(&source)
	replacement.Data = encodeSnapshotToken(3, 1)
	require.NoError(t, e.persist.SaveSnap(replacement))
	require.NoError(t, e.persist.Sync())
	require.NoError(t, closePersist(e.persist))
	reopened, err := openDiskState(OpenConfig{DataDir: e.dataDir, StateMachine: &testStateMachine{}}, nil)
	require.NoError(t, err)
	e.persist = reopened.Persist
	require.Equal(t, source.Data, reopened.LocalSnap.Data)
	require.Equal(t, source.Metadata.GetIndex(), reopened.LocalSnap.Metadata.GetIndex())
	require.Equal(t, source.Metadata.GetTerm(), reopened.LocalSnap.Metadata.GetTerm())
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = os.Stat(path + pipelineSnapshotBackupSuffix)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPipelineSnapshotBackupRejectsCorruptionBeforeReplacement(t *testing.T) {
	dir := t.TempDir()
	source := raftTestSnapshot(3, 2, []uint64{1, 2}, []byte{0, 0, 0, 0})
	require.NoError(t, snap.New(zap.NewNop(), dir).SaveSnap(&source))
	path := filepath.Join(dir, "0000000000000002-0000000000000003.snap")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path+pipelineSnapshotBackupSuffix, []byte("partial backup"), 0o600))
	require.Error(t, recoverPipelineSnapshotBackups(dir))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

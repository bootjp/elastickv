package filesystem

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/stretchr/testify/require"
)

func newJournalReadObservationService(t *testing.T) (*Service, *recordingReadObserver, context.Context) {
	t.Helper()
	ctx := t.Context()
	observer := &recordingReadObserver{leader: true}
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4, 5, 6}, WithReadObserver(observer))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	observer.reset()
	return svc, observer, ctx
}

func journalReadSnapshot(t *testing.T, ctx context.Context, svc *Service) operationPlanningSnapshot {
	t.Helper()
	ts, err := svc.readTS(ctx)
	require.NoError(t, err)
	return operationPlanningSnapshot{ReadTS: ts}
}

func TestJournalMutationReadObservationNamespace(t *testing.T) {
	svc, observer, ctx := newJournalReadObservationService(t)
	snapshot := journalReadSnapshot(t, ctx, svc)
	_, _, semanticErr, err := svc.planUnlinkOperation(ctx, snapshot, RootInode, []byte("missing"), false, nil)
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotFound)
	require.Equal(t, [][]byte{
		fskeys.InodeKey(RootInode), fskeys.DirEntryKey(RootInode, []byte("missing")),
	}, observer.keys)
	require.Empty(t, observer.marks)

	dir, err := svc.Mkdir(ctx, RootInode, []byte("dir"), CreateOptions{Mode: testDirMode})
	require.NoError(t, err)
	_, err = svc.Create(ctx, RootInode, []byte("source"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	observer.reset()
	snapshot = journalReadSnapshot(t, ctx, svc)
	_, _, semanticErr, err = svc.planRenameOperation(ctx, snapshot, RootInode, []byte("source"), []byte("source"), nil)
	require.NoError(t, err)
	require.NoError(t, semanticErr)
	require.Equal(t, [][]byte{
		fskeys.InodeKey(RootInode), fskeys.DirEntryKey(RootInode, []byte("source")),
	}, observer.keys)

	observer.reset()
	_, _, semanticErr, err = svc.planRenameOperation(ctx, snapshot, RootInode, []byte("dir"), []byte("source"), nil)
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotDir)
	require.Equal(t, [][]byte{
		fskeys.InodeKey(RootInode), fskeys.DirEntryKey(RootInode, []byte("dir")),
		fskeys.DirEntryKey(RootInode, []byte("source")),
	}, observer.keys)

	_, err = svc.Create(ctx, dir.Inode, []byte("child"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	observer.reset()
	snapshot = journalReadSnapshot(t, ctx, svc)
	_, _, semanticErr, err = svc.planUnlinkOperation(ctx, snapshot, RootInode, []byte("dir"), true, nil)
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotEmpty)
	require.Equal(t, [][]byte{
		fskeys.InodeKey(RootInode), fskeys.DirEntryKey(RootInode, []byte("dir")), fskeys.InodeKey(dir.Inode),
	}, observer.keys)
	require.Equal(t, []uint64{0}, observer.marks)
}

func TestJournalMutationReadObservationWriteAndSetAttr(t *testing.T) {
	svc, observer, ctx := newJournalReadObservationService(t)
	snapshot := journalReadSnapshot(t, ctx, svc)
	_, semanticErr, err := svc.planJournalWriteAt(ctx, snapshot, nil, 99, 0, 1, []byte("x"), nil)
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotFound)
	require.Equal(t, [][]byte{fskeys.InodeKey(99)}, observer.keys)

	observer.reset()
	_, err = svc.writeOperationEffectLimit(ctx, 99, 0, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{fskeys.InodeKey(99)}, observer.keys)

	observer.reset()
	_, _, semanticErr, err = svc.planSetAttrOperation(ctx, snapshot, nil, 99, SetAttrMask{Mode: true}, SetAttr{Mode: 0o600}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotFound)
	require.Equal(t, [][]byte{fskeys.InodeKey(99)}, observer.keys)
	require.Empty(t, observer.marks)
}

func TestJournalMutationReadObservationTruncateScan(t *testing.T) {
	ctx := t.Context()
	observer := &recordingReadObserver{leader: true}
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4}, WithChunkSize(2), WithReadObserver(observer))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("abcdef"))
	require.NoError(t, err)
	snapshot := journalReadSnapshot(t, ctx, svc)
	meta, err := svc.inodeAt(ctx, file.Inode, snapshot.ReadTS)
	require.NoError(t, err)
	cleaned := *meta
	plan := operationAttemptPlan{ReadTS: snapshot.ReadTS}
	observer.reset()
	_, err = svc.planSetAttrSize(ctx, &plan, meta, &cleaned, 1)
	require.NoError(t, err)
	require.Contains(t, observer.keys, fskeys.HomeKey(file.Inode))
	require.Contains(t, observer.keys, fskeys.ChunkKey(meta.HomeSlot, file.Inode, 0))
	require.Equal(t, []uint64{0}, observer.marks)
}

func TestJournalMutationReadObservationHandles(t *testing.T) {
	svc, observer, ctx := newJournalReadObservationService(t)
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	clientID := []byte("client")
	fh, err := svc.Open(ctx, file.Inode, clientID)
	require.NoError(t, err)
	observer.reset()
	snapshot := journalReadSnapshot(t, ctx, svc)
	_, semanticErr, err := svc.planLeaseOperation(ctx, snapshot, file.Inode, fh+1, clientID, nil)
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotFound)
	require.Equal(t, [][]byte{
		fskeys.InodeKey(file.Inode), fskeys.RefKey(file.Inode, clientID, fh+1),
	}, observer.keys)

	observer.reset()
	_, _, _, err = svc.planOpenLease(ctx, file.Inode, clientID, fh, snapshot.ReadTS)
	require.Error(t, err)
	require.Equal(t, [][]byte{fskeys.RefKey(file.Inode, clientID, fh)}, observer.keys)
	observer.reset()
	observer.leader = false
	_, semanticErr, err = svc.planLeaseOperation(ctx, snapshot, file.Inode, fh+1, clientID, nil)
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotFound)
	require.Empty(t, observer.keys)
}

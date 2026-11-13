package filesystem

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type recordingReadObserver struct {
	kv.Coordinator
	keys   [][]byte
	marks  []uint64
	leader bool
	groups map[uint64]bool
}

func (o *recordingReadObserver) IsLeaderForKey([]byte) bool { return o.leader }

func (o *recordingReadObserver) IsLeaderForGroup(groupID uint64) bool { return o.groups[groupID] }

func (o *recordingReadObserver) ObserveReadForKey(_ context.Context, key []byte) {
	o.keys = append(o.keys, bytes.Clone(key))
}

func (o *recordingReadObserver) MarkUnattributableClientRead(routeID uint64) {
	o.marks = append(o.marks, routeID)
}

func (o *recordingReadObserver) reset() {
	o.keys = nil
	o.marks = nil
}

func TestServiceReadObservationPointReads(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	observer := &recordingReadObserver{leader: true}
	svc := newTestServiceWithOptions(t, []uint64{2}, WithReadObserver(observer))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, []byte("abc"))
	require.NoError(t, err)

	_, err = svc.Resolve(ctx, RootInode, []byte("file"))
	require.NoError(t, err)
	_, err = svc.Resolve(ctx, RootInode, []byte("missing"))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.GetAttr(ctx, file.Inode)
	require.NoError(t, err)
	_, err = svc.GetAttr(ctx, 99)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.Read(ctx, file.Inode, file.FH, 0, 3)
	require.NoError(t, err)
	_, err = svc.Read(ctx, file.Inode, file.FH, 3, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{
		fskeys.InodeKey(RootInode), fskeys.DirEntryKey(RootInode, []byte("file")),
		fskeys.InodeKey(RootInode), fskeys.DirEntryKey(RootInode, []byte("missing")),
		fskeys.InodeKey(file.Inode), fskeys.InodeKey(99),
		fskeys.InodeKey(file.Inode), fskeys.ChunkKey(file.Inode, file.Inode, 0),
		fskeys.InodeKey(file.Inode),
	}, observer.keys)
	require.Empty(t, observer.marks)

	observer.reset()
	observer.leader = false
	_, err = svc.GetAttr(ctx, file.Inode)
	require.NoError(t, err)
	_, err = svc.Read(ctx, file.Inode, file.FH, 0, 3)
	require.NoError(t, err)
	require.Empty(t, observer.keys)
	observer.leader = true
	_, err = svc.Resolve(ctx, RootInode, []byte("bad/name"))
	require.ErrorIs(t, err, ErrInvalid)
	_, err = svc.Read(ctx, file.Inode, file.FH, 0, maxReadSize+1)
	require.ErrorIs(t, err, ErrInvalid)
	require.Empty(t, observer.keys)
}

func TestServiceReadObservationScansAndBound(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	observer := &recordingReadObserver{leader: true}
	svc := newTestServiceWithOptions(t, []uint64{2}, WithChunkSize(1), WithReadObserver(observer))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	_, err = svc.Write(ctx, file.Inode, file.FH, 0, bytes.Repeat([]byte("x"), maxObservedReadKeys))
	require.NoError(t, err)

	_, err = svc.Readdir(ctx, RootInode, "invalid", 4)
	require.ErrorIs(t, err, ErrInvalid)
	require.Empty(t, observer.marks)
	_, err = svc.Readdir(ctx, RootInode, "", 2)
	require.NoError(t, err)
	require.Empty(t, observer.marks)
	observer.reset()
	_, err = svc.Readdir(ctx, RootInode, "", 4)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, observer.marks)

	observer.reset()
	_, err = svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, observer.marks)
	require.Empty(t, observer.keys)

	observer.reset()
	_, err = svc.Read(ctx, file.Inode, file.FH, 0, maxObservedReadKeys-1)
	require.NoError(t, err)
	require.Len(t, observer.keys, maxObservedReadKeys)
	require.Empty(t, observer.marks)
	observer.reset()
	_, err = svc.Read(ctx, file.Inode, file.FH, 0, maxObservedReadKeys)
	require.NoError(t, err)
	require.Empty(t, observer.keys)
	require.Equal(t, []uint64{0}, observer.marks)

	observer.reset()
	observer.leader = false
	_, err = svc.Readdir(ctx, RootInode, "", 4)
	require.NoError(t, err)
	_, err = svc.StatFS(ctx, RootInode)
	require.NoError(t, err)
	_, err = svc.Read(ctx, file.Inode, file.FH, 0, maxObservedReadKeys)
	require.NoError(t, err)
	require.Empty(t, observer.keys)
	require.Empty(t, observer.marks)
}

func TestServiceReadObservationOpenPaths(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	observer := &recordingReadObserver{leader: true}
	svc := newTestServiceWithOptions(t, []uint64{2, 3, 4, 5, 6}, WithReadObserver(observer))
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)

	_, err = svc.Open(ctx, file.Inode, []byte("client"))
	require.NoError(t, err)
	_, err = svc.Open(ctx, 99, []byte("client"))
	require.ErrorIs(t, err, ErrNotFound)
	snapshot := operationPlanningSnapshot{ReadTS: svc.store.LastCommitTS()}
	_, semanticErr, err := svc.planOpenOperation(ctx, snapshot, file.Inode, []byte("client"), []byte("intent"))
	require.NoError(t, err)
	require.NoError(t, semanticErr)
	_, semanticErr, err = svc.planOpenOperation(ctx, snapshot, 99, []byte("client"), []byte("intent"))
	require.NoError(t, err)
	require.ErrorIs(t, semanticErr, ErrNotFound)
	require.Equal(t, [][]byte{
		fskeys.InodeKey(file.Inode), fskeys.InodeKey(99),
		fskeys.InodeKey(file.Inode), fskeys.InodeKey(99),
	}, observer.keys)
	require.Empty(t, observer.marks)
}

type readRangeTargetStore struct {
	store.MVCCStore
	targets []kv.ReadFenceTarget
}

func (s readRangeTargetStore) ReadFenceTargetsForRange([]byte, []byte) []kv.ReadFenceTarget {
	return s.targets
}

func TestServiceReadObservationMultiRouteScan(t *testing.T) {
	t.Parallel()
	observer := &recordingReadObserver{groups: map[uint64]bool{2: true}}
	svc := &Service{
		store: readRangeTargetStore{targets: []kv.ReadFenceTarget{
			{GroupID: 1, Key: []byte("a")},
			{GroupID: 2, Key: []byte("m")},
		}},
		readObserver: observer,
	}
	svc.markUnattributableReadForRange([]byte("a"), []byte("z"))
	require.Equal(t, []uint64{0}, observer.marks)
	observer.reset()
	observer.groups[2] = false
	svc.markUnattributableReadForRange([]byte("a"), []byte("z"))
	require.Empty(t, observer.marks)
}

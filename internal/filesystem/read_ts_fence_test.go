package filesystem

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// allGroupsTimestampFenceCoordinator mimics ShardedCoordinator on a follower:
// the all-groups fence forwards to the leaders and reports their greatest
// applied commit timestamp, which can run ahead of this node's local store.
type allGroupsTimestampFenceCoordinator struct {
	inner     *testCoordinator
	fenceTS   uint64
	fenceErr  error
	calls     int
	plainCall int
}

func (c *allGroupsTimestampFenceCoordinator) LeaseReadAllGroupsTimestamp(context.Context) (uint64, error) {
	c.calls++
	return c.fenceTS, c.fenceErr
}

func (c *allGroupsTimestampFenceCoordinator) LeaseReadAllGroups(context.Context) error {
	c.plainCall++
	return c.fenceErr
}

func (c *allGroupsTimestampFenceCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *allGroupsTimestampFenceCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return c.inner.Dispatch(ctx, req)
}

// laggingStore reports an applied watermark behind the versions it actually
// holds, the way a follower's local store lags the leader it proxies reads to.
type laggingStore struct {
	store.MVCCStore
	lag uint64
}

func (s *laggingStore) LastCommitTS() uint64 {
	ts := s.MVCCStore.LastCommitTS()
	if ts < s.lag {
		return 0
	}
	return ts - s.lag
}

func TestServiceReadTSRaisesSnapshotToAllGroupsFenceTimestamp(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("key"), []byte("value"), 7, 0))
	coord := &allGroupsTimestampFenceCoordinator{inner: &testCoordinator{st: st}, fenceTS: 9}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize))
	require.NoError(t, err)

	ts, err := svc.readTS(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 9, ts, "fence watermark ahead of the local store wins")
	require.Equal(t, 1, coord.calls)
	require.Zero(t, coord.plainCall, "timestamp fence must be preferred over the plain all-groups fence")

	coord.fenceTS = 5
	ts, err = svc.readTS(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 7, ts, "local store ahead of the fence keeps its own watermark")
}

func TestServiceReadTSPropagatesAllGroupsFenceTimestampError(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := &allGroupsTimestampFenceCoordinator{inner: &testCoordinator{st: st}, fenceErr: kv.ErrLeaderNotFound}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize))
	require.NoError(t, err)

	_, err = svc.readTS(ctx)
	require.ErrorIs(t, err, kv.ErrLeaderNotFound)
	require.ErrorContains(t, err, "filesystem all-groups lease read")
}

func TestServiceFollowerReadsObserveOwnForwardedWrite(t *testing.T) {
	ctx := context.Background()
	backing := store.NewMVCCStore()
	local := &laggingStore{MVCCStore: backing}
	inner := &testCoordinator{st: backing}
	coord := &allGroupsTimestampFenceCoordinator{inner: inner}
	svc, err := NewService(local, coord, WithChunkSize(testChunkSize), WithIDAllocator(sequenceIDAllocator(2)))
	require.NoError(t, err)
	fence := func() { coord.fenceTS = backing.LastCommitTS() }

	fence()
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	fence()
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	fence()
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("abcd"))
	require.NoError(t, err)

	// The write committed on the leader (backing store) but this node's
	// applied watermark still trails it by one commit.
	local.lag = 1
	fence()

	stat, err := svc.GetAttr(ctx, file.Inode)
	require.NoError(t, err)
	require.EqualValues(t, 4, stat.Size, "follower must read at the fence watermark, not its lagging local one")

	data, err := svc.Read(ctx, file.Inode, 0, 0, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("abcd"), data)

	inode, err := svc.Resolve(ctx, RootInode, []byte("file"))
	require.NoError(t, err)
	require.Equal(t, file.Inode, inode)
}

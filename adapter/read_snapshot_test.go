package adapter

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type fencedSnapshotTestStore struct {
	store.MVCCStore
	floor    uint64
	fenceErr error
	samples  int
}

func (s *fencedSnapshotTestStore) LastCommitTS() uint64 { s.samples++; return 2 }
func (s *fencedSnapshotTestStore) GlobalLastCommitTS(context.Context) uint64 {
	panic("best-effort fallback is forbidden")
}
func (s *fencedSnapshotTestStore) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return s.floor, s.fenceErr
}
func (s *fencedSnapshotTestStore) CommittedTimestampFloorForKeys(context.Context, [][]byte) (uint64, error) {
	return s.floor, s.fenceErr
}

func TestAdapterLatestSnapshotsUseStrictCommittedFloors(t *testing.T) {
	constructors := map[string]func(context.Context, store.MVCCStore, kv.Coordinator) (uint64, error){
		"redis": func(ctx context.Context, st store.MVCCStore, c kv.Coordinator) (uint64, error) {
			return (&RedisServer{store: st, coordinator: c}).snapshotReadTS(ctx)
		},
		"s3": func(ctx context.Context, st store.MVCCStore, c kv.Coordinator) (uint64, error) {
			return (&S3Server{store: st, coordinator: c}).snapshotReadTS(ctx)
		},
		"sqs": func(ctx context.Context, st store.MVCCStore, c kv.Coordinator) (uint64, error) {
			return (&SQSServer{store: st, coordinator: c}).snapshotReadTS(ctx)
		},
		"dynamodb": func(ctx context.Context, st store.MVCCStore, c kv.Coordinator) (uint64, error) {
			ts, err := (&DynamoDBServer{store: st, coordinator: c}).beginTxnReadTimestamp(ctx, "test", []byte("key"))
			return ts.Timestamp(), err
		},
		"grpc": func(ctx context.Context, st store.MVCCStore, c kv.Coordinator) (uint64, error) {
			return (&GRPCServer{store: st, coordinator: c}).beginLatestReadSnapshot(ctx, 0, []byte("key"))
		},
	}
	for name, begin := range constructors {
		t.Run(name, func(t *testing.T) {
			base := store.NewMVCCStore()
			t.Cleanup(func() { _ = base.Close() })
			st := &fencedSnapshotTestStore{MVCCStore: base, floor: 42}
			c := newLocalAdapterCoordinator(base)
			ts, err := begin(t.Context(), st, c)
			require.NoError(t, err)
			require.Equal(t, uint64(42), ts)
			require.Zero(t, st.samples)
			st.fenceErr = errors.New("quorum unavailable")
			_, err = begin(t.Context(), st, c)
			require.ErrorIs(t, err, st.fenceErr)
			require.Zero(t, st.samples, "failed fences must never use local/best-effort timestamps")
		})
	}
}

func TestSnapshotFactoryFencesBeforeSamplingBareStore(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { _ = base.Close() })
	require.NoError(t, base.PutAt(t.Context(), []byte("key"), []byte("0"), 2, 0))
	c := &snapshotApplyCoordinator{Coordinator: newLocalAdapterCoordinator(base), apply: func() error { return base.PutAt(t.Context(), []byte("key"), []byte("1"), 42, 0) }}
	stamp, err := beginFencedReadSnapshot(t.Context(), c, base, "test")
	require.NoError(t, err)
	require.Equal(t, uint64(42), stamp.Timestamp())
	require.Equal(t, 1, c.fences)
	_, err = beginFencedReadSnapshot(t.Context(), nil, base, "test")
	require.Error(t, err)
}

type snapshotApplyCoordinator struct {
	kv.Coordinator
	apply  func() error
	fences int
}

func (c *snapshotApplyCoordinator) LinearizableRead(context.Context) (uint64, error) {
	c.fences++
	return 0, c.apply()
}

func TestGRPCLatestReadsFenceButExplicitHistoricalReadsKeepTheirTimestamp(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { _ = base.Close() })
	require.NoError(t, base.PutAt(t.Context(), []byte("key"), []byte("0"), 2, 0))
	require.NoError(t, base.PutAt(t.Context(), []byte("key"), []byte("1"), 42, 0))
	st := &fencedSnapshotTestStore{MVCCStore: base, floor: 42}
	server := NewGRPCServer(st, newLocalAdapterCoordinator(base))
	latest, err := server.Get(t.Context(), &pb.GetRequest{Key: []byte("key")})
	require.NoError(t, err)
	require.Equal(t, []byte("1"), latest.Value)
	historical, err := server.RawGet(t.Context(), &pb.RawGetRequest{Key: []byte("key"), Ts: 2})
	require.NoError(t, err)
	require.Equal(t, []byte("0"), historical.Value)
	st.fenceErr = errors.New("quorum unavailable")
	_, err = server.Get(t.Context(), &pb.GetRequest{Key: []byte("key")})
	require.ErrorIs(t, err, st.fenceErr)
	historical, err = server.RawGet(t.Context(), &pb.RawGetRequest{Key: []byte("key"), Ts: 2})
	require.NoError(t, err)
	require.Equal(t, []byte("0"), historical.Value)
}

func TestCatalogSnapshotWaitsForAppliedBarrierBeforeSampling(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { _ = base.Close() })
	require.NoError(t, base.PutAt(t.Context(), distribution.CatalogVersionKey(), []byte("old"), 2, 0))
	coord := &catalogSnapshotFenceCoordinator{Coordinator: newLocalAdapterCoordinator(base)}
	coord.apply = func() error { return base.PutAt(t.Context(), distribution.CatalogVersionKey(), []byte("new"), 42, 0) }
	sampled := 0
	observed := &observingLastCommitStore{MVCCStore: base, onLastCommitTS: func() { sampled++; require.Equal(t, distribution.CatalogVersionKey(), coord.key) }}
	server := &DistributionServer{catalog: distribution.NewCatalogStore(observed), coordinator: coord}
	stamp, err := server.beginCatalogReadSnapshot(t.Context(), "test")
	require.NoError(t, err)
	require.Equal(t, uint64(42), stamp.Timestamp())
	require.Positive(t, sampled)
	coord.apply = func() error { return kv.ErrLeaderNotFound }
	sampled = 0
	_, err = server.beginCatalogReadSnapshot(t.Context(), "test")
	require.ErrorIs(t, err, kv.ErrLeaderNotFound)
	require.Zero(t, sampled)
}

type catalogSnapshotFenceCoordinator struct {
	kv.Coordinator
	key   []byte
	apply func() error
}

func (c *catalogSnapshotFenceCoordinator) LinearizableReadForKey(_ context.Context, key []byte) (uint64, error) {
	c.key = append([]byte(nil), key...)
	return 0, c.apply()
}
func (c *catalogSnapshotFenceCoordinator) LinearizableRead(context.Context) (uint64, error) {
	panic("catalog snapshot must fence its own group")
}

func TestCanceledSnapshotCannotIssueTokenOrSampleStore(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { _ = base.Close() })
	st := &fencedSnapshotTestStore{MVCCStore: base, floor: 42}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	stamp, err := beginFencedReadSnapshot(ctx, newLocalAdapterCoordinator(base), st, "test")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, stamp.Timestamp())
	require.Zero(t, st.samples)
	live, stop := context.WithCancel(t.Context())
	defer stop()
	coord := &snapshotApplyCoordinator{Coordinator: newLocalAdapterCoordinator(base), apply: func() error { stop(); return nil }}
	stamp, err = beginFencedReadSnapshot(live, coord, base, "test")
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, stamp.Timestamp())
}

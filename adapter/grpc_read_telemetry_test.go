package adapter

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type grpcReadTelemetryCoordinator struct {
	stubAdapterCoordinator
	keys         [][]byte
	marks        []uint64
	notLeader    map[string]bool
	routeGroup   uint64
	groupLeaders map[uint64]bool
}

func (c *grpcReadTelemetryCoordinator) IsLeaderForKey(key []byte) bool {
	return !c.notLeader[string(key)] && c.stubAdapterCoordinator.IsLeaderForKey(key)
}

func (c *grpcReadTelemetryCoordinator) ObserveReadForKey(_ context.Context, key []byte) {
	c.keys = append(c.keys, bytes.Clone(key))
}

func (c *grpcReadTelemetryCoordinator) MarkUnattributableClientRead(routeID uint64) {
	c.marks = append(c.marks, routeID)
}

func (c *grpcReadTelemetryCoordinator) IsLeaderForGroup(groupID uint64) bool {
	if c.groupLeaders != nil {
		return c.groupLeaders[groupID]
	}
	return true
}

func (c *grpcReadTelemetryCoordinator) EngineGroupIDForKey([]byte) uint64 {
	return c.routeGroup
}

func TestGRPCReadTelemetryPointReads(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	defer st.Close()
	require.NoError(t, st.PutAt(ctx, []byte("present"), []byte("v"), 1, 0))
	coord := &grpcReadTelemetryCoordinator{}
	srv := NewGRPCServer(st, coord)

	for _, key := range []string{"present", "missing"} {
		_, err := srv.RawGet(ctx, &pb.RawGetRequest{Key: []byte(key), Ts: 1})
		require.NoError(t, err)
		_, err = srv.Get(ctx, &pb.GetRequest{Key: []byte(key)})
		require.NoError(t, err)
	}
	require.Equal(t, [][]byte{[]byte("present"), []byte("present"), []byte("missing"), []byte("missing")}, coord.keys)
	require.Empty(t, coord.marks)

	coord.keys = nil
	coord.leaderSet, coord.leader = true, false
	_, err := srv.RawGet(ctx, &pb.RawGetRequest{Key: []byte("present"), Ts: 1})
	require.NoError(t, err)
	_, err = srv.Get(ctx, &pb.GetRequest{Key: []byte("present")})
	require.NoError(t, err)
	require.Empty(t, coord.keys)
}

func TestGRPCReadTelemetryExplicitGroupRequiresMatchingRoute(t *testing.T) {
	coord := &grpcReadTelemetryCoordinator{routeGroup: 2}
	srv := &GRPCServer{coordinator: coord}

	for _, groupID := range []uint64{1, 2} {
		srv.observeGRPCPointRead(context.Background(), []byte("missing"), groupID)
	}
	require.Equal(t, []uint64{0}, coord.marks)
	require.Equal(t, [][]byte{[]byte("missing")}, coord.keys)
}

func TestGRPCReadTelemetryScans(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	defer st.Close()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("v"), 1, 0))
	coord := &grpcReadTelemetryCoordinator{}
	srv := NewGRPCServer(st, coord)

	for _, keysOnly := range []bool{false, true} {
		_, err := srv.RawScanAt(ctx, &pb.RawScanAtRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2, Ts: 1, KeysOnly: keysOnly})
		require.NoError(t, err)
	}
	_, err := srv.Scan(ctx, &pb.ScanRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 0, 0}, coord.marks)
	require.Empty(t, coord.keys)

	_, err = srv.RawScanAt(ctx, &pb.RawScanAtRequest{Limit: -1})
	require.Error(t, err)
	_, err = srv.Scan(ctx, &pb.ScanRequest{Limit: ^uint64(0)})
	require.Error(t, err)
	require.Len(t, coord.marks, 3)
	coord.notLeader = map[string]bool{"a": true}
	_, err = srv.RawScanAt(ctx, &pb.RawScanAtRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2, Ts: 1})
	require.NoError(t, err)
	require.Len(t, coord.marks, 3)

	coord.leaderSet, coord.leader = true, false
	_, err = srv.RawScanAt(ctx, &pb.RawScanAtRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2, Ts: 1})
	require.NoError(t, err)
	_, err = srv.Scan(ctx, &pb.ScanRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2})
	require.NoError(t, err)
	require.Len(t, coord.marks, 3)
}

type grpcReadRangeStore struct {
	store.MVCCStore
	targets []kv.ReadFenceTarget
}

type grpcReadErrorStore struct{ store.MVCCStore }

func (grpcReadErrorStore) GetAt(context.Context, []byte, uint64) ([]byte, error) {
	return nil, store.ErrReadTSCompacted
}

func (grpcReadErrorStore) ScanAt(context.Context, []byte, []byte, int, uint64) ([]*store.KVPair, error) {
	return nil, errors.New("scan failed")
}

func (grpcReadErrorStore) ScanKeysAt(context.Context, []byte, []byte, int, uint64) ([][]byte, error) {
	return nil, errors.New("scan keys failed")
}

func TestGRPCReadTelemetryStoreErrors(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &grpcReadTelemetryCoordinator{}
	srv := NewGRPCServer(grpcReadErrorStore{MVCCStore: st}, coord)

	_, err := srv.RawGet(ctx, &pb.RawGetRequest{Key: []byte("key"), Ts: 1})
	require.Error(t, err)
	_, err = srv.Get(ctx, &pb.GetRequest{Key: []byte("key")})
	require.Error(t, err)
	for _, keysOnly := range []bool{false, true} {
		_, err = srv.RawScanAt(ctx, &pb.RawScanAtRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2, Ts: 1, KeysOnly: keysOnly})
		require.Error(t, err)
	}
	_, err = srv.Scan(ctx, &pb.ScanRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2})
	require.Error(t, err)
	require.Equal(t, [][]byte{[]byte("key"), []byte("key")}, coord.keys)
	require.Equal(t, []uint64{0, 0, 0}, coord.marks)
}

func (s grpcReadRangeStore) ReadFenceTargetsForRange([]byte, []byte) []kv.ReadFenceTarget {
	return s.targets
}

func TestGRPCReadTelemetryEmptyCrossGroupScanMarksLocalLeader(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &grpcReadTelemetryCoordinator{
		groupLeaders: map[uint64]bool{1: false, 2: true},
		notLeader:    map[string]bool{"a": true},
	}
	srv := NewGRPCServer(grpcReadRangeStore{MVCCStore: st, targets: []kv.ReadFenceTarget{
		{GroupID: 1, Key: []byte("a")},
		{GroupID: 2, Key: []byte("m")},
	}}, coord)

	_, err := srv.RawScanAt(ctx, &pb.RawScanAtRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2, Ts: 1})
	require.NoError(t, err)
	_, err = srv.Scan(ctx, &pb.ScanRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 2})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 0}, coord.marks)
}

func TestGRPCReadTelemetryVersionProbesAndBlockedReads(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &grpcReadTelemetryCoordinator{}
	srv := NewGRPCServer(st, coord)
	_, err := srv.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k")})
	require.NoError(t, err)
	_, err = srv.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{})
	require.NoError(t, err)
	_, err = srv.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{
		KeyBatch:           pb.EncodeRawLatestCommitTSKeyBatch([][]byte{[]byte("a"), []byte("b")}),
		VersionVisibleAtTs: 1,
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("k"), []byte("a"), []byte("b")}, coord.keys)
	require.Equal(t, []uint64{0}, coord.marks)

	blocked := NewGRPCServer(st, coord, WithGRPCReadGate(func() bool { return true }))
	_, err = blocked.RawGet(ctx, &pb.RawGetRequest{Key: []byte("k")})
	require.Error(t, err)
	_, err = blocked.RawScanAt(ctx, &pb.RawScanAtRequest{Limit: 1})
	require.Error(t, err)
	require.Equal(t, [][]byte{[]byte("k"), []byte("a"), []byte("b")}, coord.keys)
	require.Equal(t, []uint64{0}, coord.marks)
}

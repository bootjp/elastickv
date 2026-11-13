package adapter

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type grpcReadTelemetryCoordinator struct {
	stubAdapterCoordinator
	keys      [][]byte
	marks     []uint64
	notLeader map[string]bool
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

func TestGRPCReadTelemetryExcludesMetadataAndBlockedReads(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	defer st.Close()
	coord := &grpcReadTelemetryCoordinator{}
	srv := NewGRPCServer(st, coord)
	_, err := srv.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k")})
	require.NoError(t, err)
	_, err = srv.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{})
	require.NoError(t, err)
	require.Empty(t, coord.keys)
	require.Empty(t, coord.marks)

	blocked := NewGRPCServer(st, coord, WithGRPCReadGate(func() bool { return true }))
	_, err = blocked.RawGet(ctx, &pb.RawGetRequest{Key: []byte("k")})
	require.Error(t, err)
	_, err = blocked.RawScanAt(ctx, &pb.RawScanAtRequest{Limit: 1})
	require.Error(t, err)
	require.Empty(t, coord.keys)
	require.Empty(t, coord.marks)
}

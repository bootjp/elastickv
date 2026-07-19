package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type blockingFloorRawKVServer struct {
	pb.UnimplementedRawKVServer

	ts      uint64
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *blockingFloorRawKVServer) RawLatestCommitTS(ctx context.Context, req *pb.RawLatestCommitTSRequest) (*pb.RawLatestCommitTSResponse, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return &pb.RawLatestCommitTSResponse{
			Ts:           s.ts,
			Exists:       true,
			GroupId:      req.GetGroupId(),
			LeaderFenced: true,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestShardStoreGlobalCommittedTimestampFloorUsesEveryGroupLeader(t *testing.T) {
	t.Parallel()

	local := store.NewMVCCStore()
	require.NoError(t, local.PutAt(context.Background(), []byte("local"), []byte("v"), 42, 0))
	remote := &fakeRawKVServer{
		latestResp: &pb.RawLatestCommitTSResponse{
			Ts:           99,
			Exists:       true,
			GroupId:      2,
			LeaderFenced: true,
		},
		wantLatestGroupID: 2,
	}
	addr, stop := startRawKVServer(t, remote)
	t.Cleanup(stop)

	groups := map[uint64]*ShardGroup{
		0: {Engine: &recordingTSOEngine{state: raftengine.StateFollower}},
		1: {
			Engine: &recordingTSOEngine{state: raftengine.StateLeader},
			Store:  local,
		},
		2: {
			Engine: &recordingTSOEngine{
				state:  raftengine.StateFollower,
				leader: raftengine.LeaderInfo{Address: addr},
			},
			Store: store.NewMVCCStore(),
		},
	}
	shards := NewShardStore(nil, groups)
	t.Cleanup(func() { require.NoError(t, shards.Close()) })

	floor, err := shards.GlobalCommittedTimestampFloor(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(99), floor)
}

func TestShardStoreGlobalCommittedTimestampFloorRejectsUnfencedRemoteResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		resp *pb.RawLatestCommitTSResponse
	}{
		{
			name: "legacy response",
			resp: &pb.RawLatestCommitTSResponse{Ts: 99, Exists: true},
		},
		{
			name: "wrong group echo",
			resp: &pb.RawLatestCommitTSResponse{
				Ts:           99,
				Exists:       true,
				GroupId:      3,
				LeaderFenced: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			remote := &fakeRawKVServer{
				latestResp:        tc.resp,
				wantLatestGroupID: 2,
			}
			addr, stop := startRawKVServer(t, remote)
			t.Cleanup(stop)

			shards := NewShardStore(nil, map[uint64]*ShardGroup{
				2: {
					Engine: &recordingTSOEngine{
						state:  raftengine.StateFollower,
						leader: raftengine.LeaderInfo{Address: addr},
					},
					Store: store.NewMVCCStore(),
				},
			})
			t.Cleanup(func() { require.NoError(t, shards.Close()) })

			_, err := shards.GlobalCommittedTimestampFloor(context.Background())
			require.ErrorIs(t, err, ErrTSOCommitFloorUnavailable)
			require.ErrorIs(t, err, ErrTSOProtocolUnsupported)
		})
	}
}

func TestShardStoreGroupCommittedTimestampFloorRequiresLocalLeadership(t *testing.T) {
	t.Parallel()

	local := store.NewMVCCStore()
	require.NoError(t, local.PutAt(context.Background(), []byte("stale"), []byte("v"), 77, 0))
	engine := &recordingTSOEngine{state: raftengine.StateFollower}
	shards := NewShardStore(nil, map[uint64]*ShardGroup{
		1: {Engine: engine, Store: local},
	})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })

	_, err := shards.GroupCommittedTimestampFloor(context.Background(), 1)
	require.ErrorIs(t, err, ErrTSOCommitFloorUnavailable)

	engine.mu.Lock()
	engine.state = raftengine.StateLeader
	engine.mu.Unlock()
	floor, err := shards.GroupCommittedTimestampFloor(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(77), floor)
}

func TestShardStoreGlobalCommittedTimestampFloorQueriesGroupsConcurrently(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	started1 := make(chan struct{})
	started2 := make(chan struct{})
	addr1, stop1 := startRawKVServer(t, &blockingFloorRawKVServer{
		ts: 11, started: started1, release: release,
	})
	addr2, stop2 := startRawKVServer(t, &blockingFloorRawKVServer{
		ts: 22, started: started2, release: release,
	})
	t.Cleanup(stop1)
	t.Cleanup(stop2)

	shards := NewShardStore(nil, map[uint64]*ShardGroup{
		1: {
			Engine: &recordingTSOEngine{state: raftengine.StateFollower, leader: raftengine.LeaderInfo{Address: addr1}},
			Store:  store.NewMVCCStore(),
		},
		2: {
			Engine: &recordingTSOEngine{state: raftengine.StateFollower, leader: raftengine.LeaderInfo{Address: addr2}},
			Store:  store.NewMVCCStore(),
		},
	})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })

	type result struct {
		floor uint64
		err   error
	}
	resultCh := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	go func() {
		floor, err := shards.GlobalCommittedTimestampFloor(ctx)
		resultCh <- result{floor: floor, err: err}
	}()

	requireChannelClosed(t, started1)
	requireChannelClosed(t, started2)
	close(release)

	got := <-resultCh
	require.NoError(t, got.err)
	require.Equal(t, uint64(22), got.floor)
}

func requireChannelClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for concurrent TSO floor request")
	}
}

func TestShardStoreGlobalCommittedTimestampFloorFailsWithoutGroupLeader(t *testing.T) {
	t.Parallel()

	local := store.NewMVCCStore()
	require.NoError(t, local.PutAt(context.Background(), []byte("stale"), []byte("v"), 7, 0))
	shards := NewShardStore(nil, map[uint64]*ShardGroup{
		1: {
			Engine: &recordingTSOEngine{state: raftengine.StateFollower},
			Store:  local,
		},
	})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })

	_, err := shards.GlobalCommittedTimestampFloor(context.Background())
	require.ErrorIs(t, err, ErrTSOCommitFloorUnavailable)
}

func TestShardStoreGlobalCommittedTimestampFloorFailsWithoutRaftEngine(t *testing.T) {
	t.Parallel()

	local := store.NewMVCCStore()
	require.NoError(t, local.PutAt(context.Background(), []byte("unfenced"), []byte("v"), 11, 0))
	shards := NewShardStore(nil, map[uint64]*ShardGroup{
		1: {Store: local},
	})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })

	_, err := shards.GlobalCommittedTimestampFloor(context.Background())
	require.ErrorIs(t, err, ErrTSOCommitFloorUnavailable)
	require.ErrorContains(t, err, "no raft engine")
}

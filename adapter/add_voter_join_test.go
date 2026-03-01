package adapter

import (
	"bytes"
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	raftadminpb "github.com/Jille/raftadmin/proto"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestAddVoterJoinPath_RegistersMemberAndServesAdapterTraffic(t *testing.T) {
	t.Parallel()

	const (
		waitTimeout  = 12 * time.Second
		waitInterval = 100 * time.Millisecond
	)

	ctx := context.Background()
	nodes := setupAddVoterJoinPathNodes(t, ctx)
	t.Cleanup(func() { shutdown(nodes) })

	waitForNodeListeners(t, ctx, nodes, waitTimeout, waitInterval)
	require.Eventually(t, func() bool {
		return nodes[0].raft.State() == raft.Leader
	}, waitTimeout, waitInterval)

	adminConn, err := grpc.NewClient(nodes[0].grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminConn.Close() })
	admin := raftadminpb.NewRaftAdminClient(adminConn)

	addVotersAndAwait(t, ctx, admin, nodes, []int{1, 2})

	expectedCfg := expectedVoterConfig(nodes)
	waitForConfigReplication(t, expectedCfg, nodes, waitTimeout, waitInterval)
	waitForRaftReadiness(t, nodes, waitTimeout, waitInterval)

	followerConn, err := grpc.NewClient(nodes[1].grpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = followerConn.Close() })
	followerRaw := pb.NewRawKVClient(followerConn)

	leaderConn, err := grpc.NewClient(nodes[0].grpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = leaderConn.Close() })
	leaderRaw := pb.NewRawKVClient(leaderConn)

	putAndWaitForRead(t, ctx, followerRaw, leaderRaw, []byte("addvoter-key"), []byte("ok"), waitTimeout, waitInterval)

	// Simulate a partition-like failure by isolating node2's raft transport.
	require.NoError(t, nodes[2].tm.Close())
	nodes[2].tm = nil

	putAndWaitForRead(t, ctx, followerRaw, leaderRaw, []byte("partition-survive-key"), []byte("ok2"), waitTimeout, waitInterval)

	// Force leader change while one node is isolated, then confirm write/read path.
	require.NoError(t, nodes[0].raft.LeadershipTransferToServer(raft.ServerID("1"), raft.ServerAddress(nodes[1].raftAddress)).Error())
	require.Eventually(t, func() bool {
		return nodes[1].raft.State() == raft.Leader
	}, waitTimeout, waitInterval)

	putAndWaitForRead(t, ctx, leaderRaw, followerRaw, []byte("leader-transfer-key"), []byte("ok3"), waitTimeout, waitInterval)
}

func setupAddVoterJoinPathNodes(t *testing.T, ctx context.Context) []Node {
	t.Helper()

	ports, lis := reserveAddVoterJoinListeners(t, ctx, 3)

	// AddVoter address must point to the node's shared gRPC endpoint where
	// raft transport and adapter services are served.
	require.Equal(t, ports[1].raftAddress, ports[1].grpcAddress)
	require.Equal(t, ports[2].raftAddress, ports[2].grpcAddress)

	leaderRedisMap := map[raft.ServerAddress]string{
		raft.ServerAddress(ports[0].raftAddress): ports[0].redisAddress,
		raft.ServerAddress(ports[1].raftAddress): ports[1].redisAddress,
		raft.ServerAddress(ports[2].raftAddress): ports[2].redisAddress,
	}
	bootstrapCfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID("0"),
				Address:  raft.ServerAddress(ports[0].raftAddress),
			},
		},
	}

	nodes := make([]Node, 0, len(ports))
	for i := range ports {
		nodes = append(nodes, startAddVoterJoinNode(t, i, ports[i], lis[i], bootstrapCfg, leaderRedisMap))
	}
	return nodes
}

func reserveAddVoterJoinListeners(t *testing.T, ctx context.Context, n int) ([]portsAdress, []listeners) {
	t.Helper()

	var lc net.ListenConfig
	ports := assignPorts(n)
	lis := make([]listeners, 0, n)
	for i := range ports {
		for {
			bound, ls, retry, err := bindListeners(ctx, &lc, ports[i])
			require.NoError(t, err)
			if !retry {
				ports[i] = bound
				lis = append(lis, ls)
				break
			}
			ports[i] = assignPorts(1)[0]
		}
	}
	return ports, lis
}

func startAddVoterJoinNode(
	t *testing.T,
	idx int,
	port portsAdress,
	lis listeners,
	bootstrapCfg raft.Configuration,
	leaderRedisMap map[raft.ServerAddress]string,
) Node {
	t.Helper()

	st := store.NewMVCCStore()
	fsm := kv.NewKvFSM(st)

	electionTimeout := leaderElectionTimeout
	if idx != 0 {
		electionTimeout = followerElectionTimeout
	}

	r, tm, err := newRaft(strconv.Itoa(idx), port.raftAddress, fsm, idx == 0, bootstrapCfg, electionTimeout)
	require.NoError(t, err)

	s := grpc.NewServer()
	trx := kv.NewTransaction(r)
	coordinator := kv.NewCoordinator(trx, r)
	routedStore := kv.NewLeaderRoutedStore(st, coordinator)
	gs := NewGRPCServer(routedStore, coordinator, WithCloseStore())
	tm.Register(s)
	pb.RegisterRawKVServer(s, gs)
	pb.RegisterTransactionalKVServer(s, gs)
	pb.RegisterInternalServer(s, NewInternal(trx, r, coordinator.Clock()))
	leaderhealth.Setup(r, s, []string{"RawKV"})
	raftadmin.Register(s, r)

	go func(srv *grpc.Server, l net.Listener) {
		assert.NoError(t, srv.Serve(l))
	}(s, lis.grpc)

	rd := NewRedisServer(lis.redis, st, coordinator, leaderRedisMap)
	go func(server *RedisServer) {
		assert.NoError(t, server.Run())
	}(rd)

	ds := NewDynamoDBServer(lis.dynamo, st, coordinator)
	go func() {
		assert.NoError(t, ds.Run())
	}()

	return newNode(
		port.grpcAddress,
		port.raftAddress,
		port.redisAddress,
		port.dynamoAddress,
		r,
		tm,
		s,
		gs,
		rd,
		ds,
	)
}

func addVotersAndAwait(
	t *testing.T,
	ctx context.Context,
	admin raftadminpb.RaftAdminClient,
	nodes []Node,
	targets []int,
) {
	t.Helper()

	for _, target := range targets {
		future, err := admin.AddVoter(ctx, &raftadminpb.AddVoterRequest{
			Id:            strconv.Itoa(target),
			Address:       nodes[target].grpcAddress,
			PreviousIndex: 0,
		})
		require.NoError(t, err)

		await, err := admin.Await(ctx, future)
		require.NoError(t, err)
		require.Empty(t, await.GetError())
		require.Greater(t, await.GetIndex(), uint64(0))
	}
}

func expectedVoterConfig(nodes []Node) raft.Configuration {
	servers := make([]raft.Server, 0, len(nodes))
	for i, n := range nodes {
		servers = append(servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(strconv.Itoa(i)),
			Address:  raft.ServerAddress(n.raftAddress),
		})
	}
	return raft.Configuration{Servers: servers}
}

func putAndWaitForRead(
	t *testing.T,
	ctx context.Context,
	writer pb.RawKVClient,
	reader pb.RawKVClient,
	key []byte,
	value []byte,
	waitTimeout time.Duration,
	waitInterval time.Duration,
) {
	t.Helper()

	_, err := writer.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: value})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, getErr := reader.RawGet(ctx, &pb.RawGetRequest{Key: key})
		if getErr != nil {
			return false
		}
		return resp.Exists && bytes.Equal(resp.Value, value)
	}, waitTimeout, waitInterval)
}

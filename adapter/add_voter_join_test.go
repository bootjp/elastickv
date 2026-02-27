package adapter

import (
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
	const (
		waitTimeout  = 8 * time.Second
		waitInterval = 100 * time.Millisecond
	)

	ctx := context.Background()
	var lc net.ListenConfig
	ports := assignPorts(2)
	nodes := make([]Node, 0, 2)
	listeners := make([]listeners, 0, 2)

	for i := 0; i < 2; i++ {
		bound, ls, retry, err := bindListeners(ctx, &lc, ports[i])
		require.NoError(t, err)
		require.False(t, retry)
		ports[i] = bound
		listeners = append(listeners, ls)
	}

	// AddVoter address must point to the node's shared gRPC endpoint where
	// raft transport and adapter services are served.
	require.Equal(t, ports[1].raftAddress, ports[1].grpcAddress)

	leaderRedisMap := map[raft.ServerAddress]string{
		raft.ServerAddress(ports[0].raftAddress): ports[0].redisAddress,
		raft.ServerAddress(ports[1].raftAddress): ports[1].redisAddress,
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

	for i := 0; i < 2; i++ {
		st := store.NewMVCCStore()
		fsm := kv.NewKvFSM(st)

		electionTimeout := leaderElectionTimeout
		if i != 0 {
			electionTimeout = followerElectionTimeout
		}

		r, tm, err := newRaft(strconv.Itoa(i), ports[i].raftAddress, fsm, i == 0, bootstrapCfg, electionTimeout)
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

		grpcSock := listeners[i].grpc
		redisSock := listeners[i].redis
		dynamoSock := listeners[i].dynamo

		go func(srv *grpc.Server, lis net.Listener) {
			assert.NoError(t, srv.Serve(lis))
		}(s, grpcSock)

		rd := NewRedisServer(redisSock, st, coordinator, leaderRedisMap)
		go func(server *RedisServer) {
			assert.NoError(t, server.Run())
		}(rd)

		ds := NewDynamoDBServer(dynamoSock, st, coordinator)
		go func() {
			assert.NoError(t, ds.Run())
		}()

		nodes = append(nodes, newNode(
			ports[i].grpcAddress,
			ports[i].raftAddress,
			ports[i].redisAddress,
			ports[i].dynamoAddress,
			r,
			tm,
			s,
			gs,
			rd,
			ds,
		))
	}
	t.Cleanup(func() { shutdown(nodes) })

	waitForNodeListeners(t, ctx, nodes, waitTimeout, waitInterval)
	require.Eventually(t, func() bool {
		return nodes[0].raft.State() == raft.Leader
	}, waitTimeout, waitInterval)

	adminConn, err := grpc.NewClient(nodes[0].grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminConn.Close() })
	admin := raftadminpb.NewRaftAdminClient(adminConn)

	future, err := admin.AddVoter(ctx, &raftadminpb.AddVoterRequest{
		Id:            "1",
		Address:       nodes[1].grpcAddress,
		PreviousIndex: 0,
	})
	require.NoError(t, err)

	await, err := admin.Await(ctx, future)
	require.NoError(t, err)
	require.Empty(t, await.GetError())
	require.Greater(t, await.GetIndex(), uint64(0))

	expectedCfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID("0"),
				Address:  raft.ServerAddress(nodes[0].raftAddress),
			},
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID("1"),
				Address:  raft.ServerAddress(nodes[1].raftAddress),
			},
		},
	}
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

	key := []byte("addvoter-key")
	val := []byte("ok")
	_, err = followerRaw.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: val})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, getErr := leaderRaw.RawGet(ctx, &pb.RawGetRequest{Key: key})
		if getErr != nil {
			return false
		}
		return resp.Exists && string(resp.Value) == string(val)
	}, waitTimeout, waitInterval)
}

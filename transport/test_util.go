package transport

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func shutdown(nodes []Node) {
	for _, n := range nodes {
		n.grpcServer.Stop()
		n.redisServer.Stop()
	}
}

type portsAdress struct {
	grpc         int
	raft         int
	redis        int
	grpcAddress  string
	raftAddress  string
	redisAddress string
}

const (
	// raft and the grpc requested by the client use grpc and are received on the same port
	grpcPort = 50000
	raftPort = 50000

	redisPort = 63790
)

var mu sync.Mutex
var portGrpc atomic.Int32
var portRaft atomic.Int32
var portRedis atomic.Int32

func init() {
	portGrpc.Store(raftPort)
	portRaft.Store(grpcPort)
	portRedis.Store(redisPort)

}

func portAssigner() portsAdress {
	mu.Lock()
	defer mu.Unlock()
	gp := portGrpc.Add(1)
	rp := portRaft.Add(1)
	rd := portRedis.Add(1)
	return portsAdress{
		grpc:         int(gp),
		raft:         int(rp),
		redis:        int(rd),
		grpcAddress:  net.JoinHostPort("localhost", strconv.Itoa(int(gp))),
		raftAddress:  net.JoinHostPort("localhost", strconv.Itoa(int(rp))),
		redisAddress: net.JoinHostPort("localhost", strconv.Itoa(int(rd))),
	}
}

type Node struct {
	grpcAddress  string
	raftAddress  string
	redisAddress string
	grpcServer   *grpc.Server
	redisServer  *RedisServer
}

func newNode(grpcAddress, raftAddress, redisAddress string, grpcs *grpc.Server, rd *RedisServer) Node {
	return Node{
		grpcAddress:  grpcAddress,
		raftAddress:  raftAddress,
		redisAddress: redisAddress,
		grpcServer:   grpcs,
		redisServer:  rd,
	}
}

//nolint:unparam
func createNode(t *testing.T, n int) ([]Node, []string, []string) {
	var grpcAdders []string
	var redisAdders []string
	var nodes []Node

	cfg := raft.Configuration{}
	ports := make([]portsAdress, n)

	// port assign
	for i := 0; i < n; i++ {
		ports[i] = portAssigner()
	}

	// build raft node config
	for i := 0; i < n; i++ {
		var suffrage raft.ServerSuffrage
		if i == 0 {
			suffrage = raft.Voter
		} else {
			suffrage = raft.Nonvoter
		}

		server := raft.Server{
			Suffrage: suffrage,
			ID:       raft.ServerID(strconv.Itoa(i)),
			Address:  raft.ServerAddress(ports[i].raftAddress),
		}
		cfg.Servers = append(cfg.Servers, server)
	}

	ctx := context.Background()

	for i := 0; i < n; i++ {
		st := kv.NewMemoryStore()
		trxSt := kv.NewMemoryStore()
		fsm := kv.NewKvFSM(st, trxSt)

		port := ports[i]

		r, tm, err := kv.NewRaft(ctx, strconv.Itoa(i), port.raftAddress, fsm, i == 0, cfg)
		assert.NoError(t, err)

		s := grpc.NewServer()
		coordinator := kv.NewCoordinator(kv.NewTransaction(r))
		gs := NewGRPCServer(st, coordinator)
		tm.Register(s)
		pb.RegisterRawKVServer(s, gs)
		pb.RegisterTransactionalKVServer(s, gs)
		leaderhealth.Setup(r, s, []string{"Example"})
		raftadmin.Register(s, r)

		grpcSock, err := net.Listen("tcp", port.grpcAddress)
		assert.NoError(t, err)

		grpcAdders = append(grpcAdders, port.grpcAddress)
		redisAdders = append(redisAdders, port.redisAddress)
		go func() {
			assert.NoError(t, s.Serve(grpcSock))
		}()

		l, err := net.Listen("tcp", port.redisAddress)
		assert.NoError(t, err)
		rd := NewRedisServer(l, st, coordinator)
		go func() {
			assert.NoError(t, rd.Run())
		}()

		nodes = append(nodes, newNode(
			port.grpcAddress,
			port.raftAddress,
			port.redisAddress,
			s,
			rd),
		)

	}

	//nolint:gomnd
	time.Sleep(3 * time.Second)

	return nodes, grpcAdders, redisAdders
}

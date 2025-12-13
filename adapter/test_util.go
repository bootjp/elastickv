package adapter

import (
	"context"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func shutdown(nodes []Node) {
	for _, n := range nodes {
		n.grpcServer.Stop()
		n.redisServer.Stop()
		if n.dynamoServer != nil {
			n.dynamoServer.Stop()
		}
		if n.raft != nil {
			n.raft.Shutdown()
		}
		if n.tm != nil {
			if err := n.tm.Close(); err != nil {
				log.Printf("transport close: %v", err)
			}
		}
	}
}

type portsAdress struct {
	grpc          int
	raft          int
	redis         int
	dynamo        int
	grpcAddress   string
	raftAddress   string
	redisAddress  string
	dynamoAddress string
}

const (
	// raft and the grpc requested by the client use grpc and are received on the same port
	grpcPort   = 50000
	raftPort   = 50000
	redisPort  = 63790
	dynamoPort = 28000

	// followers wait longer before starting elections to give the leader time to bootstrap and share config.
	followerElectionTimeout = 10 * time.Second
)

var mu sync.Mutex
var portGrpc atomic.Int32
var portRaft atomic.Int32
var portRedis atomic.Int32
var portDynamo atomic.Int32

func init() {
	portGrpc.Store(raftPort)
	portRaft.Store(grpcPort)
	portRedis.Store(redisPort)
	portDynamo.Store(dynamoPort)
}

func portAssigner() portsAdress {
	mu.Lock()
	defer mu.Unlock()
	gp := portGrpc.Add(1)
	rp := portRaft.Add(1)
	rd := portRedis.Add(1)
	dn := portDynamo.Add(1)
	return portsAdress{
		grpc:          int(gp),
		raft:          int(rp),
		redis:         int(rd),
		dynamo:        int(dn),
		grpcAddress:   net.JoinHostPort("localhost", strconv.Itoa(int(gp))),
		raftAddress:   net.JoinHostPort("localhost", strconv.Itoa(int(rp))),
		redisAddress:  net.JoinHostPort("localhost", strconv.Itoa(int(rd))),
		dynamoAddress: net.JoinHostPort("localhost", strconv.Itoa(int(dn))),
	}
}

type Node struct {
	grpcAddress   string
	raftAddress   string
	redisAddress  string
	dynamoAddress string
	grpcServer    *grpc.Server
	redisServer   *RedisServer
	dynamoServer  *DynamoDBServer
	raft          *raft.Raft
	tm            *transport.Manager
}

func newNode(grpcAddress, raftAddress, redisAddress, dynamoAddress string, r *raft.Raft, tm *transport.Manager, grpcs *grpc.Server, rd *RedisServer, ds *DynamoDBServer) Node {
	return Node{
		grpcAddress:   grpcAddress,
		raftAddress:   raftAddress,
		redisAddress:  redisAddress,
		dynamoAddress: dynamoAddress,
		grpcServer:    grpcs,
		redisServer:   rd,
		dynamoServer:  ds,
		raft:          r,
		tm:            tm,
	}
}

//nolint:unparam
func createNode(t *testing.T, n int) ([]Node, []string, []string) {
	const (
		waitTimeout  = 5 * time.Second
		waitInterval = 100 * time.Millisecond
	)

	t.Helper()

	ctx := context.Background()

	ports := assignPorts(n)
	cfg := buildRaftConfig(n, ports)
	nodes, grpcAdders, redisAdders := setupNodes(t, ctx, n, ports, cfg)

	waitForNodeListeners(t, ctx, nodes, waitTimeout, waitInterval)
	waitForConfigReplication(t, cfg, nodes, waitTimeout, waitInterval)
	waitForRaftReadiness(t, nodes, waitTimeout, waitInterval)

	return nodes, grpcAdders, redisAdders
}

func waitForNodeListeners(t *testing.T, ctx context.Context, nodes []Node, waitTimeout, waitInterval time.Duration) {
	t.Helper()
	d := &net.Dialer{Timeout: time.Second}
	for _, n := range nodes {
		assert.Eventually(t, func() bool {
			conn, err := d.DialContext(ctx, "tcp", n.grpcAddress)
			if err != nil {
				return false
			}
			_ = conn.Close()
			conn, err = d.DialContext(ctx, "tcp", n.redisAddress)
			if err != nil {
				return false
			}
			_ = conn.Close()
			return true
		}, waitTimeout, waitInterval)
	}
}

func waitForRaftReadiness(t *testing.T, nodes []Node, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	expectedLeader := raft.ServerAddress(nodes[0].raftAddress)
	assert.Eventually(t, func() bool {
		for i, n := range nodes {
			state := n.raft.State()
			if i == 0 {
				if state != raft.Leader {
					return false
				}
			} else if state != raft.Follower {
				return false
			}

			addr, _ := n.raft.LeaderWithID()
			if addr != expectedLeader {
				return false
			}
		}
		return true
	}, waitTimeout, waitInterval)
}

func waitForConfigReplication(t *testing.T, cfg raft.Configuration, nodes []Node, waitTimeout, waitInterval time.Duration) {
	t.Helper()

	assert.Eventually(t, func() bool {
		for _, n := range nodes {
			future := n.raft.GetConfiguration()
			if future.Error() != nil {
				return false
			}

			current := future.Configuration().Servers
			if len(current) != len(cfg.Servers) {
				return false
			}

			for _, expected := range cfg.Servers {
				if !containsServer(current, expected) {
					return false
				}
			}
		}
		return true
	}, waitTimeout, waitInterval)
}

func containsServer(servers []raft.Server, expected raft.Server) bool {
	for _, s := range servers {
		if s.ID == expected.ID && s.Address == expected.Address && s.Suffrage == expected.Suffrage {
			return true
		}
	}
	return false
}

func assignPorts(n int) []portsAdress {
	ports := make([]portsAdress, n)
	for i := 0; i < n; i++ {
		ports[i] = portAssigner()
	}
	return ports
}

func buildRaftConfig(n int, ports []portsAdress) raft.Configuration {
	cfg := raft.Configuration{}
	for i := 0; i < n; i++ {
		suffrage := raft.Nonvoter
		if i == 0 {
			suffrage = raft.Voter
		}

		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: suffrage,
			ID:       raft.ServerID(strconv.Itoa(i)),
			Address:  raft.ServerAddress(ports[i].raftAddress),
		})
	}

	return cfg
}

const leaderElectionTimeout = 0 * time.Second

func setupNodes(t *testing.T, ctx context.Context, n int, ports []portsAdress, cfg raft.Configuration) ([]Node, []string, []string) {
	t.Helper()
	var grpcAdders []string
	var redisAdders []string
	var nodes []Node
	var lc net.ListenConfig

	for i := 0; i < n; i++ {
		st := store.NewRbMemoryStore()
		trxSt := store.NewMemoryStoreDefaultTTL()
		fsm := kv.NewKvFSM(st, trxSt)

		port := ports[i]

		// リーダーが先に投票を開始させる
		electionTimeout := leaderElectionTimeout
		if i != 0 {
			electionTimeout = followerElectionTimeout
		}

		r, tm, err := newRaft(strconv.Itoa(i), port.raftAddress, fsm, i == 0, cfg, electionTimeout)
		assert.NoError(t, err)

		s := grpc.NewServer()
		trx := kv.NewTransaction(r)
		coordinator := kv.NewCoordinator(trx, r)
		gs := NewGRPCServer(st, coordinator)
		tm.Register(s)
		pb.RegisterRawKVServer(s, gs)
		pb.RegisterTransactionalKVServer(s, gs)
		pb.RegisterInternalServer(s, NewInternal(trx, r))

		leaderhealth.Setup(r, s, []string{"Example"})
		raftadmin.Register(s, r)

		grpcSock, err := lc.Listen(ctx, "tcp", port.grpcAddress)
		require.NoError(t, err)

		grpcAdders = append(grpcAdders, port.grpcAddress)
		redisAdders = append(redisAdders, port.redisAddress)
		go func(srv *grpc.Server, lis net.Listener) {
			assert.NoError(t, srv.Serve(lis))
		}(s, grpcSock)

		l, err := lc.Listen(ctx, "tcp", port.redisAddress)
		require.NoError(t, err)
		rd := NewRedisServer(l, st, coordinator)
		go func(server *RedisServer) {
			assert.NoError(t, server.Run())
		}(rd)

		dl, err := lc.Listen(ctx, "tcp", port.dynamoAddress)
		assert.NoError(t, err)
		ds := NewDynamoDBServer(dl, st, coordinator)
		go func() {
			assert.NoError(t, ds.Run())
		}()

		nodes = append(nodes, newNode(
			port.grpcAddress,
			port.raftAddress,
			port.redisAddress,
			port.dynamoAddress,
			r,
			tm,
			s,
			rd,
			ds,
		))
	}

	return nodes, grpcAdders, redisAdders
}

func newRaft(myID string, myAddress string, fsm raft.FSM, bootstrap bool, cfg raft.Configuration, electionTimeout time.Duration) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

	if electionTimeout > 0 {
		c.ElectionTimeout = electionTimeout
	}

	// this config is for development
	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()

	c.Logger = hclog.New(&hclog.LoggerOptions{
		Name:  "raft-" + myID,
		Level: hclog.LevelFromString("WARN"),
	})

	tm := transport.New(raft.ServerAddress(myAddress), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if bootstrap {
		f := r.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			return nil, nil, errors.WithStack(err)
		}
	}

	return r, tm, nil
}

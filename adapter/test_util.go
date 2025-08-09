package adapter

import (
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
	grpcPort = 50000
	raftPort = 50000

	redisPort  = 63790
	dynamoPort = 28000
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
}

func newNode(grpcAddress, raftAddress, redisAddress, dynamoAddress string, grpcs *grpc.Server, rd *RedisServer, ds *DynamoDBServer) Node {
	return Node{
		grpcAddress:   grpcAddress,
		raftAddress:   raftAddress,
		redisAddress:  redisAddress,
		dynamoAddress: dynamoAddress,
		grpcServer:    grpcs,
		redisServer:   rd,
		dynamoServer:  ds,
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

	for i := 0; i < n; i++ {
		st := store.NewRbMemoryStore()
		trxSt := store.NewMemoryStoreDefaultTTL()
		fsm := kv.NewKvFSM(st, trxSt)

		port := ports[i]

		r, tm, err := newRaft(strconv.Itoa(i), port.raftAddress, fsm, i == 0, cfg)
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

		dl, err := net.Listen("tcp", port.dynamoAddress)
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
			s,
			rd,
			ds),
		)

	}

	//nolint:mnd
	time.Sleep(10 * time.Second)

	return nodes, grpcAdders, redisAdders
}

func newRaft(myID string, myAddress string, fsm raft.FSM, bootstrap bool, cfg raft.Configuration) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(myID)

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

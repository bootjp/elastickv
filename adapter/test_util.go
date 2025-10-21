package adapter

import (
	"context"
	"log"
	"net"
	"strconv"
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

// Node groups the servers and addresses used in tests.

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
	var grpcAdders []string
	var redisAdders []string
	var nodes []Node

	const (
		waitTimeout  = 5 * time.Second
		waitInterval = 100 * time.Millisecond
	)

	ctx := context.Background()

	// allocate listeners for gRPC/raft in advance so ports are reserved
	grpcListeners := make([]net.Listener, n)
	cfg := raft.Configuration{}
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		grpcListeners[i] = l
		addr := l.Addr().String()
		grpcAdders = append(grpcAdders, addr)

		var suffrage raft.ServerSuffrage
		if i == 0 {
			suffrage = raft.Voter
		} else {
			suffrage = raft.Nonvoter
		}
		cfg.Servers = append(cfg.Servers, raft.Server{
			Suffrage: suffrage,
			ID:       raft.ServerID(strconv.Itoa(i)),
			Address:  raft.ServerAddress(addr),
		})
	}

	for i := 0; i < n; i++ {
		st := store.NewRbMemoryStore()
		trxSt := store.NewMemoryStoreDefaultTTL()
		fsm := kv.NewKvFSM(st, trxSt)

		r, tm, err := newRaft(strconv.Itoa(i), grpcAdders[i], fsm, i == 0, cfg)
		require.NoError(t, err)

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

		go func(l net.Listener) {
			require.NoError(t, s.Serve(l))
		}(grpcListeners[i])

		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		redisAddr := l.Addr().String()
		redisAdders = append(redisAdders, redisAddr)
		rd := NewRedisServer(l, st, coordinator)
		go func() {
			require.NoError(t, rd.Run())
		}()

		dl, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		dynamoAddr := dl.Addr().String()
		ds := NewDynamoDBServer(dl, st, coordinator)
		go func() {
			require.NoError(t, ds.Run())
		}()

		nodes = append(nodes, newNode(
			grpcAdders[i],
			grpcAdders[i],
			redisAddr,
			dynamoAddr,
			r,
			tm,
			s,
			rd,
			ds,
		))
	}

	d := &net.Dialer{Timeout: time.Second}
	for _, n := range nodes {
		require.Eventually(t, func() bool {
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
			conn, err = d.DialContext(ctx, "tcp", n.dynamoAddress)
			if err != nil {
				return false
			}
			_ = conn.Close()
			return true
		}, waitTimeout, waitInterval)
	}

	require.Eventually(t, func() bool {
		return nodes[0].raft.State() == raft.Leader
	}, waitTimeout, waitInterval)

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

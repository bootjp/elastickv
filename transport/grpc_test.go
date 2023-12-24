package transport

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
)

func shutdown(nodes []*grpc.Server) {
	for _, n := range nodes {
		n.Stop()
	}
}

type portsAdress struct {
	grpc        int
	raft        int
	grpcAddress string
	raftAddress string
}

var portGrpc atomic.Int32
var portRaft atomic.Int32

func init() {
	portGrpc.Store(50000)
	portRaft.Store(50000)
}

func portAssigner() portsAdress {

	gp := portGrpc.Add(1)
	rp := portRaft.Add(1)
	return portsAdress{
		grpc:        int(gp),
		raft:        int(rp),
		grpcAddress: net.JoinHostPort("localhost", strconv.Itoa(int(gp))),
		raftAddress: net.JoinHostPort("localhost", strconv.Itoa(int(rp))),
	}
}

//nolint:unparam
func createNode(t *testing.T, n int) ([]*grpc.Server, []string) {
	var grpcAdders []string
	var nodes []*grpc.Server

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
		gs := NewGRPCServer(st, r)
		tm.Register(s)
		pb.RegisterRawKVServer(s, gs)
		pb.RegisterTransactionalKVServer(s, gs)
		leaderhealth.Setup(r, s, []string{"Example"})
		raftadmin.Register(s, r)

		grpcSock, err := net.Listen("tcp", port.grpcAddress)
		assert.NoError(t, err)

		grpcAdders = append(grpcAdders, port.grpcAddress)
		go func() {
			assert.NoError(t, s.Serve(grpcSock))
		}()

		nodes = append(nodes, s)
	}

	time.Sleep(3 * time.Second)

	return nodes, grpcAdders
}

func Test_value_can_be_deleted(t *testing.T) {
	t.Parallel()
	nodes, adders := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key")
	want := []byte("v")

	_, err := c.RawPut(
		context.Background(),
		&pb.RawPutRequest{Key: key, Value: want},
	)
	assert.NoError(t, err, "Put RPC failed")

	_, err = c.RawPut(context.TODO(), &pb.RawPutRequest{Key: key, Value: want})
	assert.NoError(t, err, "Put RPC failed")
	assert.Nil(t, err)

	resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.NoError(t, err, "Get RPC failed")
	assert.Nil(t, err)
	assert.Equal(t, want, resp.Value)

	_, err = c.RawDelete(context.TODO(), &pb.RawDeleteRequest{Key: key})
	assert.NoError(t, err, "Delete RPC failed")

	_, err = c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.NoError(t, err, "Get RPC failed")
}

func Test_consistency_satisfy_write_after_read_for_parallel(t *testing.T) {
	t.Parallel()
	nodes, adders := createNode(t, 3)
	c := rawKVClient(t, adders)

	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			key := []byte("test-key-parallel" + strconv.Itoa(i))
			want := []byte(strconv.Itoa(i))
			_, err := c.RawPut(
				context.Background(),
				&pb.RawPutRequest{Key: key, Value: want},
			)
			assert.NoError(t, err, "Put RPC failed")
			_, err = c.RawPut(context.TODO(), &pb.RawPutRequest{Key: key, Value: want})
			assert.NoError(t, err, "Put RPC failed")

			resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
			assert.NoError(t, err, "Get RPC failed")
			assert.Equal(t, want, resp.Value, "consistency check failed")
			wg.Done()
		}(i)
	}
	wg.Wait()
	shutdown(nodes)
}

func Test_consistency_satisfy_write_after_read_sequence(t *testing.T) {
	t.Parallel()
	nodes, adders := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	for i := 0; i < 99999; i++ {
		want := []byte("sequence" + strconv.Itoa(i))
		_, err := c.RawPut(
			context.Background(),
			&pb.RawPutRequest{Key: key, Value: want},
		)
		assert.NoError(t, err, "Put RPC failed")

		_, err = c.RawPut(context.TODO(), &pb.RawPutRequest{Key: key, Value: want})
		assert.NoError(t, err, "Put RPC failed")

		resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
		assert.NoError(t, err, "Get RPC failed")

		assert.Equal(t, want, resp.Value, "consistency check failed")
	}
}

func Test_grpc_transaction(t *testing.T) {
	t.Parallel()
	nodes, adders := createNode(t, 3)
	c := transactionalKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	for i := 0; i < 9999; i++ {
		want := []byte("sequence" + strconv.Itoa(i))
		_, err := c.Put(
			context.Background(),
			&pb.PutRequest{Key: key, Value: want},
		)
		assert.NoError(t, err, "Put RPC failed")
		resp, err := c.Get(context.TODO(), &pb.GetRequest{Key: key})
		assert.NoError(t, err, "Get RPC failed")
		assert.Equal(t, want, resp.Value, "consistency check failed")

		_, err = c.Delete(context.TODO(), &pb.DeleteRequest{Key: key})
		assert.NoError(t, err, "Delete RPC failed")

		resp, err = c.Get(context.TODO(), &pb.GetRequest{Key: key})
		assert.NoError(t, err, "Get RPC failed")
		assert.Nil(t, resp.Value, "consistency check failed")
	}
}

func rawKVClient(t *testing.T, hosts []string) pb.RawKVClient {
	dials := "multi:///" + strings.Join(hosts, ",")
	conn, err := grpc.Dial(dials,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewRawKVClient(conn)
}

func transactionalKVClient(t *testing.T, hosts []string) pb.TransactionalKVClient {
	dials := "multi:///" + strings.Join(hosts, ",")
	conn, err := grpc.Dial(dials,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewTransactionalKVClient(conn)
}

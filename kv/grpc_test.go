package kv

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"
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

func createNode(n int) ([]*grpc.Server, []string, error) {
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
		st := NewMemoryStore()
		fsm := NewKvFSM(st)

		port := ports[i]

		r, tm, err := NewRaft(ctx, strconv.Itoa(i), port.raftAddress, fsm, i == 0, cfg)
		if err != nil {
			return nil, nil, err
		}
		s := grpc.NewServer()
		tm.Register(s)
		pb.RegisterRawKVServer(s, NewGRPCServer(fsm, st, r))
		leaderhealth.Setup(r, s, []string{"Example"})
		raftadmin.Register(s, r)
		reflection.Register(s)

		grpcSock, err := net.Listen("tcp", port.grpcAddress)
		if err != nil {
			return nil, nil, err
		}

		grpcAdders = append(grpcAdders, port.grpcAddress)
		go func() {
			if err := s.Serve(grpcSock); err != nil {
				panic(err)
			}
		}()

		nodes = append(nodes, s)
	}

	time.Sleep(10 * time.Second)

	return nodes, grpcAdders, nil
}

func Test_value_can_be_deleted(t *testing.T) {
	t.Parallel()
	nodes, adders, err := createNode(3)
	assert.NoError(t, err)
	c := client(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key")
	want := []byte("v")

	_, err = c.RawPut(
		context.Background(),
		&pb.RawPutRequest{Key: key, Value: want},
	)
	assert.Nil(t, err)
	_, err = c.RawPut(context.TODO(), &pb.RawPutRequest{Key: key, Value: want})
	assert.Nil(t, err)

	resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.Nil(t, err)

	assert.Equal(t, want, resp.Value)

	_, err = c.RawDelete(context.TODO(), &pb.RawDeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("Delete RPC failed: %v", err)
	}

	_, err = c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	if err != nil {
		t.Fatalf("RawGet RPC failed: %v", err)
	}
}

func Test_consistency_satisfy_write_after_read_for_parallel(t *testing.T) {
	t.Parallel()
	_, adders, err := createNode(3)
	assert.NoError(t, err)
	c := client(t, adders)

	for i := 0; i < 1000; i++ {
		go func(i int) {
			key := []byte("test-key-parallel" + strconv.Itoa(i))
			want := []byte(strconv.Itoa(i))
			_, err := c.RawPut(
				context.Background(),
				&pb.RawPutRequest{Key: key, Value: want},
			)
			if err != nil {
				t.Errorf("Add RPC failed: %v", err)
				return
			}
			_, err = c.RawPut(context.TODO(), &pb.RawPutRequest{Key: key, Value: want})
			if err != nil {
				t.Errorf("Add RPC failed: %v", err)
				return
			}
			resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
			if err != nil {
				t.Errorf("Get RPC failed: %v", err)
				return
			}

			if !reflect.DeepEqual(want, resp.Value) {
				t.Errorf("consistency check failed want %v got %v", want, resp.Value)
				return
			}
		}(i)
	}
}

func Test_consistency_satisfy_write_after_read_sequence(t *testing.T) {
	t.Parallel()

	nodes, adders, err := createNode(3)
	assert.NoError(t, err)
	c := client(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	for i := 0; i < 99999; i++ {
		want := []byte("sequence" + strconv.Itoa(i))
		_, err := c.RawPut(
			context.Background(),
			&pb.RawPutRequest{Key: key, Value: want},
		)
		if err != nil {
			t.Errorf("Add RPC failed: %v", err)
			return
		}
		_, err = c.RawPut(context.TODO(), &pb.RawPutRequest{Key: key, Value: want})
		if err != nil {
			t.Errorf("Add RPC failed: %v", err)
			return
		}
		resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
		if err != nil {
			t.Errorf("Get RPC failed: %v", err)
			return
		}

		if !reflect.DeepEqual(want, resp.Value) {
			t.Errorf("consistency check failed want %v got %v", want, resp.Value)
			return
		}
	}
}

func client(t *testing.T, hosts []string) pb.RawKVClient {
	dials := "multi://"
	for _, h := range hosts {
		dials += h + ","
	}
	dials = strings.TrimSuffix(dials, ",")

	//conn, err := grpc.Dial(dials,
	//	grpc.WithTransportCredentials(insecure.NewCredentials()),
	//	grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	//)
	conn, err := grpc.Dial("multi:///localhost:50000,localhost:50001,localhost:50002",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Errorf("failed to dial: %v", err)
	}
	return pb.NewRawKVClient(conn)
}

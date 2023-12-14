package kv

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"reflect"
	"strconv"
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

var raftHostformat = "localhost:6000%d"
var hostformat = "localhost:5000%d"

var kvs map[string]Store
var node []*grpc.Server

func TestMain(m *testing.M) {
	kvs = make(map[string]Store)
	_ = createNode(3)
	fmt.Println("finish create node")
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	for _, server := range node {
		server.Stop()
	}
}

func createNode(n int) []*grpc.Server {
	cfg := raft.Configuration{}
	for i := 0; i < n; i++ {
		fmt.Println("create node", fmt.Sprintf(hostformat, i))
		var suffrage raft.ServerSuffrage
		if i == 0 {
			suffrage = raft.Voter
		} else {
			suffrage = raft.Nonvoter
		}
		addr := fmt.Sprintf(hostformat, i)
		server := raft.Server{
			Suffrage: suffrage,
			ID:       raft.ServerID(strconv.Itoa(i)),
			Address:  raft.ServerAddress(addr),
		}
		cfg.Servers = append(cfg.Servers, server)
	}

	for i := 0; i < n; i++ {
		ctx := context.Background()
		addr := fmt.Sprintf(hostformat, i)
		raftAddr := fmt.Sprintf(raftHostformat, i)
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			log.Fatalf("failed to parse local address (%q): %v", fmt.Sprintf(hostformat, i), err)
		}
		sock, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%s", port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		st := NewMemoryStore()
		fsm := NewKvFSM(st)

		kvs[strconv.Itoa(i)] = st
		r, err := NewRaft(ctx, strconv.Itoa(i), raftAddr, fsm, i == 0, cfg)
		if err != nil {
			log.Fatalf("failed to start raft: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterRawKVServer(s, NewGRPCServer(fsm, st, r))
		leaderhealth.Setup(r, s, []string{"Example"})
		raftadmin.Register(s, r)
		reflection.Register(s)

		node = append(node, s)
		go func() {
			if err := s.Serve(sock); err != nil {
				log.Fatalf("failed to serve: %v", err)
			}
		}()
	}

	time.Sleep(10 * time.Second)

	return node
}

func Test_value_can_be_deleted(t *testing.T) {
	c := client()
	key := []byte("test-key")
	want := []byte("v")
	_, err := c.Put(
		context.Background(),
		&pb.PutRequest{Key: key, Value: want},
	)
	assert.Nil(t, err)
	_, err = c.Put(context.TODO(), &pb.PutRequest{Key: key, Value: want})
	assert.Nil(t, err)

	resp, err := c.Get(context.TODO(), &pb.GetRequest{Key: key})
	assert.Nil(t, err)

	assert.Equal(t, want, resp.Value)

	_, err = c.Delete(context.TODO(), &pb.DeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("Delete RPC failed: %v", err)
	}

	_, err = c.Get(context.TODO(), &pb.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("RawGet RPC failed: %v", err)
	}
}

func Test_consistency_satisfy_write_after_read_for_parallel(t *testing.T) {
	c := client()

	for i := 0; i < 1000; i++ {
		go func(i int) {
			key := []byte("test-key-parallel" + strconv.Itoa(i))
			want := []byte(strconv.Itoa(i))
			_, err := c.Put(
				context.Background(),
				&pb.PutRequest{Key: key, Value: want},
			)
			if err != nil {
				t.Errorf("Add RPC failed: %v", err)
				return
			}
			_, err = c.Put(context.TODO(), &pb.PutRequest{Key: key, Value: want})
			if err != nil {
				t.Errorf("Add RPC failed: %v", err)
				return
			}
			resp, err := c.Get(context.TODO(), &pb.GetRequest{Key: key})
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
	c := client()

	key := []byte("test-key-sequence")

	for i := 0; i < 99999; i++ {
		want := []byte("sequence" + strconv.Itoa(i))
		_, err := c.Put(
			context.Background(),
			&pb.PutRequest{Key: key, Value: want},
		)
		if err != nil {
			t.Errorf("Add RPC failed: %v", err)
			return
		}
		_, err = c.Put(context.TODO(), &pb.PutRequest{Key: key, Value: want})
		if err != nil {
			t.Errorf("Add RPC failed: %v", err)
			return
		}
		resp, err := c.Get(context.TODO(), &pb.GetRequest{Key: key})
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

func client() pb.RawKVClient {
	conn, err := grpc.Dial("multi:///localhost:50000,localhost:50001,localhost:50002",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		log.Fatalf("dialing failed: %v", err)
	}
	return pb.NewRawKVClient(conn)
}

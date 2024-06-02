package adapter

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	_ "github.com/Jille/grpc-multi-resolver"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
)

func Test_value_can_be_deleted(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
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

func Test_grpc_scan(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := transactionalKVClient(t, adders)
	defer shutdown(nodes)

	for i := 0; i < 10; i++ {
		key := []byte("test-key-" + strconv.Itoa(i))
		want := []byte(strconv.Itoa(i))
		res, err := c.Put(
			context.Background(),
			&pb.PutRequest{Key: key, Value: want},
		)
		assert.NoError(t, err, "Put RPC failed")
		assert.True(t, res.Success, "Put RPC failed")
		t.Log(res.CommitIndex)
	}

	resp, err := c.Scan(context.TODO(), &pb.ScanRequest{
		StartKey: []byte("test-key"),
		EndKey:   []byte("z" + strconv.Itoa(100)),
		Limit:    10,
	})
	assert.NoError(t, err, "Scan RPC failed")
	assert.Equal(t, 10, len(resp.Kv), "Scan RPC failed")

	for i := 0; i < 10; i++ {
		key := []byte("test-key-" + strconv.Itoa(i))
		want := []byte(strconv.Itoa(i))
		assert.Equal(t, key, resp.Kv[i].Key, "Scan RPC failed")
		assert.Equal(t, want, resp.Kv[i].Value, "Scan RPC failed")
	}
}

func Test_consistency_satisfy_write_after_read_for_parallel(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
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
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	for i := 0; i < 9999; i++ {
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
	nodes, adders, _ := createNode(t, 3)
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
	conn, err := grpc.NewClient(dials,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewRawKVClient(conn)
}

func transactionalKVClient(t *testing.T, hosts []string) pb.TransactionalKVClient {
	dials := "multi:///" + strings.Join(hosts, ",")
	conn, err := grpc.NewClient(dials,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewTransactionalKVClient(conn)
}

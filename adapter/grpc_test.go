package adapter

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	_ "github.com/Jille/grpc-multi-resolver"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.True(t, resp.Exists)
	assert.Equal(t, want, resp.Value)

	_, err = c.RawDelete(context.TODO(), &pb.RawDeleteRequest{Key: key})
	assert.NoError(t, err, "Delete RPC failed")

	resp, err = c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.NoError(t, err, "Get RPC failed")
	assert.False(t, resp.Exists)
}

func Test_grpc_raw_get_empty_value(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("empty-key")
	empty := []byte{}

	_, err := c.RawPut(context.Background(), &pb.RawPutRequest{Key: key, Value: empty})
	assert.NoError(t, err, "Put RPC failed")

	resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.NoError(t, err, "Get RPC failed")
	assert.True(t, resp.Exists)
	assert.Equal(t, 0, len(resp.Value))
}

func Test_grpc_scan(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := transactionalKVClient(t, adders)
	defer shutdown(nodes)

	for i := range 10 {
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

	for i := range 10 {
		key := []byte("test-key-" + strconv.Itoa(i))
		want := []byte(strconv.Itoa(i))
		assert.Equal(t, key, resp.Kv[i].Key, "Scan RPC failed")
		assert.Equal(t, want, resp.Kv[i].Value, "Scan RPC failed")
	}
}

func TestGRPCServer_RawLatestCommitTS_EmptyKeyReturnsGlobalWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), 77, 0))

	s := NewGRPCServer(st, nil)

	// Empty key should return global LastCommitTS, not an error.
	resp, err := s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{})
	assert.NoError(t, err)
	assert.Equal(t, uint64(77), resp.GetTs())
	assert.True(t, resp.GetExists())

	// Non-empty key should still work as before.
	resp, err = s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k")})
	assert.NoError(t, err)
	assert.Equal(t, uint64(77), resp.GetTs())
}

func TestGRPCServer_RawScanAt_RejectsOversizedLimit(t *testing.T) {
	t.Parallel()

	s := NewGRPCServer(store.NewMVCCStore(), nil)

	_, err := s.RawScanAt(context.Background(), &pb.RawScanAtRequest{
		Limit: maxGRPCScanLimit + 1,
	})

	assert.Error(t, err)
}

func TestGRPCServer_Scan_RejectsOversizedLimit(t *testing.T) {
	t.Parallel()

	s := NewGRPCServer(store.NewMVCCStore(), nil)

	_, err := s.Scan(context.Background(), &pb.ScanRequest{
		Limit: maxGRPCScanLimit + 1,
	})

	assert.Error(t, err)
}

func Test_consistency_satisfy_write_after_read_for_parallel(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)

	// 1000 concurrent clients × 3 RPCs saturates the single raft leader
	// hard enough to provoke brief quorum checks to fail on CI, so retry
	// transient leader-unavailable errors. The *Eventually helpers are
	// intentionally NOT used here: they end in require.NoError, and
	// require calls t.FailNow() which must run on the main test goroutine.
	// Workers use retryNotLeader + an errors channel instead so all
	// require/assert calls happen on the main goroutine after wg.Wait().
	ctx := context.Background()
	const workers = 1000
	errCh := make(chan error, workers)
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			key := []byte("test-key-parallel" + strconv.Itoa(i))
			want := []byte(strconv.Itoa(i))
			put := func() error {
				_, err := c.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: want})
				return err
			}
			if err := retryNotLeader(ctx, put); err != nil {
				errCh <- err
				return
			}
			if err := retryNotLeader(ctx, put); err != nil {
				errCh <- err
				return
			}
			var resp *pb.RawGetResponse
			err := retryNotLeader(ctx, func() error {
				r, err := c.RawGet(ctx, &pb.RawGetRequest{Key: key})
				if err != nil {
					return err
				}
				resp = r
				return nil
			})
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(want, resp.Value) {
				errCh <- fmt.Errorf("consistency check failed for key %s: want %q got %q", key, want, resp.Value)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		assert.NoError(t, err)
	}
	shutdown(nodes)
}

func Test_consistency_satisfy_write_after_read_sequence(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	// Use *Eventually helpers because a 9999-iteration loop across three
	// t.Parallel adapter tests loads CI enough that the raft leader can
	// briefly lose quorum and step down mid-run, surfacing as transient
	// "not leader" / "leader not found" RPC errors. The helpers retry
	// only those transient errors; any other error still fails the test.
	ctx := context.Background()
	for i := range 9999 {
		want := []byte("sequence" + strconv.Itoa(i))
		rawPutEventually(t, ctx, c, &pb.RawPutRequest{Key: key, Value: want})
		rawPutEventually(t, ctx, c, &pb.RawPutRequest{Key: key, Value: want})

		resp := rawGetEventually(t, ctx, c, &pb.RawGetRequest{Key: key})
		assert.Equal(t, want, resp.Value, "consistency check failed")
	}
}

func Test_grpc_transaction(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := transactionalKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	// See Test_consistency_satisfy_write_after_read_sequence for why the
	// *Eventually helpers are necessary here.
	ctx := context.Background()
	for i := range 9999 {
		want := []byte("sequence" + strconv.Itoa(i))
		txnPutEventually(t, ctx, c, &pb.PutRequest{Key: key, Value: want})
		resp := txnGetEventually(t, ctx, c, &pb.GetRequest{Key: key})
		assert.Equal(t, want, resp.Value, "consistency check failed")

		txnDeleteEventually(t, ctx, c, &pb.DeleteRequest{Key: key})

		resp = txnGetEventually(t, ctx, c, &pb.GetRequest{Key: key})
		assert.Nil(t, resp.Value, "consistency check failed")
	}
}

func rawKVClient(t *testing.T, hosts []string) pb.RawKVClient {
	conn, err := grpc.NewClient(hosts[0],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewRawKVClient(conn)
}

func transactionalKVClient(t *testing.T, hosts []string) pb.TransactionalKVClient {
	conn, err := grpc.NewClient(hosts[0],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewTransactionalKVClient(conn)
}

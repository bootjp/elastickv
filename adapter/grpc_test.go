package adapter

import (
	"context"
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
	defer shutdown(nodes)

	wg := sync.WaitGroup{}
	const workers = 1000
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			key := []byte("test-key-parallel" + strconv.Itoa(i))
			want := []byte(strconv.Itoa(i))
			_, err := c.RawPut(
				context.Background(),
				&pb.RawPutRequest{Key: key, Value: want},
			)
			if !assert.NoError(t, err, "Put RPC failed") {
				return
			}
			_, err = c.RawPut(context.Background(), &pb.RawPutRequest{Key: key, Value: want})
			if !assert.NoError(t, err, "Put RPC failed") {
				return
			}

			resp, err := c.RawGet(context.Background(), &pb.RawGetRequest{Key: key})
			if !assert.NoError(t, err, "Get RPC failed") {
				return
			}
			assert.Equal(t, want, resp.Value, "consistency check failed")
		}(i)
	}
	wg.Wait()
}

func Test_consistency_satisfy_write_after_read_sequence(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	// Each RPC is wrapped in retryNotLeader so an in-flight Raft
	// re-election (which can fire mid-loop on a busy CI runner — emit
	// "leader not found" / "etcd raft engine is not leader" — and is
	// purely an availability hiccup, not a consistency violation) does
	// not abort the test. The post-RPC assert.Equal still pins the
	// consistency invariant: once Put eventually succeeds, the
	// subsequent Get must return the same value, otherwise we fail.
	for i := range 9999 {
		want := []byte("sequence" + strconv.Itoa(i))
		err := retryNotLeader(context.Background(), func() error {
			_, perr := c.RawPut(context.Background(),
				&pb.RawPutRequest{Key: key, Value: want})
			return perr
		})
		// Stop at the first non-leader-churn RPC failure instead of
		// continuing: a genuine regression would otherwise cascade
		// into 9998 more iterations, each reporting the same broken
		// invariant, and drown the real cause in test-output noise.
		if !assert.NoError(t, err, "Put RPC failed") {
			break
		}

		err = retryNotLeader(context.Background(), func() error {
			_, perr := c.RawPut(context.Background(),
				&pb.RawPutRequest{Key: key, Value: want})
			return perr
		})
		if !assert.NoError(t, err, "Put RPC failed") {
			break
		}

		var resp *pb.RawGetResponse
		err = retryNotLeader(context.Background(), func() error {
			var gerr error
			resp, gerr = c.RawGet(context.Background(), &pb.RawGetRequest{Key: key})
			return gerr
		})
		if !assert.NoError(t, err, "Get RPC failed") {
			break
		}

		// Consistency invariant — the entire reason this test exists.
		// Wrapped RPCs only mask transport-layer flakes; if the
		// cluster ever returns a stale Get result here it is still
		// flagged loudly.
		assert.Equal(t, want, resp.Value, "consistency check failed")
	}
}

func Test_grpc_transaction(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := transactionalKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key-sequence")

	// Same retryNotLeader wrap as Test_consistency_satisfy_write_after_read
	// _sequence: tolerate transient leader churn (purely availability,
	// not consistency) while keeping the Put → Get → Delete → Get
	// invariants strict.
	for i := range 9999 {
		want := []byte("sequence" + strconv.Itoa(i))
		err := retryNotLeader(context.Background(), func() error {
			_, perr := c.Put(context.Background(), &pb.PutRequest{Key: key, Value: want})
			return perr
		})
		// See Test_consistency_satisfy_write_after_read_sequence:
		// break on first RPC failure so a single broken invariant
		// does not amplify into thousands of assertion lines.
		if !assert.NoError(t, err, "Put RPC failed") {
			break
		}
		var resp *pb.GetResponse
		err = retryNotLeader(context.Background(), func() error {
			var gerr error
			resp, gerr = c.Get(context.Background(), &pb.GetRequest{Key: key})
			return gerr
		})
		if !assert.NoError(t, err, "Get RPC failed") {
			break
		}
		assert.Equal(t, want, resp.Value, "consistency check failed")

		err = retryNotLeader(context.Background(), func() error {
			_, derr := c.Delete(context.Background(), &pb.DeleteRequest{Key: key})
			return derr
		})
		if !assert.NoError(t, err, "Delete RPC failed") {
			break
		}

		err = retryNotLeader(context.Background(), func() error {
			var gerr error
			resp, gerr = c.Get(context.Background(), &pb.GetRequest{Key: key})
			return gerr
		})
		if !assert.NoError(t, err, "Get RPC failed") {
			break
		}
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

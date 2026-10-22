package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// Pause after the adapter allocates C, but before the real coordinator proposes
// it. No store/FSM/transport is mocked: the later seal must reject this plan.
type delayedSealRaftCoordinator struct {
	kv.Coordinator
	blocked   atomic.Bool
	entered   chan uint64
	release   chan struct{}
	result    chan error
	unstamped bool
}

func (c *delayedSealRaftCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req == nil || !req.IsTxn || (req.CommitTS == 0 && !c.unstamped) || isDynamoSnapshotSealRequest(req) || !c.blocked.CompareAndSwap(false, true) {
		return c.Coordinator.Dispatch(ctx, req)
	}
	stamp := req.CommitTS
	if stamp == 0 {
		stamp = req.StartTS
	}
	c.entered <- stamp
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	resp, err := c.Coordinator.Dispatch(ctx, req)
	c.result <- err
	return resp, err
}

func newDelayedSealRaftServer(t *testing.T, server *DynamoDBServer, unstamped bool) (*DynamoDBServer, *delayedSealRaftCoordinator, func()) {
	t.Helper()
	coord := &delayedSealRaftCoordinator{
		Coordinator: server.coordinator,
		entered:     make(chan uint64, 1),
		release:     make(chan struct{}),
		result:      make(chan error, 1),
		unstamped:   unstamped,
	}
	var once sync.Once
	release := func() { once.Do(func() { close(coord.release) }) }
	t.Cleanup(release)
	return NewDynamoDBServer(nil, server.store, coord), coord, release
}

func sealRaftSeed(t *testing.T, ctx context.Context, server *DynamoDBServer, schema *dynamoTableSchema) {
	t.Helper()
	require.NoError(t, server.createTableWithRetry(ctx, schema.TableName, schema))
	_, err := server.putItemWithRetry(ctx, putItemInput{TableName: schema.TableName, Item: map[string]attributeValue{
		"pk": newStringAttributeValue(dedupTestKey), "val": numberListAttr("1"),
	}})
	require.NoError(t, err)
}

func sealRaftAppend(table, value string) updateItemInput {
	in := appendListInput()
	in.TableName = table
	in.ExpressionAttributeValues[":val"] = numberListAttr(value)
	return in
}

func sealRaftTxnUpdate(table, value string) transactWriteItem {
	in := sealRaftAppend(table, value)
	return transactWriteItem{Update: &transactUpdateInput{
		TableName: in.TableName, Key: in.Key, UpdateExpression: in.UpdateExpression,
		ExpressionAttributeNames: in.ExpressionAttributeNames, ExpressionAttributeValues: in.ExpressionAttributeValues,
	}}
}

func sealRaftDelayedAppend(ctx context.Context, server *DynamoDBServer, table string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := server.updateItemWithRetry(ctx, sealRaftAppend(table, "2"))
		done <- err
	}()
	return done
}

func sealRaftReceive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("waiting for delayed Raft write: %v", ctx.Err())
		var zero T
		return zero
	}
}

func sealRaftHTTP(t *testing.T, ctx context.Context, address, action string, input any) {
	t.Helper()
	require.NoError(t, sealRaftHTTPCall(ctx, address, action, input))
}

func sealRaftHTTPCall(ctx context.Context, address, action string, input any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DynamoDB HTTP %d: %s", resp.StatusCode, response)
	}
	return nil
}

func TestDynamoDBSnapshotSealRealRaftLeaderChangeFollowerIngress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)
	schema := dedupItemTable()
	leader := nodes[0].dynamoServer
	sealRaftSeed(t, ctx, leader, schema)
	delayed, coord, release := newDelayedSealRaftServer(t, leader, false)
	defer release()
	done := sealRaftDelayedAppend(ctx, delayed, schema.TableName)
	commitTS := sealRaftReceive(t, ctx, coord.entered)
	sealRaftAdvanceWatermark(t, ctx, leader.coordinator, commitTS)

	admin, ok := nodes[0].engine.(raftengine.Admin)
	require.True(t, ok)
	require.NoError(t, admin.TransferLeadershipToServer(ctx, "1", nodes[1].raftAddress))
	require.Eventually(t, func() bool {
		return nodes[1].engine.State() == raftengine.StateLeader &&
			nodes[0].engine.State() == raftengine.StateFollower && nodes[2].engine.State() == raftengine.StateFollower &&
			nodes[0].engine.Leader().Address == nodes[1].raftAddress &&
			nodes[2].engine.Leader().Address == nodes[1].raftAddress
	}, 5*time.Second, 10*time.Millisecond)
	follower := nodes[2].dynamoServer
	require.Empty(t, follower.leaderDynamo, "HTTP forwarding would hide the follower timestamp path")
	second, secondCoord, releaseSecond := newDelayedSealRaftServer(t, follower, true)
	defer releaseSecond()
	httpServer := httptest.NewServer(http.HandlerFunc(second.handle))
	defer func() {
		releaseSecond()
		cancel()
		httpServer.Close()
	}()
	txnDone := make(chan error, 1)
	go func() {
		txnDone <- sealRaftHTTPCall(ctx, strings.TrimPrefix(httpServer.URL, "http://"), "TransactWriteItems", transactWriteItemsInput{
			TransactItems: []transactWriteItem{sealRaftTxnUpdate(schema.TableName, "3")},
		})
	}()
	var sealed uint64
	select {
	case sealed = <-secondCoord.entered:
	case err := <-txnDone:
		t.Fatalf("transaction finished before the planned-write barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Greater(t, sealed, commitTS)
	release()
	require.ErrorContains(t, sealRaftReceive(t, ctx, coord.result), dynamoSealPrefix, "only the seal changed since the first read")
	require.NoError(t, sealRaftReceive(t, ctx, done))
	releaseSecond()
	require.NoError(t, sealRaftReceive(t, ctx, txnDone))
	require.Equal(t, []string{"1", "2", "3"}, readListValues(t, nodes[1].dynamoServer, schema))
	// Also exercise the follower's single-item legacy timestamp allocation.
	sealRaftHTTP(t, ctx, nodes[2].dynamoAddress, "UpdateItem", sealRaftAppend(schema.TableName, "4"))
	require.Equal(t, []string{"1", "2", "3", "4"}, readListValues(t, nodes[1].dynamoServer, schema))
	t.Logf("delayed C=%d, successor seal R=%d, old write rejected before transaction commit", commitTS, sealed)
}

func sealRaftAdvanceWatermark(t *testing.T, ctx context.Context, coord kv.Coordinator, pending uint64) {
	t.Helper()
	ts, err := kv.NextTimestampAfterThrough(ctx, coord, pending, "advance unrelated watermark")
	require.NoError(t, err)
	require.Greater(t, ts, pending)
	_, err = coord.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		CommitTS: ts,
		Elems:    []*kv.Elem[kv.OP]{{Op: kv.Put, Key: []byte("watermark"), Value: []byte("x")}},
	})
	require.NoError(t, err)
}

func TestDynamoDBSnapshotSealRealRaftTwoDataGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	left, right := dedupItemTable(), dedupItemTable()
	left.TableName, right.TableName = "alpha", "beta"
	split := kv.RouteKey(dynamoTableMetaKey(right.TableName))
	require.Negative(t, bytes.Compare(kv.RouteKey(dynamoTableMetaKey(left.TableName)), split))
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, split, 1)
	routes.UpdateRoute(split, nil, 2)
	hlc := kv.NewHLC()
	st1, st2 := store.NewMVCCStore(), store.NewMVCCStore()
	eng1, stop1 := newSingleRaftForDistributionE2E(t, "seal-group-1", kv.NewKvFSMWithHLC(st1, hlc))
	defer stop1()
	eng2, stop2 := newSingleRaftForDistributionE2E(t, "seal-group-2", kv.NewKvFSMWithHLC(st2, hlc))
	defer stop2()
	groups := map[uint64]*kv.ShardGroup{
		1: {Engine: eng1, Store: st1, Txn: kv.NewLeaderProxyWithEngine(eng1)},
		2: {Engine: eng2, Store: st2, Txn: kv.NewLeaderProxyWithEngine(eng2)},
	}
	st := kv.NewShardStore(routes, groups)
	defer func() {
		stop1()
		stop2()
		require.NoError(t, st.Close())
	}()
	coordinator := kv.NewShardedCoordinator(routes, groups, 1, hlc, st)
	server := NewDynamoDBServer(nil, st, coordinator)
	sealRaftSeed(t, ctx, server, left)
	sealRaftSeed(t, ctx, server, right)
	delayed, coord, release := newDelayedSealRaftServer(t, server, false)
	defer release()
	done := sealRaftDelayedAppend(ctx, delayed, right.TableName)
	commitTS := sealRaftReceive(t, ctx, coord.entered)
	sealRaftAdvanceWatermark(t, ctx, coordinator, commitTS)

	// This independent frontend has no shared per-item locks with the writer.
	second, secondCoord, releaseSecond := newDelayedSealRaftServer(t, server, true)
	defer releaseSecond()
	input := transactWriteItemsInput{TransactItems: []transactWriteItem{
		sealRaftTxnUpdate(left.TableName, "3"), sealRaftTxnUpdate(right.TableName, "3"),
	}}
	body, err := json.Marshal(input)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	txnDone := make(chan struct{})
	go func() {
		second.transactWriteItems(recorder, httptest.NewRequestWithContext(ctx, http.MethodPost, "/", bytes.NewReader(body)))
		close(txnDone)
	}()
	sealed := sealRaftReceive(t, ctx, secondCoord.entered)
	require.Greater(t, sealed, commitTS)
	key := map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)}
	leftKey, err := left.itemKeyFromAttributes(key)
	require.NoError(t, err)
	rightKey, err := right.itemKeyFromAttributes(key)
	require.NoError(t, err)
	seal1, seal2 := dynamoItemSnapshotSealKey(left.TableName, 1, leftKey), dynamoItemSnapshotSealKey(right.TableName, 1, rightKey)
	ts1, found, err := st1.LatestCommitTS(ctx, seal1)
	require.NoError(t, err)
	require.True(t, found)
	ts2, found, err := st2.LatestCommitTS(ctx, seal2)
	require.NoError(t, err)
	require.True(t, found)
	require.Greater(t, ts1, commitTS)
	require.Equal(t, ts1, ts2, "both real Raft groups must seal at the same R")
	release()
	require.ErrorIs(t, sealRaftReceive(t, ctx, coord.result), store.ErrWriteConflict)
	require.NoError(t, sealRaftReceive(t, ctx, done))
	releaseSecond()
	sealRaftReceive(t, ctx, txnDone)
	require.Equal(t, http.StatusOK, recorder.Code, "%s", recorder.Body.String())
	require.Equal(t, []string{"1", "3"}, readListValues(t, second, left))
	require.Equal(t, []string{"1", "2", "3"}, readListValues(t, second, right))
	t.Logf("two data groups: delayed C=%d, shared R=%d", commitTS, ts1)
}

func TestDynamoDBSnapshotSealRealRaftSnapshotRestart(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dir := t.TempDir()
	st := store.NewMVCCStore()
	server, engine, stop := sealRaftRestartableServer(t, dir, st)
	schema := dedupItemTable()
	sealRaftSeed(t, ctx, server, schema)
	key, err := schema.itemKeyFromAttributes(map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)})
	require.NoError(t, err)
	seal := dynamoItemSnapshotSealKey(schema.TableName, 1, key)
	before, found, err := st.LatestCommitTS(ctx, seal)
	require.NoError(t, err)
	require.True(t, found)
	stamp, err := server.beginTxnWriteTimestamp(ctx, "restart seal", [][]byte{seal})
	require.NoError(t, err)
	sealed := stamp.Timestamp()
	require.Greater(t, sealed, before)
	index := engine.Status().AppliedIndex
	require.Eventually(t, func() bool { return engine.Status().LastSnapshotIndex >= index }, 5*time.Second, 10*time.Millisecond)
	stop()
	require.NoError(t, st.Close())

	// Reopen the same Raft directory with a genuinely empty state machine.
	// The item and seal below must come from its durable snapshot/WAL, not RAM.
	restored := store.NewMVCCStore()
	restarted, _, stopRestarted := sealRaftRestartableServer(t, dir, restored)
	defer restored.Close()
	defer stopRestarted()
	sealTS, found, err := restored.LatestCommitTS(ctx, seal)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, sealed, sealTS)
	require.Equal(t, []string{"1"}, readListValues(t, restarted, schema))
	_, err = restarted.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: before, ReadKeys: [][]byte{seal},
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: []byte("stale plan")}},
	})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	_, err = restarted.updateItemWithRetry(ctx, sealRaftAppend(schema.TableName, "2"))
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, readListValues(t, restarted, schema))
}

func sealRaftRestartableServer(t testing.TB, dir string, st store.MVCCStore) (*DynamoDBServer, raftengine.Engine, func()) {
	t.Helper()
	hlc := kv.NewHLC()
	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10,
		MaxSizePerMsg: 1 << 20, MaxInflightMsg: 256,
	})
	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID: "seal-restart", LocalAddress: "seal-restart", DataDir: dir, Bootstrap: true,
		StateMachine: kv.NewKvFSMWithHLC(st, hlc),
	})
	require.NoError(t, err)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			require.NoError(t, result.Engine.Close())
			if result.Close != nil {
				require.NoError(t, result.Close())
			}
		})
	}
	t.Cleanup(stop)
	require.Eventually(t, func() bool { return result.Engine.State() == raftengine.StateLeader }, 5*time.Second, 10*time.Millisecond)
	coord := kv.NewCoordinatorWithEngine(kv.NewTransactionWithProposer(result.Engine), result.Engine, kv.WithHLC(hlc))
	return NewDynamoDBServer(nil, st, coord), result.Engine, stop
}

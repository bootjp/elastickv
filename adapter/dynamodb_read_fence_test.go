package adapter

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// pendingDynamoApplyCoordinator models a committed Raft entry whose timestamp
// is below another key's already-applied timestamp. A read fence must wait for
// that entry before a new transaction selects its snapshot watermark.
type pendingDynamoApplyCoordinator struct {
	*occAdapterCoordinator
	applyPending func() error
	applyErr     error
}

type blockedDynamoDispatchCoordinator struct {
	*occAdapterCoordinator
	firstBlocked  atomic.Bool
	secondBlocked atomic.Bool
	switched      atomic.Bool
	forward       kv.Coordinator
	firstEntered  chan uint64
	firstRelease  chan struct{}
	secondEntered chan struct{}
	secondRelease chan struct{}
}

func (c *blockedDynamoDispatchCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil && req.IsTxn && !isDynamoSnapshotSealRequest(req) {
		var err error
		if req.CommitTS != 0 {
			err = c.blockFirstDispatch(ctx, req.CommitTS)
		} else {
			err = c.blockSecondDispatch(ctx)
		}
		if err != nil {
			return nil, err
		}
	}
	if c.switched.Load() && c.forward != nil {
		return c.forward.Dispatch(ctx, req)
	}
	return c.occAdapterCoordinator.Dispatch(ctx, req)
}

func isDynamoSnapshotSealRequest(req *kv.OperationGroup[kv.OP]) bool {
	if req == nil || len(req.Elems) == 0 {
		return false
	}
	for _, elem := range req.Elems {
		if elem == nil || !bytes.HasPrefix(elem.Key, []byte(dynamoSealPrefix)) {
			return false
		}
	}
	return true
}

func TestDynamoDBSnapshotSealRoutesWithItem(t *testing.T) {
	schema := dedupItemTable()
	itemKey, err := schema.itemKeyFromAttributes(map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)})
	require.NoError(t, err)
	sealKey := dynamoItemSnapshotSealKey(schema.TableName, schema.Generation, itemKey)
	require.Equal(t, kv.RouteKey(itemKey), kv.RouteKey(sealKey))
	require.NotContains(t, string(sealKey), string(itemKey))
	require.False(t, bytes.HasPrefix(sealKey, dynamoItemPrefixForTable(schema.TableName, schema.Generation)))
	require.True(t, bytes.HasPrefix(sealKey, dynamoSealPrefixForTable(schema.TableName, schema.Generation)))
	require.Len(t, bytes.TrimPrefix(sealKey, dynamoSealPrefixForTable(schema.TableName, schema.Generation)), 4)
	require.Equal(t, sealKey, dynamoItemSnapshotSealKey(schema.TableName, schema.Generation, itemKey))
	require.NotEqual(t, sealKey, dynamoItemSnapshotSealKey(schema.TableName, schema.Generation+1, itemKey))
}

type sealResultCoordinator struct {
	*occAdapterCoordinator
	sealCalls int
	sealTS    uint64
	failAt    int
	omitTS    bool
}

func (c *sealResultCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if !isDynamoSnapshotSealRequest(req) {
		return c.occAdapterCoordinator.Dispatch(ctx, req)
	}
	c.sealCalls++
	if c.sealTS == 0 {
		c.sealTS = req.CommitTS
	} else if c.sealTS != req.CommitTS {
		return nil, errors.New("seal groups used different commit timestamps")
	}
	for _, elem := range req.Elems {
		if !bytes.Equal(kv.RouteKey(elem.Key), kv.RouteKey(req.Elems[0].Key)) {
			return nil, errors.New("seal request spans table routes")
		}
	}
	if c.sealCalls == c.failAt {
		return nil, errors.New("seal group unavailable")
	}
	resp, err := c.occAdapterCoordinator.Dispatch(ctx, req)
	if resp != nil && c.omitTS {
		resp.CommitTS = 0
	}
	return resp, err
}

func TestDynamoDBSnapshotSealFailsWhenSecondTableFails(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	coord := &sealResultCoordinator{occAdapterCoordinator: newOCCAdapterCoordinator(st), failAt: 2}
	server := NewDynamoDBServer(nil, st, coord)
	keys := [][]byte{
		dynamoItemSnapshotSealKey("alpha", 1, []byte("a")),
		dynamoItemSnapshotSealKey("beta", 1, []byte("b")),
	}
	_, err := server.beginTxnWriteTimestamp(context.Background(), "two tables", keys)
	require.ErrorContains(t, err, "seal group unavailable")
	require.Equal(t, 2, coord.sealCalls)
	require.NotZero(t, coord.sealTS)
}

func TestDynamoDBSnapshotSealRejectsMissingCommitTimestamp(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	coord := &sealResultCoordinator{occAdapterCoordinator: newOCCAdapterCoordinator(st), omitTS: true}
	server := NewDynamoDBServer(nil, st, coord)
	key := dynamoItemSnapshotSealKey("alpha", 1, []byte("a"))
	_, err := server.beginTxnWriteTimestamp(context.Background(), "missing timestamp", [][]byte{key})
	require.ErrorContains(t, err, "no matching commit timestamp")
	require.Equal(t, 1, coord.sealCalls)
}

func TestDynamoDBSnapshotSealDetectsTableRecreation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	schema := dedupItemTable()
	writer.writeSchema(schema)
	server := NewDynamoDBServer(nil, st, newOCCAdapterCoordinator(st))
	attrs := map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)}
	oldSeal, oldGeneration, err := server.itemSealKey(ctx, schema.TableName, attrs)
	require.NoError(t, err)

	recreated := *schema
	recreated.Generation++
	writer.writeSchema(&recreated)
	sealed, err := server.beginTxnWriteTimestamp(ctx, "recreated table", [][]byte{oldSeal})
	require.NoError(t, err)
	require.ErrorIs(t, server.verifySealedTableGeneration(ctx, schema.TableName, oldGeneration, sealed.Timestamp()), errDynamoSealGenerationChanged)
	newSeal, newGeneration, err := server.itemSealKey(ctx, schema.TableName, attrs)
	require.NoError(t, err)
	require.Equal(t, recreated.Generation, newGeneration)
	require.NotEqual(t, oldSeal, newSeal)
}

func (c *blockedDynamoDispatchCoordinator) blockFirstDispatch(ctx context.Context, commitTS uint64) error {
	if !c.firstBlocked.CompareAndSwap(false, true) {
		return nil
	}
	c.firstEntered <- commitTS
	select {
	case <-c.firstRelease:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *blockedDynamoDispatchCoordinator) blockSecondDispatch(ctx context.Context) error {
	if !c.secondBlocked.CompareAndSwap(false, true) {
		return nil
	}
	close(c.secondEntered)
	select {
	case <-c.secondRelease:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func dynamoHandlerStatus(t *testing.T, call func(http.ResponseWriter, *http.Request), input any) <-chan int {
	t.Helper()
	body, err := json.Marshal(input)
	require.NoError(t, err)
	done := make(chan int, 1)
	go func() {
		recorder := httptest.NewRecorder()
		call(recorder, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
		done <- recorder.Code
	}()
	return done
}

func TestDynamoDBTransactWriteWaitsForPendingItemUpdate(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	schema := dedupItemTable()
	seedDedupItem(t, st, schema, "1")
	coord := &blockedDynamoDispatchCoordinator{
		occAdapterCoordinator: newOCCAdapterCoordinator(st),
		firstEntered:          make(chan uint64, 1),
		firstRelease:          make(chan struct{}),
		secondEntered:         make(chan struct{}),
		secondRelease:         make(chan struct{}),
	}
	server := NewDynamoDBServer(nil, st, coord)
	first := appendListInput()
	first.ExpressionAttributeValues[":val"] = numberListAttr("2")
	updateDone := dynamoHandlerStatus(t, server.updateItem, first)

	var firstCommitTS uint64
	select {
	case firstCommitTS = <-coord.firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first update did not reach dispatch")
	}
	// Raise the global watermark past the pending item write. A read fence
	// cannot observe an entry that has not been proposed to Raft yet.
	require.NoError(t, st.PutAt(ctx, []byte("other"), []byte("x"), firstCommitTS+1, 0))
	second := transactWriteItemsInput{TransactItems: []transactWriteItem{{Update: &transactUpdateInput{
		TableName:                 first.TableName,
		Key:                       first.Key,
		UpdateExpression:          first.UpdateExpression,
		ExpressionAttributeNames:  first.ExpressionAttributeNames,
		ExpressionAttributeValues: map[string]attributeValue{":empty": {L: []attributeValue{}}, ":val": numberListAttr("3")},
	}}}}
	txnDone := dynamoHandlerStatus(t, server.transactWriteItems, second)
	select {
	case <-coord.secondEntered:
		// An unfenced transaction has already planned from the old item.
		// Apply the older timestamp first, then let that stale plan commit.
		close(coord.firstRelease)
		require.Equal(t, http.StatusOK, <-updateDone)
	case <-time.After(100 * time.Millisecond):
		// The transaction is waiting on the first write's item lock.
		close(coord.firstRelease)
		require.Equal(t, http.StatusOK, <-updateDone)
		select {
		case <-coord.secondEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("transaction did not reach dispatch after update completed")
		}
	}
	close(coord.secondRelease)
	require.Equal(t, http.StatusOK, <-txnDone)
	require.Equal(t, []string{"1", "2", "3"}, readListValues(t, server, schema))
}

func TestDynamoDBTwoServersSealPendingItemWrite(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	schema := dedupItemTable()
	seedDedupItem(t, st, schema, "1")
	coord := &blockedDynamoDispatchCoordinator{
		occAdapterCoordinator: newOCCAdapterCoordinator(st),
		firstEntered:          make(chan uint64, 1),
		firstRelease:          make(chan struct{}),
		secondEntered:         make(chan struct{}),
		secondRelease:         make(chan struct{}),
	}
	firstServer := NewDynamoDBServer(nil, st, coord)
	secondServer := NewDynamoDBServer(nil, st, coord)
	first := appendListInput()
	first.ExpressionAttributeValues[":val"] = numberListAttr("2")
	updateDone := dynamoHandlerStatus(t, firstServer.updateItem, first)
	var firstCommitTS uint64
	select {
	case firstCommitTS = <-coord.firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first update did not reach dispatch")
	}
	require.NoError(t, st.PutAt(ctx, []byte("other"), []byte("x"), firstCommitTS+1, 0))
	second := transactWriteItemsInput{TransactItems: []transactWriteItem{{Update: &transactUpdateInput{
		TableName:                 first.TableName,
		Key:                       first.Key,
		UpdateExpression:          first.UpdateExpression,
		ExpressionAttributeNames:  first.ExpressionAttributeNames,
		ExpressionAttributeValues: map[string]attributeValue{":empty": {L: []attributeValue{}}, ":val": numberListAttr("3")},
	}}}}
	txnDone := dynamoHandlerStatus(t, secondServer.transactWriteItems, second)
	select {
	case <-coord.secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second server did not reach dispatch")
	}
	close(coord.secondRelease)
	require.Equal(t, http.StatusOK, <-txnDone)
	close(coord.firstRelease)
	require.Equal(t, http.StatusOK, <-updateDone)
	values := readListValues(t, secondServer, schema)
	require.Equal(t, 3, len(values))
	require.ElementsMatch(t, []string{"1", "2", "3"}, values)
}

func TestDynamoDBSnapshotSealSurvivesLeaderSwitch(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	schema := dedupItemTable()
	seedDedupItem(t, st, schema, "1")
	newLeader := newOCCAdapterCoordinator(st)
	oldLeader := &blockedDynamoDispatchCoordinator{
		occAdapterCoordinator: newOCCAdapterCoordinator(st),
		forward:               newLeader,
		firstEntered:          make(chan uint64, 1),
		firstRelease:          make(chan struct{}),
		secondEntered:         make(chan struct{}),
		secondRelease:         make(chan struct{}),
	}
	firstServer := NewDynamoDBServer(nil, st, oldLeader)
	secondServer := NewDynamoDBServer(nil, st, newLeader)
	first := appendListInput()
	first.ExpressionAttributeValues[":val"] = numberListAttr("2")
	updateDone := dynamoHandlerStatus(t, firstServer.updateItem, first)
	var firstCommitTS uint64
	select {
	case firstCommitTS = <-oldLeader.firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("old leader did not allocate the write timestamp")
	}
	require.NoError(t, st.PutAt(ctx, []byte("other"), []byte("x"), firstCommitTS+1, 0))
	newLeader.Clock().Observe(firstCommitTS + 1)
	oldLeader.switched.Store(true)
	second := transactWriteItemsInput{TransactItems: []transactWriteItem{{Update: &transactUpdateInput{
		TableName:                 first.TableName,
		Key:                       first.Key,
		UpdateExpression:          first.UpdateExpression,
		ExpressionAttributeNames:  first.ExpressionAttributeNames,
		ExpressionAttributeValues: map[string]attributeValue{":empty": {L: []attributeValue{}}, ":val": numberListAttr("3")},
	}}}}
	require.Equal(t, http.StatusOK, <-dynamoHandlerStatus(t, secondServer.transactWriteItems, second))
	close(oldLeader.firstRelease)
	require.Equal(t, http.StatusOK, <-updateDone)
	values := readListValues(t, secondServer, schema)
	require.Equal(t, 3, len(values))
	require.ElementsMatch(t, []string{"1", "2", "3"}, values)
}

func TestDynamoDBItemLockNormalizesKeyAndHonorsCancellation(t *testing.T) {
	st := store.NewMVCCStore()
	schema := dedupItemTable()
	seedDedupItem(t, st, schema, "1")
	server := NewDynamoDBServer(nil, st, newOCCAdapterCoordinator(st))
	key := map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)}
	unlock, err := server.lockItemForWrite(context.Background(), schema.TableName, key)
	require.NoError(t, err)
	defer unlock()
	keyWithExtra := map[string]attributeValue{
		"pk":    newStringAttributeValue(dedupTestKey),
		"extra": newStringAttributeValue("ignored"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		gotUnlock, lockErr := server.lockItemForWrite(ctx, schema.TableName, keyWithExtra)
		if lockErr == nil {
			gotUnlock()
		}
		done <- lockErr
	}()
	select {
	case err := <-done:
		t.Fatalf("same item bypassed its lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("canceled lock waiter did not return")
	}
}

func TestDynamoDBConditionCheckUsesItemLock(t *testing.T) {
	st := store.NewMVCCStore()
	schema := dedupItemTable()
	seedDedupItem(t, st, schema, "1")
	server := NewDynamoDBServer(nil, st, newOCCAdapterCoordinator(st))
	key := map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)}
	unlock, err := server.lockItemForWrite(context.Background(), schema.TableName, key)
	require.NoError(t, err)
	defer unlock()
	input := transactWriteItemsInput{TransactItems: []transactWriteItem{{ConditionCheck: &transactConditionInput{
		TableName:           schema.TableName,
		Key:                 key,
		ConditionExpression: "attribute_exists(pk)",
	}}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		gotUnlock, _, _, lockErr := server.lockTransactWriteItems(ctx, input)
		if lockErr == nil {
			gotUnlock()
		}
		done <- lockErr
	}()
	select {
	case err := <-done:
		t.Fatalf("ConditionCheck bypassed its lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("canceled ConditionCheck waiter did not return")
	}
}

func TestDynamoDBAdminPutUsesItemLock(t *testing.T) {
	st := store.NewMVCCStore()
	schema := dedupItemTable()
	seedDedupItem(t, st, schema, "1")
	server := NewDynamoDBServer(nil, st, newOCCAdapterCoordinator(st))
	key := map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)}
	unlock, err := server.lockItemForWrite(context.Background(), schema.TableName, key)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- server.AdminPutItem(context.Background(), fullAdminPrincipal, schema.TableName, AdminItem{
			Attributes: map[string]AdminAttributeValue{"pk": stringAttr(dedupTestKey)},
		})
	}()
	select {
	case err := <-done:
		unlock()
		t.Fatalf("admin write bypassed its lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("admin write did not finish after unlock")
	}
}

func (c *pendingDynamoApplyCoordinator) apply() error {
	if c.applyPending != nil {
		apply := c.applyPending
		c.applyPending = nil
		c.applyErr = apply()
	}
	return c.applyErr
}

func (c *pendingDynamoApplyCoordinator) LinearizableRead(context.Context) (uint64, error) {
	return 0, c.apply()
}

func (c *pendingDynamoApplyCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if err := c.apply(); err != nil {
		return nil, err
	}
	return c.occAdapterCoordinator.Dispatch(ctx, req)
}

func TestDynamoDBUpdateItemFencesBeforeSnapshotWatermark(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	schema := dedupItemTable()
	seedDedupItem(t, st, schema, "1")

	// A different key advances LastCommitTS while the hot-key entry is still
	// pending. Its older commit timestamp must not make the pending entry
	// invisible to the next transaction's OCC check.
	require.NoError(t, st.PutAt(ctx, []byte("other"), []byte("x"), 20, 0))
	itemKey, err := schema.itemKeyFromAttributes(map[string]attributeValue{
		"pk": newStringAttributeValue(dedupTestKey),
	})
	require.NoError(t, err)
	pendingValue, err := encodeStoredDynamoItem(map[string]attributeValue{
		"pk":  newStringAttributeValue(dedupTestKey),
		"val": numberListAttr("1", "2"),
	})
	require.NoError(t, err)
	coord := &pendingDynamoApplyCoordinator{
		occAdapterCoordinator: newOCCAdapterCoordinator(st),
		applyPending: func() error {
			return st.PutAt(ctx, itemKey, pendingValue, 15, 0)
		},
	}
	server := NewDynamoDBServer(nil, st, coord)

	_, err = server.updateItemWithRetry(ctx, appendListInput())
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "3"}, readListValues(t, server, schema),
		"a successful older append must not be overwritten by a later update")
}

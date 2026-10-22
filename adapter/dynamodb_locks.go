package adapter

import (
	"bytes"
	"context"
	"hash/fnv"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

var errDynamoSealGenerationChanged = errors.New("DynamoDB table generation changed while sealing item snapshot")

func (d *DynamoDBServer) lockItemUpdate(ctx context.Context, lockKey string) (func(), error) {
	idx := stripeIndex(lockKey, itemUpdateLockStripeCount)
	select {
	case <-ctx.Done():
		return nil, errors.WithStack(ctx.Err())
	case <-d.itemUpdateLocks[idx]:
		return func() { d.itemUpdateLocks[idx] <- struct{}{} }, nil
	}
}

// lockItemForWrite uses the same schema-derived identity for every single-item
// entry point, including HTTP, batch, and admin requests. The schema lookup
// uses a fenced timestamp because the ingress node may lag a remote shard.
func (d *DynamoDBServer) lockItemForWrite(ctx context.Context, tableName string, attrs map[string]attributeValue) (func(), error) {
	keyAttrs, _, _, err := d.itemWriteIdentity(ctx, tableName, attrs)
	if err != nil {
		return nil, err
	}
	lockKey, err := dynamoItemUpdateLockKey(tableName, keyAttrs)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return d.lockItemUpdate(ctx, lockKey)
}

func (d *DynamoDBServer) itemSealKey(ctx context.Context, tableName string, attrs map[string]attributeValue) ([]byte, uint64, error) {
	_, itemKey, generation, err := d.itemWriteIdentity(ctx, tableName, attrs)
	if err != nil {
		return nil, 0, err
	}
	return dynamoItemSnapshotSealKey(tableName, generation, itemKey), generation, nil
}

func (d *DynamoDBServer) itemWriteIdentity(ctx context.Context, tableName string, attrs map[string]attributeValue) (map[string]attributeValue, []byte, uint64, error) {
	readTimestamp, err := d.beginTxnReadTimestamp(ctx, "dynamodb item lock schema: begin read timestamp", dynamoTableMetaKey(tableName))
	if err != nil {
		return nil, nil, 0, err
	}
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTimestamp.Timestamp())
	if err != nil {
		return nil, nil, 0, err
	}
	if !exists {
		return nil, nil, 0, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	keyAttrs, err := primaryKeyAttributes(schema.PrimaryKey, attrs)
	if err != nil {
		return nil, nil, 0, err
	}
	itemKey, err := schema.itemKeyFromAttributes(keyAttrs)
	if err != nil {
		return nil, nil, 0, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return keyAttrs, itemKey, schema.Generation, nil
}

func (d *DynamoDBServer) verifySealedTableGeneration(ctx context.Context, tableName string, generation uint64, readTS uint64) error {
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
	if err != nil {
		return errors.WithStack(err)
	}
	if !exists || schema.Generation != generation {
		return errDynamoSealGenerationChanged
	}
	return nil
}

// lockTransactWriteItems holds every item's stripe from snapshot selection
// through commit. In particular, a one-phase UpdateItem can have an allocated
// commit timestamp but no Raft entry yet; a read fence cannot see that write.
// Sharing these locks prevents a TransactWriteItems plan from reading past it.
func (d *DynamoDBServer) lockTransactWriteItems(ctx context.Context, in transactWriteItemsInput) (func(), [][]byte, map[string]uint64, error) {
	tableNames, err := collectTransactWriteTableNames(in)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, name := range tableNames {
		if err := d.ensureLegacyTableMigration(ctx, name); err != nil {
			return nil, nil, nil, err
		}
	}
	idxs, sealKeys, generations, err := d.transactWriteLockIndexes(ctx, in, tableNames)
	if err != nil {
		return nil, nil, nil, err
	}
	sort.Ints(idxs)
	locked := 0
	for _, idx := range idxs {
		select {
		case <-ctx.Done():
			for i := locked - 1; i >= 0; i-- {
				d.itemUpdateLocks[idxs[i]] <- struct{}{}
			}
			return nil, nil, nil, errors.WithStack(ctx.Err())
		case <-d.itemUpdateLocks[idx]:
			locked++
		}
	}
	return func() {
		for i := len(idxs) - 1; i >= 0; i-- {
			d.itemUpdateLocks[idxs[i]] <- struct{}{}
		}
	}, sealKeys, generations, nil
}

func (d *DynamoDBServer) transactWriteLockIndexes(ctx context.Context, in transactWriteItemsInput, tableNames []string) ([]int, [][]byte, map[string]uint64, error) {
	idxs := make([]int, 0, len(in.TransactItems))
	sealKeys := make([][]byte, 0, len(in.TransactItems))
	seen := make(map[int]struct{}, len(in.TransactItems))
	schemas := make(map[string]*dynamoTableSchema, len(tableNames))
	readTimestamp, err := d.beginTxnReadTimestamp(ctx, "dynamodb transaction lock schema: begin read timestamp", tableMetadataKeys(tableNames)...)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, item := range in.TransactItems {
		name, err := transactWriteItemTableName(item)
		if err != nil {
			return nil, nil, nil, err
		}
		schema, err := d.transactWriteLockSchema(ctx, name, readTimestamp.Timestamp(), schemas)
		if err != nil {
			return nil, nil, nil, err
		}
		keyAttrs, err := transactWriteItemPrimaryKeyAttributes(schema, item)
		if err != nil {
			return nil, nil, nil, err
		}
		lockKey, err := dynamoItemUpdateLockKey(name, keyAttrs)
		if err != nil {
			return nil, nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
		}
		itemKey, err := schema.itemKeyFromAttributes(keyAttrs)
		if err != nil {
			return nil, nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
		}
		sealKeys = append(sealKeys, dynamoItemSnapshotSealKey(name, schema.Generation, itemKey))
		idx := stripeIndex(lockKey, itemUpdateLockStripeCount)
		if _, ok := seen[idx]; !ok {
			seen[idx] = struct{}{}
			idxs = append(idxs, idx)
		}
	}
	generations := make(map[string]uint64, len(schemas))
	for name, schema := range schemas {
		generations[name] = schema.Generation
	}
	return idxs, sealKeys, generations, nil
}

func tableMetadataKeys(tableNames []string) [][]byte {
	keys := make([][]byte, 0, len(tableNames))
	for _, name := range tableNames {
		keys = append(keys, dynamoTableMetaKey(name))
	}
	return keys
}

func (d *DynamoDBServer) transactWriteLockSchema(ctx context.Context, name string, readTS uint64, schemas map[string]*dynamoTableSchema) (*dynamoTableSchema, error) {
	if schema := schemas[name]; schema != nil {
		return schema, nil
	}
	schema, exists, err := d.loadTableSchemaAt(ctx, name, readTS)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	schemas[name] = schema
	return schema, nil
}

func (d *DynamoDBServer) lockTableOperations(tableNames []string) func() {
	if len(tableNames) == 0 {
		return func() {}
	}
	idxs := make([]int, 0, len(tableNames))
	seen := map[int]struct{}{}
	for _, tableName := range tableNames {
		idx := stripeIndex(tableName, tableLockStripeCount)
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	for _, idx := range idxs {
		d.tableLocks[idx].Lock()
	}
	return func() {
		for i := len(idxs) - 1; i >= 0; i-- {
			d.tableLocks[idxs[i]].Unlock()
		}
	}
}

func stripeIndex(key string, stripeCount uint32) int {
	if stripeCount == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % stripeCount)
}

func dynamoItemUpdateLockKey(tableName string, key map[string]attributeValue) (string, error) {
	parts := make([]string, 0, len(key))
	for name := range key {
		parts = append(parts, name)
	}
	sort.Strings(parts)
	var b strings.Builder
	b.WriteString(tableName)
	b.WriteByte('|')
	for _, name := range parts {
		val, err := attributeValueAsKey(key[name])
		if err != nil {
			return "", errors.WithStack(err)
		}
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(val)
		b.WriteByte('|')
	}
	return b.String(), nil
}

// globalLastCommitTSProvider is the interface satisfied by stores (e.g.
// LeaderRoutedStore) that can proxy the leader's LastCommitTS.  Defined as a
// local interface so DynamoDBServer avoids a hard dependency on the concrete
// kv type.
type globalLastCommitTSProvider interface {
	GlobalLastCommitTS(ctx context.Context) uint64
}

type committedTimestampFloorProvider interface {
	GlobalCommittedTimestampFloor(ctx context.Context) (uint64, error)
}

type keyedCommittedTimestampFloorProvider interface {
	CommittedTimestampFloorForKeys(ctx context.Context, keys [][]byte) (uint64, error)
}

func (d *DynamoDBServer) nextTxnReadTS() uint64 {
	// On a follower the local store.LastCommitTS() may lag behind the leader.
	// Use GlobalLastCommitTS so ConsistentRead snapshots and transaction
	// start timestamps are aligned with the leader's committed watermark,
	// preventing stale pre-reads that cause false Jepsen anomalies and
	// unnecessary WriteConflict retries on every follower request.
	maxTS := uint64(0)
	if p, ok := d.store.(globalLastCommitTSProvider); ok {
		maxTS = p.GlobalLastCommitTS(context.Background())
	} else if d.store != nil {
		maxTS = d.store.LastCommitTS()
	}

	// Advance the HLC so subsequent commitTS calls produce values > maxTS,
	// but return maxTS directly as the snapshot — NOT clock.Next().
	//
	// clock.Next() can be ahead of store.LastCommitTS() because concurrent
	// dispatchTxn calls advance the HLC before their Raft entry is applied.
	// If readTS = clock.Next() = T and a concurrent write obtained
	// commitTS = T-1 (still in the Raft pipeline), the version at T-1 is
	// not yet in Pebble.  Reads would see stale data and the FSM conflict
	// check (latestTS > startTS: T-1 > T → false) would silently pass,
	// allowing corrupted writes.  Returning maxTS closes this gap: every
	// version at ≤ maxTS is guaranteed visible, and any concurrent write at
	// > maxTS triggers a WriteConflict and a retry.
	clock := d.coordinator.Clock()
	if clock != nil && maxTS > 0 {
		clock.Observe(maxTS)
	}
	if maxTS == 0 {
		return 1
	}
	return maxTS
}

func (d *DynamoDBServer) beginTxnReadTimestamp(ctx context.Context, label string, keys ...[]byte) (kv.ReadTimestamp, error) {
	// LastCommitTS is a maximum, not an applied-log barrier: a Raft entry
	// allocated an older commit timestamp can still be pending after another
	// entry has advanced the watermark. Fence each touched group before sampling it,
	// or OCC may accept a write built without that pending entry and overwrite
	// its successful update.
	var (
		fencedTS uint64
		err      error
	)
	if floor, ok := d.store.(keyedCommittedTimestampFloorProvider); ok && len(keys) > 0 {
		fencedTS, err = floor.CommittedTimestampFloorForKeys(ctx, keys)
	} else if floor, ok := d.store.(committedTimestampFloorProvider); ok {
		fencedTS, err = floor.GlobalCommittedTimestampFloor(ctx)
	} else {
		// Bare test stores have no Raft-aware floor. Keep their coordinator
		// boundary strict too, rather than letting a lease fast path mask a
		// pending committed entry.
		_, err = d.coordinator.LinearizableRead(ctx)
		if err == nil {
			fencedTS = d.nextTxnReadTS()
		}
	}
	if err != nil {
		return kv.ReadTimestamp{}, errors.Wrap(err, label+": fence read snapshot")
	}
	if fencedTS == 0 {
		fencedTS = 1
	}
	if clock := d.coordinator.Clock(); clock != nil {
		clock.Observe(fencedTS)
	}
	readTimestamp, err := kv.BeginReadTimestampThrough(ctx, d.coordinator, fencedTS, label)
	return readTimestamp, errors.WithStack(err)
}

// beginTxnWriteTimestamp places a seal for each affected item in its Raft
// group before planning from the returned snapshot. Every item write reads
// that item's seal key at commit, so a write allocated but not proposed
// before this snapshot conflicts if it reaches Raft after the seal.
func (d *DynamoDBServer) beginTxnWriteTimestamp(ctx context.Context, label string, sealKeys [][]byte) (kv.ReadTimestamp, error) {
	if len(sealKeys) == 0 {
		return d.beginTxnReadTimestamp(ctx, label)
	}
	groups := snapshotSealGroups(sealKeys)
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTimestamp, err := d.tryTxnWriteTimestamp(ctx, label, sealKeys, groups)
		if err == nil {
			return readTimestamp, nil
		}
		if !isRetryableTransactWriteError(err) {
			return kv.ReadTimestamp{}, errors.WithStack(err)
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return kv.ReadTimestamp{}, errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return kv.ReadTimestamp{}, newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "snapshot seal retry attempts exhausted")
}

func snapshotSealGroups(sealKeys [][]byte) [][]*kv.Elem[kv.OP] {
	keys := append([][]byte(nil), sealKeys...)
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	byRoute := make(map[string][]*kv.Elem[kv.OP], len(keys))
	for i, key := range keys {
		if i > 0 && bytes.Equal(key, keys[i-1]) {
			continue
		}
		route := string(kv.RouteKey(key))
		byRoute[route] = append(byRoute[route], &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: []byte{1}})
	}
	routes := make([]string, 0, len(byRoute))
	for route := range byRoute {
		routes = append(routes, route)
	}
	sort.Strings(routes)
	groups := make([][]*kv.Elem[kv.OP], 0, len(routes))
	for _, route := range routes {
		groups = append(groups, byRoute[route])
	}
	return groups
}

func (d *DynamoDBServer) tryTxnWriteTimestamp(ctx context.Context, label string, sealKeys [][]byte, groups [][]*kv.Elem[kv.OP]) (kv.ReadTimestamp, error) {
	floor, err := d.beginTxnReadTimestamp(ctx, label+": before seal", sealKeys...)
	if err != nil {
		return kv.ReadTimestamp{}, err
	}
	var sealTS uint64
	for _, elems := range groups {
		request := &kv.OperationGroup[kv.OP]{IsTxn: true, StartTS: floor.Timestamp(), CommitTS: sealTS, Elems: elems}
		var resp *kv.CoordinateResponse
		if sealTS == 0 {
			resp, err = kv.DispatchWithLeaderIssuedCommitTS(floor.WithDispatchVoucher(ctx), d.coordinator, request)
			err = normalizeDynamoWriteDispatchError(err)
		} else {
			resp, err = d.dispatchWriteWithReadTimestamp(floor.WithDispatchVoucher(ctx), request)
		}
		if err != nil {
			return kv.ReadTimestamp{}, errors.WithStack(err)
		}
		if resp == nil || resp.CommitTS <= floor.Timestamp() || (sealTS != 0 && resp.CommitTS != sealTS) {
			return kv.ReadTimestamp{}, errors.New("DynamoDB snapshot seal response has no matching commit timestamp")
		}
		sealTS = resp.CommitTS
	}
	readTimestamp, err := kv.BeginReadTimestampThrough(ctx, d.coordinator, sealTS, label+": sealed snapshot")
	return readTimestamp, errors.WithStack(err)
}

func (d *DynamoDBServer) pinReadTS(ts uint64) *kv.ActiveTimestampToken {
	if d == nil || d.readTracker == nil {
		return &kv.ActiveTimestampToken{}
	}
	return d.readTracker.Pin(ts)
}

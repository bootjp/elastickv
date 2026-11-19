package adapter

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

type conditionOnlyTrace struct {
	kv.CoordinatorForwarder
	seals      []uint64
	afterSeal  func(uint64)
	afterFinal func(error)
	final      []*kv.OperationGroup[kv.OP]
}

func (c *conditionOnlyTrace) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := c.Inner.Dispatch(ctx, req)
	if !isDynamoSnapshotSealRequest(req) {
		c.final = append(c.final, req)
		if c.afterFinal != nil {
			c.afterFinal(err)
		}
	} else if err == nil {
		c.seals = append(c.seals, resp.CommitTS)
		if c.afterSeal != nil {
			c.afterSeal(resp.CommitTS)
		}
	}
	return resp, err
}

func runConditionTransaction(t *testing.T, d *DynamoDBServer, items ...transactWriteItem) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(transactWriteItemsInput{TransactItems: items})
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", transactWriteItemsTarget)
	rec := httptest.NewRecorder()
	d.handle(rec, req)
	return rec
}

func tableCondition(table, key, expr string) transactWriteItem {
	return transactWriteItem{ConditionCheck: &transactConditionInput{
		TableName: table, Key: map[string]attributeValue{"pk": newStringAttributeValue(key)},
		ConditionExpression: expr,
	}}
}

type conditionRaftPhases struct {
	kv.Transactional
	phases []pb.Phase
}

func (g *conditionRaftPhases) Commit(ctx context.Context, reqs []*pb.Request) (*kv.TransactionResponse, error) {
	result, err := g.Transactional.Commit(ctx, reqs)
	if err == nil {
		for _, req := range reqs {
			g.phases = append(g.phases, req.Phase)
		}
	}
	return result, err
}

func TestDynamoDBConditionOnlyRaftProposals(t *testing.T) {
	ctx := t.Context()
	routes := distribution.NewEngine()
	split := kv.RouteKey(dynamoTableMetaKey("beta"))
	routes.UpdateRoute(nil, split, 1)
	routes.UpdateRoute(split, nil, 2)
	hlc := kv.NewHLC()
	groups := make(map[uint64]*kv.ShardGroup)
	var entries []*conditionRaftPhases
	var engines []raftengine.Engine
	var keys [][]byte
	var checks []transactWriteItem
	for i, name := range []string{"alpha", "beta"} {
		schema := dedupItemTable()
		schema.TableName = name
		check := tableCondition(name, dedupTestKey, "attribute_exists(pk)")
		checks = append(checks, check)
		key, err := schema.itemKeyFromAttributes(check.ConditionCheck.Key)
		require.NoError(t, err)
		keys = append(keys, key)
		st := store.NewMVCCStore()
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		engine, _ := newSingleRaftForDistributionE2E(t, name, kv.NewKvFSMWithHLC(st, hlc))
		engines = append(engines, engine)
		log := &conditionRaftPhases{Transactional: kv.NewLeaderProxyWithEngine(engine)}
		entries = append(entries, log)
		groups[uint64(i+1)] = &kv.ShardGroup{Store: st, Engine: engine, Txn: log}
	}
	shards := kv.NewShardStore(routes, groups)
	coord := kv.NewShardedCoordinator(routes, groups, 1, hlc, shards)
	trace := &conditionOnlyTrace{CoordinatorForwarder: kv.CoordinatorForwarder{Inner: coord}}
	server := NewDynamoDBServer(nil, shards, trace)
	for i, check := range checks {
		schema := dedupItemTable()
		schema.TableName = check.ConditionCheck.TableName
		sealRaftSeed(t, ctx, server, schema)
		gid, err := coord.ResolveOperationGroupForKey(keys[i])
		require.NoError(t, err)
		require.EqualValues(t, i+1, gid)
	}
	versions := func() []uint64 {
		var versions []uint64
		for i, key := range keys {
			ts, ok, err := groups[uint64(i+1)].Store.LatestCommitTS(ctx, key)
			require.NoError(t, err)
			require.True(t, ok)
			versions = append(versions, ts)
		}
		return versions
	}
	before := versions()
	var indices []uint64
	for i, engine := range engines {
		entries[i].phases = nil
		indices = append(indices, engine.Status().AppliedIndex)
	}
	trace.seals, trace.final = nil, nil
	rec := runConditionTransaction(t, server, checks...)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var deltas []uint64
	for i, engine := range engines {
		deltas = append(deltas, engine.Status().AppliedIndex-indices[i])
	}
	t.Logf("condition-only: applied Raft entries %v, phases %v and %v; seal dispatches %d; final dispatches %d",
		deltas, entries[0].phases, entries[1].phases, len(trace.seals), len(trace.final))
	require.Len(t, trace.seals, 2)
	require.Equal(t, trace.seals[0], trace.seals[1])
	for i, key := range keys {
		seal := dynamoItemSnapshotSealKey(checks[i].ConditionCheck.TableName, 1, key)
		ts, ok, err := groups[uint64(i+1)].Store.LatestCommitTS(ctx, seal)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, trace.seals[0], ts)
	}
	require.Equal(t, []uint64{1, 1}, deltas)
	require.Empty(t, trace.final)
	for _, group := range entries {
		require.Equal(t, []pb.Phase{pb.Phase_NONE}, group.phases)
	}
	require.Equal(t, before, versions())

	writer := NewDynamoDBServer(nil, shards, coord)
	checkHistoricalConditions(t, server, writer, trace, checks, versions)
	checkMixedAndInvalidConditions(t, server, trace, checks, keys[0])
	checkConditionCatalogChanges(t, server, trace, routes, groups[2].Store, checks, versions)
}

func checkHistoricalConditions(t *testing.T, server, writer *DynamoDBServer, trace *conditionOnlyTrace, checks []transactWriteItem, versions func() []uint64) {
	t.Helper()
	// A separate frontend commits new values on BOTH routes after the last
	// seal. Taking a latest snapshot for either predicate would cancel.
	ctx := t.Context()
	trace.seals = nil
	trace.afterSeal = func(uint64) {
		if len(trace.seals) != 2 {
			return
		}
		for _, check := range checks {
			_, err := writer.updateItemWithRetry(ctx, sealRaftAppend(check.ConditionCheck.TableName, "2"))
			require.NoError(t, err)
		}
	}
	var historical []transactWriteItem
	for _, check := range checks {
		old := tableCondition(check.ConditionCheck.TableName, dedupTestKey, "#v = :old")
		old.ConditionCheck.ExpressionAttributeNames = map[string]string{"#v": "val"}
		old.ConditionCheck.ExpressionAttributeValues = map[string]attributeValue{":old": numberListAttr("1")}
		historical = append(historical, old)
	}
	rec := runConditionTransaction(t, server, historical...)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, trace.seals, 2)
	require.Empty(t, trace.final)
	for _, ts := range versions() {
		require.Greater(t, ts, trace.seals[0])
	}
	trace.afterSeal = nil
	rec = runConditionTransaction(t, server, historical...)
	require.Equal(t, dynamoErrTransactionCanceled, rec.Header().Get("x-amzn-ErrorType"))
}

func checkMixedAndInvalidConditions(t *testing.T, server *DynamoDBServer, trace *conditionOnlyTrace, checks []transactWriteItem, key []byte) {
	t.Helper()
	for _, write := range []transactWriteItem{
		{Delete: &transactDeleteInput{TableName: "beta", Key: map[string]attributeValue{"pk": newStringAttributeValue("absent")}}},
		sealRaftTxnUpdate("beta"),
	} {
		trace.final = nil
		rec := runConditionTransaction(t, server, checks[0], write)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Len(t, trace.final, 1)
		guard := trace.final[0].Elems[0]
		require.Equal(t, kv.Put, guard.Op)
		require.Equal(t, key, guard.Key)
		require.Contains(t, trace.final[0].ReadKeys, key)
		require.Contains(t, trace.final[0].ReadKeys, dynamoItemSnapshotSealKey("alpha", 1, key))
	}

	for _, tc := range []struct {
		items []transactWriteItem
		code  string
	}{
		{[]transactWriteItem{checks[0], tableCondition("beta", dedupTestKey, "attribute_not_exists(pk)")}, dynamoErrTransactionCanceled},
		{[]transactWriteItem{checks[0], checks[0]}, dynamoErrValidation},
		{[]transactWriteItem{tableCondition("alpha", dedupTestKey, "")}, dynamoErrValidation},
	} {
		trace.final = nil
		rec := runConditionTransaction(t, server, tc.items...)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Equal(t, tc.code, rec.Header().Get("x-amzn-ErrorType"))
		require.Empty(t, trace.final)
	}
}

func checkConditionCatalogChanges(t *testing.T, server *DynamoDBServer, trace *conditionOnlyTrace, routes *distribution.Engine, beta store.MVCCStore, checks []transactWriteItem, versions func() []uint64) {
	t.Helper()
	// A catalog publication between sealing and checking invalidates the
	// fast path even if this publication keeps both owners in place.
	trace.seals, trace.final = nil, nil
	trace.afterSeal = func(uint64) {
		if len(trace.seals) == 2 {
			snapshot := routes.AppliedCatalogSnapshot()
			snapshot.Version++
			for i := range snapshot.Routes {
				snapshot.Routes[i].RouteID = uint64(i + 1)
			}
			require.NoError(t, routes.ApplySnapshot(snapshot))
		}
	}
	rec := runConditionTransaction(t, server, checks...)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, trace.final, 1)
	require.Len(t, trace.final[0].Elems, 2)
	trace.afterSeal = nil
	trace.final = nil

	// Recreate beta after the cut. Its old item satisfies the predicate at R,
	// but the post-evaluation generation check must force a fresh attempt.
	before := versions()
	trace.seals = nil
	trace.afterSeal = func(ts uint64) {
		if len(trace.seals) != 2 {
			return
		}
		recreated := dedupItemTable()
		recreated.TableName, recreated.Generation = "beta", 2
		seed := newDynamoFixtureWriter(t, beta)
		seed.nextTS = ts + 1
		seed.writeSchema(recreated)
	}
	rec = runConditionTransaction(t, server, checks...)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, dynamoErrTransactionCanceled, rec.Header().Get("x-amzn-ErrorType"))
	require.Greater(t, len(trace.seals), 2)
	require.Empty(t, trace.final, "no generation cleanup for elided guards")
	require.Equal(t, before, versions())
}

func conditionTestShardStore(st store.MVCCStore) *kv.ShardStore {
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	return kv.NewShardStore(routes, map[uint64]*kv.ShardGroup{1: {Store: st}})
}

func TestDynamoDBConditionOnlyPreparedBeforeSeal(t *testing.T) {
	ctx := t.Context()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	server, engine, _ := sealRaftRestartableServer(t, t.TempDir(), st)
	schema := dedupItemTable()
	sealRaftSeed(t, ctx, server, schema)
	check := tableCondition(schema.TableName, dedupTestKey, "attribute_exists(pk)")
	key, err := schema.itemKeyFromAttributes(check.ConditionCheck.Key)
	require.NoError(t, err)
	other := tableCondition(schema.TableName, "other", "attribute_exists(pk)")
	_, err = server.putItemWithRetry(ctx, putItemInput{TableName: schema.TableName, Item: other.ConditionCheck.Key})
	require.NoError(t, err)
	otherKey, err := schema.itemKeyFromAttributes(other.ConditionCheck.Key)
	require.NoError(t, err)
	start := st.LastCommitTS()
	meta := kv.TxnMeta{PrimaryKey: key, LockTTLms: 60000}
	txn := kv.NewTransactionWithProposer(engine)
	phase := func(phase pb.Phase) {
		request := &pb.Request{IsTxn: true, Phase: phase, Ts: start, Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(meta)},
			{Op: pb.Op_DEL, Key: key},
		}}
		_, err := txn.Commit(ctx, []*pb.Request{request})
		require.NoError(t, err)
	}
	phase(pb.Phase_PREPARE)
	trace := &conditionOnlyTrace{CoordinatorForwarder: kv.CoordinatorForwarder{Inner: server.coordinator}}
	trace.afterFinal = func(err error) {
		// The local MVCC backend still exposes the pre-PREPARE value. Its
		// synthetic guard must conflict before we let the delete resolve.
		require.ErrorIs(t, err, kv.ErrTxnLocked)
		meta.CommitTS = start + 1
		require.Greater(t, trace.seals[0], meta.CommitTS)
		phase(pb.Phase_COMMIT)
	}
	shards := conditionTestShardStore(st)
	trace.afterSeal = func(ts uint64) {
		seal := dynamoItemSnapshotSealKey(schema.TableName, schema.Generation, key)
		if len(trace.final) == 0 {
			require.False(t, shards.CanReadSealedItemAt(ctx, key, seal, ts), "pending intent must prevent elision")
		}
	}
	reader := NewDynamoDBServer(nil, st, trace)
	rec := runConditionTransaction(t, reader, other, check)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Equal(t, dynamoErrTransactionCanceled, rec.Header().Get("x-amzn-ErrorType"))
	require.Len(t, trace.final, 1)
	require.Len(t, trace.final[0].Elems, 2)
	require.Equal(t, otherKey, trace.final[0].Elems[0].Key)
	require.Equal(t, key, trace.final[0].Elems[1].Key)
}

func TestDynamoDBConditionOnlyLegacyMigration(t *testing.T) {
	schema, server, st := newLegacyMigrationTestServer(t, false, "N")
	seed := newDynamoFixtureWriter(t, st)
	seed.writeSchema(schema)
	check := tableCondition(schema.TableName, "tenant", "attribute_exists(pk)")
	check.ConditionCheck.Key["sk"] = newNumberAttributeValue("2")
	seed.writeItem(schema, check.ConditionCheck.Key)
	rec := runConditionTransaction(t, server, check)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	migrated, found, err := server.loadTableSchema(t.Context(), schema.TableName)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 2, migrated.Generation)
	require.False(t, migrated.needsLegacyKeyMigration())
	key, err := migrated.itemKeyFromAttributes(check.ConditionCheck.Key)
	require.NoError(t, err)
	version, found, err := st.LatestCommitTS(t.Context(), key)
	require.NoError(t, err)
	require.True(t, found)
	seal := dynamoItemSnapshotSealKey(migrated.TableName, migrated.Generation, key)
	cut, found, err := st.LatestCommitTS(t.Context(), seal)
	require.NoError(t, err)
	require.True(t, found)
	require.Greater(t, version, cut, "a raw store retains its same-value guard after migration")
}

// A concurrent table lifecycle change can land after the migration preflight
// and before the lock identities are collected. Record the source read at R.
type conditionMigrationStore struct {
	store.MVCCStore
	meta, source   []byte
	afterPreflight func()
	sourceReads    []uint64
}

func (s *conditionMigrationStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	value, err := s.MVCCStore.GetAt(ctx, key, ts)
	if bytes.Equal(key, s.meta) && s.afterPreflight != nil {
		s.afterPreflight()
		s.afterPreflight = nil
	}
	if bytes.Equal(key, s.source) {
		s.sourceReads = append(s.sourceReads, ts)
	}
	return value, err
}

func TestDynamoDBConditionOnlyMigrationSourceGuard(t *testing.T) {
	raw := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, raw.Close()) })
	stable := dedupItemTable()
	seed := newDynamoFixtureWriter(t, raw)
	seed.writeSchema(stable)
	target := *stable
	target.Generation, target.MigratingFromGeneration = 3, 2
	source := target.migrationSourceSchema()
	check := tableCondition(stable.TableName, dedupTestKey, "attribute_exists(pk)")
	sourceKey, err := source.itemKeyFromAttributes(check.ConditionCheck.Key)
	require.NoError(t, err)
	st := &conditionMigrationStore{
		MVCCStore: raw, meta: dynamoTableMetaKey(stable.TableName), source: sourceKey,
		afterPreflight: func() {
			seed.writeItem(source, check.ConditionCheck.Key)
			seed.writeSchema(&target)
		},
	}
	trace := &conditionOnlyTrace{CoordinatorForwarder: kv.CoordinatorForwarder{Inner: newOCCAdapterCoordinator(raw)}}
	rec := runConditionTransaction(t, NewDynamoDBServer(nil, st, trace), check)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, trace.seals, 1)
	require.Equal(t, trace.seals, st.sourceReads)
	require.Len(t, trace.final, 1)
	require.Equal(t, trace.seals[0], trace.final[0].StartTS)
	require.Len(t, trace.final[0].Elems, 1)
	require.Equal(t, sourceKey, trace.final[0].Elems[0].Key)
}

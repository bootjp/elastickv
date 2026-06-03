package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// These tests drive the option-2 one-phase idempotency dedup on the DynamoDB
// single-item write path (UpdateItem list_append), reusing the OCC-aware
// dedupTestCoordinator from redis_list_dedup_test.go. Because the OCC layer is
// real (store.ApplyMutations against StartTS) and maybeProbe mimics the FSM's
// exact-ts dedup probe on kv.PrimaryKeyForElems, a passing dedup test proves the
// probe is load-bearing: without it a reuse would conflict against attempt 1's
// own version, recompute the list, and duplicate.
// See docs/design/2026_06_03_proposed_dynamodb_onephase_dedup.md.

const dedupTestKey = "k"

func numberListAttr(ns ...string) attributeValue {
	l := make([]attributeValue, 0, len(ns))
	for _, n := range ns {
		v := n
		l = append(l, attributeValue{N: &v})
	}
	return attributeValue{L: l}
}

// dedupItemTable is the GSI-free, hash-key-only table the Jepsen
// list-append workload uses (jepsen_append: pk HASH only). With no GSI the
// item write is a single Put(itemKey, …), so kv.PrimaryKeyForElems == itemKey.
func dedupItemTable() *dynamoTableSchema {
	return &dynamoTableSchema{
		TableName:            "jepsen_append",
		Generation:           1,
		KeyEncodingVersion:   dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{"pk": "S"},
		PrimaryKey:           dynamoKeySchema{HashKey: "pk"},
	}
}

func appendListInput() updateItemInput {
	const value = "3" // the unique per-key append value the assertions expect
	return updateItemInput{
		TableName:                "jepsen_append",
		Key:                      map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)},
		UpdateExpression:         "SET #v = list_append(if_not_exists(#v, :empty), :val)",
		ExpressionAttributeNames: map[string]string{"#v": "val"},
		ExpressionAttributeValues: map[string]attributeValue{
			":empty": {L: []attributeValue{}},
			":val":   numberListAttr(value),
		},
	}
}

func readListValues(t *testing.T, server *DynamoDBServer, schema *dynamoTableSchema) []string {
	t.Helper()
	loc, found, err := server.readLogicalItemAt(
		context.Background(),
		schema,
		map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)},
		consistentReadLatestTS,
	)
	require.NoError(t, err)
	require.True(t, found, "item must exist")
	out := make([]string, 0, len(loc.item["val"].L))
	for _, e := range loc.item["val"].L {
		out = append(out, e.numberValue())
	}
	return out
}

func newDedupItemWriteServer(st store.MVCCStore, coord kv.Coordinator, dedup bool) (*dynamoTableSchema, *DynamoDBServer) {
	var opts []DynamoDBServerOption
	if dedup {
		opts = append(opts, WithDynamoOnePhaseTxnDedup(true))
	}
	server := NewDynamoDBServer(nil, st, coord, opts...)
	return dedupItemTable(), server
}

func seedDedupItem(t *testing.T, st store.MVCCStore, schema *dynamoTableSchema, values ...string) {
	t.Helper()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(schema)
	writer.writeItem(schema, map[string]attributeValue{
		"pk":  newStringAttributeValue(dedupTestKey),
		"val": numberListAttr(values...),
	})
}

// TestItemWriteDedup_LandedPriorAttempt_NoDuplicate is the headline: attempt 1
// commits the list_append but returns an ambiguous WriteConflict (leadership
// churn); the retry reuses the same write set with prev_commit_ts, the probe
// finds the landed version at exactly that ts and no-ops, and the stored list
// has exactly ONE copy of the appended element. Without the probe, the reuse
// would OCC-conflict against attempt 1's own version, recompute, and duplicate
// — the :duplicate-elements anomaly from Jepsen run 26856696842.
func TestItemWriteDedup_LandedPriorAttempt_NoDuplicate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, true) // dispatch 1 lands then errors
	schema, server := newDedupItemWriteServer(st, coord, true)
	seedDedupItem(t, st, schema, "1", "2")

	plan, err := server.updateItemWithRetry(ctx, appendListInput())
	require.NoError(t, err)
	require.NotNil(t, plan)

	require.Equal(t, []string{"1", "2", "3"}, readListValues(t, server, schema),
		"only attempt 1's apply lands; the reuse dedups via the exact-ts probe — no duplicate")
	require.Equal(t, 2, coord.dispatches, "one ambiguous-land attempt + one reuse")
	require.Equal(t, 1, coord.probeNoOps, "the reuse must dedup via the FSM exact-ts probe")
}

// TestItemWriteDedup_PriorAttemptDidNotLand_Applies: attempt 1 pre-rejects
// (definitely did not commit); the reuse's probe misses, so it applies the
// reused write set at a fresh commit_ts. One element, no duplicate.
func TestItemWriteDedup_PriorAttemptDidNotLand_Applies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // dispatch 1 pre-rejects, nothing applied
	schema, server := newDedupItemWriteServer(st, coord, true)
	seedDedupItem(t, st, schema, "1", "2")

	plan, err := server.updateItemWithRetry(ctx, appendListInput())
	require.NoError(t, err)
	require.NotNil(t, plan)

	require.Equal(t, []string{"1", "2", "3"}, readListValues(t, server, schema))
	require.Equal(t, 2, coord.dispatches, "attempt 1 pre-rejects + reuse applies")
	require.Equal(t, 0, coord.probeNoOps, "nothing landed, so the probe must miss and the reuse applies")
}

// TestItemWriteDedup_SelfInflictedReuseConflict_ReturnsSuccess: attempt 1
// pre-rejects, the reuse then LANDS but surfaces WriteConflict (self-inflicted
// conflict under churn). The adapter-side self-conflict guard probes the reuse's
// own commit_ts, finds it landed, and returns the cached result — no recompute,
// no double-apply.
func TestItemWriteDedup_SelfInflictedReuseConflict_ReturnsSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 pre-rejects (no land)
	coord.landThenWriteConflictAtDispatch = 2      // the reuse lands then errors
	schema, server := newDedupItemWriteServer(st, coord, true)
	seedDedupItem(t, st, schema, "1", "2")

	plan, err := server.updateItemWithRetry(ctx, appendListInput())
	require.NoError(t, err)
	require.NotNil(t, plan)

	require.Equal(t, []string{"1", "2", "3"}, readListValues(t, server, schema),
		"the reuse applied exactly once; the self-conflict guard hid the ambiguous error")
	require.Equal(t, 2, coord.dispatches, "no recompute should fire; the reuse landed")
	require.Equal(t, 0, coord.probeNoOps, "the FSM probe at the prior ts misses; the self-conflict guard is adapter-side")
}

// TestItemWriteDedup_GenuineConflictRecomputes: attempt 1 pre-rejects; before
// the reuse a CONCURRENT write advances the item past the reused snapshot, so
// the reuse genuinely OCC-conflicts and the self-conflict probe misses (our
// attempt never landed). The adapter drops the reuse and recomputes from a
// fresh read, appending exactly once on top of the concurrent value: no
// duplicate, no lost update.
func TestItemWriteDedup_GenuineConflictRecomputes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, false) // attempt 1 pre-rejects
	schema, server := newDedupItemWriteServer(st, coord, true)
	seedDedupItem(t, st, schema, "1", "2")

	itemKey, err := schema.itemKeyFromAttributes(map[string]attributeValue{"pk": newStringAttributeValue(dedupTestKey)})
	require.NoError(t, err)
	coord.beforeDispatch = func(n int) {
		if n != 2 {
			return
		}
		// A concurrent txn appends 99 at a commit_ts newer than the reused
		// snapshot, so the reuse conflicts and must recompute.
		body, encErr := encodeStoredDynamoItem(map[string]attributeValue{
			"pk":  newStringAttributeValue(dedupTestKey),
			"val": numberListAttr("1", "2", "99"),
		})
		require.NoError(t, encErr)
		require.NoError(t, st.PutAt(ctx, itemKey, body, 100, 0))
	}

	plan, err := server.updateItemWithRetry(ctx, appendListInput())
	require.NoError(t, err)
	require.NotNil(t, plan)

	require.Equal(t, []string{"1", "2", "99", "3"}, readListValues(t, server, schema),
		"genuine conflict recomputes on top of the concurrent value; element 3 appears once")
	require.Equal(t, 3, coord.dispatches, "attempt 1 pre-reject + conflicting reuse + recompute")
	require.Equal(t, 0, coord.probeNoOps, "neither the prior attempt nor the reuse landed; the probe must miss")
}

// TestItemWriteDedup_DisabledKeepsLegacyPath pins that the gate is load-bearing:
// with onePhaseTxnDedup OFF (the default), the legacy retry RE-READS and
// recomputes, so a landed-then-ambiguous attempt 1 is double-applied — the
// :duplicate-elements bug. This characterizes the pre-fix behavior the gate
// closes; flipping the gate on (the headline test) eliminates the duplicate.
func TestItemWriteDedup_DisabledKeepsLegacyPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newDedupTestCoordinator(st, 1, true) // dispatch 1 lands then errors
	schema, server := newDedupItemWriteServer(st, coord, false)
	seedDedupItem(t, st, schema, "1", "2")

	plan, err := server.updateItemWithRetry(ctx, appendListInput())
	require.NoError(t, err)
	require.NotNil(t, plan)

	require.Equal(t, []string{"1", "2", "3", "3"}, readListValues(t, server, schema),
		"legacy path re-reads and re-appends across the leader-churn retry — the duplicate the dedup gate fixes")
	require.Equal(t, 2, coord.dispatches, "landed attempt 1 + recompute")
	require.Equal(t, 0, coord.probeNoOps, "no prev_commit_ts is emitted while the gate is off")
}

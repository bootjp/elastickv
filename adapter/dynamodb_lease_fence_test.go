package adapter

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// multiGroupLeaseCoordinator is a fake coordinator that owns more than one
// Raft group and records which groups a lease pre-pass fenced. It models the
// invariant codex P1-A targets: a keyless multi-shard read (Scan, GSI/whole-
// table Query) must fence EVERY group, not just the default one.
//
//   - LeaseRead (keyless, default-group only) records groupDefault.
//   - LeaseReadAllGroups records every owned group.
//   - LeaseReadForKey records the key's owning group.
//
// EngineGroupIDForKey routes a key to a group by its first byte so tests can
// place keys on distinct groups deterministically.
type multiGroupLeaseCoordinator struct {
	*stubAdapterCoordinator

	mu           sync.Mutex
	fencedGroups map[uint64]int
}

const (
	multiGroupDefaultID = uint64(1)
	multiGroupOtherID   = uint64(2)
)

func newMultiGroupLeaseCoordinator() *multiGroupLeaseCoordinator {
	return &multiGroupLeaseCoordinator{
		stubAdapterCoordinator: &stubAdapterCoordinator{},
		fencedGroups:           make(map[uint64]int),
	}
}

func (c *multiGroupLeaseCoordinator) recordGroup(gid uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fencedGroups[gid]++
}

func (c *multiGroupLeaseCoordinator) fencedGroupSet() map[uint64]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[uint64]int, len(c.fencedGroups))
	for gid, n := range c.fencedGroups {
		out[gid] = n
	}
	return out
}

// EngineGroupIDForKey routes by first byte: keys starting with 'b' land on
// the non-default group, everything else on the default group.
func (c *multiGroupLeaseCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	if len(key) > 0 && key[0] == 'b' {
		return multiGroupOtherID
	}
	return multiGroupDefaultID
}

func (c *multiGroupLeaseCoordinator) LeaseRead(_ context.Context) (uint64, error) {
	// Keyless lease read fences only the default group — the pre-P1-A
	// behavior that left non-default groups unfenced for whole-table reads.
	c.recordGroup(multiGroupDefaultID)
	return 0, nil
}

func (c *multiGroupLeaseCoordinator) LeaseReadForKey(_ context.Context, key []byte) (uint64, error) {
	c.recordGroup(c.EngineGroupIDForKey(key))
	return 0, nil
}

func (c *multiGroupLeaseCoordinator) LeaseReadAllGroups(_ context.Context) error {
	c.recordGroup(multiGroupDefaultID)
	c.recordGroup(multiGroupOtherID)
	return nil
}

// TestDynamoDB_ScanLeaseFencesAllGroups asserts that the Scan handler's
// keyless lease pre-pass fences EVERY shard group, not just the default group
// (codex P1-A). On the pre-fix code leaseReadKeyless called LeaseReadThrough,
// which only fenced the default group, so this test fails: the non-default
// group never appears in fencedGroupSet.
func TestDynamoDB_ScanLeaseFencesAllGroups(t *testing.T) {
	t.Parallel()

	coord := newMultiGroupLeaseCoordinator()
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseReadKeyless(rec, req))

	fenced := coord.fencedGroupSet()
	require.Contains(t, fenced, multiGroupDefaultID,
		"keyless scan lease must fence the default group")
	require.Contains(t, fenced, multiGroupOtherID,
		"keyless scan lease must fence the non-default group too (codex P1-A) — "+
			"scanning all intersecting routes without fencing every group can read stale data")
}

// TestDynamoDB_QueryLeaseFallbackFencesAllGroups asserts a GSI query (which
// falls back to the keyless check) fences every group, not just the default.
func TestDynamoDB_QueryLeaseFallbackFencesAllGroups(t *testing.T) {
	t.Parallel()

	coord := newMultiGroupLeaseCoordinator()
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	// IndexName set => queryLeaseKey returns (_, false, nil) => keyless fallback.
	in := queryInput{
		TableName:              "t",
		IndexName:              "gsi1",
		KeyConditionExpression: "gk = :gk",
		ExpressionAttributeValues: map[string]attributeValue{
			":gk": newStringAttributeValue("x"),
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckQuery(rec, req, in))

	fenced := coord.fencedGroupSet()
	require.Contains(t, fenced, multiGroupDefaultID,
		"GSI query keyless fallback must fence the default group")
	require.Contains(t, fenced, multiGroupOtherID,
		"GSI query keyless fallback must fence the non-default group too (codex P1-A)")
}

// transientSchemaStore makes the FIRST GetAt on the table meta key fail with a
// non-ErrKeyNotFound store error, modeling a transient Pebble/backpressure
// failure during the lease pre-pass schema read.
type transientSchemaStore struct {
	store.MVCCStore

	metaKey []byte
	mu      sync.Mutex
	failed  bool
}

var errTransientSchemaRead = errors.New("injected transient schema read failure")

func (s *transientSchemaStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.mu.Lock()
	if !s.failed && bytes.Equal(key, s.metaKey) {
		s.failed = true
		s.mu.Unlock()
		return nil, errTransientSchemaRead
	}
	s.mu.Unlock()
	return s.MVCCStore.GetAt(ctx, key, ts)
}

// TestDynamoDB_TransactGetItems_TransientSchemaReadFailsClosed asserts that a
// transient schema-read error during the TransactGetItems lease pre-pass makes
// the handler fail closed (InternalServerError) rather than silently dropping
// the item from the lease-check set and reading it unfenced (codex P1-B). On
// the pre-fix code transactGetItemKey returned (_, false) for ANY schema error,
// so the item was skipped and leaseCheckTransactGetItems returned true (no
// fence); this test fails because it expects the handler to fail closed.
func TestDynamoDB_TransactGetItems_TransientSchemaReadFailsClosed(t *testing.T) {
	t.Parallel()

	schema := &dynamoTableSchema{
		TableName:          "t",
		Generation:         1,
		KeyEncodingVersion: dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{
			"pk": "S",
			"sk": "S",
		},
		PrimaryKey: dynamoKeySchema{HashKey: "pk", RangeKey: "sk"},
	}

	recording := &transientSchemaStore{
		MVCCStore: store.NewMVCCStore(),
		metaKey:   dynamoTableMetaKey(schema.TableName),
	}
	writer := newDynamoFixtureWriter(t, recording.MVCCStore)
	writer.writeSchema(schema)

	coord := &leaseReadCountingCoordinator{stubAdapterCoordinator: &stubAdapterCoordinator{}}
	server := NewDynamoDBServer(nil, recording, coord)

	in := transactGetItemsInput{
		TransactItems: []transactGetItem{
			{Get: &transactGetItemGet{
				TableName: "t",
				Key: map[string]attributeValue{
					"pk": newStringAttributeValue("tenant"),
					"sk": newStringAttributeValue("0001"),
				},
			}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.False(t, server.leaseCheckTransactGetItems(rec, req, in),
		"a transient schema read failure must fail the lease check, not skip the item (codex P1-B)")
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), dynamoErrInternal)
}

// TestDynamoDB_TransactGetItems_MalformedItemStillSkipped asserts the
// malformed-input path is unchanged: an item whose schema does not exist
// (ResourceNotFoundException, a validation error) is still skipped, not failed
// closed, so error mapping for malformed input stays byte-identical.
func TestDynamoDB_TransactGetItems_MalformedItemStillSkipped(t *testing.T) {
	t.Parallel()

	coord := &leaseReadCountingCoordinator{stubAdapterCoordinator: &stubAdapterCoordinator{}}
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	// No schema written => resolveTransactTableSchema returns a
	// ResourceNotFoundException (*dynamoAPIError) => malformed => skipped.
	in := transactGetItemsInput{
		TransactItems: []transactGetItem{
			{Get: &transactGetItemGet{
				TableName: "missing",
				Key:       map[string]attributeValue{"pk": newStringAttributeValue("x")},
			}},
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckTransactGetItems(rec, req, in),
		"a malformed item (table not found) must still be skipped, not failed closed")
	require.Equal(t, http.StatusOK, rec.Code, "no error response should be written for malformed input")
	require.Equal(t, int64(0), coord.leaseReadForKeyCalls.Load())
	require.Equal(t, int64(0), coord.leaseReadCalls.Load())
}

// TestDynamoDB_QueryLeaseKey_TransientSchemaReadFailsClosed asserts a transient
// schema read during the Query lease pre-pass fences all groups via the keyless
// fallback rather than proceeding with no fence (codex P1-B, consistent with
// P1-A). On the pre-fix code queryLeaseKey returned (_, false) for any schema
// error and leaseCheckQuery fell back to a keyless DEFAULT-group-only check,
// leaving non-default groups unfenced.
func TestDynamoDB_QueryLeaseKey_TransientSchemaReadFailsClosed(t *testing.T) {
	t.Parallel()

	schema := &dynamoTableSchema{
		TableName:          "t",
		Generation:         1,
		KeyEncodingVersion: dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{
			"pk": "S",
		},
		PrimaryKey: dynamoKeySchema{HashKey: "pk"},
	}

	recording := &transientSchemaStore{
		MVCCStore: store.NewMVCCStore(),
		metaKey:   dynamoTableMetaKey(schema.TableName),
	}
	writer := newDynamoFixtureWriter(t, recording.MVCCStore)
	writer.writeSchema(schema)

	coord := newMultiGroupLeaseCoordinator()
	server := NewDynamoDBServer(nil, recording, coord)

	in := queryInput{
		TableName:              "t",
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckQuery(rec, req, in))

	fenced := coord.fencedGroupSet()
	require.Contains(t, fenced, multiGroupDefaultID,
		"transient schema read must still fence the default group")
	require.Contains(t, fenced, multiGroupOtherID,
		"transient schema read must fail closed by fencing every group (codex P1-B + P1-A)")
}

var _ kv.AllGroupsLeaseReadableCoordinator = (*multiGroupLeaseCoordinator)(nil)

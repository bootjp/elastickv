package adapter

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// schemaDeadlineStore records, for the first GetAt on a DynamoDB table
// meta key, whether the context carried a deadline. The lease-read
// pre-pass resolves keys by reading the schema; if that read runs under
// a context with no deadline, a stalled schema read can block the handler
// indefinitely before the bounded lease phase begins (claude #952 issue #2).
type schemaDeadlineStore struct {
	store.MVCCStore

	metaKey []byte

	// mu guards sawMetaGet/metaHadDeadline so the check-then-set in GetAt is
	// race-free if the handler ever issues concurrent schema reads.
	mu              sync.Mutex
	sawMetaGet      bool
	metaHadDeadline bool
}

func (s *schemaDeadlineStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if bytes.Equal(key, s.metaKey) {
		s.mu.Lock()
		if !s.sawMetaGet {
			s.sawMetaGet = true
			_, s.metaHadDeadline = ctx.Deadline()
		}
		s.mu.Unlock()
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *schemaDeadlineStore) observed() (sawMetaGet bool, metaHadDeadline bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawMetaGet, s.metaHadDeadline
}

// newSchemaDeadlineServer builds a DynamoDB server over a real MVCC store
// fronted by schemaDeadlineStore, seeds one table schema + item, and
// returns the server and the recording store.
func newSchemaDeadlineServer(t *testing.T) (*DynamoDBServer, *schemaDeadlineStore) {
	t.Helper()

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

	recording := &schemaDeadlineStore{
		MVCCStore: store.NewMVCCStore(),
		metaKey:   dynamoTableMetaKey(schema.TableName),
	}
	writer := newDynamoFixtureWriter(t, recording.MVCCStore)
	writer.writeSchema(schema)
	writer.writeItem(schema, map[string]attributeValue{
		"pk": newStringAttributeValue("tenant"),
		"sk": newStringAttributeValue("0001"),
	})

	server := NewDynamoDBServer(nil, recording, &stubAdapterCoordinator{})
	return server, recording
}

// noDeadlineRequest returns a request whose context has NO deadline, so the
// only way the schema pre-fetch can observe a deadline is via the bounded
// leaseCtx the handler must establish before resolving keys.
func noDeadlineRequest(t *testing.T) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	_, hasDeadline := req.Context().Deadline()
	require.False(t, hasDeadline, "test precondition: request context must have no deadline")
	return req
}

// TestDynamoDB_LeaseCheckQuery_SchemaReadBounded asserts the base-table
// Query lease pre-pass resolves the routing key under the bounded leaseCtx,
// so a stalled schema read cannot block past dynamoLeaseReadTimeout before
// the lease phase begins (claude #952 issue #2).
func TestDynamoDB_LeaseCheckQuery_SchemaReadBounded(t *testing.T) {
	t.Parallel()

	server, recording := newSchemaDeadlineServer(t)

	in := queryInput{
		TableName:              "t",
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
	}

	rec := httptest.NewRecorder()
	require.True(t, server.leaseCheckQuery(rec, noDeadlineRequest(t), in))

	sawMetaGet, metaHadDeadline := recording.observed()
	require.True(t, sawMetaGet, "lease pre-pass must read the table schema")
	require.True(t, metaHadDeadline,
		"queryLeaseKey schema read must run under the bounded leaseCtx, not the deadline-free request context")
}

// TestDynamoDB_LeaseCheckTransactGetItems_SchemaReadBounded asserts the
// TransactGetItems lease pre-pass resolves item keys under the bounded
// leaseCtx, so a stalled schema read cannot block past dynamoLeaseReadTimeout
// before the lease phase begins (claude #952 issue #2).
func TestDynamoDB_LeaseCheckTransactGetItems_SchemaReadBounded(t *testing.T) {
	t.Parallel()

	server, recording := newSchemaDeadlineServer(t)

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
	require.True(t, server.leaseCheckTransactGetItems(rec, noDeadlineRequest(t), in))

	sawMetaGet, metaHadDeadline := recording.observed()
	require.True(t, sawMetaGet, "lease pre-pass must read the table schema")
	require.True(t, metaHadDeadline,
		"transactGetItemKey schema read must run under the bounded leaseCtx, not the deadline-free request context")
}

// TestDynamoDB_LeaseCheckTransactGetItems_AllItemsSkippedNoLeaseRead asserts
// that when every TransactItems entry fails key resolution (malformed Get),
// the pre-pass touches no shard and returns true without issuing a lease
// read — making the implicit empty-uniqueKeys fallback explicit
// (claude #952 issue #1).
func TestDynamoDB_LeaseCheckTransactGetItems_AllItemsSkippedNoLeaseRead(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	wrapped := &leaseReadCountingCoordinator{stubAdapterCoordinator: &stubAdapterCoordinator{}}
	server := NewDynamoDBServer(nil, st, wrapped)

	// No schema is written, so transactGetItemKey cannot resolve any key:
	// every item is skipped and uniqueKeys stays empty.
	in := transactGetItemsInput{
		TransactItems: []transactGetItem{
			{Get: &transactGetItemGet{
				TableName: "missing",
				Key:       map[string]attributeValue{"pk": newStringAttributeValue("x")},
			}},
			{Get: nil},
		},
	}

	rec := httptest.NewRecorder()
	require.True(t, server.leaseCheckTransactGetItems(rec, noDeadlineRequest(t), in))
	require.Equal(t, int64(0), wrapped.leaseReadForKeyCalls.Load(),
		"all-items-skipped path must not issue a lease read when no shard is touched")
	require.Equal(t, int64(0), wrapped.leaseReadCalls.Load(),
		"all-items-skipped path must not fall back to the keyless lease check")
	require.Equal(t, http.StatusOK, rec.Code, "no error response should be written")
}

// leaseReadCountingCoordinator wraps stubAdapterCoordinator and counts
// lease-read calls so a test can assert none were issued. The counters are
// atomic so the assertions stay race-free if a handler ever fans out lease
// reads concurrently.
type leaseReadCountingCoordinator struct {
	*stubAdapterCoordinator
	leaseReadCalls       atomic.Int64
	leaseReadForKeyCalls atomic.Int64
}

func (c *leaseReadCountingCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	c.leaseReadCalls.Add(1)
	return c.stubAdapterCoordinator.LeaseRead(ctx)
}

func (c *leaseReadCountingCoordinator) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	c.leaseReadForKeyCalls.Add(1)
	return c.stubAdapterCoordinator.LeaseReadForKey(ctx, key)
}

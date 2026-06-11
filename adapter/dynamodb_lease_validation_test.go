package adapter

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var errDegradedLease = errors.New("injected degraded-deployment lease failure")

// degradedLeaseCoordinator models a multi-group deployment where no group can
// confirm its lease (partition, slow follower). Every lease path fails. It is
// used to prove that a CLIENT-side-invalid Query (missing table, malformed
// KeyConditionExpression) must NOT be turned into an InternalServerError by the
// lease fallback: those requests never touch data, so the read path must surface
// the deterministic 4xx instead (codex #952 P2).
type degradedLeaseCoordinator struct {
	*stubAdapterCoordinator

	allGroupsCalls int
	forKeyCalls    int
}

func newDegradedLeaseCoordinator() *degradedLeaseCoordinator {
	return &degradedLeaseCoordinator{stubAdapterCoordinator: &stubAdapterCoordinator{}}
}

func (c *degradedLeaseCoordinator) LeaseRead(_ context.Context) (uint64, error) {
	c.allGroupsCalls++
	return 0, errDegradedLease
}

func (c *degradedLeaseCoordinator) LeaseReadForKey(_ context.Context, _ []byte) (uint64, error) {
	c.forKeyCalls++
	return 0, errDegradedLease
}

func (c *degradedLeaseCoordinator) LeaseReadAllGroups(_ context.Context) error {
	c.allGroupsCalls++
	return errDegradedLease
}

func (c *degradedLeaseCoordinator) EngineGroupIDForKey(_ []byte) uint64 {
	return multiGroupDefaultID
}

var _ kv.AllGroupsLeaseReadableCoordinator = (*degradedLeaseCoordinator)(nil)

// TestDynamoDB_LeaseCheckQuery_MissingTableSkipsLeaseFallback asserts that a
// Query against a table that does not exist does NOT take the all-groups lease
// fallback. In a degraded deployment that fallback would fail and emit a 500,
// masking the deterministic ResourceNotFoundException the read path produces.
// The pre-pass must return true without issuing any lease read so the handler
// surfaces the client error (codex #952 P2).
func TestDynamoDB_LeaseCheckQuery_MissingTableSkipsLeaseFallback(t *testing.T) {
	t.Parallel()

	coord := newDegradedLeaseCoordinator()
	// No schema written => loadTableSchemaAt reports the table missing.
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	in := queryInput{
		TableName:              "missing",
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckQuery(rec, req, in),
		"a missing-table query must skip the lease fallback so the read path can return ResourceNotFoundException")
	require.Equal(t, http.StatusOK, rec.Code, "no lease error response should be written for a client-invalid query")
	require.Equal(t, 0, coord.allGroupsCalls,
		"missing-table query must not fall back to the all-groups lease check")
	require.Equal(t, 0, coord.forKeyCalls,
		"missing-table query must not issue a per-key lease read")
}

// TestDynamoDB_LeaseCheckQuery_MalformedConditionSkipsLeaseFallback asserts that
// a Query with a malformed KeyConditionExpression skips the lease fallback for
// the same reason: it is a deterministic ValidationException, not a data read.
func TestDynamoDB_LeaseCheckQuery_MalformedConditionSkipsLeaseFallback(t *testing.T) {
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

	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(schema)

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, st, coord)

	// KeyConditionExpression references an attribute that is not the table's
	// hash key, so buildQueryCondition rejects it as a ValidationException.
	in := queryInput{
		TableName:              "t",
		KeyConditionExpression: "nope = :v",
		ExpressionAttributeValues: map[string]attributeValue{
			":v": newStringAttributeValue("x"),
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckQuery(rec, req, in),
		"a malformed-condition query must skip the lease fallback so the read path can return ValidationException")
	require.Equal(t, http.StatusOK, rec.Code, "no lease error response should be written for a malformed-condition query")
	require.Equal(t, 0, coord.allGroupsCalls,
		"malformed-condition query must not fall back to the all-groups lease check")
	require.Equal(t, 0, coord.forKeyCalls,
		"malformed-condition query must not issue a per-key lease read")
}

// TestDynamoDB_Query_MissingTableReturns400UnderDegradedLease is the end-to-end
// guard: the Query handler must return the deterministic 400
// ResourceNotFoundException for a missing table even when every group's lease is
// failing — not the 500 the lease fallback would otherwise produce (codex #952
// P2).
func TestDynamoDB_Query_MissingTableReturns400UnderDegradedLease(t *testing.T) {
	t.Parallel()

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	body := `{"TableName":"missing","KeyConditionExpression":"pk = :pk",` +
		`"ExpressionAttributeValues":{":pk":{"S":"tenant"}}}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	server.query(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code,
		"missing-table Query must return 400, not the 500 the degraded lease fallback would produce")
	require.Contains(t, rec.Body.String(), dynamoErrResourceNotFound)
}

// TestDynamoDB_LeaseCheckQuery_WholeTableQueryStillFencesAllGroups asserts the
// fix preserves the all-groups fence for a VALID multi-shard read: a query whose
// hash key is not the primary hash key reads the whole-table prefix (multiple
// shards), so it must still fail closed when the lease cannot be confirmed.
func TestDynamoDB_LeaseCheckQuery_WholeTableQueryStillFencesAllGroups(t *testing.T) {
	t.Parallel()

	coord := newMultiGroupLeaseCoordinator()
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	// IndexName set => valid multi-shard GSI read => keyless all-groups fence,
	// not a skip. (Whole-table base-table queries take the same all-groups path
	// through queryLeasePrefix's HashKey mismatch branch.)
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
		"valid multi-shard query must still fence the default group")
	require.Contains(t, fenced, multiGroupOtherID,
		"valid multi-shard query must still fence every group (must not be skipped as a validation case)")
}

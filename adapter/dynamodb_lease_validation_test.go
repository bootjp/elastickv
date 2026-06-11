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

	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(leaseGSIFixtureSchema())

	coord := newMultiGroupLeaseCoordinator()
	server := NewDynamoDBServer(nil, st, coord)

	// IndexName set + a VALID GSI on an EXISTING table => valid multi-shard
	// read => keyless all-groups fence, not a skip. (Whole-table base-table
	// queries take the same all-groups path through queryLeasePrefix's HashKey
	// mismatch branch.) The schema must exist so the GSI is genuinely valid:
	// after the P2-B fix an indexed query against a missing table or unknown
	// index is classified as a skip, not an all-groups fence.
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

// leaseGSIFixtureSchema is a table with a single GSI ("gsi1", ALL projection)
// used by the lease pre-pass tests that exercise VALID multi-shard GSI reads.
func leaseGSIFixtureSchema() *dynamoTableSchema {
	return &dynamoTableSchema{
		TableName:          "t",
		Generation:         1,
		KeyEncodingVersion: dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{
			"pk": "S",
			"gk": "S",
		},
		PrimaryKey: dynamoKeySchema{HashKey: "pk"},
		GlobalSecondaryIndexes: map[string]dynamoGlobalSecondaryIndex{
			"gsi1": {
				KeySchema:  dynamoKeySchema{HashKey: "gk"},
				Projection: dynamoGSIProjection{ProjectionType: "ALL"},
			},
		},
	}
}

// TestDynamoDB_LeaseCheckQuery_MissingTableGSISkipsLeaseFallback asserts that a
// GSI Query (IndexName set) against a table that does not exist skips the lease
// fallback (codex #952 P2-B). On the pre-fix code queryLeaseKey returned
// queryLeaseAllGroups for ANY non-empty IndexName without loading the schema,
// so a degraded all-groups lease produced a 500 that masked the deterministic
// ResourceNotFoundException the read path returns.
func TestDynamoDB_LeaseCheckQuery_MissingTableGSISkipsLeaseFallback(t *testing.T) {
	t.Parallel()

	coord := newDegradedLeaseCoordinator()
	// No schema written => the table is missing.
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	in := queryInput{
		TableName:              "missing",
		IndexName:              "gsi1",
		KeyConditionExpression: "gk = :gk",
		ExpressionAttributeValues: map[string]attributeValue{
			":gk": newStringAttributeValue("x"),
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckQuery(rec, req, in),
		"a missing-table GSI query must skip the lease fallback so the read path returns ResourceNotFoundException")
	require.Equal(t, http.StatusOK, rec.Code, "no lease error response should be written for a missing-table GSI query")
	require.Equal(t, 0, coord.allGroupsCalls,
		"missing-table GSI query must not fall back to the all-groups lease check")
	require.Equal(t, 0, coord.forKeyCalls,
		"missing-table GSI query must not issue a per-key lease read")
}

// TestDynamoDB_LeaseCheckQuery_UnknownGSISkipsLeaseFallback asserts that a Query
// naming an index that does not exist on the table skips the lease fallback: it
// is a deterministic ValidationException, not a data read (codex #952 P2-B).
func TestDynamoDB_LeaseCheckQuery_UnknownGSISkipsLeaseFallback(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(leaseGSIFixtureSchema())

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, st, coord)

	in := queryInput{
		TableName:              "t",
		IndexName:              "does-not-exist",
		KeyConditionExpression: "gk = :gk",
		ExpressionAttributeValues: map[string]attributeValue{
			":gk": newStringAttributeValue("x"),
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckQuery(rec, req, in),
		"an unknown-index query must skip the lease fallback so the read path returns ValidationException")
	require.Equal(t, http.StatusOK, rec.Code, "no lease error response should be written for an unknown-index query")
	require.Equal(t, 0, coord.allGroupsCalls,
		"unknown-index query must not fall back to the all-groups lease check")
	require.Equal(t, 0, coord.forKeyCalls,
		"unknown-index query must not issue a per-key lease read")
}

// TestDynamoDB_LeaseCheckQuery_GSIConsistentReadSkipsLeaseFallback asserts that
// a Query requesting ConsistentRead=true on a GSI skips the lease fallback: GSI
// reads cannot be strongly consistent, so the read path rejects it with a
// ValidationException without touching data (codex #952 P2-B).
func TestDynamoDB_LeaseCheckQuery_GSIConsistentReadSkipsLeaseFallback(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(leaseGSIFixtureSchema())

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, st, coord)

	consistent := true
	in := queryInput{
		TableName:              "t",
		IndexName:              "gsi1",
		KeyConditionExpression: "gk = :gk",
		ConsistentRead:         &consistent,
		ExpressionAttributeValues: map[string]attributeValue{
			":gk": newStringAttributeValue("x"),
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckQuery(rec, req, in),
		"a GSI ConsistentRead query must skip the lease fallback so the read path returns ValidationException")
	require.Equal(t, http.StatusOK, rec.Code, "no lease error response should be written for a GSI ConsistentRead query")
	require.Equal(t, 0, coord.allGroupsCalls,
		"GSI ConsistentRead query must not fall back to the all-groups lease check")
}

// TestDynamoDB_Query_InvalidGSIReturns4xxUnderDegradedLease is the end-to-end
// guard for P2-B: a Query naming an unknown index must return the deterministic
// 400 ValidationException even when every group's lease is failing — not the 500
// the all-groups fence would otherwise produce.
func TestDynamoDB_Query_InvalidGSIReturns4xxUnderDegradedLease(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(leaseGSIFixtureSchema())

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, st, coord)

	body := `{"TableName":"t","IndexName":"does-not-exist","KeyConditionExpression":"gk = :gk",` +
		`"ExpressionAttributeValues":{":gk":{"S":"x"}}}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	server.query(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code,
		"invalid-GSI Query must return 4xx, not the 500 the degraded all-groups fence would produce")
	require.Contains(t, rec.Body.String(), dynamoErrValidation)
}

// TestDynamoDB_LeaseCheckScan_MissingTableSkipsLeaseFallback asserts that a Scan
// against a table that does not exist skips the keyless lease fallback (codex
// #952 P2-A). On the pre-fix code scan() called leaseReadKeyless
// unconditionally before scanItems validated the table, so a degraded all-groups
// lease produced a 500 that masked the deterministic ResourceNotFoundException.
func TestDynamoDB_LeaseCheckScan_MissingTableSkipsLeaseFallback(t *testing.T) {
	t.Parallel()

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckScan(rec, req, scanInput{TableName: "missing"}),
		"a missing-table scan must skip the lease fallback so the read path returns ResourceNotFoundException")
	require.Equal(t, http.StatusOK, rec.Code, "no lease error response should be written for a missing-table scan")
	require.Equal(t, 0, coord.allGroupsCalls,
		"missing-table scan must not fall back to the all-groups lease check")
}

// TestDynamoDB_LeaseCheckScan_UnknownGSISkipsLeaseFallback asserts that a Scan
// naming an index that does not exist skips the lease fallback (codex #952
// P2-A): it is a deterministic ValidationException, not a data read.
func TestDynamoDB_LeaseCheckScan_UnknownGSISkipsLeaseFallback(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(leaseGSIFixtureSchema())

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, st, coord)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckScan(rec, req, scanInput{TableName: "t", IndexName: "does-not-exist"}),
		"an unknown-index scan must skip the lease fallback so the read path returns ValidationException")
	require.Equal(t, http.StatusOK, rec.Code, "no lease error response should be written for an unknown-index scan")
	require.Equal(t, 0, coord.allGroupsCalls,
		"unknown-index scan must not fall back to the all-groups lease check")
}

// TestDynamoDB_Scan_MissingTableReturns400UnderDegradedLease is the end-to-end
// guard for P2-A: the Scan handler must return the deterministic 400
// ResourceNotFoundException for a missing table even when every group's lease is
// failing — not the 500 the keyless fallback would otherwise produce.
func TestDynamoDB_Scan_MissingTableReturns400UnderDegradedLease(t *testing.T) {
	t.Parallel()

	coord := newDegradedLeaseCoordinator()
	server := NewDynamoDBServer(nil, store.NewMVCCStore(), coord)

	body := `{"TableName":"missing"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(body)))
	server.scan(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code,
		"missing-table Scan must return 400, not the 500 the degraded lease fallback would produce")
	require.Contains(t, rec.Body.String(), dynamoErrResourceNotFound)
}

// TestDynamoDB_LeaseCheckScan_ValidTableStillFencesAllGroups asserts the fix
// preserves the all-groups fence for a VALID Scan: a scan over an existing table
// reads every shard, so it must still fence every group (fail closed) when the
// lease cannot be confirmed.
func TestDynamoDB_LeaseCheckScan_ValidTableStillFencesAllGroups(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(leaseGSIFixtureSchema())

	coord := newMultiGroupLeaseCoordinator()
	server := NewDynamoDBServer(nil, st, coord)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckScan(rec, req, scanInput{TableName: "t"}))

	fenced := coord.fencedGroupSet()
	require.Contains(t, fenced, multiGroupDefaultID,
		"valid scan must still fence the default group")
	require.Contains(t, fenced, multiGroupOtherID,
		"valid scan must still fence every group (must not be skipped as a validation case)")
}

// TestDynamoDB_LeaseCheckScan_TransientSchemaReadFailsClosed asserts a transient
// schema-read failure during the Scan lease pre-pass fails closed by fencing
// every group, rather than skipping the lease and reading unfenced.
func TestDynamoDB_LeaseCheckScan_TransientSchemaReadFailsClosed(t *testing.T) {
	t.Parallel()

	schema := leaseGSIFixtureSchema()
	recording := &transientSchemaStore{
		MVCCStore: store.NewMVCCStore(),
		metaKey:   dynamoTableMetaKey(schema.TableName),
	}
	writer := newDynamoFixtureWriter(t, recording.MVCCStore)
	writer.writeSchema(schema)

	coord := newMultiGroupLeaseCoordinator()
	server := NewDynamoDBServer(nil, recording, coord)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(nil))
	require.True(t, server.leaseCheckScan(rec, req, scanInput{TableName: "t"}))

	fenced := coord.fencedGroupSet()
	require.Contains(t, fenced, multiGroupDefaultID,
		"transient schema read must still fence the default group")
	require.Contains(t, fenced, multiGroupOtherID,
		"transient schema read must fail closed by fencing every group")
}

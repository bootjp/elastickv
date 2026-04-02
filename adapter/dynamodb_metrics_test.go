package adapter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBServerHandleObservesSuccessMetrics(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewDynamoDBServer(nil, nil, &stubAdapterCoordinator{}, WithDynamoDBRequestObserver(registry.DynamoDBObserver()))
	server.targetHandlers = map[string]func(http.ResponseWriter, *http.Request){
		putItemTarget: func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(maxDynamoBodyReader(w, r))
			server.observeTables(r.Context(), "orders")
			server.observeWrittenItems(r.Context(), "orders", 1)
			writeDynamoJSON(w, map[string]any{"ok": true})
		},
	}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(`{"TableName":"orders"}`))
	req.Header.Set("X-Amz-Target", putItemTarget)
	rec := httptest.NewRecorder()
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_dynamodb_requests_total Total number of DynamoDB-compatible API requests by operation and outcome.
# TYPE elastickv_dynamodb_requests_total counter
elastickv_dynamodb_requests_total{node_address="10.0.0.1:50051",node_id="n1",operation="PutItem",outcome="success"} 1
# HELP elastickv_dynamodb_written_items_total Total number of items written or deleted by DynamoDB-compatible write APIs.
# TYPE elastickv_dynamodb_written_items_total counter
elastickv_dynamodb_written_items_total{node_address="10.0.0.1:50051",node_id="n1",operation="PutItem",table="orders"} 1
`),
		"elastickv_dynamodb_requests_total",
		"elastickv_dynamodb_written_items_total",
	)
	require.NoError(t, err)
}

func TestDynamoDBServerHandleObservesConditionalFailures(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewDynamoDBServer(nil, nil, &stubAdapterCoordinator{}, WithDynamoDBRequestObserver(registry.DynamoDBObserver()))
	server.targetHandlers = map[string]func(http.ResponseWriter, *http.Request){
		updateItemTarget: func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(maxDynamoBodyReader(w, r))
			server.observeTables(r.Context(), "orders")
			writeDynamoError(w, http.StatusBadRequest, dynamoErrConditionalFailed, "condition failed")
		},
	}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(`{"TableName":"orders"}`))
	req.Header.Set("X-Amz-Target", updateItemTarget)
	rec := httptest.NewRecorder()
	server.handle(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_dynamodb_conditional_check_failed_total Total number of conditional check failures returned by DynamoDB-compatible APIs.
# TYPE elastickv_dynamodb_conditional_check_failed_total counter
elastickv_dynamodb_conditional_check_failed_total{node_address="10.0.0.1:50051",node_id="n1",operation="UpdateItem",table="orders"} 1
`),
		"elastickv_dynamodb_conditional_check_failed_total",
	)
	require.NoError(t, err)
}

func TestDynamoDBServerHealthzSkipsDynamoMetrics(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewDynamoDBServer(nil, nil, &stubAdapterCoordinator{}, WithDynamoDBRequestObserver(registry.DynamoDBObserver()))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, dynamoHealthPath, nil)
	rec := httptest.NewRecorder()
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok\n", rec.Body.String())

	metricFamilies, err := registry.Gatherer().Gather()
	require.NoError(t, err)
	for _, family := range metricFamilies {
		require.NotEqual(t, "elastickv_dynamodb_requests_total", family.GetName())
	}
}

func TestDynamoDBServerLeaderHealthzSkipsDynamoMetrics(t *testing.T) {
	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	server := NewDynamoDBServer(nil, nil, &stubAdapterCoordinator{}, WithDynamoDBRequestObserver(registry.DynamoDBObserver()))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, dynamoLeaderHealthPath, nil)
	rec := httptest.NewRecorder()
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok\n", rec.Body.String())

	metricFamilies, err := registry.Gatherer().Gather()
	require.NoError(t, err)
	for _, family := range metricFamilies {
		require.NotEqual(t, "elastickv_dynamodb_requests_total", family.GetName())
	}
}

func TestDynamoDBServerLeaderHealthzReturnsServiceUnavailableWhenNotLeader(t *testing.T) {
	server := NewDynamoDBServer(nil, nil, &stubAdapterCoordinator{verifyLeaderErr: kv.ErrLeaderNotFound})

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, dynamoLeaderHealthPath, nil)
	rec := httptest.NewRecorder()
	server.handle(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "not leader\n", rec.Body.String())
}

func TestDynamoRequestMetricsStateAddsMetricsWithoutExplicitTableRegistration(t *testing.T) {
	state := &dynamoRequestMetricsState{}

	state.addTableMetrics("orders", 0, 0, 1)
	require.Equal(t, map[string]monitoring.DynamoDBTableMetrics{
		"orders": {WrittenItems: 1},
	}, state.tableMetrics())
}

func TestDynamoOperationNameRejectsUnknownTargets(t *testing.T) {
	require.Equal(t, "PutItem", dynamoOperationName(putItemTarget))
	require.Equal(t, "unknown", dynamoOperationName(targetPrefix+"PutItem-random"))
	require.Equal(t, "unknown", dynamoOperationName("random"))
}

func TestDynamoDBServerHandleObservesTableMetricsForGetItemMiss(t *testing.T) {
	server, registry := newObservedDynamoMetricsTestServer(t)
	rec := performObservedDynamoRequest(t, server, getItemTarget, `{"TableName":"orders","Key":{"pk":{"S":"missing"}}}`)

	require.Equal(t, http.StatusOK, rec.Code)
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_dynamodb_table_requests_total Total number of table-scoped DynamoDB-compatible API requests by operation and outcome.
# TYPE elastickv_dynamodb_table_requests_total counter
elastickv_dynamodb_table_requests_total{node_address="10.0.0.1:50051",node_id="n1",operation="GetItem",outcome="success",table="orders"} 1
`),
		"elastickv_dynamodb_table_requests_total",
	)
	require.NoError(t, err)
}

func TestDynamoDBServerHandleObservesTableMetricsForDeleteItemMiss(t *testing.T) {
	server, registry := newObservedDynamoMetricsTestServer(t)
	rec := performObservedDynamoRequest(t, server, deleteItemTarget, `{"TableName":"orders","Key":{"pk":{"S":"missing"}}}`)

	require.Equal(t, http.StatusOK, rec.Code)
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_dynamodb_table_requests_total Total number of table-scoped DynamoDB-compatible API requests by operation and outcome.
# TYPE elastickv_dynamodb_table_requests_total counter
elastickv_dynamodb_table_requests_total{node_address="10.0.0.1:50051",node_id="n1",operation="DeleteItem",outcome="success",table="orders"} 1
`),
		"elastickv_dynamodb_table_requests_total",
	)
	require.NoError(t, err)
}

func TestDynamoDBServerHandleObservesTableMetricsForPutItemValidationFailure(t *testing.T) {
	server, registry := newObservedDynamoMetricsTestServer(t)
	rec := performObservedDynamoRequest(t, server, putItemTarget, `{"TableName":"orders","Item":{"value":{"S":"x"}}}`)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_dynamodb_table_requests_total Total number of table-scoped DynamoDB-compatible API requests by operation and outcome.
# TYPE elastickv_dynamodb_table_requests_total counter
elastickv_dynamodb_table_requests_total{node_address="10.0.0.1:50051",node_id="n1",operation="PutItem",outcome="user_error",table="orders"} 1
# HELP elastickv_dynamodb_user_errors_total Total number of client-caused DynamoDB-compatible API errors.
# TYPE elastickv_dynamodb_user_errors_total counter
elastickv_dynamodb_user_errors_total{node_address="10.0.0.1:50051",node_id="n1",operation="PutItem",table="orders"} 1
`),
		"elastickv_dynamodb_table_requests_total",
		"elastickv_dynamodb_user_errors_total",
	)
	require.NoError(t, err)
}

func TestDynamoDBServerHandleObservesTableMetricsForUpdateItemValidationFailure(t *testing.T) {
	server, registry := newObservedDynamoMetricsTestServer(t)
	rec := performObservedDynamoRequest(t, server, updateItemTarget, `{"TableName":"orders","Key":{},"UpdateExpression":"SET #v = :v","ExpressionAttributeNames":{"#v":"value"},"ExpressionAttributeValues":{":v":{"S":"x"}}}`)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_dynamodb_table_requests_total Total number of table-scoped DynamoDB-compatible API requests by operation and outcome.
# TYPE elastickv_dynamodb_table_requests_total counter
elastickv_dynamodb_table_requests_total{node_address="10.0.0.1:50051",node_id="n1",operation="UpdateItem",outcome="user_error",table="orders"} 1
# HELP elastickv_dynamodb_user_errors_total Total number of client-caused DynamoDB-compatible API errors.
# TYPE elastickv_dynamodb_user_errors_total counter
elastickv_dynamodb_user_errors_total{node_address="10.0.0.1:50051",node_id="n1",operation="UpdateItem",table="orders"} 1
`),
		"elastickv_dynamodb_table_requests_total",
		"elastickv_dynamodb_user_errors_total",
	)
	require.NoError(t, err)
}

func newObservedDynamoMetricsTestServer(t *testing.T) (*DynamoDBServer, *monitoring.Registry) {
	t.Helper()

	registry := monitoring.NewRegistry("n1", "10.0.0.1:50051")
	st := store.NewMVCCStore()
	t.Cleanup(func() {
		require.NoError(t, st.Close())
	})

	server := NewDynamoDBServer(nil, st, newLocalAdapterCoordinator(st), WithDynamoDBRequestObserver(registry.DynamoDBObserver()))
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(&dynamoTableSchema{
		TableName:          "orders",
		Generation:         1,
		KeyEncodingVersion: dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{
			"pk": "S",
		},
		PrimaryKey: dynamoKeySchema{
			HashKey: "pk",
		},
	})

	return server, registry
}

func performObservedDynamoRequest(t *testing.T, server *DynamoDBServer, target string, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", target)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	return rec
}

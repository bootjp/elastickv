package adapter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/monitoring"
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

func TestDynamoRequestMetricsStateIgnoresUnknownTableMetrics(t *testing.T) {
	state := &dynamoRequestMetricsState{}

	state.addTableMetrics("orders", 0, 0, 1)
	require.Nil(t, state.tableMetrics())

	state.recordTable("orders")
	state.addTableMetrics("orders", 0, 0, 1)
	require.Equal(t, map[string]monitoring.DynamoDBTableMetrics{
		"orders": {WrittenItems: 1},
	}, state.tableMetrics())
}

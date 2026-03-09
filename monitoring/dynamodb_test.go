package monitoring

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDynamoDBMetricsObserveRequest(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	metrics, ok := registry.DynamoDBObserver().(*DynamoDBMetrics)
	require.True(t, ok)

	metrics.ObserveInFlightChange("PutItem", 1)
	metrics.ObserveInFlightChange("PutItem", -1)
	metrics.ObserveDynamoDBRequest(DynamoDBRequestReport{
		Operation:     "PutItem",
		HTTPStatus:    200,
		Duration:      12 * time.Millisecond,
		RequestBytes:  256,
		ResponseBytes: 64,
		Tables:        []string{"orders", "orders"},
		TableMetrics: map[string]DynamoDBTableMetrics{
			"orders": {WrittenItems: 1},
		},
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_dynamodb_requests_total Total number of DynamoDB-compatible API requests by operation and outcome.
# TYPE elastickv_dynamodb_requests_total counter
elastickv_dynamodb_requests_total{node_address="10.0.0.1:50051",node_id="n1",operation="PutItem",outcome="success"} 1
# HELP elastickv_dynamodb_table_requests_total Total number of table-scoped DynamoDB-compatible API requests by operation and outcome.
# TYPE elastickv_dynamodb_table_requests_total counter
elastickv_dynamodb_table_requests_total{node_address="10.0.0.1:50051",node_id="n1",operation="PutItem",outcome="success",table="orders"} 1
# HELP elastickv_dynamodb_written_items_total Total number of items written or deleted by DynamoDB-compatible write APIs.
# TYPE elastickv_dynamodb_written_items_total counter
elastickv_dynamodb_written_items_total{node_address="10.0.0.1:50051",node_id="n1",operation="PutItem",table="orders"} 1
`),
		"elastickv_dynamodb_requests_total",
		"elastickv_dynamodb_table_requests_total",
		"elastickv_dynamodb_written_items_total",
	)
	require.NoError(t, err)
}

func TestDynamoDBMetricsClassifyConditionalFailure(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	metrics, ok := registry.DynamoDBObserver().(*DynamoDBMetrics)
	require.True(t, ok)

	metrics.ObserveDynamoDBRequest(DynamoDBRequestReport{
		Operation:  "UpdateItem",
		HTTPStatus: 400,
		ErrorType:  "ConditionalCheckFailedException",
		Duration:   time.Millisecond,
		Tables:     []string{"orders"},
	})

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

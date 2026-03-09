package monitoring

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	dynamoOutcomeSuccess                = "success"
	dynamoOutcomeUserError              = "user_error"
	dynamoOutcomeSystemError            = "system_error"
	dynamoOutcomeConditionalCheckFailed = "conditional_check_failed"
	dynamoOperationUnknown              = "unknown"
)

// DynamoDBRequestObserver records per-request DynamoDB-compatible API metrics.
type DynamoDBRequestObserver interface {
	ObserveInFlightChange(operation string, delta float64)
	ObserveDynamoDBRequest(report DynamoDBRequestReport)
}

// DynamoDBRequestReport is the normalized result of a single request.
type DynamoDBRequestReport struct {
	Operation     string
	HTTPStatus    int
	ErrorType     string
	Duration      time.Duration
	RequestBytes  int
	ResponseBytes int
	Tables        []string
	TableMetrics  map[string]DynamoDBTableMetrics
}

// DynamoDBTableMetrics captures table-scoped work done by a request.
type DynamoDBTableMetrics struct {
	ReturnedItems int
	ScannedItems  int
	WrittenItems  int
}

type DynamoDBMetrics struct {
	inflightRequests *prometheus.GaugeVec
	requestsTotal    *prometheus.CounterVec
	tableRequests    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	requestSize      *prometheus.HistogramVec
	responseSize     *prometheus.HistogramVec
	returnedItems    *prometheus.CounterVec
	scannedItems     *prometheus.CounterVec
	writtenItems     *prometheus.CounterVec
	userErrors       *prometheus.CounterVec
	systemErrors     *prometheus.CounterVec
	conditionalFails *prometheus.CounterVec
}

func newDynamoDBMetrics(registerer prometheus.Registerer) *DynamoDBMetrics {
	m := &DynamoDBMetrics{
		inflightRequests: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_dynamodb_inflight_requests",
				Help: "Current number of in-flight DynamoDB-compatible API requests.",
			},
			[]string{"operation"},
		),
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_requests_total",
				Help: "Total number of DynamoDB-compatible API requests by operation and outcome.",
			},
			[]string{"operation", "outcome"},
		),
		tableRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_table_requests_total",
				Help: "Total number of table-scoped DynamoDB-compatible API requests by operation and outcome.",
			},
			[]string{"operation", "table", "outcome"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_dynamodb_request_duration_seconds",
				Help:    "End-to-end latency of DynamoDB-compatible API requests.",
				Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
			},
			[]string{"operation", "outcome"},
		),
		requestSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_dynamodb_request_size_bytes",
				Help:    "Observed DynamoDB-compatible request sizes in bytes.",
				Buckets: []float64{128, 512, 1024, 4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024},
			},
			[]string{"operation"},
		),
		responseSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_dynamodb_response_size_bytes",
				Help:    "Observed DynamoDB-compatible response sizes in bytes.",
				Buckets: []float64{128, 512, 1024, 4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024},
			},
			[]string{"operation", "outcome"},
		),
		returnedItems: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_returned_items_total",
				Help: "Total number of items returned by DynamoDB-compatible read APIs.",
			},
			[]string{"operation", "table"},
		),
		scannedItems: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_scanned_items_total",
				Help: "Total number of items scanned by DynamoDB-compatible read APIs.",
			},
			[]string{"operation", "table"},
		),
		writtenItems: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_written_items_total",
				Help: "Total number of items written or deleted by DynamoDB-compatible write APIs.",
			},
			[]string{"operation", "table"},
		),
		userErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_user_errors_total",
				Help: "Total number of client-caused DynamoDB-compatible API errors.",
			},
			[]string{"operation", "table"},
		),
		systemErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_system_errors_total",
				Help: "Total number of server-side DynamoDB-compatible API errors.",
			},
			[]string{"operation", "table"},
		),
		conditionalFails: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_dynamodb_conditional_check_failed_total",
				Help: "Total number of conditional check failures returned by DynamoDB-compatible APIs.",
			},
			[]string{"operation", "table"},
		),
	}

	registerer.MustRegister(
		m.inflightRequests,
		m.requestsTotal,
		m.tableRequests,
		m.requestDuration,
		m.requestSize,
		m.responseSize,
		m.returnedItems,
		m.scannedItems,
		m.writtenItems,
		m.userErrors,
		m.systemErrors,
		m.conditionalFails,
	)

	return m
}

// ObserveInFlightChange adjusts the in-flight request gauge for an operation.
func (m *DynamoDBMetrics) ObserveInFlightChange(operation string, delta float64) {
	if m == nil {
		return
	}
	operation = normalizeDynamoOperation(operation)
	m.inflightRequests.WithLabelValues(operation).Add(delta)
}

// ObserveDynamoDBRequest records the final outcome of a request.
func (m *DynamoDBMetrics) ObserveDynamoDBRequest(report DynamoDBRequestReport) {
	if m == nil {
		return
	}

	operation := normalizeDynamoOperation(report.Operation)
	outcome := classifyDynamoOutcome(report.HTTPStatus, report.ErrorType)
	tableMetrics := report.TableMetrics
	if tableMetrics == nil {
		tableMetrics = map[string]DynamoDBTableMetrics{}
	}

	m.observeRequest(operation, outcome, report)
	m.observeTables(operation, outcome, mergeReportTables(report.Tables, tableMetrics), tableMetrics)
}

func normalizeDynamoOperation(operation string) string {
	operation = strings.TrimSpace(operation)
	if operation == "" {
		return dynamoOperationUnknown
	}
	return operation
}

func (m *DynamoDBMetrics) observeRequest(operation string, outcome string, report DynamoDBRequestReport) {
	m.requestsTotal.WithLabelValues(operation, outcome).Inc()
	m.requestDuration.WithLabelValues(operation, outcome).Observe(report.Duration.Seconds())
	m.requestSize.WithLabelValues(operation).Observe(float64(max(report.RequestBytes, 0)))
	m.responseSize.WithLabelValues(operation, outcome).Observe(float64(max(report.ResponseBytes, 0)))
}

func mergeReportTables(tables []string, tableMetrics map[string]DynamoDBTableMetrics) []string {
	merged := append([]string(nil), tables...)
	for table := range tableMetrics {
		if table = strings.TrimSpace(table); table != "" {
			merged = append(merged, table)
		}
	}
	return normalizedTables(merged)
}

func (m *DynamoDBMetrics) observeTables(operation string, outcome string, tables []string, tableMetrics map[string]DynamoDBTableMetrics) {
	for _, table := range tables {
		m.tableRequests.WithLabelValues(operation, table, outcome).Inc()
		m.observeTableActivity(operation, table, tableMetrics[table])
		m.observeTableOutcome(operation, table, outcome)
	}
}

func (m *DynamoDBMetrics) observeTableActivity(operation string, table string, metrics DynamoDBTableMetrics) {
	if metrics.ReturnedItems > 0 {
		m.returnedItems.WithLabelValues(operation, table).Add(float64(metrics.ReturnedItems))
	}
	if metrics.ScannedItems > 0 {
		m.scannedItems.WithLabelValues(operation, table).Add(float64(metrics.ScannedItems))
	}
	if metrics.WrittenItems > 0 {
		m.writtenItems.WithLabelValues(operation, table).Add(float64(metrics.WrittenItems))
	}
}

func (m *DynamoDBMetrics) observeTableOutcome(operation string, table string, outcome string) {
	switch outcome {
	case dynamoOutcomeConditionalCheckFailed:
		m.conditionalFails.WithLabelValues(operation, table).Inc()
	case dynamoOutcomeUserError:
		m.userErrors.WithLabelValues(operation, table).Inc()
	case dynamoOutcomeSystemError:
		m.systemErrors.WithLabelValues(operation, table).Inc()
	}
}

func normalizedTables(tables []string) []string {
	out := make([]string, 0, len(tables))
	seen := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		table = strings.TrimSpace(table)
		if table == "" {
			continue
		}
		if _, ok := seen[table]; ok {
			continue
		}
		seen[table] = struct{}{}
		out = append(out, table)
	}
	slices.Sort(out)
	return out
}

func classifyDynamoOutcome(status int, errorType string) string {
	if strings.EqualFold(strings.TrimSpace(errorType), "ConditionalCheckFailedException") {
		return dynamoOutcomeConditionalCheckFailed
	}
	switch {
	case status >= 200 && status < 300:
		return dynamoOutcomeSuccess
	case status >= http.StatusInternalServerError:
		return dynamoOutcomeSystemError
	default:
		return dynamoOutcomeUserError
	}
}

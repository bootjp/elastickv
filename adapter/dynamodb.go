package adapter

import (
	"context"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	targetPrefix             = "DynamoDB_20120810."
	createTableTarget        = targetPrefix + "CreateTable"
	deleteTableTarget        = targetPrefix + "DeleteTable"
	describeTableTarget      = targetPrefix + "DescribeTable"
	listTablesTarget         = targetPrefix + "ListTables"
	putItemTarget            = targetPrefix + "PutItem"
	deleteItemTarget         = targetPrefix + "DeleteItem"
	getItemTarget            = targetPrefix + "GetItem"
	queryTarget              = targetPrefix + "Query"
	scanTarget               = targetPrefix + "Scan"
	updateItemTarget         = targetPrefix + "UpdateItem"
	batchWriteItemTarget     = targetPrefix + "BatchWriteItem"
	transactWriteItemsTarget = targetPrefix + "TransactWriteItems"
	transactGetItemsTarget   = targetPrefix + "TransactGetItems"
)

const (
	dynamoHealthPath       = "/healthz"
	dynamoLeaderHealthPath = "/healthz/leader"
)

const dynamoHealthMaxRequestBodyBytes = 1024

const (
	updateSplitCount            = 2
	splitPartsInitialCapacity   = 2
	replacerArgPairSize         = 2
	gsiQueryReadWorkerCount     = 16
	numericUpdateScaleDigits    = 20
	transactRetryMaxAttempts    = 128
	transactRetryMaxDuration    = 2 * time.Second
	transactRetryInitialBackoff = 1 * time.Millisecond
	transactRetryMaxBackoff     = 10 * time.Millisecond
	transactRetryBackoffFactor  = 2
	tableCleanupAsyncTimeout    = 5 * time.Minute
	// dynamoLeaseReadTimeout bounds how long LeaseReadForKey's slow
	// path (LinearizableRead) may block before returning an error to
	// the HTTP client. Matches the order of magnitude of Redis's
	// redisDispatchTimeout so both adapters give up at similar
	// wall-clock budgets on quorum loss.
	dynamoLeaseReadTimeout    = 5 * time.Second
	itemUpdateLockStripeCount = 256
	tableLockStripeCount      = 128
	batchWriteItemMaxItems    = 25
	transactGetItemsMaxItems  = 100
	dynamoMaxRequestBodyBytes = 1 << 20

	dynamoTableMetaPrefix       = kv.DynamoTableMetaPrefix
	dynamoTableGenerationPrefix = kv.DynamoTableGenerationPrefix
	dynamoItemPrefix            = kv.DynamoItemPrefix
	dynamoGSIPrefix             = kv.DynamoGSIPrefix
	dynamoScanPageLimit         = 1024
	dynamoKeyEscapeByte         = byte(0x00)
	dynamoKeyTerminatorByte     = byte(0x01)
	dynamoKeyEscapedZeroByte    = byte(0xFF)
	dynamoKeySegmentOverhead    = 2
	dynamoOrderedKeyEncodingV2  = 2
)

var dynamoOperationTargets = map[string]string{
	batchWriteItemTarget:     "BatchWriteItem",
	createTableTarget:        "CreateTable",
	deleteItemTarget:         "DeleteItem",
	deleteTableTarget:        "DeleteTable",
	describeTableTarget:      "DescribeTable",
	getItemTarget:            "GetItem",
	listTablesTarget:         "ListTables",
	putItemTarget:            "PutItem",
	queryTarget:              "Query",
	scanTarget:               "Scan",
	transactWriteItemsTarget: "TransactWriteItems",
	transactGetItemsTarget:   "TransactGetItems",
	updateItemTarget:         "UpdateItem",
}

const (
	dynamoErrValidation          = "ValidationException"
	dynamoErrInternal            = "InternalServerError"
	dynamoErrConditionalFailed   = "ConditionalCheckFailedException"
	dynamoErrTransactionCanceled = "TransactionCanceledException"
	dynamoErrResourceNotFound    = "ResourceNotFoundException"
	dynamoErrResourceInUse       = "ResourceInUseException"
)

const (
	dynamoReturnValueNone       = "NONE"
	dynamoReturnValueAllOld     = "ALL_OLD"
	dynamoReturnValueUpdatedOld = "UPDATED_OLD"
	dynamoReturnValueAllNew     = "ALL_NEW"
	dynamoReturnValueUpdatedNew = "UPDATED_NEW"
	dynamoSelectCount           = "COUNT"
)

type DynamoDBServerOption func(*DynamoDBServer)

type DynamoDBServer struct {
	listen          net.Listener
	store           store.MVCCStore
	coordinator     kv.Coordinator
	httpServer      *http.Server
	targetHandlers  map[string]func(http.ResponseWriter, *http.Request)
	readTracker     *kv.ActiveTimestampTracker
	requestObserver monitoring.DynamoDBRequestObserver
	itemUpdateLocks [itemUpdateLockStripeCount]sync.Mutex
	tableLocks      [tableLockStripeCount]sync.Mutex
	leaderDynamo    map[string]string

	// onePhaseTxnDedup enables option-2 one-phase idempotency on the
	// single-item write path (UpdateItem / PutItem / DeleteItem): on a
	// retryable write error, the retry REUSES the failed attempt's write set
	// under a fresh commit_ts and carries prev_commit_ts so the FSM no-ops a
	// commit that already landed under leadership churn (the :duplicate-elements
	// anomaly). It MUST stay off until every node runs a probe-aware binary —
	// see R5 in docs/design/2026_06_03_partial_dynamodb_onephase_dedup.md.
	// Default off; enabled via WithDynamoOnePhaseTxnDedup or the
	// ELASTICKV_DYNAMODB_ONEPHASE_DEDUP env var.
	onePhaseTxnDedup bool
}

// WithDynamoDBRequestObserver enables Prometheus-compatible request metrics.
func WithDynamoDBRequestObserver(observer monitoring.DynamoDBRequestObserver) DynamoDBServerOption {
	return func(server *DynamoDBServer) {
		server.requestObserver = observer
	}
}

func WithDynamoDBActiveTimestampTracker(tracker *kv.ActiveTimestampTracker) DynamoDBServerOption {
	return func(server *DynamoDBServer) {
		server.readTracker = tracker
	}
}

// WithDynamoDBLeaderMap configures the Raft-address-to-DynamoDB-address mapping
// used to forward requests from followers to the current leader.
// The format mirrors the raftRedisMap / raftS3Map convention.
func WithDynamoDBLeaderMap(m map[string]string) DynamoDBServerOption {
	return func(server *DynamoDBServer) {
		server.leaderDynamo = make(map[string]string, len(m))
		for k, v := range m {
			server.leaderDynamo[k] = v
		}
	}
}

// WithDynamoOnePhaseTxnDedup enables the option-2 one-phase idempotency dedup on
// the single-item write retry path (see DynamoDBServer.onePhaseTxnDedup). Off by
// default; enable only after the whole cluster runs a probe-aware binary.
func WithDynamoOnePhaseTxnDedup(enabled bool) DynamoDBServerOption {
	return func(server *DynamoDBServer) {
		server.onePhaseTxnDedup = enabled
	}
}

type dynamoRequestMetricsContextKey struct{}

type dynamoRequestMetricsState struct {
	tables map[string]monitoring.DynamoDBTableMetrics
}

type countingReadCloser struct {
	io.ReadCloser
	bytesRead int
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	if c == nil || c.ReadCloser == nil {
		return 0, io.EOF
	}
	n, err := c.ReadCloser.Read(p)
	c.bytesRead += n
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.EOF
		}
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (c *countingReadCloser) Close() error {
	if c == nil || c.ReadCloser == nil {
		return nil
	}
	if err := c.ReadCloser.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *countingReadCloser) BytesRead() int {
	if c == nil {
		return 0
	}
	return c.bytesRead
}

type dynamoResponseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
}

func (r *dynamoResponseRecorder) WriteHeader(statusCode int) {
	if r.statusCode == 0 {
		r.statusCode = statusCode
	}
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *dynamoResponseRecorder) Write(p []byte) (int, error) {
	if r.statusCode == 0 {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytesWritten += n
	if err != nil {
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (r *dynamoResponseRecorder) StatusCode() int {
	if r == nil || r.statusCode == 0 {
		return http.StatusOK
	}
	return r.statusCode
}

func (r *dynamoResponseRecorder) BytesWritten() int {
	if r == nil {
		return 0
	}
	return r.bytesWritten
}

func NewDynamoDBServer(listen net.Listener, st store.MVCCStore, coordinate kv.Coordinator, opts ...DynamoDBServerOption) *DynamoDBServer {
	d := &DynamoDBServer{
		listen:           listen,
		store:            st,
		coordinator:      coordinate,
		onePhaseTxnDedup: os.Getenv("ELASTICKV_DYNAMODB_ONEPHASE_DEDUP") == "1",
	}
	d.targetHandlers = map[string]func(http.ResponseWriter, *http.Request){
		createTableTarget:        d.createTable,
		deleteTableTarget:        d.deleteTable,
		describeTableTarget:      d.describeTable,
		listTablesTarget:         d.listTables,
		putItemTarget:            d.putItem,
		deleteItemTarget:         d.deleteItem,
		getItemTarget:            d.getItem,
		queryTarget:              d.query,
		scanTarget:               d.scan,
		updateItemTarget:         d.updateItem,
		batchWriteItemTarget:     d.batchWriteItem,
		transactWriteItemsTarget: d.transactWriteItems,
		transactGetItemsTarget:   d.transactGetItems,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handle)
	d.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	for _, opt := range opts {
		if opt != nil {
			opt(d)
		}
	}
	return d
}

func (d *DynamoDBServer) Run() error {
	if err := d.httpServer.Serve(d.listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DynamoDBServer) Stop() {
	if d.httpServer != nil {
		_ = d.httpServer.Shutdown(context.Background())
	}
}

// proxyToLeader forwards the HTTP request to the current DynamoDB leader when
// this node is not the Raft leader.  Returns true if the request was handled
// (either proxied or an error response was written), false if the request
// should be handled locally (i.e. this node is the leader or no leader map is
// configured).
//
// Serving reads or writes locally on a follower would expose G2-item-realtime
// stale reads, so every follower request is forwarded to the leader.
func (d *DynamoDBServer) proxyToLeader(w http.ResponseWriter, r *http.Request) bool {
	return proxyHTTPRequestToLeader(d.coordinator, d.leaderDynamo, dynamoLeaderProxyErrorWriter, w, r)
}

func dynamoLeaderProxyErrorWriter(w http.ResponseWriter, status int, message string) {
	writeDynamoError(w, status, dynamoErrInternal, message)
}

func (d *DynamoDBServer) handle(w http.ResponseWriter, r *http.Request) {
	if d.serveHealthz(w, r) {
		return
	}
	if d.proxyToLeader(w, r) {
		return
	}

	target := r.Header.Get("X-Amz-Target")
	if d.requestObserver == nil {
		d.dispatchOrWriteUnsupported(target, w, r)
		return
	}

	operation := dynamoOperationName(target)
	d.requestObserver.ObserveInFlightChange(operation, 1)
	defer d.requestObserver.ObserveInFlightChange(operation, -1)

	state := &dynamoRequestMetricsState{tables: make(map[string]monitoring.DynamoDBTableMetrics)}
	r = r.WithContext(context.WithValue(r.Context(), dynamoRequestMetricsContextKey{}, state))
	bodyCounter := &countingReadCloser{ReadCloser: r.Body}
	r.Body = bodyCounter
	recorder := &dynamoResponseRecorder{ResponseWriter: w}
	started := time.Now()

	d.dispatchOrWriteUnsupported(target, recorder, r)

	d.requestObserver.ObserveDynamoDBRequest(monitoring.DynamoDBRequestReport{
		Operation:     operation,
		HTTPStatus:    recorder.StatusCode(),
		ErrorType:     recorder.Header().Get("x-amzn-ErrorType"),
		Duration:      time.Since(started),
		RequestBytes:  bodyCounter.BytesRead(),
		ResponseBytes: recorder.BytesWritten(),
		Tables:        state.tableNames(),
		TableMetrics:  state.tableMetrics(),
	})
}

func (d *DynamoDBServer) serveHealthz(w http.ResponseWriter, r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}

	switch r.URL.Path {
	case dynamoHealthPath:
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, dynamoHealthMaxRequestBodyBytes)
		}
		serveDynamoHealthz(w, r)
		return true
	case dynamoLeaderHealthPath:
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, dynamoHealthMaxRequestBodyBytes)
		}
		d.serveDynamoLeaderHealthz(w, r)
		return true
	default:
		return false
	}
}

func serveDynamoHealthz(w http.ResponseWriter, r *http.Request) {
	if !writeDynamoHealthMethod(w, r) {
		return
	}
	writeDynamoHealthBody(w, r, http.StatusOK, "ok\n")
}

func (d *DynamoDBServer) serveDynamoLeaderHealthz(w http.ResponseWriter, r *http.Request) {
	if !writeDynamoHealthMethod(w, r) {
		return
	}

	if isVerifiedDynamoLeader(r.Context(), d.coordinator) {
		writeDynamoHealthBody(w, r, http.StatusOK, "ok\n")
		return
	}

	writeDynamoHealthBody(w, r, http.StatusServiceUnavailable, "not leader\n")
}

func isVerifiedDynamoLeader(ctx context.Context, coordinator kv.Coordinator) bool {
	if coordinator == nil || !coordinator.IsLeader() {
		return false
	}
	return coordinator.VerifyLeader(ctx) == nil
}

func writeDynamoHealthMethod(w http.ResponseWriter, r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return true
	default:
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return false
	}
}

func writeDynamoHealthBody(w http.ResponseWriter, r *http.Request, statusCode int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.WriteString(w, body)
}

func maxDynamoBodyReader(w http.ResponseWriter, r *http.Request) io.Reader {
	return http.MaxBytesReader(w, r.Body, dynamoMaxRequestBodyBytes)
}

func (d *DynamoDBServer) dispatchByTarget(target string, w http.ResponseWriter, r *http.Request) bool {
	handler, ok := d.targetHandlers[target]
	if !ok {
		return false
	}
	handler(w, r)
	return true
}

func (d *DynamoDBServer) dispatchOrWriteUnsupported(target string, w http.ResponseWriter, r *http.Request) {
	if d.dispatchByTarget(target, w, r) {
		return
	}
	writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, "unsupported operation")
}

func dynamoOperationName(target string) string {
	if operation, ok := dynamoOperationTargets[strings.TrimSpace(target)]; ok {
		return operation
	}
	return "unknown"
}

func dynamoRequestMetricsFromContext(ctx context.Context) *dynamoRequestMetricsState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(dynamoRequestMetricsContextKey{}).(*dynamoRequestMetricsState)
	return state
}

func (s *dynamoRequestMetricsState) recordTable(table string) {
	if s == nil {
		return
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return
	}
	if s.tables == nil {
		s.tables = make(map[string]monitoring.DynamoDBTableMetrics)
	}
	if _, ok := s.tables[table]; !ok {
		s.tables[table] = monitoring.DynamoDBTableMetrics{}
	}
}

func (s *dynamoRequestMetricsState) addTableMetrics(table string, returnedItems int, scannedItems int, writtenItems int) {
	if s == nil {
		return
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return
	}
	s.recordTable(table)
	metrics := s.tables[table]
	metrics.ReturnedItems += returnedItems
	metrics.ScannedItems += scannedItems
	metrics.WrittenItems += writtenItems
	s.tables[table] = metrics
}

func (s *dynamoRequestMetricsState) tableNames() []string {
	if s == nil || len(s.tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.tables))
	for table := range s.tables {
		names = append(names, table)
	}
	sort.Strings(names)
	return names
}

func (s *dynamoRequestMetricsState) tableMetrics() map[string]monitoring.DynamoDBTableMetrics {
	if s == nil || len(s.tables) == 0 {
		return nil
	}
	out := make(map[string]monitoring.DynamoDBTableMetrics, len(s.tables))
	maps.Copy(out, s.tables)
	return out
}

func (d *DynamoDBServer) observeTables(ctx context.Context, tables ...string) {
	state := dynamoRequestMetricsFromContext(ctx)
	if state == nil {
		return
	}
	for _, table := range tables {
		state.recordTable(table)
	}
}

func (d *DynamoDBServer) observeReadMetrics(ctx context.Context, table string, returnedItems int, scannedItems int) {
	state := dynamoRequestMetricsFromContext(ctx)
	if state == nil {
		return
	}
	state.addTableMetrics(table, returnedItems, scannedItems, 0)
}

func (d *DynamoDBServer) observeWrittenItems(ctx context.Context, table string, writtenItems int) {
	state := dynamoRequestMetricsFromContext(ctx)
	if state == nil {
		return
	}
	state.addTableMetrics(table, 0, 0, writtenItems)
}

package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"hash/fnv"
	"io"
	"log/slog"
	"maps"
	"math/big"
	"net"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
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
)

const dynamoHealthPath = "/healthz"

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
	itemUpdateLockStripeCount   = 256
	tableLockStripeCount        = 128
	tableCleanupDeleteBatchSize = 256
	batchWriteItemMaxItems      = 25
	dynamoMaxRequestBodyBytes   = 1 << 20

	dynamoTableMetaPrefix       = "!ddb|meta|table|"
	dynamoTableGenerationPrefix = "!ddb|meta|gen|"
	dynamoItemPrefix            = "!ddb|item|"
	dynamoGSIPrefix             = "!ddb|gsi|"
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
	updateItemTarget:         "UpdateItem",
}

const (
	dynamoErrValidation        = "ValidationException"
	dynamoErrInternal          = "InternalServerError"
	dynamoErrConditionalFailed = "ConditionalCheckFailedException"
	dynamoErrResourceNotFound  = "ResourceNotFoundException"
	dynamoErrResourceInUse     = "ResourceInUseException"
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
		listen:      listen,
		store:       st,
		coordinator: coordinate,
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

func (d *DynamoDBServer) handle(w http.ResponseWriter, r *http.Request) {
	if serveDynamoHealthz(w, r) {
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

func serveDynamoHealthz(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != dynamoHealthPath {
		return false
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead {
			return true
		}
		_, _ = w.Write([]byte("ok\n"))
		return true
	default:
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return true
	}
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

type createTableAttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"`
}

type createTableKeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"`
}

type createTableGSI struct {
	IndexName  string                        `json:"IndexName"`
	KeySchema  []createTableKeySchemaElement `json:"KeySchema"`
	Projection createTableProjection         `json:"Projection"`
}

type createTableProjection struct {
	ProjectionType   string   `json:"ProjectionType"`
	NonKeyAttributes []string `json:"NonKeyAttributes"`
}

type createTableInput struct {
	TableName              string                           `json:"TableName"`
	AttributeDefinitions   []createTableAttributeDefinition `json:"AttributeDefinitions"`
	KeySchema              []createTableKeySchemaElement    `json:"KeySchema"`
	GlobalSecondaryIndexes []createTableGSI                 `json:"GlobalSecondaryIndexes"`
}

type deleteTableInput struct {
	TableName string `json:"TableName"`
}

type describeTableInput struct {
	TableName string `json:"TableName"`
}

type listTablesInput struct {
	ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
	Limit                   int32  `json:"Limit"`
}

type queryInput struct {
	TableName                 string                    `json:"TableName"`
	IndexName                 string                    `json:"IndexName"`
	KeyConditionExpression    string                    `json:"KeyConditionExpression"`
	FilterExpression          string                    `json:"FilterExpression"`
	ProjectionExpression      string                    `json:"ProjectionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ScanIndexForward          *bool                     `json:"ScanIndexForward"`
	Limit                     *int32                    `json:"Limit"`
	ExclusiveStartKey         map[string]attributeValue `json:"ExclusiveStartKey"`
	Select                    string                    `json:"Select"`
	ConsistentRead            *bool                     `json:"ConsistentRead"`
}

type getItemInput struct {
	TableName                string                    `json:"TableName"`
	Key                      map[string]attributeValue `json:"Key"`
	ProjectionExpression     string                    `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string         `json:"ExpressionAttributeNames"`
	ConsistentRead           *bool                     `json:"ConsistentRead"`
}

type scanInput struct {
	TableName                 string                    `json:"TableName"`
	IndexName                 string                    `json:"IndexName"`
	FilterExpression          string                    `json:"FilterExpression"`
	ProjectionExpression      string                    `json:"ProjectionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ExclusiveStartKey         map[string]attributeValue `json:"ExclusiveStartKey"`
	Limit                     *int32                    `json:"Limit"`
	Select                    string                    `json:"Select"`
	ConsistentRead            *bool                     `json:"ConsistentRead"`
}

type updateItemInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	UpdateExpression          string                    `json:"UpdateExpression"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues              string                    `json:"ReturnValues"`
}

type putItemInput struct {
	TableName                 string                    `json:"TableName"`
	Item                      map[string]attributeValue `json:"Item"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues              string                    `json:"ReturnValues"`
}

type deleteItemInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ReturnValues              string                    `json:"ReturnValues"`
}

type batchWriteItemInput struct {
	RequestItems map[string][]batchWriteRequest `json:"RequestItems"`
}

type batchWriteRequest struct {
	PutRequest    *batchPutRequest    `json:"PutRequest,omitempty"`
	DeleteRequest *batchDeleteRequest `json:"DeleteRequest,omitempty"`
}

type batchPutRequest struct {
	Item map[string]attributeValue `json:"Item"`
}

type batchDeleteRequest struct {
	Key map[string]attributeValue `json:"Key"`
}

type transactWriteItemsInput struct {
	TransactItems []transactWriteItem `json:"TransactItems"`
}

type transactWriteItem struct {
	Put            *putItemInput           `json:"Put,omitempty"`
	Update         *transactUpdateInput    `json:"Update,omitempty"`
	Delete         *transactDeleteInput    `json:"Delete,omitempty"`
	ConditionCheck *transactConditionInput `json:"ConditionCheck,omitempty"`
}

type transactUpdateInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	UpdateExpression          string                    `json:"UpdateExpression"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type transactDeleteInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type transactConditionInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type dynamoKeySchema struct {
	HashKey  string `json:"hash_key"`
	RangeKey string `json:"range_key,omitempty"`
}

type dynamoGSIProjection struct {
	ProjectionType   string   `json:"projection_type"`
	NonKeyAttributes []string `json:"non_key_attributes,omitempty"`
}

type dynamoGlobalSecondaryIndex struct {
	KeySchema  dynamoKeySchema     `json:"key_schema"`
	Projection dynamoGSIProjection `json:"projection"`
}

func (g *dynamoGlobalSecondaryIndex) UnmarshalJSON(b []byte) error {
	type rawGSI struct {
		KeySchema  *dynamoKeySchema     `json:"key_schema"`
		Projection *dynamoGSIProjection `json:"projection"`
		HashKey    string               `json:"hash_key"`
		RangeKey   string               `json:"range_key"`
	}

	var raw rawGSI
	if err := json.Unmarshal(b, &raw); err != nil {
		return errors.WithStack(err)
	}

	if raw.KeySchema != nil {
		g.KeySchema = *raw.KeySchema
	} else {
		g.KeySchema = dynamoKeySchema{
			HashKey:  raw.HashKey,
			RangeKey: raw.RangeKey,
		}
	}

	if raw.Projection != nil && strings.TrimSpace(raw.Projection.ProjectionType) != "" {
		g.Projection = *raw.Projection
	} else {
		// Older schema snapshots stored only the key schema. Those GSIs behaved
		// like ALL projections, so preserve that behavior when normalizing.
		g.Projection = dynamoGSIProjection{ProjectionType: "ALL"}
	}

	return nil
}

type dynamoTableSchema struct {
	TableName               string                                `json:"table_name"`
	AttributeDefinitions    map[string]string                     `json:"attribute_definitions,omitempty"`
	PrimaryKey              dynamoKeySchema                       `json:"primary_key"`
	GlobalSecondaryIndexes  map[string]dynamoGlobalSecondaryIndex `json:"global_secondary_indexes,omitempty"`
	KeyEncodingVersion      int                                   `json:"key_encoding_version,omitempty"`
	MigratingFromGeneration uint64                                `json:"migrating_from_generation,omitempty"`
	Generation              uint64                                `json:"generation"`
}

func (d *DynamoDBServer) createTable(w http.ResponseWriter, r *http.Request) {
	in, err := decodeCreateTableInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	unlock := d.lockTableOperations([]string{in.TableName})
	defer unlock()
	schema, err := buildCreateTableSchema(in)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	if err := d.createTableWithRetry(r.Context(), in.TableName, schema); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	d.observeTables(r.Context(), schema.TableName)
	writeDynamoJSON(w, map[string]any{
		"TableDescription": map[string]any{
			"TableName":   in.TableName,
			"TableStatus": "ACTIVE",
		},
	})
}

func decodeCreateTableInput(bodyReader io.Reader) (createTableInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return createTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in createTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		return createTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return createTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	return in, nil
}

func buildCreateTableSchema(in createTableInput) (*dynamoTableSchema, error) {
	primary, err := parseCreateTableKeySchema(in.KeySchema)
	if err != nil {
		return nil, err
	}
	attrDefs := make(map[string]string, len(in.AttributeDefinitions))
	for _, def := range in.AttributeDefinitions {
		if strings.TrimSpace(def.AttributeName) == "" {
			return nil, errors.New("invalid attribute definition")
		}
		attrDefs[def.AttributeName] = def.AttributeType
	}
	gsis := make(map[string]dynamoGlobalSecondaryIndex, len(in.GlobalSecondaryIndexes))
	for _, gsi := range in.GlobalSecondaryIndexes {
		if strings.TrimSpace(gsi.IndexName) == "" {
			return nil, errors.New("invalid global secondary index")
		}
		ks, err := parseCreateTableKeySchema(gsi.KeySchema)
		if err != nil {
			return nil, err
		}
		projection, err := buildCreateTableProjection(gsi.Projection)
		if err != nil {
			return nil, err
		}
		gsis[gsi.IndexName] = dynamoGlobalSecondaryIndex{
			KeySchema:  ks,
			Projection: projection,
		}
	}
	return &dynamoTableSchema{
		TableName:              in.TableName,
		AttributeDefinitions:   attrDefs,
		PrimaryKey:             primary,
		GlobalSecondaryIndexes: gsis,
		KeyEncodingVersion:     dynamoOrderedKeyEncodingV2,
	}, nil
}

func buildCreateTableProjection(in createTableProjection) (dynamoGSIProjection, error) {
	switch strings.TrimSpace(in.ProjectionType) {
	case "", "ALL":
		return dynamoGSIProjection{ProjectionType: "ALL"}, nil
	case "KEYS_ONLY":
		return dynamoGSIProjection{ProjectionType: "KEYS_ONLY"}, nil
	case "INCLUDE":
		return dynamoGSIProjection{
			ProjectionType:   "INCLUDE",
			NonKeyAttributes: append([]string(nil), in.NonKeyAttributes...),
		}, nil
	default:
		return dynamoGSIProjection{}, errors.New("invalid projection")
	}
}

func (d *DynamoDBServer) createTableWithRetry(ctx context.Context, tableName string, baseSchema *dynamoTableSchema) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := d.nextTxnReadTS()
		exists, err := d.tableExistsAt(ctx, tableName, readTS)
		if err != nil {
			return err
		}
		if exists {
			return newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceInUse, "table already exists")
		}
		nextGeneration, err := d.nextTableGenerationAt(ctx, tableName, readTS)
		if err != nil {
			return err
		}
		req, err := makeCreateTableRequest(baseSchema, nextGeneration)
		if err != nil {
			return err
		}
		if _, err := d.coordinator.Dispatch(ctx, req); err == nil {
			return nil
		}
		if !isRetryableTransactWriteError(err) {
			return errors.WithStack(err)
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "create table retry attempts exhausted")
}

func (d *DynamoDBServer) tableExistsAt(ctx context.Context, tableName string, readTS uint64) (bool, error) {
	_, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return exists, nil
}

func (d *DynamoDBServer) nextTableGenerationAt(ctx context.Context, tableName string, readTS uint64) (uint64, error) {
	lastGeneration, err := d.loadTableGenerationAt(ctx, tableName, readTS)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return lastGeneration + 1, nil
}

func makeCreateTableRequest(baseSchema *dynamoTableSchema, nextGeneration uint64) (*kv.OperationGroup[kv.OP], error) {
	schema := &dynamoTableSchema{
		TableName:               baseSchema.TableName,
		AttributeDefinitions:    baseSchema.AttributeDefinitions,
		PrimaryKey:              baseSchema.PrimaryKey,
		GlobalSecondaryIndexes:  baseSchema.GlobalSecondaryIndexes,
		KeyEncodingVersion:      baseSchema.KeyEncodingVersion,
		MigratingFromGeneration: baseSchema.MigratingFromGeneration,
		Generation:              nextGeneration,
	}
	schemaBytes, err := encodeStoredDynamoTableSchema(schema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: 0,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: dynamoTableMetaKey(baseSchema.TableName), Value: schemaBytes},
			{Op: kv.Put, Key: dynamoTableGenerationKey(baseSchema.TableName), Value: []byte(strconv.FormatUint(nextGeneration, 10))},
		},
	}, nil
}

func (d *DynamoDBServer) deleteTable(w http.ResponseWriter, r *http.Request) {
	in, err := decodeDeleteTableInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	unlock := d.lockTableOperations([]string{in.TableName})
	defer unlock()
	if err := d.deleteTableWithRetry(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	resp := map[string]any{
		"TableDescription": map[string]any{
			"TableName":   in.TableName,
			"TableStatus": "DELETING",
		},
	}
	writeDynamoJSON(w, resp)
}

func decodeDeleteTableInput(bodyReader io.Reader) (deleteTableInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return deleteTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in deleteTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		return deleteTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return deleteTableInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	return in, nil
}

func (d *DynamoDBServer) deleteTableWithRetry(ctx context.Context, tableName string) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := d.nextTxnReadTS()
		schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists {
			return newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
		}

		req := &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: 0,
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Del, Key: dynamoTableMetaKey(tableName)},
			},
		}
		if _, err := d.coordinator.Dispatch(ctx, req); err != nil {
			if !isRetryableTransactWriteError(err) {
				return errors.WithStack(err)
			}
		} else {
			d.launchDeletedTableCleanup(tableName, schema.Generation)
			return nil
		}

		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "delete table retry attempts exhausted")
}

func (d *DynamoDBServer) launchDeletedTableCleanup(tableName string, generation uint64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), tableCleanupAsyncTimeout)
		defer cancel()
		if err := d.cleanupDeletedTableGeneration(ctx, tableName, generation); err != nil {
			slog.Error("dynamodb delete table cleanup failed",
				"table", tableName,
				"generation", generation,
				"error", err,
			)
		}
	}()
}

func (d *DynamoDBServer) cleanupDeletedTableGeneration(ctx context.Context, tableName string, generation uint64) error {
	prefixes := [][]byte{
		dynamoItemPrefixForTable(tableName, generation),
		dynamoGSIPrefixForTable(tableName, generation),
	}
	for {
		keys, err := d.scanDeleteCandidateKeysByPrefixes(ctx, prefixes)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if err := d.deleteKeysByBatches(ctx, keys); err != nil {
			return err
		}
		if err := waitTransactRetryBackoff(ctx, transactRetryInitialBackoff); err != nil {
			return errors.WithStack(err)
		}
	}
}

func (d *DynamoDBServer) scanDeleteCandidateKeysByPrefixes(ctx context.Context, prefixes [][]byte) ([][]byte, error) {
	keys := make([][]byte, 0, len(prefixes)*dynamoScanPageLimit)
	for _, prefix := range prefixes {
		prefixKeys, err := d.scanDeleteCandidateKeys(ctx, prefix)
		if err != nil {
			return nil, err
		}
		keys = append(keys, prefixKeys...)
	}
	return uniqueKeys(keys), nil
}

func (d *DynamoDBServer) scanDeleteCandidateKeys(ctx context.Context, prefix []byte) ([][]byte, error) {
	kvs, err := d.scanAllByPrefix(ctx, prefix)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	keys := make([][]byte, 0, len(kvs))
	for _, kvp := range kvs {
		if !bytes.HasPrefix(kvp.Key, prefix) {
			continue
		}
		keys = append(keys, kvp.Key)
	}
	return keys, nil
}

func (d *DynamoDBServer) deleteKeysByBatches(ctx context.Context, keys [][]byte) error {
	for start := 0; start < len(keys); start += tableCleanupDeleteBatchSize {
		end := min(start+tableCleanupDeleteBatchSize, len(keys))
		if err := d.dispatchDeleteBatch(ctx, keys[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (d *DynamoDBServer) dispatchDeleteBatch(ctx context.Context, keys [][]byte) error {
	elems := make([]*kv.Elem[kv.OP], 0, len(keys))
	for _, key := range keys {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}
	req := &kv.OperationGroup[kv.OP]{
		IsTxn: false,
		Elems: elems,
	}
	_, err := d.coordinator.Dispatch(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DynamoDBServer) describeTable(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	var in describeTableInput
	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	if strings.TrimSpace(in.TableName) == "" {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, "missing table name")
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	schema, exists, err := d.loadTableSchema(r.Context(), in.TableName)
	if err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return
	}
	if !exists {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
		return
	}
	writeDynamoJSON(w, map[string]any{"Table": describeTableShape(schema)})
}

func (d *DynamoDBServer) listTables(w http.ResponseWriter, r *http.Request) {
	in, err := decodeListTablesInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	names, err := d.listTableNames(r.Context())
	if err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return
	}
	outNames, hasNext := paginateTableNames(names, in)

	resp := map[string]any{"TableNames": outNames}
	if hasNext && len(outNames) > 0 {
		resp["LastEvaluatedTableName"] = outNames[len(outNames)-1]
	}
	writeDynamoJSON(w, resp)
}

func decodeListTablesInput(bodyReader io.Reader) (listTablesInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return listTablesInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in listTablesInput
	if len(bytes.TrimSpace(body)) == 0 {
		return in, nil
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return listTablesInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return in, nil
}

func paginateTableNames(names []string, in listTablesInput) ([]string, bool) {
	start := findExclusiveStartIndex(names, in.ExclusiveStartTableName)
	limit := resolveTableListLimit(in.Limit, len(names))
	end := min(start+limit, len(names))
	return names[start:end], end < len(names)
}

func findExclusiveStartIndex(names []string, startName string) int {
	if startName == "" {
		return 0
	}
	for i, name := range names {
		if name == startName {
			return i + 1
		}
	}
	return 0
}

func resolveTableListLimit(limit int32, tableCount int) int {
	if limit <= 0 || int(limit) >= tableCount {
		return tableCount
	}
	return int(limit)
}

func (d *DynamoDBServer) listTableNames(ctx context.Context) ([]string, error) {
	kvs, err := d.scanAllByPrefix(ctx, []byte(dynamoTableMetaPrefix))
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(kvs))
	for _, kvp := range kvs {
		name, ok := tableNameFromMetaKey(kvp.Key)
		if !ok {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (d *DynamoDBServer) putItem(w http.ResponseWriter, r *http.Request) {
	in, err := decodePutItemInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	plan, err := d.putItemWithRetry(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	d.observeWrittenItems(r.Context(), in.TableName, 1)
	resp := map[string]any{}
	if attrs := putItemReturnAttributes(in.ReturnValues, plan.current); len(attrs) > 0 {
		resp["Attributes"] = attrs
	}
	writeDynamoJSON(w, resp)
}

func decodePutItemInput(bodyReader io.Reader) (putItemInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return putItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in putItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return putItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return putItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	if err := validatePutItemReturnValues(in.ReturnValues); err != nil {
		return putItemInput{}, err
	}
	return in, nil
}

func validatePutItemReturnValues(returnValues string) error {
	switch strings.TrimSpace(returnValues) {
	case "", dynamoReturnValueNone, dynamoReturnValueAllOld:
		return nil
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported ReturnValues")
	}
}

func (d *DynamoDBServer) putItemWithRetry(ctx context.Context, in putItemInput) (*itemWritePlan, error) {
	return d.retryItemWriteWithGeneration(
		ctx,
		in.TableName,
		"put item retry attempts exhausted",
		func(readTS uint64) (*itemWritePlan, error) {
			return d.preparePutItemWrite(ctx, in, readTS)
		},
	)
}

type itemWritePlan struct {
	req        *kv.OperationGroup[kv.OP]
	generation uint64
	cleanup    [][]byte
	current    map[string]attributeValue
	next       map[string]attributeValue
}

func (d *DynamoDBServer) retryItemWriteWithGeneration(
	ctx context.Context,
	tableName string,
	exhaustedMessage string,
	prepare func(readTS uint64) (*itemWritePlan, error),
) (*itemWritePlan, error) {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := d.nextTxnReadTS()
		plan, err := prepare(readTS)
		if err != nil {
			return nil, err
		}
		if plan.req == nil {
			return plan, nil
		}
		plan.req.StartTS = readTS
		if err = d.commitItemWrite(ctx, plan.req); err != nil {
			if !isRetryableTransactWriteError(err) {
				return nil, errors.WithStack(err)
			}
		} else {
			retry, verifyErr := d.handleGenerationFenceResult(
				ctx,
				d.verifyTableGeneration(ctx, tableName, plan.generation),
				plan.cleanup,
			)
			if verifyErr != nil {
				return nil, verifyErr
			}
			if !retry {
				return plan, nil
			}
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return nil, errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return nil, newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, exhaustedMessage)
}

func (d *DynamoDBServer) preparePutItemWrite(ctx context.Context, in putItemInput, readTS uint64) (*itemWritePlan, error) {
	schema, exists, err := d.loadTableSchemaAt(ctx, in.TableName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	itemKey, err := schema.itemKeyFromAttributes(in.Item)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	keyAttrs, err := primaryKeyAttributes(schema.PrimaryKey, in.Item)
	if err != nil {
		return nil, err
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, keyAttrs, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var current map[string]attributeValue
	if found {
		current = currentLocation.item
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	req, cleanup, err := buildItemWriteRequestWithSource(schema, itemKey, in.Item, currentLocation)
	if err != nil {
		return nil, err
	}
	return &itemWritePlan{
		req:        req,
		generation: schema.Generation,
		cleanup:    cleanup,
		current:    cloneAttributeValueMap(current),
		next:       cloneAttributeValueMap(in.Item),
	}, nil
}

func (d *DynamoDBServer) commitItemWrite(ctx context.Context, req *kv.OperationGroup[kv.OP]) error {
	_, err := d.coordinator.Dispatch(ctx, req)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DynamoDBServer) getItem(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	var in getItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	if strings.TrimSpace(in.TableName) == "" {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, "missing table name")
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}

	readTS := d.resolveDynamoReadTS(in.ConsistentRead)
	schema, exists, err := d.loadTableSchemaAt(r.Context(), in.TableName, readTS)
	if err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return
	}
	if !exists {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
		return
	}

	current, found, err := d.readLogicalItemAt(r.Context(), schema, in.Key, readTS)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	if !found {
		writeDynamoJSON(w, map[string]any{})
		return
	}
	d.observeReadMetrics(r.Context(), in.TableName, 1, 1)
	projected, err := projectItem(current.item, in.ProjectionExpression, in.ExpressionAttributeNames)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	writeDynamoJSON(w, map[string]any{"Item": projected})
}

func (d *DynamoDBServer) deleteItem(w http.ResponseWriter, r *http.Request) {
	in, shouldReturnOld, err := decodeDeleteItemInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	lockKey, err := dynamoItemUpdateLockKey(in.TableName, in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	unlock := d.lockItemUpdate(lockKey)
	defer unlock()
	plan, err := d.deleteItemWithRetry(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if len(plan.current) > 0 {
		d.observeWrittenItems(r.Context(), in.TableName, 1)
	}
	resp := map[string]any{}
	if shouldReturnOld && len(plan.current) > 0 {
		resp["Attributes"] = plan.current
	}
	writeDynamoJSON(w, resp)
}

func decodeDeleteItemInput(bodyReader io.Reader) (deleteItemInput, bool, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return deleteItemInput{}, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in deleteItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return deleteItemInput{}, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return deleteItemInput{}, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	shouldReturnOld, err := parseDeleteItemReturnValues(in.ReturnValues)
	if err != nil {
		return deleteItemInput{}, false, err
	}
	return in, shouldReturnOld, nil
}

func parseDeleteItemReturnValues(returnValues string) (bool, error) {
	switch strings.TrimSpace(returnValues) {
	case "", dynamoReturnValueNone:
		return false, nil
	case dynamoReturnValueAllOld:
		return true, nil
	default:
		return false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported ReturnValues")
	}
}

type deleteItemPlan struct {
	req        *kv.OperationGroup[kv.OP]
	generation uint64
	current    map[string]attributeValue
}

func (d *DynamoDBServer) deleteItemWithRetry(ctx context.Context, in deleteItemInput) (*deleteItemPlan, error) {
	var deletePlan *deleteItemPlan
	_, err := d.retryItemWriteWithGeneration(
		ctx,
		in.TableName,
		"delete retry attempts exhausted",
		func(readTS uint64) (*itemWritePlan, error) {
			var err error
			deletePlan, err = d.prepareDeleteItemWrite(ctx, in, readTS)
			if err != nil {
				return nil, err
			}
			return &itemWritePlan{
				req:        deletePlan.req,
				generation: deletePlan.generation,
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return deletePlan, nil
}

func (d *DynamoDBServer) prepareDeleteItemWrite(ctx context.Context, in deleteItemInput, readTS uint64) (*deleteItemPlan, error) {
	schema, exists, err := d.loadTableSchemaAt(ctx, in.TableName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, in.Key, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	current := map[string]attributeValue(nil)
	if found {
		current = currentLocation.item
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	if !found {
		return &deleteItemPlan{current: nil}, nil
	}
	req, err := buildItemDeleteRequestWithSource(currentLocation)
	if err != nil {
		return nil, err
	}
	return &deleteItemPlan{
		req:        req,
		generation: schema.Generation,
		current:    cloneAttributeValueMap(current),
	}, nil
}

func (d *DynamoDBServer) updateItem(w http.ResponseWriter, r *http.Request) {
	in, err := decodeUpdateItemInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.ensureLegacyTableMigration(r.Context(), in.TableName); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	lockKey, err := dynamoItemUpdateLockKey(in.TableName, in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	unlock := d.lockItemUpdate(lockKey)
	defer unlock()
	plan, err := d.updateItemWithRetry(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	d.observeWrittenItems(r.Context(), in.TableName, 1)
	resp := map[string]any{}
	if attrs := updateItemReturnAttributes(in.ReturnValues, plan.current, plan.next); len(attrs) > 0 {
		resp["Attributes"] = attrs
	}
	writeDynamoJSON(w, resp)
}

func decodeUpdateItemInput(bodyReader io.Reader) (updateItemInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return updateItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in updateItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return updateItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return updateItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	if err := validateUpdateItemReturnValues(in.ReturnValues); err != nil {
		return updateItemInput{}, err
	}
	return in, nil
}

func validateUpdateItemReturnValues(returnValues string) error {
	switch strings.TrimSpace(returnValues) {
	case "",
		dynamoReturnValueNone,
		dynamoReturnValueAllOld,
		dynamoReturnValueUpdatedOld,
		dynamoReturnValueAllNew,
		dynamoReturnValueUpdatedNew:
		return nil
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported ReturnValues")
	}
}

func (d *DynamoDBServer) updateItemWithRetry(ctx context.Context, in updateItemInput) (*itemWritePlan, error) {
	return d.retryItemWriteWithGeneration(
		ctx,
		in.TableName,
		"update retry attempts exhausted",
		func(readTS uint64) (*itemWritePlan, error) {
			return d.prepareUpdateItemWrite(ctx, in, readTS)
		},
	)
}

func (d *DynamoDBServer) prepareUpdateItemWrite(ctx context.Context, in updateItemInput, readTS uint64) (*itemWritePlan, error) {
	schema, exists, err := d.loadTableSchemaAt(ctx, in.TableName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	itemKey, err := schema.itemKeyFromAttributes(in.Key)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, in.Key, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var current map[string]attributeValue
	if !found {
		current = map[string]attributeValue{}
	} else {
		current = currentLocation.item
	}
	nextItem, err := buildUpdatedItem(schema, in, current)
	if err != nil {
		return nil, err
	}
	req, cleanup, err := buildItemWriteRequestWithSource(schema, itemKey, nextItem, currentLocation)
	if err != nil {
		return nil, err
	}
	return &itemWritePlan{
		req:        req,
		generation: schema.Generation,
		cleanup:    cleanup,
		current:    cloneAttributeValueMap(current),
		next:       cloneAttributeValueMap(nextItem),
	}, nil
}

func buildUpdatedItem(schema *dynamoTableSchema, in updateItemInput, current map[string]attributeValue) (map[string]attributeValue, error) {
	if err := validateConditionOnItem(in.ConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, current); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	nextItem := cloneAttributeValueMap(current)
	maps.Copy(nextItem, in.Key)
	if err := applyUpdateExpression(in.UpdateExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, nextItem); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if err := ensurePrimaryKeyUnchanged(schema.PrimaryKey, in.Key, nextItem); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return nextItem, nil
}

func ensurePrimaryKeyUnchanged(keySchema dynamoKeySchema, originalKey map[string]attributeValue, nextItem map[string]attributeValue) error {
	if err := ensureSinglePrimaryKeyUnchanged(keySchema.HashKey, originalKey, nextItem); err != nil {
		return err
	}
	if keySchema.RangeKey != "" {
		if err := ensureSinglePrimaryKeyUnchanged(keySchema.RangeKey, originalKey, nextItem); err != nil {
			return err
		}
	}
	return nil
}

func ensureSinglePrimaryKeyUnchanged(attrName string, originalKey map[string]attributeValue, nextItem map[string]attributeValue) error {
	keyVal, ok := originalKey[attrName]
	if !ok {
		return errors.New("missing key attribute")
	}
	nextVal, ok := nextItem[attrName]
	if !ok {
		return errors.New("cannot remove key attribute")
	}
	if !attributeValueEqual(keyVal, nextVal) {
		return errors.New("cannot update primary key attribute")
	}
	return nil
}

type dynamoItemLocation struct {
	schema *dynamoTableSchema
	key    []byte
	item   map[string]attributeValue
}

func buildItemWriteRequestWithSource(
	targetSchema *dynamoTableSchema,
	targetKey []byte,
	nextItem map[string]attributeValue,
	current *dynamoItemLocation,
) (*kv.OperationGroup[kv.OP], [][]byte, error) {
	payload, err := encodeStoredDynamoItem(nextItem)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: targetKey, Value: payload}}
	cleanup := [][]byte{targetKey}
	delKeys, putKeys, err := itemStorageDelta(targetSchema, targetKey, nextItem, current)
	if err != nil {
		return nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	for _, key := range delKeys {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}
	for _, key := range putKeys {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: targetKey})
		cleanup = append(cleanup, key)
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: 0,
		Elems:   elems,
	}, cleanup, nil
}

func itemStorageDelta(
	targetSchema *dynamoTableSchema,
	targetKey []byte,
	nextItem map[string]attributeValue,
	current *dynamoItemLocation,
) ([][]byte, [][]byte, error) {
	oldKeys, err := itemStorageKeys(current)
	if err != nil {
		return nil, nil, err
	}
	newKeys, err := targetSchema.gsiEntryKeysForItem(nextItem)
	if err != nil {
		return nil, nil, err
	}
	newSet := bytesToSet(newKeys)
	oldSet := bytesToSet(oldKeys)
	delete(oldSet, string(targetKey))
	delKeys := make([][]byte, 0, len(oldKeys))
	for key, raw := range oldSet {
		if _, ok := newSet[key]; ok {
			continue
		}
		delKeys = append(delKeys, raw)
	}
	putKeys := make([][]byte, 0, len(newKeys))
	for key, raw := range newSet {
		if _, ok := oldSet[key]; ok {
			continue
		}
		putKeys = append(putKeys, raw)
	}
	return delKeys, putKeys, nil
}

func itemStorageKeys(current *dynamoItemLocation) ([][]byte, error) {
	if current == nil || len(current.item) == 0 {
		return nil, nil
	}
	gsiKeys, err := current.schema.gsiEntryKeysForItem(current.item)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(gsiKeys)+1)
	out = append(out, bytes.Clone(current.key))
	out = append(out, gsiKeys...)
	return out, nil
}

func bytesToSet(keys [][]byte) map[string][]byte {
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		out[string(key)] = key
	}
	return out
}

func putItemReturnAttributes(returnValues string, current map[string]attributeValue) map[string]attributeValue {
	if !strings.EqualFold(strings.TrimSpace(returnValues), dynamoReturnValueAllOld) || len(current) == 0 {
		return nil
	}
	return cloneAttributeValueMap(current)
}

func updateItemReturnAttributes(returnValues string, current map[string]attributeValue, next map[string]attributeValue) map[string]attributeValue {
	switch strings.TrimSpace(returnValues) {
	case "", dynamoReturnValueNone:
		return nil
	case dynamoReturnValueAllOld:
		if len(current) == 0 {
			return nil
		}
		return cloneAttributeValueMap(current)
	case dynamoReturnValueAllNew:
		return cloneAttributeValueMap(next)
	case dynamoReturnValueUpdatedOld:
		return selectUpdatedAttributes(current, next, true)
	case dynamoReturnValueUpdatedNew:
		return selectUpdatedAttributes(current, next, false)
	default:
		return nil
	}
}

func selectUpdatedAttributes(current map[string]attributeValue, next map[string]attributeValue, oldValues bool) map[string]attributeValue {
	keys := updatedAttributeNames(current, next)
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]attributeValue, len(keys))
	for _, key := range keys {
		if oldValues {
			if value, ok := current[key]; ok {
				out[key] = value
			}
			continue
		}
		if value, ok := next[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func updatedAttributeNames(current map[string]attributeValue, next map[string]attributeValue) []string {
	seen := make(map[string]struct{}, len(current)+len(next))
	for name := range current {
		seen[name] = struct{}{}
	}
	for name := range next {
		seen[name] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		oldVal, oldOK := current[name]
		newVal, newOK := next[name]
		if !oldOK && !newOK {
			continue
		}
		if oldOK && newOK && attributeValueEqual(oldVal, newVal) {
			continue
		}
		out = append(out, name)
	}
	return out
}

func (d *DynamoDBServer) query(w http.ResponseWriter, r *http.Request) {
	in, err := decodeQueryInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	out, err := d.queryItems(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	d.observeReadMetrics(r.Context(), in.TableName, out.count, out.scannedCount)
	resp := map[string]any{
		"Items":        out.items,
		"Count":        out.count,
		"ScannedCount": out.scannedCount,
	}
	if len(out.lastEvaluatedKey) > 0 {
		resp["LastEvaluatedKey"] = out.lastEvaluatedKey
	}
	writeDynamoJSON(w, resp)
}

func (d *DynamoDBServer) scan(w http.ResponseWriter, r *http.Request) {
	in, err := decodeScanInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	out, err := d.scanItems(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	d.observeReadMetrics(r.Context(), in.TableName, out.count, out.scannedCount)
	resp := map[string]any{
		"Items":        out.items,
		"Count":        out.count,
		"ScannedCount": out.scannedCount,
	}
	if len(out.lastEvaluatedKey) > 0 {
		resp["LastEvaluatedKey"] = out.lastEvaluatedKey
	}
	writeDynamoJSON(w, resp)
}

func decodeQueryInput(bodyReader io.Reader) (queryInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return queryInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in queryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return queryInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return queryInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	if err := validateReadSelect(in.Select); err != nil {
		return queryInput{}, err
	}
	if _, _, err := resolveReadLimit(in.Limit); err != nil {
		return queryInput{}, err
	}
	return in, nil
}

func decodeScanInput(bodyReader io.Reader) (scanInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return scanInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in scanInput
	if err := json.Unmarshal(body, &in); err != nil {
		return scanInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.TableName) == "" {
		return scanInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
	}
	if err := validateReadSelect(in.Select); err != nil {
		return scanInput{}, err
	}
	if _, _, err := resolveReadLimit(in.Limit); err != nil {
		return scanInput{}, err
	}
	return in, nil
}

type queryOutput struct {
	items            []map[string]attributeValue
	count            int
	scannedCount     int
	lastEvaluatedKey map[string]attributeValue
}

type readPageOptions struct {
	filterExpression          string
	projectionExpression      string
	expressionAttributeNames  map[string]string
	expressionAttributeValues map[string]attributeValue
	exclusiveStartKey         map[string]attributeValue
	limit                     *int32
	selectValue               string
	lastEvaluatedKeyBuilder   func(map[string]attributeValue) map[string]attributeValue
}

type dynamoReadIterator interface {
	Next(context.Context) (map[string]attributeValue, bool, error)
}

type queryRangeOperator string

const (
	queryRangeOpEqual      queryRangeOperator = "="
	queryRangeOpLessThan   queryRangeOperator = "<"
	queryRangeOpLessOrEq   queryRangeOperator = "<="
	queryRangeOpGreater    queryRangeOperator = ">"
	queryRangeOpGreaterEq  queryRangeOperator = ">="
	queryRangeOpBetween    queryRangeOperator = "BETWEEN"
	queryRangeOpBeginsWith queryRangeOperator = "BEGINS_WITH"
)

type queryRangeCondition struct {
	attr   string
	op     queryRangeOperator
	value1 attributeValue
	value2 attributeValue
}

type queryCondition struct {
	hashAttr  string
	hashValue attributeValue
	rangeCond *queryRangeCondition
}

func (d *DynamoDBServer) queryItems(ctx context.Context, in queryInput) (*queryOutput, error) {
	schema, readTS, err := d.prepareReadSchema(ctx, in.TableName, in.IndexName, in.Select, in.ProjectionExpression, in.ExpressionAttributeNames, in.ConsistentRead)
	if err != nil {
		return nil, err
	}
	readPin := d.pinReadTS(readTS)
	defer readPin.Release()
	keySchema, cond, err := resolveQueryCondition(in, schema)
	if err != nil {
		return nil, err
	}
	opts := readPageOptions{
		filterExpression:          in.FilterExpression,
		projectionExpression:      in.ProjectionExpression,
		expressionAttributeNames:  in.ExpressionAttributeNames,
		expressionAttributeValues: in.ExpressionAttributeValues,
		exclusiveStartKey:         in.ExclusiveStartKey,
		limit:                     in.Limit,
		selectValue:               in.Select,
		lastEvaluatedKeyBuilder: func(item map[string]attributeValue) map[string]attributeValue {
			return makeReadLastEvaluatedKey(schema.PrimaryKey, keySchema, item)
		},
	}
	if schema.MigratingFromGeneration == 0 {
		if out, ok, err := d.streamQueryItems(ctx, in, schema, keySchema, cond, readTS, opts); ok || err != nil {
			return out, err
		}
	}
	items, err := d.loadQueryItemsWithMigration(ctx, in, schema, keySchema, cond, readTS)
	if err != nil {
		return nil, err
	}
	items, err = projectReadItemsForIndex(schema, in.IndexName, items)
	if err != nil {
		return nil, err
	}
	orderQueryItems(items, keySchema.RangeKey, in.ScanIndexForward)
	return finalizeReadPage(schema, items, opts)
}

func (d *DynamoDBServer) scanItems(ctx context.Context, in scanInput) (*queryOutput, error) {
	schema, readTS, err := d.prepareReadSchema(ctx, in.TableName, in.IndexName, in.Select, in.ProjectionExpression, in.ExpressionAttributeNames, in.ConsistentRead)
	if err != nil {
		return nil, err
	}
	readPin := d.pinReadTS(readTS)
	defer readPin.Release()
	indexKeySchema := schema.PrimaryKey
	if strings.TrimSpace(in.IndexName) != "" {
		indexKeySchema, err = schema.keySchemaForQuery(in.IndexName)
		if err != nil {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
		}
	}
	opts := readPageOptions{
		filterExpression:          in.FilterExpression,
		projectionExpression:      in.ProjectionExpression,
		expressionAttributeNames:  in.ExpressionAttributeNames,
		expressionAttributeValues: in.ExpressionAttributeValues,
		exclusiveStartKey:         in.ExclusiveStartKey,
		limit:                     in.Limit,
		selectValue:               in.Select,
		lastEvaluatedKeyBuilder: func(item map[string]attributeValue) map[string]attributeValue {
			return makeReadLastEvaluatedKey(schema.PrimaryKey, indexKeySchema, item)
		},
	}
	if schema.MigratingFromGeneration == 0 {
		if out, ok, err := d.streamScanItems(ctx, in, schema, readTS, opts); ok || err != nil {
			return out, err
		}
	}
	items, err := d.loadScanItemsWithMigration(ctx, in, schema, indexKeySchema, readTS)
	if err != nil {
		return nil, err
	}
	items, err = projectReadItemsForIndex(schema, in.IndexName, items)
	if err != nil {
		return nil, err
	}
	return finalizeReadPage(schema, items, opts)
}

func (d *DynamoDBServer) prepareReadSchema(
	ctx context.Context,
	tableName string,
	indexName string,
	selectValue string,
	projectionExpression string,
	names map[string]string,
	consistentRead *bool,
) (*dynamoTableSchema, uint64, error) {
	if err := d.ensureLegacyTableMigration(ctx, tableName); err != nil {
		return nil, 0, err
	}
	readTS := d.resolveDynamoReadTS(consistentRead)
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	if !exists {
		return nil, 0, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	if err := validateGSIReadOptions(schema, indexName, selectValue, projectionExpression, names, consistentRead); err != nil {
		return nil, 0, err
	}
	return schema, readTS, nil
}

func (d *DynamoDBServer) loadQueryItemsWithMigration(
	ctx context.Context,
	in queryInput,
	schema *dynamoTableSchema,
	keySchema dynamoKeySchema,
	cond queryCondition,
	readTS uint64,
) ([]map[string]attributeValue, error) {
	items, err := d.queryItemsByKeyCondition(ctx, in, schema, keySchema, cond, readTS)
	if err != nil {
		return nil, err
	}
	return d.mergeReadItemsFromSourceSchema(schema, keySchema, items, func(sourceSchema *dynamoTableSchema) ([]map[string]attributeValue, error) {
		return d.queryItemsByKeyCondition(ctx, in, sourceSchema, keySchema, cond, readTS)
	})
}

func (d *DynamoDBServer) loadScanItemsWithMigration(
	ctx context.Context,
	in scanInput,
	schema *dynamoTableSchema,
	indexKeySchema dynamoKeySchema,
	readTS uint64,
) ([]map[string]attributeValue, error) {
	items, err := d.scanItemsBySource(ctx, in, schema, readTS)
	if err != nil {
		return nil, err
	}
	return d.mergeReadItemsFromSourceSchema(schema, indexKeySchema, items, func(sourceSchema *dynamoTableSchema) ([]map[string]attributeValue, error) {
		return d.scanItemsBySource(ctx, in, sourceSchema, readTS)
	})
}

func (d *DynamoDBServer) mergeReadItemsFromSourceSchema(
	schema *dynamoTableSchema,
	orderKey dynamoKeySchema,
	items []map[string]attributeValue,
	loadSource func(*dynamoTableSchema) ([]map[string]attributeValue, error),
) ([]map[string]attributeValue, error) {
	sourceSchema := schema.migrationSourceSchema()
	if sourceSchema == nil {
		return items, nil
	}
	sourceItems, err := loadSource(sourceSchema)
	if err != nil {
		return nil, err
	}
	return mergeMigratingReadItems(schema.PrimaryKey, orderKey, items, sourceItems)
}

func (d *DynamoDBServer) resolveDynamoReadTS(consistentRead *bool) uint64 {
	if consistentRead != nil && *consistentRead {
		return d.nextTxnReadTS()
	}
	return snapshotTS(d.coordinator.Clock(), d.store)
}

func validateGSIReadOptions(
	schema *dynamoTableSchema,
	indexName string,
	selectValue string,
	projectionExpression string,
	names map[string]string,
	consistentRead *bool,
) error {
	if strings.TrimSpace(indexName) == "" {
		return nil
	}
	attrs, err := resolveProjectionAttributes(projectionExpression, names)
	if err != nil {
		return err
	}
	return validateProjectedGSIRead(schema, indexName, selectValue, attrs, consistentRead)
}

func validateProjectedGSIRead(
	schema *dynamoTableSchema,
	indexName string,
	selectValue string,
	attrs []string,
	consistentRead *bool,
) error {
	if err := validateGSIConsistentRead(consistentRead); err != nil {
		return err
	}
	allProjected, projected, err := schema.gsiProjectedAttributeSet(indexName)
	if err != nil {
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if err := validateGSISelectValue(selectValue, allProjected); err != nil {
		return err
	}
	return validateProjectedAttributes(attrs, projected, allProjected)
}

func validateGSIConsistentRead(consistentRead *bool) error {
	if consistentRead == nil || !*consistentRead {
		return nil
	}
	return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "ConsistentRead is not supported on global secondary indexes")
}

func validateGSISelectValue(selectValue string, allProjected bool) error {
	if !strings.EqualFold(strings.TrimSpace(selectValue), "ALL_ATTRIBUTES") || allProjected {
		return nil
	}
	return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "ALL_ATTRIBUTES is not supported for this index projection")
}

func validateProjectedAttributes(attrs []string, projected map[string]struct{}, allProjected bool) error {
	if allProjected || len(attrs) == 0 {
		return nil
	}
	for _, attr := range attrs {
		if _, ok := projected[attr]; ok {
			continue
		}
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "requested attribute is not projected into index")
	}
	return nil
}

func projectReadItemsForIndex(schema *dynamoTableSchema, indexName string, items []map[string]attributeValue) ([]map[string]attributeValue, error) {
	if strings.TrimSpace(indexName) == "" || len(items) == 0 {
		return items, nil
	}
	out := make([]map[string]attributeValue, 0, len(items))
	for _, item := range items {
		projected, err := schema.projectItemForIndex(indexName, item)
		if err != nil {
			return nil, err
		}
		out = append(out, projected)
	}
	return out, nil
}

func resolveQueryCondition(in queryInput, schema *dynamoTableSchema) (dynamoKeySchema, queryCondition, error) {
	keySchema, err := schema.keySchemaForQuery(in.IndexName)
	if err != nil {
		return dynamoKeySchema{}, queryCondition{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	keyExpr, err := replaceNames(in.KeyConditionExpression, in.ExpressionAttributeNames)
	if err != nil {
		return dynamoKeySchema{}, queryCondition{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	parsed, err := parseKeyConditionExpression(keyExpr)
	if err != nil {
		return dynamoKeySchema{}, queryCondition{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	cond, err := buildQueryCondition(keySchema, parsed, in.ExpressionAttributeValues)
	if err != nil {
		return dynamoKeySchema{}, queryCondition{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return keySchema, cond, nil
}

func filterQueryItems(kvs []*store.KVPair, cond queryCondition) ([]map[string]attributeValue, error) {
	items := make([]map[string]attributeValue, 0, len(kvs))
	for _, kvp := range kvs {
		item, err := decodeStoredDynamoItem(kvp.Value)
		if err != nil {
			return nil, err
		}
		if !matchesQueryCondition(item, cond) {
			continue
		}
		items = append(items, item)
	}
	return items, nil
}

func orderQueryItems(items []map[string]attributeValue, rangeKey string, scanIndexForward *bool) {
	if rangeKey != "" {
		sort.SliceStable(items, func(i, j int) bool {
			return compareAttributeValueSortKey(items[i][rangeKey], items[j][rangeKey]) < 0
		})
	}
	scanForward := true
	if scanIndexForward != nil {
		scanForward = *scanIndexForward
	}
	if !scanForward {
		reverseItems(items)
	}
}

func mergeMigratingReadItems(
	primaryKey dynamoKeySchema,
	orderKey dynamoKeySchema,
	preferred []map[string]attributeValue,
	source []map[string]attributeValue,
) ([]map[string]attributeValue, error) {
	if len(source) == 0 {
		return preferred, nil
	}
	out := make([]map[string]attributeValue, 0, len(preferred)+len(source))
	seen := make(map[string]struct{}, len(preferred)+len(source))
	appendItem := func(item map[string]attributeValue) error {
		identity, err := itemPrimaryIdentity(primaryKey, item)
		if err != nil {
			return err
		}
		if _, ok := seen[identity]; ok {
			return nil
		}
		seen[identity] = struct{}{}
		out = append(out, item)
		return nil
	}
	for _, item := range preferred {
		if err := appendItem(item); err != nil {
			return nil, err
		}
	}
	for _, item := range source {
		if err := appendItem(item); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return compareReadOrder(orderKey, primaryKey, out[i], out[j]) < 0
	})
	return out, nil
}

func itemPrimaryIdentity(keySchema dynamoKeySchema, item map[string]attributeValue) (string, error) {
	var b strings.Builder
	if err := appendIdentityPart(&b, item, keySchema.HashKey); err != nil {
		return "", err
	}
	if keySchema.RangeKey != "" {
		if err := appendIdentityPart(&b, item, keySchema.RangeKey); err != nil {
			return "", err
		}
	}
	return b.String(), nil
}

func appendIdentityPart(b *strings.Builder, item map[string]attributeValue, attrName string) error {
	attr, ok := item[attrName]
	if !ok {
		return errors.New("missing key attribute")
	}
	key, err := attributeValueAsKeySegment(attr)
	if err != nil {
		return err
	}
	b.WriteString(attrName)
	b.WriteByte('=')
	b.WriteString(base64.RawURLEncoding.EncodeToString(key))
	b.WriteByte('|')
	return nil
}

func compareReadOrder(orderKey dynamoKeySchema, primaryKey dynamoKeySchema, left map[string]attributeValue, right map[string]attributeValue) int {
	if cmp := compareAttributeValueByName(orderKey.HashKey, left, right); cmp != 0 {
		return cmp
	}
	if orderKey.RangeKey != "" {
		if cmp := compareAttributeValueByName(orderKey.RangeKey, left, right); cmp != 0 {
			return cmp
		}
	}
	if cmp := compareAttributeValueByName(primaryKey.HashKey, left, right); cmp != 0 {
		return cmp
	}
	if primaryKey.RangeKey != "" {
		if cmp := compareAttributeValueByName(primaryKey.RangeKey, left, right); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareAttributeValueByName(attrName string, left map[string]attributeValue, right map[string]attributeValue) int {
	if attrName == "" {
		return 0
	}
	leftAttr, leftOK := left[attrName]
	rightAttr, rightOK := right[attrName]
	switch {
	case !leftOK && !rightOK:
		return 0
	case !leftOK:
		return -1
	case !rightOK:
		return 1
	default:
		return compareAttributeValueSortKey(leftAttr, rightAttr)
	}
}

func validateReadSelect(selectValue string) error {
	switch strings.TrimSpace(selectValue) {
	case "", "ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT":
		return nil
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported Select")
	}
}

func resolveReadLimit(limit *int32) (int, bool, error) {
	if limit == nil {
		return 0, false, nil
	}
	if *limit <= 0 {
		return 0, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid Limit")
	}
	return int(*limit), true, nil
}

func readIteratorPageLimit(limit *int32) int {
	resolved, hasLimit, err := resolveReadLimit(limit)
	if err != nil || !hasLimit {
		return dynamoScanPageLimit
	}
	pageLimit := resolved + 1
	if pageLimit > dynamoScanPageLimit {
		return dynamoScanPageLimit
	}
	if pageLimit < 1 {
		return 1
	}
	return pageLimit
}

func (d *DynamoDBServer) streamQueryItems(
	ctx context.Context,
	in queryInput,
	schema *dynamoTableSchema,
	keySchema dynamoKeySchema,
	cond queryCondition,
	readTS uint64,
	opts readPageOptions,
) (*queryOutput, bool, error) {
	iter, ok, err := d.newQueryReadIterator(in, schema, keySchema, cond, readTS, opts)
	if err != nil || !ok {
		return nil, ok, err
	}
	out, err := finalizeReadIterator(ctx, schema, iter, opts)
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

func (d *DynamoDBServer) streamScanItems(
	ctx context.Context,
	in scanInput,
	schema *dynamoTableSchema,
	readTS uint64,
	opts readPageOptions,
) (*queryOutput, bool, error) {
	iter, ok, err := d.newScanReadIterator(in, schema, readTS, opts)
	if err != nil || !ok {
		return nil, ok, err
	}
	out, err := finalizeReadIterator(ctx, schema, iter, opts)
	if err != nil {
		return nil, true, err
	}
	return out, true, nil
}

func finalizeReadIterator(
	ctx context.Context,
	schema *dynamoTableSchema,
	iter dynamoReadIterator,
	opts readPageOptions,
) (*queryOutput, error) {
	state, err := newReadPageState(schema, opts)
	if err != nil {
		return nil, err
	}
	if err := state.consumeIterator(ctx, iter); err != nil {
		return nil, err
	}
	return state.output(), nil
}

func (d *DynamoDBServer) newQueryReadIterator(
	in queryInput,
	schema *dynamoTableSchema,
	keySchema dynamoKeySchema,
	cond queryCondition,
	readTS uint64,
	opts readPageOptions,
) (dynamoReadIterator, bool, error) {
	projector := d.readItemProjector(schema, in.IndexName)
	filter := itemReadFilter(func(item map[string]attributeValue) bool {
		return matchesQueryCondition(item, cond)
	})
	pageLimit := readIteratorPageLimit(opts.limit)
	bounds, ok, err := resolveQueryReadBounds(schema, in, keySchema, cond, opts.exclusiveStartKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	if strings.TrimSpace(in.IndexName) == "" {
		return newTableReadIterator(d, bounds, readTS, pageLimit, projector, filter), true, nil
	}
	return newGSIReadIterator(d, bounds, readTS, pageLimit, projector, filter), true, nil
}

func (d *DynamoDBServer) newScanReadIterator(
	in scanInput,
	schema *dynamoTableSchema,
	readTS uint64,
	opts readPageOptions,
) (dynamoReadIterator, bool, error) {
	projector := d.readItemProjector(schema, in.IndexName)
	pageLimit := readIteratorPageLimit(opts.limit)
	if strings.TrimSpace(in.IndexName) == "" {
		bounds, err := resolveTableReadBounds(schema, in.TableName, opts.exclusiveStartKey)
		if err != nil {
			return nil, false, err
		}
		return newTableReadIterator(d, bounds, readTS, pageLimit, projector, nil), true, nil
	}
	if _, err := schema.keySchemaForQuery(in.IndexName); err != nil {
		return nil, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	bounds, ok, err := resolveGSIReadBounds(schema, in.TableName, in.IndexName, opts.exclusiveStartKey)
	if err != nil {
		return nil, false, err
	}
	if len(opts.exclusiveStartKey) > 0 && !ok {
		return nil, false, nil
	}
	return newGSIReadIterator(d, bounds, readTS, pageLimit, projector, nil), true, nil
}

func resolveTableReadBounds(
	schema *dynamoTableSchema,
	tableName string,
	startKey map[string]attributeValue,
) (dynamoReadBounds, error) {
	lower := dynamoItemPrefixForTable(tableName, schema.Generation)
	upper := prefixScanEnd(lower)
	if len(startKey) == 0 {
		return dynamoReadBounds{lower: lower, upper: upper}, nil
	}
	key, err := schema.itemKeyFromAttributes(startKey)
	if err != nil {
		return dynamoReadBounds{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ExclusiveStartKey")
	}
	return dynamoReadBounds{lower: maxBytes(lower, nextScanCursor(key)), upper: upper}, nil
}

func resolveGSIReadBounds(
	schema *dynamoTableSchema,
	tableName string,
	indexName string,
	startKey map[string]attributeValue,
) (dynamoReadBounds, bool, error) {
	lower := dynamoGSIIndexPrefixForTable(tableName, schema.Generation, indexName)
	upper := prefixScanEnd(lower)
	if len(startKey) == 0 {
		return dynamoReadBounds{lower: lower, upper: upper}, true, nil
	}
	key, ok, err := schema.gsiKeyFromAttributes(indexName, startKey)
	if err != nil {
		return dynamoReadBounds{}, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ExclusiveStartKey")
	}
	if !ok {
		return dynamoReadBounds{}, false, nil
	}
	return dynamoReadBounds{lower: maxBytes(lower, nextScanCursor(key)), upper: upper}, true, nil
}

func resolveQueryReadBounds(
	schema *dynamoTableSchema,
	in queryInput,
	keySchema dynamoKeySchema,
	cond queryCondition,
	startKey map[string]attributeValue,
) (dynamoReadBounds, bool, error) {
	if !schema.usesOrderedKeyEncoding() {
		return dynamoReadBounds{}, false, nil
	}
	basePrefix, err := queryScanPrefix(schema, in, keySchema, cond.hashValue)
	if err != nil {
		return dynamoReadBounds{}, false, err
	}
	bounds := dynamoReadBounds{
		lower:   basePrefix,
		upper:   prefixScanEnd(basePrefix),
		reverse: queryReadReverse(in.ScanIndexForward),
	}
	if keySchema.RangeKey != "" && cond.rangeCond != nil {
		bounds, err = refineQueryReadBounds(bounds, basePrefix, *cond.rangeCond)
		if err != nil {
			return dynamoReadBounds{}, false, err
		}
	}
	if len(startKey) == 0 {
		return bounds, true, nil
	}
	startCursor, ok, err := resolveQueryExclusiveStartKey(schema, in, startKey)
	if err != nil {
		return dynamoReadBounds{}, false, err
	}
	if !ok {
		return dynamoReadBounds{}, false, nil
	}
	if bounds.reverse {
		bounds.upper = minBytes(bounds.upper, startCursor)
	} else {
		bounds.lower = maxBytes(bounds.lower, nextScanCursor(startCursor))
	}
	return bounds, true, nil
}

func resolveQueryExclusiveStartKey(
	schema *dynamoTableSchema,
	in queryInput,
	startKey map[string]attributeValue,
) ([]byte, bool, error) {
	if len(startKey) == 0 {
		return nil, true, nil
	}
	if strings.TrimSpace(in.IndexName) == "" {
		key, err := schema.itemKeyFromAttributes(startKey)
		if err != nil {
			return nil, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ExclusiveStartKey")
		}
		return key, true, nil
	}
	key, ok, err := schema.gsiKeyFromAttributes(in.IndexName, startKey)
	if err != nil {
		return nil, false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ExclusiveStartKey")
	}
	return key, ok, nil
}

func queryReadReverse(scanIndexForward *bool) bool {
	return scanIndexForward != nil && !*scanIndexForward
}

type readItemProjector func(map[string]attributeValue) (map[string]attributeValue, error)

func (d *DynamoDBServer) readItemProjector(schema *dynamoTableSchema, indexName string) readItemProjector {
	if strings.TrimSpace(indexName) == "" {
		return identityReadItemProjector
	}
	return func(item map[string]attributeValue) (map[string]attributeValue, error) {
		return schema.projectItemForIndex(indexName, item)
	}
}

func identityReadItemProjector(item map[string]attributeValue) (map[string]attributeValue, error) {
	return item, nil
}

func finalizeReadPage(schema *dynamoTableSchema, items []map[string]attributeValue, opts readPageOptions) (*queryOutput, error) {
	items, err := applyQueryExclusiveStartKey(schema, opts.exclusiveStartKey, items)
	if err != nil {
		return nil, err
	}
	state, err := newReadPageState(schema, opts)
	if err != nil {
		return nil, err
	}
	if err := state.consume(items); err != nil {
		return nil, err
	}
	return state.output(), nil
}

type readPageState struct {
	schema           *dynamoTableSchema
	opts             readPageOptions
	projection       []string
	filterExpr       string
	includeItems     bool
	limit            int
	hasLimit         bool
	outItems         []map[string]attributeValue
	outCount         int
	scannedCount     int
	lastEvaluatedKey map[string]attributeValue
}

type dynamoReadBounds struct {
	lower   []byte
	upper   []byte
	reverse bool
}

type keyRangeKVIterator struct {
	server    *DynamoDBServer
	lower     []byte
	upper     []byte
	cursor    []byte
	readTS    uint64
	pageLimit int
	reverse   bool
	page      []*store.KVPair
	index     int
	done      bool
}

type emptyReadIterator struct{}

type tableReadIterator struct {
	kv        *keyRangeKVIterator
	projector readItemProjector
	filter    itemReadFilter
}

type gsiReadIterator struct {
	server    *DynamoDBServer
	kv        *keyRangeKVIterator
	readTS    uint64
	projector readItemProjector
	filter    itemReadFilter
	seen      map[string]struct{}
}

func newReadPageState(schema *dynamoTableSchema, opts readPageOptions) (*readPageState, error) {
	limit, hasLimit, err := resolveReadLimit(opts.limit)
	if err != nil {
		return nil, err
	}
	projection, err := resolveProjectionAttributes(opts.projectionExpression, opts.expressionAttributeNames)
	if err != nil {
		return nil, err
	}
	filterExpr, err := replaceNames(opts.filterExpression, opts.expressionAttributeNames)
	if err != nil {
		return nil, err
	}
	return &readPageState{
		schema:       schema,
		opts:         opts,
		projection:   projection,
		filterExpr:   strings.TrimSpace(filterExpr),
		includeItems: !strings.EqualFold(strings.TrimSpace(opts.selectValue), dynamoSelectCount),
		limit:        limit,
		hasLimit:     hasLimit,
		outItems:     make([]map[string]attributeValue, 0),
	}, nil
}

func (s *readPageState) consume(items []map[string]attributeValue) error {
	for i, item := range items {
		if s.reachedLimit() {
			break
		}
		if err := s.consumeItem(i, item, len(items)); err != nil {
			return err
		}
	}
	return nil
}

func (s *readPageState) consumeIterator(ctx context.Context, iter dynamoReadIterator) error {
	var lastItem map[string]attributeValue
	for !s.reachedLimit() {
		item, ok, err := iter.Next(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		if !ok {
			return nil
		}
		if err := s.consumeReadItem(item); err != nil {
			return err
		}
		lastItem = item
	}
	if lastItem == nil {
		return nil
	}
	if nextItem, ok, err := iter.Next(ctx); err != nil {
		return errors.WithStack(err)
	} else if ok && nextItem != nil {
		s.lastEvaluatedKey = s.buildLastEvaluatedKey(lastItem)
	}
	return nil
}

func (s *readPageState) reachedLimit() bool {
	return s.hasLimit && s.scannedCount == s.limit
}

func (s *readPageState) consumeReadItem(item map[string]attributeValue) error {
	s.scannedCount++
	match, err := matchesReadFilter(s.filterExpr, item, s.opts.expressionAttributeValues)
	if err != nil {
		return err
	}
	if match {
		s.recordMatch(item)
	}
	return nil
}

func (s *readPageState) consumeItem(i int, item map[string]attributeValue, totalItems int) error {
	if err := s.consumeReadItem(item); err != nil {
		return err
	}
	if s.shouldSetLastEvaluatedKey(i, totalItems) {
		s.lastEvaluatedKey = s.buildLastEvaluatedKey(item)
	}
	return nil
}

func (s *readPageState) recordMatch(item map[string]attributeValue) {
	s.outCount++
	if !s.includeItems {
		return
	}
	s.outItems = append(s.outItems, projectItemByAttributes(item, s.projection))
}

func (s *readPageState) shouldSetLastEvaluatedKey(i int, totalItems int) bool {
	return s.hasLimit && s.scannedCount == s.limit && i < totalItems-1
}

func (s *readPageState) buildLastEvaluatedKey(item map[string]attributeValue) map[string]attributeValue {
	if s.opts.lastEvaluatedKeyBuilder != nil {
		return s.opts.lastEvaluatedKeyBuilder(item)
	}
	return makeLastEvaluatedKey(s.schema.PrimaryKey, item)
}

func (s *readPageState) output() *queryOutput {
	items := s.outItems
	if !s.includeItems {
		items = nil
	}
	return &queryOutput{
		items:            items,
		count:            s.outCount,
		scannedCount:     s.scannedCount,
		lastEvaluatedKey: s.lastEvaluatedKey,
	}
}

func (emptyReadIterator) Next(context.Context) (map[string]attributeValue, bool, error) {
	return nil, false, nil
}

func newKeyRangeKVIterator(
	server *DynamoDBServer,
	bounds dynamoReadBounds,
	readTS uint64,
	pageLimit int,
) *keyRangeKVIterator {
	cursor := bytes.Clone(bounds.lower)
	if bounds.reverse {
		cursor = bytes.Clone(bounds.upper)
	}
	return &keyRangeKVIterator{
		server:    server,
		lower:     bytes.Clone(bounds.lower),
		upper:     bytes.Clone(bounds.upper),
		cursor:    cursor,
		readTS:    readTS,
		pageLimit: pageLimit,
		reverse:   bounds.reverse,
	}
}

func (it *keyRangeKVIterator) Next(ctx context.Context) (*store.KVPair, bool, error) {
	for {
		if it.index < len(it.page) {
			kvp := it.page[it.index]
			it.index++
			return kvp, true, nil
		}
		if it.done {
			return nil, false, nil
		}
		if err := it.loadNextPage(ctx); err != nil {
			return nil, false, err
		}
	}
}

func (it *keyRangeKVIterator) loadNextPage(ctx context.Context) error {
	if it.reverse {
		return it.loadNextPageReverse(ctx)
	}
	return it.loadNextPageForward(ctx)
}

func (it *keyRangeKVIterator) loadNextPageForward(ctx context.Context) error {
	kvs, err := it.server.store.ScanAt(ctx, it.cursor, it.upper, it.pageLimit, it.readTS)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(kvs) == 0 {
		it.done = true
		it.page = nil
		return nil
	}
	it.page, it.done = filterBoundedKVPairsForward(kvs, it.lower, it.upper, it.pageLimit)
	it.index = 0
	if !it.done {
		it.cursor = nextScanCursor(kvs[len(kvs)-1].Key)
		if it.upper != nil && bytes.Compare(it.cursor, it.upper) >= 0 {
			it.done = true
		}
	}
	return nil
}

func (it *keyRangeKVIterator) loadNextPageReverse(ctx context.Context) error {
	kvs, err := it.server.store.ReverseScanAt(ctx, it.lower, it.cursor, it.pageLimit, it.readTS)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(kvs) == 0 {
		it.done = true
		it.page = nil
		return nil
	}
	it.page, it.done = filterBoundedKVPairsReverse(kvs, it.lower, it.cursor, it.pageLimit)
	it.index = 0
	if !it.done {
		it.cursor = bytes.Clone(kvs[len(kvs)-1].Key)
	}
	return nil
}

func filterBoundedKVPairsForward(kvs []*store.KVPair, lower []byte, upper []byte, pageLimit int) ([]*store.KVPair, bool) {
	page := make([]*store.KVPair, 0, minInt(len(kvs), pageLimit))
	done := len(kvs) < pageLimit
	for _, kvp := range kvs {
		if lower != nil && bytes.Compare(kvp.Key, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(kvp.Key, upper) >= 0 {
			done = true
			break
		}
		page = append(page, kvp)
	}
	if len(page) == 0 {
		done = true
	}
	return page, done
}

func filterBoundedKVPairsReverse(kvs []*store.KVPair, lower []byte, upper []byte, pageLimit int) ([]*store.KVPair, bool) {
	page := make([]*store.KVPair, 0, minInt(len(kvs), pageLimit))
	done := len(kvs) < pageLimit
	for _, kvp := range kvs {
		if lower != nil && bytes.Compare(kvp.Key, lower) < 0 {
			done = true
			break
		}
		if upper != nil && bytes.Compare(kvp.Key, upper) >= 0 {
			continue
		}
		page = append(page, kvp)
	}
	if len(page) == 0 {
		done = true
	}
	return page, done
}

func newTableReadIterator(
	server *DynamoDBServer,
	bounds dynamoReadBounds,
	readTS uint64,
	pageLimit int,
	projector readItemProjector,
	filter itemReadFilter,
) dynamoReadIterator {
	if bounds.upper != nil && bytes.Compare(bounds.lower, bounds.upper) >= 0 {
		return emptyReadIterator{}
	}
	return &tableReadIterator{
		kv:        newKeyRangeKVIterator(server, bounds, readTS, pageLimit),
		projector: projector,
		filter:    filter,
	}
}

func (it *tableReadIterator) Next(ctx context.Context) (map[string]attributeValue, bool, error) {
	for {
		kvp, ok, err := it.kv.Next(ctx)
		if err != nil || !ok {
			return nil, ok, err
		}
		item, err := decodeStoredDynamoItem(kvp.Value)
		if err != nil {
			return nil, false, err
		}
		item, err = it.projector(item)
		if err != nil {
			return nil, false, err
		}
		if it.filter != nil && !it.filter(item) {
			continue
		}
		return item, true, nil
	}
}

func newGSIReadIterator(
	server *DynamoDBServer,
	bounds dynamoReadBounds,
	readTS uint64,
	pageLimit int,
	projector readItemProjector,
	filter itemReadFilter,
) dynamoReadIterator {
	if bounds.upper != nil && bytes.Compare(bounds.lower, bounds.upper) >= 0 {
		return emptyReadIterator{}
	}
	return &gsiReadIterator{
		server:    server,
		kv:        newKeyRangeKVIterator(server, bounds, readTS, pageLimit),
		readTS:    readTS,
		projector: projector,
		filter:    filter,
		seen:      map[string]struct{}{},
	}
}

func (it *gsiReadIterator) Next(ctx context.Context) (map[string]attributeValue, bool, error) {
	for {
		kvp, ok, err := it.kv.Next(ctx)
		if err != nil || !ok {
			return nil, ok, err
		}
		itemKey := string(kvp.Value)
		if _, exists := it.seen[itemKey]; exists {
			continue
		}
		it.seen[itemKey] = struct{}{}
		item, found, err := it.server.readItemAtKeyAt(ctx, kvp.Value, it.readTS)
		if err != nil {
			return nil, false, err
		}
		if !found {
			continue
		}
		item, err = it.projector(item)
		if err != nil {
			return nil, false, err
		}
		if it.filter != nil && !it.filter(item) {
			continue
		}
		return item, true, nil
	}
}

func matchesReadFilter(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	if strings.TrimSpace(expr) == "" {
		return true, nil
	}
	ok, err := evalConditionExpression(expr, item, values)
	if err != nil {
		return false, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return ok, nil
}

func resolveProjectionAttributes(expr string, names map[string]string) ([]string, error) {
	projectionExpr, err := replaceNames(expr, names)
	if err != nil {
		return nil, err
	}
	projection := strings.TrimSpace(projectionExpr)
	if projection == "" {
		return nil, nil
	}
	parts, err := splitTopLevelByComma(projection)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ProjectionExpression")
	}
	attrs := make([]string, 0, len(parts))
	for _, part := range parts {
		attr := strings.TrimSpace(part)
		if attr == "" {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ProjectionExpression")
		}
		attrs = append(attrs, attr)
	}
	return attrs, nil
}

func projectItem(item map[string]attributeValue, expr string, names map[string]string) (map[string]attributeValue, error) {
	attrs, err := resolveProjectionAttributes(expr, names)
	if err != nil {
		return nil, err
	}
	return projectItemByAttributes(item, attrs), nil
}

func projectItemByAttributes(item map[string]attributeValue, attrs []string) map[string]attributeValue {
	if len(attrs) == 0 {
		return cloneAttributeValueMap(item)
	}
	out := make(map[string]attributeValue, len(attrs))
	for _, attr := range attrs {
		if value, ok := item[attr]; ok {
			out[attr] = value
		}
	}
	return out
}

func decodeItemsFromKVPairs(kvs []*store.KVPair) ([]map[string]attributeValue, error) {
	items := make([]map[string]attributeValue, 0, len(kvs))
	for _, kvp := range kvs {
		item, err := decodeStoredDynamoItem(kvp.Value)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (d *DynamoDBServer) queryItemsByKeyCondition(
	ctx context.Context,
	in queryInput,
	schema *dynamoTableSchema,
	keySchema dynamoKeySchema,
	cond queryCondition,
	readTS uint64,
) ([]map[string]attributeValue, error) {
	if strings.TrimSpace(in.IndexName) != "" {
		return d.queryItemsByGSI(ctx, in, schema, cond, readTS)
	}
	scanPrefix, err := queryScanPrefix(schema, in, keySchema, cond.hashValue)
	if err != nil {
		return nil, err
	}
	kvs, err := d.scanAllByPrefixAt(ctx, scanPrefix, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	items, err := filterQueryItems(kvs, cond)
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (d *DynamoDBServer) queryItemsByGSI(
	ctx context.Context,
	in queryInput,
	schema *dynamoTableSchema,
	cond queryCondition,
	readTS uint64,
) ([]map[string]attributeValue, error) {
	keySchema, err := schema.keySchemaForQuery(in.IndexName)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	prefix, err := queryScanPrefix(schema, in, keySchema, cond.hashValue)
	if err != nil {
		return nil, err
	}
	kvs, err := d.scanAllByPrefixAt(ctx, prefix, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	itemKeys := uniqueGSIItemKeys(kvs)
	items, err := d.readItemsForGSIQuery(ctx, itemKeys, readTS, cond)
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (d *DynamoDBServer) scanItemsBySource(
	ctx context.Context,
	in scanInput,
	schema *dynamoTableSchema,
	readTS uint64,
) ([]map[string]attributeValue, error) {
	if strings.TrimSpace(in.IndexName) == "" {
		kvs, err := d.scanAllByPrefixAt(ctx, dynamoItemPrefixForTable(in.TableName, schema.Generation), readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return decodeItemsFromKVPairs(kvs)
	}
	if _, err := schema.keySchemaForQuery(in.IndexName); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	kvs, err := d.scanAllByPrefixAt(ctx, dynamoGSIIndexPrefixForTable(in.TableName, schema.Generation, in.IndexName), readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	itemKeys := uniqueGSIItemKeys(kvs)
	return d.readItemsAtKeys(ctx, itemKeys, readTS)
}

func uniqueGSIItemKeys(kvs []*store.KVPair) [][]byte {
	if len(kvs) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(kvs))
	seen := make(map[string]struct{}, len(kvs))
	for _, kvp := range kvs {
		key := string(kvp.Value)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, bytes.Clone(kvp.Value))
	}
	return out
}

type gsiReadJob struct {
	index int
	key   []byte
}

type gsiReadResult struct {
	index int
	item  map[string]attributeValue
	err   error
}

type itemReadFilter func(map[string]attributeValue) bool

func resolveGSIReadWorkerCount(n int) int {
	if n <= 0 {
		return 0
	}
	if n < gsiQueryReadWorkerCount {
		return n
	}
	return gsiQueryReadWorkerCount
}

func (d *DynamoDBServer) readItemsForGSIQuery(
	ctx context.Context,
	itemKeys [][]byte,
	readTS uint64,
	cond queryCondition,
) ([]map[string]attributeValue, error) {
	return d.readItemsAtKeysMatching(ctx, itemKeys, readTS, func(item map[string]attributeValue) bool {
		return matchesQueryCondition(item, cond)
	})
}

func (d *DynamoDBServer) readItemsAtKeys(
	ctx context.Context,
	itemKeys [][]byte,
	readTS uint64,
) ([]map[string]attributeValue, error) {
	return d.readItemsAtKeysMatching(ctx, itemKeys, readTS, nil)
}

func (d *DynamoDBServer) readItemsAtKeysMatching(
	ctx context.Context,
	itemKeys [][]byte,
	readTS uint64,
	filter itemReadFilter,
) ([]map[string]attributeValue, error) {
	if len(itemKeys) == 0 {
		return nil, nil
	}
	workerCount := resolveGSIReadWorkerCount(len(itemKeys))
	jobs := make(chan gsiReadJob)
	results := make(chan gsiReadResult, len(itemKeys))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	d.startGSIReadWorkers(&wg, workerCount, workerCtx, readTS, filter, jobs, results, cancel)
	enqueueGSIReadJobs(workerCtx, jobs, itemKeys)
	close(jobs)
	wg.Wait()
	close(results)

	return collectOrderedGSIReadResults(itemKeys, results)
}

func (d *DynamoDBServer) startGSIReadWorkers(
	wg *sync.WaitGroup,
	workerCount int,
	ctx context.Context,
	readTS uint64,
	filter itemReadFilter,
	jobs <-chan gsiReadJob,
	results chan<- gsiReadResult,
	cancel context.CancelFunc,
) {
	for range workerCount {
		wg.Go(func() {
			d.gsiReadWorker(ctx, readTS, filter, jobs, results, cancel)
		})
	}
}

func enqueueGSIReadJobs(ctx context.Context, jobs chan<- gsiReadJob, itemKeys [][]byte) {
enqueueLoop:
	for i, key := range itemKeys {
		select {
		case <-ctx.Done():
			break enqueueLoop
		case jobs <- gsiReadJob{index: i, key: key}:
		}
	}
}

func collectOrderedGSIReadResults(
	itemKeys [][]byte,
	results <-chan gsiReadResult,
) ([]map[string]attributeValue, error) {
	indexed := make(map[int]map[string]attributeValue, len(itemKeys))
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		if res.item != nil {
			indexed[res.index] = res.item
		}
	}
	items := make([]map[string]attributeValue, 0, len(indexed))
	for i := range itemKeys {
		if item := indexed[i]; item != nil {
			items = append(items, item)
		}
	}
	return items, nil
}

func (d *DynamoDBServer) gsiReadWorker(
	ctx context.Context,
	readTS uint64,
	filter itemReadFilter,
	jobs <-chan gsiReadJob,
	results chan<- gsiReadResult,
	cancel context.CancelFunc,
) {
	for {
		job, ok := nextGSIReadJob(ctx, jobs)
		if !ok {
			return
		}
		item, emit, err := d.executeGSIReadJob(ctx, readTS, filter, job.key)
		if err != nil {
			sendGSIReadError(results, err)
			cancel()
			return
		}
		if !emit {
			continue
		}
		if !sendGSIReadResult(ctx, results, gsiReadResult{index: job.index, item: item}) {
			return
		}
	}
}

func nextGSIReadJob(ctx context.Context, jobs <-chan gsiReadJob) (gsiReadJob, bool) {
	select {
	case <-ctx.Done():
		return gsiReadJob{}, false
	case job, ok := <-jobs:
		if !ok {
			return gsiReadJob{}, false
		}
		return job, true
	}
}

func (d *DynamoDBServer) executeGSIReadJob(
	ctx context.Context,
	readTS uint64,
	filter itemReadFilter,
	key []byte,
) (map[string]attributeValue, bool, error) {
	item, found, err := d.readItemAtKeyAt(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if filter != nil && !filter(item) {
		return nil, false, nil
	}
	return item, true, nil
}

func sendGSIReadError(results chan<- gsiReadResult, err error) {
	select {
	case results <- gsiReadResult{err: err}:
	default:
	}
}

func sendGSIReadResult(ctx context.Context, results chan<- gsiReadResult, result gsiReadResult) bool {
	select {
	case results <- result:
		return true
	case <-ctx.Done():
		return false
	}
}

func queryScanPrefix(schema *dynamoTableSchema, in queryInput, keySchema dynamoKeySchema, hashValue attributeValue) ([]byte, error) {
	if !schema.usesOrderedKeyEncoding() {
		hashKey, err := attributeValueAsKey(hashValue)
		if err != nil {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
		}
		if strings.TrimSpace(in.IndexName) != "" {
			return legacyDynamoGSIHashPrefixForTable(in.TableName, schema.Generation, in.IndexName, hashKey), nil
		}
		if keySchema.HashKey != schema.PrimaryKey.HashKey {
			return dynamoItemPrefixForTable(in.TableName, schema.Generation), nil
		}
		return legacyDynamoItemHashPrefixForTable(in.TableName, schema.Generation, hashKey), nil
	}
	hashKey, err := attributeValueAsKeySegment(hashValue)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if strings.TrimSpace(in.IndexName) != "" {
		return dynamoGSIHashPrefixForTable(in.TableName, schema.Generation, in.IndexName, hashKey), nil
	}
	if keySchema.HashKey != schema.PrimaryKey.HashKey {
		return dynamoItemPrefixForTable(in.TableName, schema.Generation), nil
	}
	return dynamoItemHashPrefixForTable(in.TableName, schema.Generation, hashKey), nil
}

func refineQueryReadBounds(
	bounds dynamoReadBounds,
	basePrefix []byte,
	cond queryRangeCondition,
) (dynamoReadBounds, error) {
	switch cond.op {
	case queryRangeOpEqual, queryRangeOpLessThan, queryRangeOpLessOrEq, queryRangeOpGreater, queryRangeOpGreaterEq:
		return refineQueryComparisonBounds(bounds, basePrefix, cond)
	case queryRangeOpBetween:
		return refineQueryBetweenBounds(bounds, basePrefix, cond)
	case queryRangeOpBeginsWith:
		return refineQueryBeginsWithBounds(bounds, basePrefix, cond.value1)
	default:
		return bounds, nil
	}
}

func refineQueryComparisonBounds(
	bounds dynamoReadBounds,
	basePrefix []byte,
	cond queryRangeCondition,
) (dynamoReadBounds, error) {
	prefix, err := appendRangeConditionPrefix(basePrefix, cond.value1)
	if err != nil {
		return dynamoReadBounds{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if cond.op == queryRangeOpEqual {
		bounds.lower = prefix
		bounds.upper = prefixScanEnd(prefix)
		return bounds, nil
	}
	if cond.op == queryRangeOpLessThan {
		bounds.upper = minBytes(bounds.upper, prefix)
		return bounds, nil
	}
	if cond.op == queryRangeOpLessOrEq {
		bounds.upper = minBytes(bounds.upper, prefixScanEnd(prefix))
		return bounds, nil
	}
	if cond.op == queryRangeOpGreater {
		bounds.lower = maxBytes(bounds.lower, prefixScanEnd(prefix))
		return bounds, nil
	}
	if cond.op == queryRangeOpGreaterEq {
		bounds.lower = maxBytes(bounds.lower, prefix)
		return bounds, nil
	}
	return bounds, nil
}

func refineQueryBetweenBounds(
	bounds dynamoReadBounds,
	basePrefix []byte,
	cond queryRangeCondition,
) (dynamoReadBounds, error) {
	lower, err := appendRangeConditionPrefix(basePrefix, cond.value1)
	if err != nil {
		return dynamoReadBounds{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	upper, err := appendRangeConditionPrefix(basePrefix, cond.value2)
	if err != nil {
		return dynamoReadBounds{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	bounds.lower = maxBytes(bounds.lower, lower)
	bounds.upper = minBytes(bounds.upper, prefixScanEnd(upper))
	return bounds, nil
}

func refineQueryBeginsWithBounds(
	bounds dynamoReadBounds,
	basePrefix []byte,
	value attributeValue,
) (dynamoReadBounds, error) {
	prefix, err := appendRangeConditionPrefixMatch(basePrefix, value)
	if err != nil {
		return dynamoReadBounds{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	bounds.lower = maxBytes(bounds.lower, prefix)
	bounds.upper = minBytes(bounds.upper, prefixScanEnd(prefix))
	return bounds, nil
}

func appendRangeConditionPrefix(basePrefix []byte, value attributeValue) ([]byte, error) {
	segment, err := attributeValueAsKeySegment(value)
	if err != nil {
		return nil, err
	}
	return append(bytes.Clone(basePrefix), segment...), nil
}

func appendRangeConditionPrefixMatch(basePrefix []byte, value attributeValue) ([]byte, error) {
	raw, err := attributeValueAsKeyBytes(value)
	if err != nil {
		return nil, err
	}
	segment := encodeDynamoKeySegmentPrefix(raw)
	return append(bytes.Clone(basePrefix), segment...), nil
}

func maxBytes(left []byte, right []byte) []byte {
	if left == nil {
		return bytes.Clone(right)
	}
	if right == nil {
		return bytes.Clone(left)
	}
	if bytes.Compare(left, right) >= 0 {
		return bytes.Clone(left)
	}
	return bytes.Clone(right)
}

func minBytes(left []byte, right []byte) []byte {
	if left == nil {
		return bytes.Clone(right)
	}
	if right == nil {
		return bytes.Clone(left)
	}
	if bytes.Compare(left, right) <= 0 {
		return bytes.Clone(left)
	}
	return bytes.Clone(right)
}

func applyQueryExclusiveStartKey(schema *dynamoTableSchema, startKey map[string]attributeValue, items []map[string]attributeValue) ([]map[string]attributeValue, error) {
	if len(startKey) == 0 {
		return items, nil
	}
	startItemKey, err := schema.itemKeyFromAttributes(startKey)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ExclusiveStartKey")
	}
	descending, hasDirection := queryItemOrderDirection(schema, items)
	for i, item := range items {
		if remaining, ok := exclusiveStartRemainingItems(schema, item, items, i, startItemKey, descending, hasDirection); ok {
			return remaining, nil
		}
	}
	return nil, nil
}

func exclusiveStartRemainingItems(
	schema *dynamoTableSchema,
	item map[string]attributeValue,
	items []map[string]attributeValue,
	index int,
	startItemKey []byte,
	descending bool,
	hasDirection bool,
) ([]map[string]attributeValue, bool) {
	itemKey, err := schema.itemKeyFromAttributes(item)
	if err != nil {
		return nil, false
	}
	if bytes.Equal(itemKey, startItemKey) {
		return items[index+1:], true
	}
	if !hasDirection || !exclusiveStartShouldAdvance(descending, itemKey, startItemKey) {
		return nil, false
	}
	return items[index:], true
}

func exclusiveStartShouldAdvance(descending bool, itemKey []byte, startItemKey []byte) bool {
	cmp := bytes.Compare(itemKey, startItemKey)
	return (!descending && cmp > 0) || (descending && cmp < 0)
}

func queryItemOrderDirection(schema *dynamoTableSchema, items []map[string]attributeValue) (bool, bool) {
	var previous []byte
	for _, item := range items {
		itemKey, err := schema.itemKeyFromAttributes(item)
		if err != nil {
			continue
		}
		if previous == nil {
			previous = itemKey
			continue
		}
		cmp := bytes.Compare(itemKey, previous)
		if cmp == 0 {
			continue
		}
		return cmp < 0, true
	}
	return false, false
}

func makeLastEvaluatedKey(keySchema dynamoKeySchema, item map[string]attributeValue) map[string]attributeValue {
	out := map[string]attributeValue{}
	if hash, ok := item[keySchema.HashKey]; ok {
		out[keySchema.HashKey] = hash
	}
	if keySchema.RangeKey != "" {
		if rk, ok := item[keySchema.RangeKey]; ok {
			out[keySchema.RangeKey] = rk
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func makeReadLastEvaluatedKey(primary dynamoKeySchema, index dynamoKeySchema, item map[string]attributeValue) map[string]attributeValue {
	out := makeLastEvaluatedKey(primary, item)
	if len(out) == 0 {
		out = map[string]attributeValue{}
	}
	if hash, ok := item[index.HashKey]; ok {
		out[index.HashKey] = hash
	}
	if index.RangeKey != "" {
		if rk, ok := item[index.RangeKey]; ok {
			out[index.RangeKey] = rk
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (d *DynamoDBServer) batchWriteItem(w http.ResponseWriter, r *http.Request) {
	in, err := decodeBatchWriteItemInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	unprocessed, err := d.batchWriteItems(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	for table, written := range batchWriteCommittedCounts(in, unprocessed) {
		d.observeWrittenItems(r.Context(), table, written)
	}
	writeDynamoJSON(w, map[string]any{"UnprocessedItems": unprocessed})
}

func decodeBatchWriteItemInput(bodyReader io.Reader) (batchWriteItemInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return batchWriteItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in batchWriteItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		return batchWriteItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if len(in.RequestItems) == 0 {
		return batchWriteItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing RequestItems")
	}
	total := 0
	for tableName, requests := range in.RequestItems {
		if strings.TrimSpace(tableName) == "" {
			return batchWriteItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
		}
		total += len(requests)
	}
	if total == 0 {
		return batchWriteItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing write requests")
	}
	if total > batchWriteItemMaxItems {
		return batchWriteItemInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "too many items in BatchWriteItem")
	}
	return in, nil
}

func batchWriteCommittedCounts(in batchWriteItemInput, unprocessed map[string][]batchWriteRequest) map[string]int {
	out := make(map[string]int, len(in.RequestItems))
	for table, requests := range in.RequestItems {
		written := len(requests) - len(unprocessed[table])
		if written > 0 {
			out[table] = written
		}
	}
	return out
}

func (d *DynamoDBServer) batchWriteItems(
	ctx context.Context,
	in batchWriteItemInput,
) (map[string][]batchWriteRequest, error) {
	tableNames := make([]string, 0, len(in.RequestItems))
	for tableName := range in.RequestItems {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	unlock := d.lockTableOperations(tableNames)
	defer unlock()
	for _, tableName := range tableNames {
		if err := d.ensureLegacyTableMigrationLocked(ctx, tableName); err != nil {
			return nil, err
		}
	}
	if err := d.validateBatchWriteRequests(ctx, tableNames, in.RequestItems); err != nil {
		return nil, err
	}
	unprocessed := make(map[string][]batchWriteRequest)
	for _, tableName := range tableNames {
		requests := in.RequestItems[tableName]
		for _, request := range requests {
			err := d.executeBatchWriteRequest(ctx, tableName, request)
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return nil, errors.WithStack(ctx.Err())
			}
			unprocessed[tableName] = append(unprocessed[tableName], request)
		}
	}
	return unprocessed, nil
}

func (d *DynamoDBServer) validateBatchWriteRequests(
	ctx context.Context,
	tableNames []string,
	requestItems map[string][]batchWriteRequest,
) error {
	for _, tableName := range tableNames {
		schema, exists, err := d.loadTableSchema(ctx, tableName)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists {
			return newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
		}
		seenKeys := make(map[string]struct{}, len(requestItems[tableName]))
		for _, request := range requestItems[tableName] {
			key, err := validateBatchWriteRequestForSchema(schema, request)
			if err != nil {
				return err
			}
			if _, ok := seenKeys[string(key)]; ok {
				return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "duplicate item in BatchWriteItem")
			}
			seenKeys[string(key)] = struct{}{}
		}
	}
	return nil
}

func validateBatchWriteRequestForSchema(schema *dynamoTableSchema, request batchWriteRequest) ([]byte, error) {
	switch countBatchWriteActions(request) {
	case 1:
	default:
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid batch write request")
	}
	switch {
	case request.PutRequest != nil:
		key, err := schema.itemKeyFromAttributes(request.PutRequest.Item)
		if err != nil {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
		}
		return key, nil
	case request.DeleteRequest != nil:
		key, err := schema.itemKeyFromAttributes(request.DeleteRequest.Key)
		if err != nil {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
		}
		return key, nil
	default:
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid batch write request")
	}
}

func (d *DynamoDBServer) executeBatchWriteRequest(
	ctx context.Context,
	tableName string,
	request batchWriteRequest,
) error {
	schema, exists, err := d.loadTableSchema(ctx, tableName)
	if err != nil {
		return errors.WithStack(err)
	}
	if !exists {
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	keyAttrs, err := batchWriteRequestKey(schema, request)
	if err != nil {
		return err
	}
	lockKey, err := dynamoItemUpdateLockKey(tableName, keyAttrs)
	if err != nil {
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	unlock := d.lockItemUpdate(lockKey)
	defer unlock()
	switch countBatchWriteActions(request) {
	case 1:
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid batch write request")
	}
	switch {
	case request.PutRequest != nil:
		_, err := d.putItemWithRetry(ctx, putItemInput{
			TableName: tableName,
			Item:      request.PutRequest.Item,
		})
		return err
	case request.DeleteRequest != nil:
		_, err := d.deleteItemWithRetry(ctx, deleteItemInput{
			TableName: tableName,
			Key:       request.DeleteRequest.Key,
		})
		return err
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid batch write request")
	}
}

func batchWriteRequestKey(schema *dynamoTableSchema, request batchWriteRequest) (map[string]attributeValue, error) {
	switch {
	case request.PutRequest != nil:
		return primaryKeyAttributes(schema.PrimaryKey, request.PutRequest.Item)
	case request.DeleteRequest != nil:
		return primaryKeyAttributes(schema.PrimaryKey, request.DeleteRequest.Key)
	default:
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid batch write request")
	}
}

func primaryKeyAttributes(keySchema dynamoKeySchema, attrs map[string]attributeValue) (map[string]attributeValue, error) {
	out := make(map[string]attributeValue, primaryKeyAttributeCapacity(keySchema))
	hash, ok := attrs[keySchema.HashKey]
	if !ok {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing hash key attribute")
	}
	out[keySchema.HashKey] = hash
	if keySchema.RangeKey != "" {
		rangeValue, ok := attrs[keySchema.RangeKey]
		if !ok {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing range key attribute")
		}
		out[keySchema.RangeKey] = rangeValue
	}
	return out, nil
}

func primaryKeyAttributeCapacity(keySchema dynamoKeySchema) int {
	size := 1
	if keySchema.RangeKey != "" {
		size++
	}
	return size
}

func countBatchWriteActions(request batchWriteRequest) int {
	count := 0
	if request.PutRequest != nil {
		count++
	}
	if request.DeleteRequest != nil {
		count++
	}
	return count
}

func (d *DynamoDBServer) transactWriteItems(w http.ResponseWriter, r *http.Request) {
	in, err := decodeTransactWriteItemsInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.transactWriteItemsWithRetry(r.Context(), in); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	for table, written := range transactWriteWrittenCounts(in) {
		d.observeWrittenItems(r.Context(), table, written)
	}
	writeDynamoJSON(w, map[string]any{})
}

func decodeTransactWriteItemsInput(bodyReader io.Reader) (transactWriteItemsInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return transactWriteItemsInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in transactWriteItemsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return transactWriteItemsInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if len(in.TransactItems) == 0 {
		return transactWriteItemsInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing transact items")
	}
	return in, nil
}

func collectTransactWriteTableNames(in transactWriteItemsInput) ([]string, error) {
	seen := map[string]struct{}{}
	names := make([]string, 0, len(in.TransactItems))
	for _, item := range in.TransactItems {
		tableName, err := transactWriteItemTableName(item)
		if err != nil {
			return nil, err
		}
		if tableName == "" {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
		}
		if _, exists := seen[tableName]; exists {
			continue
		}
		seen[tableName] = struct{}{}
		names = append(names, tableName)
	}
	return names, nil
}

func transactWriteWrittenCounts(in transactWriteItemsInput) map[string]int {
	out := make(map[string]int)
	for _, item := range in.TransactItems {
		tableName, err := transactWriteItemTableName(item)
		if err != nil || strings.TrimSpace(tableName) == "" {
			continue
		}
		switch {
		case item.Put != nil, item.Update != nil, item.Delete != nil:
			out[tableName]++
		}
	}
	return out
}

func transactWriteItemTableName(item transactWriteItem) (string, error) {
	switch countTransactWriteItemActions(item) {
	case 0:
		return "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing transact action")
	case 1:
	default:
		return "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "multiple transact actions are not supported")
	}
	switch {
	case item.Put != nil:
		return strings.TrimSpace(item.Put.TableName), nil
	case item.Update != nil:
		return strings.TrimSpace(item.Update.TableName), nil
	case item.Delete != nil:
		return strings.TrimSpace(item.Delete.TableName), nil
	default:
		return strings.TrimSpace(item.ConditionCheck.TableName), nil
	}
}

func countTransactWriteItemActions(item transactWriteItem) int {
	count := 0
	if item.Put != nil {
		count++
	}
	if item.Update != nil {
		count++
	}
	if item.Delete != nil {
		count++
	}
	if item.ConditionCheck != nil {
		count++
	}
	return count
}

func (d *DynamoDBServer) transactWriteItemsWithRetry(ctx context.Context, in transactWriteItemsInput) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	var lastErr error
	for range transactRetryMaxAttempts {
		reqs, generations, cleanupKeys, err := d.buildTransactWriteItemsRequest(ctx, in)
		if err != nil {
			return err
		}
		done, retryErr, fatalErr := d.runTransactWriteAttempt(ctx, reqs, generations, cleanupKeys)
		if fatalErr != nil {
			return fatalErr
		}
		if done {
			return nil
		}
		if retryErr != nil {
			lastErr = retryErr
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			if lastErr != nil {
				combined := errors.Join(err, lastErr)
				return errors.Wrap(combined, "transact write retry canceled")
			}
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	if lastErr != nil {
		return errors.Wrapf(lastErr, "transact write retry attempts exhausted after %s", transactRetryMaxDuration)
	}
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "transact write retry attempts exhausted")
}

func (d *DynamoDBServer) runTransactWriteAttempt(
	ctx context.Context,
	reqs *kv.OperationGroup[kv.OP],
	generations map[string]uint64,
	cleanupKeys [][]byte,
) (bool, error, error) {
	if len(reqs.Elems) == 0 {
		return true, nil, nil
	}
	if _, err := d.coordinator.Dispatch(ctx, reqs); err != nil {
		wrapped := errors.WithStack(err)
		if !isRetryableTransactWriteError(err) {
			return false, nil, wrapped
		}
		return false, wrapped, nil
	}
	retry, verifyErr := d.handleGenerationFenceResult(
		ctx,
		d.verifyTableGenerations(ctx, generations),
		cleanupKeys,
	)
	if verifyErr != nil {
		return false, nil, verifyErr
	}
	if !retry {
		return true, nil, nil
	}
	return false, nil, nil
}

func (d *DynamoDBServer) buildTransactWriteItemsRequest(ctx context.Context, in transactWriteItemsInput) (*kv.OperationGroup[kv.OP], map[string]uint64, [][]byte, error) {
	tableNames, err := collectTransactWriteTableNames(in)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, tableName := range tableNames {
		if err := d.ensureLegacyTableMigration(ctx, tableName); err != nil {
			return nil, nil, nil, err
		}
	}
	readTS := d.nextTxnReadTS()
	reqs := &kv.OperationGroup[kv.OP]{
		IsTxn: true,
		// Keep transaction start aligned with the snapshot used to evaluate
		// ConditionCheck/ConditionExpression so concurrent writes after readTS
		// are detected as write conflicts at commit time.
		StartTS: readTS,
	}
	schemaCache := make(map[string]*dynamoTableSchema)
	tableGenerations := make(map[string]uint64)
	cleanup := make([][]byte, 0, len(in.TransactItems))
	for _, item := range in.TransactItems {
		tableName, err := transactWriteItemTableName(item)
		if err != nil {
			return nil, nil, nil, err
		}
		schema, err := d.resolveTransactTableSchema(ctx, schemaCache, tableName, readTS)
		if err != nil {
			return nil, nil, nil, err
		}
		plan, err := d.buildTransactWriteItemPlan(ctx, schema, item, readTS)
		if err != nil {
			return nil, nil, nil, err
		}
		reqs.Elems = append(reqs.Elems, plan.elems...)
		if !plan.writes {
			continue
		}
		tableGenerations[tableName] = schema.Generation
		cleanup = append(cleanup, plan.cleanup...)
	}
	return reqs, tableGenerations, cleanup, nil
}

type transactWriteItemPlan struct {
	elems   []*kv.Elem[kv.OP]
	cleanup [][]byte
	writes  bool
}

func (d *DynamoDBServer) buildTransactWriteItemPlan(
	ctx context.Context,
	schema *dynamoTableSchema,
	item transactWriteItem,
	readTS uint64,
) (*transactWriteItemPlan, error) {
	switch {
	case item.Put != nil:
		return d.buildTransactPutPlan(ctx, schema, *item.Put, readTS)
	case item.Update != nil:
		return d.buildTransactUpdatePlan(ctx, schema, *item.Update, readTS)
	case item.Delete != nil:
		return d.buildTransactDeletePlan(ctx, schema, *item.Delete, readTS)
	case item.ConditionCheck != nil:
		return d.buildTransactConditionCheckPlan(ctx, schema, *item.ConditionCheck, readTS)
	default:
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported transact item")
	}
}

func (d *DynamoDBServer) buildTransactPutPlan(
	ctx context.Context,
	schema *dynamoTableSchema,
	in putItemInput,
	readTS uint64,
) (*transactWriteItemPlan, error) {
	itemKey, err := schema.itemKeyFromAttributes(in.Item)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	keyAttrs, err := primaryKeyAttributes(schema.PrimaryKey, in.Item)
	if err != nil {
		return nil, err
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, keyAttrs, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var current map[string]attributeValue
	if found {
		current = currentLocation.item
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	req, cleanup, err := buildItemWriteRequestWithSource(schema, itemKey, in.Item, currentLocation)
	if err != nil {
		return nil, err
	}
	return &transactWriteItemPlan{
		elems:   req.Elems,
		cleanup: cleanup,
		writes:  true,
	}, nil
}

func (d *DynamoDBServer) buildTransactUpdatePlan(
	ctx context.Context,
	schema *dynamoTableSchema,
	in transactUpdateInput,
	readTS uint64,
) (*transactWriteItemPlan, error) {
	itemKey, err := schema.itemKeyFromAttributes(in.Key)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, in.Key, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var current map[string]attributeValue
	if !found {
		current = map[string]attributeValue{}
	} else {
		current = currentLocation.item
	}
	updateIn := updateItemInput{
		TableName:                 in.TableName,
		Key:                       in.Key,
		UpdateExpression:          in.UpdateExpression,
		ConditionExpression:       in.ConditionExpression,
		ExpressionAttributeNames:  in.ExpressionAttributeNames,
		ExpressionAttributeValues: in.ExpressionAttributeValues,
	}
	nextItem, err := buildUpdatedItem(schema, updateIn, current)
	if err != nil {
		return nil, err
	}
	req, cleanup, err := buildItemWriteRequestWithSource(schema, itemKey, nextItem, currentLocation)
	if err != nil {
		return nil, err
	}
	return &transactWriteItemPlan{
		elems:   req.Elems,
		cleanup: cleanup,
		writes:  true,
	}, nil
}

func (d *DynamoDBServer) buildTransactDeletePlan(
	ctx context.Context,
	schema *dynamoTableSchema,
	in transactDeleteInput,
	readTS uint64,
) (*transactWriteItemPlan, error) {
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, in.Key, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	current := map[string]attributeValue(nil)
	if found {
		current = currentLocation.item
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	if !found {
		return &transactWriteItemPlan{}, nil
	}
	req, err := buildItemDeleteRequestWithSource(currentLocation)
	if err != nil {
		return nil, err
	}
	return &transactWriteItemPlan{
		elems:  req.Elems,
		writes: true,
	}, nil
}

func (d *DynamoDBServer) buildTransactConditionCheckPlan(
	ctx context.Context,
	schema *dynamoTableSchema,
	in transactConditionInput,
	readTS uint64,
) (*transactWriteItemPlan, error) {
	if strings.TrimSpace(in.ConditionExpression) == "" {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing condition expression")
	}
	itemKey, err := schema.itemKeyFromAttributes(in.Key)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	currentLocation, found, err := d.readLogicalItemAt(ctx, schema, in.Key, readTS)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	current := map[string]attributeValue(nil)
	if found {
		current = currentLocation.item
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	lockKey := itemKey
	if currentLocation != nil {
		lockKey = currentLocation.key
	}
	lockReq, lockCleanup, err := buildConditionCheckLockRequest(lockKey, current, found)
	if err != nil {
		return nil, err
	}
	return &transactWriteItemPlan{
		elems:   lockReq.Elems,
		cleanup: lockCleanup,
		writes:  true,
	}, nil
}

func valueOrEmptyMap(item map[string]attributeValue, found bool) map[string]attributeValue {
	if found {
		return item
	}
	return map[string]attributeValue{}
}

func buildItemDeleteRequestWithSource(current *dynamoItemLocation) (*kv.OperationGroup[kv.OP], error) {
	if current == nil {
		return &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: 0,
			Elems:   nil,
		}, nil
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Del, Key: current.key}}
	delKeys, err := itemStorageKeys(current)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	for _, key := range delKeys {
		if bytes.Equal(key, current.key) {
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: 0,
		Elems:   elems,
	}, nil
}

func buildConditionCheckLockRequest(
	itemKey []byte,
	current map[string]attributeValue,
	found bool,
) (*kv.OperationGroup[kv.OP], [][]byte, error) {
	elems := make([]*kv.Elem[kv.OP], 0, 1)
	if found {
		payload, err := encodeStoredDynamoItem(current)
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: itemKey, Value: payload})
	} else {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: itemKey})
	}
	return &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: 0,
			Elems:   elems,
		},
		[][]byte{itemKey},
		nil
}

func (d *DynamoDBServer) resolveTransactTableSchema(ctx context.Context, cache map[string]*dynamoTableSchema, tableName string, readTS uint64) (*dynamoTableSchema, error) {
	if schema := cache[tableName]; schema != nil {
		return schema, nil
	}
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	cache[tableName] = schema
	return schema, nil
}

func isRetryableTransactWriteError(err error) bool {
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked)
}

func waitTransactRetryBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-timer.C:
		return nil
	}
}

func waitRetryWithDeadline(ctx context.Context, deadline time.Time, backoff time.Duration) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return errors.New("retry timeout")
	}
	delay := min(backoff, remaining)
	return waitTransactRetryBackoff(ctx, delay)
}

func nextTransactRetryBackoff(current time.Duration) time.Duration {
	next := current * transactRetryBackoffFactor
	if next > transactRetryMaxBackoff {
		return transactRetryMaxBackoff
	}
	return next
}

var errTableGenerationChanged = errors.New("table generation changed")

func (d *DynamoDBServer) verifyTableGeneration(ctx context.Context, tableName string, expectedGeneration uint64) error {
	schema, exists, err := d.loadTableSchema(ctx, tableName)
	if err != nil {
		return errors.WithStack(err)
	}
	if !exists {
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	if schema.Generation != expectedGeneration {
		return errors.Wrapf(errTableGenerationChanged,
			"table generation changed (table=%s expected=%d actual=%d)",
			tableName, expectedGeneration, schema.Generation,
		)
	}
	return nil
}

func (d *DynamoDBServer) verifyTableGenerations(ctx context.Context, generations map[string]uint64) error {
	for tableName, generation := range generations {
		if err := d.verifyTableGeneration(ctx, tableName, generation); err != nil {
			return err
		}
	}
	return nil
}

func isGenerationFenceFailure(err error) bool {
	return errors.Is(err, errTableGenerationChanged) || isTableNotFoundError(err)
}

func (d *DynamoDBServer) handleGenerationFenceResult(ctx context.Context, err error, cleanupKeys [][]byte) (bool, error) {
	if err == nil {
		return false, nil
	}
	if !isGenerationFenceFailure(err) {
		return false, err
	}
	if cleanupErr := d.cleanupCommittedKeys(ctx, cleanupKeys); cleanupErr != nil {
		return false, cleanupErr
	}
	if errors.Is(err, errTableGenerationChanged) {
		return true, nil
	}
	return false, err
}

func isTableNotFoundError(err error) bool {
	var apiErr *dynamoAPIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.errorType == dynamoErrResourceNotFound
}

func (d *DynamoDBServer) cleanupCommittedKeys(ctx context.Context, keys [][]byte) error {
	uniq := uniqueKeys(keys)
	if len(uniq) == 0 {
		return nil
	}
	return d.dispatchDeleteBatch(ctx, uniq)
}

func uniqueKeys(keys [][]byte) [][]byte {
	seen := make(map[string]struct{}, len(keys))
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		s := string(key)
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, key)
	}
	return out
}

type dynamoAPIError struct {
	status    int
	errorType string
	message   string
}

func (e *dynamoAPIError) Error() string {
	if e == nil {
		return ""
	}
	if e.message != "" {
		return e.message
	}
	return http.StatusText(e.status)
}

func newDynamoAPIError(status int, errorType string, message string) error {
	return &dynamoAPIError{
		status:    status,
		errorType: errorType,
		message:   message,
	}
}

func writeDynamoErrorFromErr(w http.ResponseWriter, err error) {
	var apiErr *dynamoAPIError
	if errors.As(err, &apiErr) {
		writeDynamoError(w, apiErr.status, apiErr.errorType, apiErr.message)
		return
	}
	writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
}

func writeDynamoError(w http.ResponseWriter, status int, errorType string, message string) {
	if message == "" {
		message = http.StatusText(status)
	}

	resp := map[string]string{"message": message}
	if errorType != "" {
		resp["__type"] = errorType
		w.Header().Set("x-amzn-ErrorType", errorType)
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func writeDynamoJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}

func replaceNames(expr string, names map[string]string) (string, error) {
	if expr == "" || len(names) == 0 {
		return expr, nil
	}
	if err := validateExpressionAttributeNames(names); err != nil {
		return "", err
	}
	keys := make([]string, 0, len(names))
	for k := range names {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) == len(keys[j]) {
			return keys[i] < keys[j]
		}
		return len(keys[i]) > len(keys[j])
	})

	// DynamoDB expression attribute names are substituted once.
	args := make([]string, 0, len(keys)*replacerArgPairSize)
	for _, key := range keys {
		args = append(args, key, names[key])
	}
	return strings.NewReplacer(args...).Replace(expr), nil
}

func validateExpressionAttributeNames(names map[string]string) error {
	for placeholder, name := range names {
		if !isExpressionAttributePlaceholder(placeholder) {
			return errors.Errorf("invalid expression attribute placeholder %q", placeholder)
		}
		if !isExpressionAttributeName(name) {
			return errors.Errorf("invalid expression attribute name %q for placeholder %q", name, placeholder)
		}
	}
	return nil
}

func isExpressionAttributePlaceholder(s string) bool {
	if len(s) <= 1 || s[0] != '#' {
		return false
	}
	return isExpressionPlaceholderIdentifier(s[1:])
}

func isExpressionPlaceholderIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if isExpressionPlaceholderIdentByte(s[i]) {
			continue
		}
		return false
	}
	return true
}

func isExpressionPlaceholderIdentByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func isExpressionAttributeName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if isExpressionAttributeNameByte(s[i]) {
			continue
		}
		return false
	}
	return true
}

func isExpressionAttributeNameByte(b byte) bool {
	return b == '_' || b == '.' || b == '-' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func applyUpdateExpression(expr string, names map[string]string, values map[string]attributeValue, item map[string]attributeValue) error {
	updExpr, err := replaceNames(expr, names)
	if err != nil {
		return err
	}
	updExpr = strings.TrimSpace(updExpr)
	sections, err := parseUpdateExpressionSections(updExpr)
	if err != nil {
		return err
	}
	for _, section := range sections {
		if err := applyUpdateExpressionSection(section, values, item); err != nil {
			return err
		}
	}
	return nil
}

type updateExpressionSection struct {
	action string
	body   string
}

func parseUpdateExpressionSections(expr string) ([]updateExpressionSection, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, errors.New("unsupported update expression")
	}
	sections := make([]updateExpressionSection, 0, updateSplitCount)
	seen := map[string]struct{}{}
	i := skipSpaces(expr, 0)
	for i < len(expr) {
		action, nextPos, ok := parseUpdateActionToken(expr, i)
		if !ok {
			return nil, errors.New("unsupported update expression")
		}
		if _, exists := seen[action]; exists {
			return nil, errors.New("duplicate update action")
		}
		seen[action] = struct{}{}
		bodyStart := skipSpaces(expr, nextPos)
		bodyEnd := findNextUpdateAction(expr, bodyStart)
		if bodyEnd < 0 {
			bodyEnd = len(expr)
		}
		body := strings.TrimSpace(expr[bodyStart:bodyEnd])
		if body == "" {
			return nil, errors.New("unsupported update expression")
		}
		sections = append(sections, updateExpressionSection{action: action, body: body})
		if bodyEnd >= len(expr) {
			break
		}
		i = bodyEnd
	}
	if len(sections) == 0 {
		return nil, errors.New("unsupported update expression")
	}
	return sections, nil
}

func applyUpdateExpressionSection(section updateExpressionSection, values map[string]attributeValue, item map[string]attributeValue) error {
	switch section.action {
	case "SET":
		return applySetUpdateAction(section.body, values, item)
	case "REMOVE":
		return applyRemoveUpdateAction(section.body, item)
	case "ADD":
		return applyAddUpdateAction(section.body, values, item)
	case "DELETE":
		return applyDeleteUpdateAction(section.body, values, item)
	default:
		return errors.New("unsupported update action")
	}
}

func parseUpdateActionToken(expr string, pos int) (string, int, bool) {
	actions := []string{"SET", "REMOVE", "ADD", "DELETE"}
	for _, action := range actions {
		end := pos + len(action)
		if end > len(expr) {
			continue
		}
		if !strings.EqualFold(expr[pos:end], action) {
			continue
		}
		if !isLogicalKeywordBoundary(expr, pos-1) || !isLogicalKeywordBoundary(expr, end) {
			continue
		}
		return action, end, true
	}
	return "", 0, false
}

func skipSpaces(expr string, pos int) int {
	for pos < len(expr) && (expr[pos] == ' ' || expr[pos] == '\t' || expr[pos] == '\n' || expr[pos] == '\r') {
		pos++
	}
	return pos
}

func findNextUpdateAction(expr string, start int) int {
	depth := 0
	for i := start; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		_, _, ok := parseUpdateActionToken(expr, i)
		if ok {
			return i
		}
	}
	return -1
}

func applySetUpdateAction(body string, values map[string]attributeValue, item map[string]attributeValue) error {
	assignments, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, assignment := range assignments {
		if err := applySingleSetAssignment(assignment, values, item); err != nil {
			return err
		}
	}
	return nil
}

func applySingleSetAssignment(assignment string, values map[string]attributeValue, item map[string]attributeValue) error {
	parts := strings.SplitN(assignment, "=", updateSplitCount)
	if len(parts) != updateSplitCount {
		return errors.New("invalid update expression")
	}
	path := strings.TrimSpace(parts[0])
	if path == "" {
		return errors.New("invalid update expression attribute")
	}
	valueExpr := strings.TrimSpace(parts[1])
	valueAttr, err := evalUpdateValueExpression(valueExpr, values, item)
	if err != nil {
		return err
	}
	return setDocumentPath(item, path, valueAttr)
}

func applyRemoveUpdateAction(body string, item map[string]attributeValue) error {
	attrs, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, attr := range attrs {
		path := strings.TrimSpace(attr)
		if path == "" {
			return errors.New("invalid update expression attribute")
		}
		if err := removeDocumentPath(item, path); err != nil {
			return err
		}
	}
	return nil
}

func applyAddUpdateAction(body string, values map[string]attributeValue, item map[string]attributeValue) error {
	terms, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, term := range terms {
		if err := applySingleAddTerm(term, values, item); err != nil {
			return err
		}
	}
	return nil
}

func applySingleAddTerm(term string, values map[string]attributeValue, item map[string]attributeValue) error {
	parts := strings.Fields(term)
	if len(parts) != updateSplitCount {
		return errors.New("invalid update expression")
	}
	path := strings.TrimSpace(parts[0])
	placeholder := strings.TrimSpace(parts[1])
	if path == "" || !strings.HasPrefix(placeholder, ":") {
		return errors.New("invalid update expression")
	}
	addValue, ok := values[placeholder]
	if !ok {
		return errors.New("missing value attribute")
	}
	current, exists, err := resolveDocumentPath(item, path)
	if err != nil {
		return err
	}
	next, err := addAttributeValue(current, exists, addValue)
	if err != nil {
		return err
	}
	return setDocumentPath(item, path, next)
}

func addNumericAttributeValues(left string, right string) (string, error) {
	leftRat, rightRat := &big.Rat{}, &big.Rat{}
	if _, ok := leftRat.SetString(strings.TrimSpace(left)); !ok {
		return "", errors.New("invalid number attribute")
	}
	if _, ok := rightRat.SetString(strings.TrimSpace(right)); !ok {
		return "", errors.New("invalid number attribute")
	}
	sum := &big.Rat{}
	sum.Add(leftRat, rightRat)
	if sum.IsInt() {
		return sum.Num().String(), nil
	}
	out := strings.TrimRight(sum.FloatString(numericUpdateScaleDigits), "0")
	out = strings.TrimRight(out, ".")
	if out == "" {
		return "0", nil
	}
	return out, nil
}

func applyDeleteUpdateAction(body string, values map[string]attributeValue, item map[string]attributeValue) error {
	terms, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, term := range terms {
		if err := applySingleDeleteTerm(term, values, item); err != nil {
			return err
		}
	}
	return nil
}

func applySingleDeleteTerm(term string, values map[string]attributeValue, item map[string]attributeValue) error {
	fields := strings.Fields(strings.TrimSpace(term))
	switch len(fields) {
	case 0:
		return errors.New("invalid update expression")
	case 1:
		return removeDocumentPath(item, fields[0])
	case updateSplitCount:
		return applyDeleteSetTerm(fields[0], fields[1], values, item)
	default:
		return errors.New("invalid update expression")
	}
}

func applyDeleteSetTerm(pathExpr string, placeholderExpr string, values map[string]attributeValue, item map[string]attributeValue) error {
	path := strings.TrimSpace(pathExpr)
	placeholder := strings.TrimSpace(placeholderExpr)
	if path == "" || !strings.HasPrefix(placeholder, ":") {
		return errors.New("invalid update expression")
	}
	deleteValue, ok := values[placeholder]
	if !ok {
		return errors.New("missing value attribute")
	}
	current, found, err := resolveDocumentPath(item, path)
	if err != nil || !found {
		return err
	}
	next, removeAttr, err := deleteAttributeValueElements(current, deleteValue)
	if err != nil {
		return err
	}
	if removeAttr {
		return removeDocumentPath(item, path)
	}
	return setDocumentPath(item, path, next)
}

func splitTopLevelByComma(expr string) ([]string, error) {
	depth := 0
	last := 0
	parts := make([]string, 0, splitPartsInitialCapacity)
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth < 0 {
			return nil, errors.New("invalid expression")
		}
		if depth != 0 || expr[i] != ',' {
			continue
		}
		part := strings.TrimSpace(expr[last:i])
		if part == "" {
			return nil, errors.New("invalid expression")
		}
		parts = append(parts, part)
		last = i + 1
	}
	if depth != 0 {
		return nil, errors.New("invalid expression")
	}
	tail := strings.TrimSpace(expr[last:])
	if tail == "" {
		return nil, errors.New("invalid expression")
	}
	return append(parts, tail), nil
}

type documentPathToken struct {
	attr    string
	index   int
	isIndex bool
}

func parseDocumentPath(path string) ([]documentPathToken, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("invalid document path")
	}
	tokens := make([]documentPathToken, 0, updateSplitCount)
	for pos := 0; pos < len(path); {
		nextPos, token, err := consumeDocumentPathToken(path, pos)
		if err != nil {
			return nil, err
		}
		pos = nextPos
		if token.attr != "" || token.isIndex {
			tokens = append(tokens, token)
		}
	}
	if len(tokens) == 0 {
		return nil, errors.New("invalid document path")
	}
	return tokens, nil
}

func consumeDocumentPathToken(path string, pos int) (int, documentPathToken, error) {
	switch path[pos] {
	case '.':
		return pos + 1, documentPathToken{}, nil
	case '[':
		return consumeDocumentPathIndex(path, pos)
	default:
		return consumeDocumentPathAttr(path, pos)
	}
}

func consumeDocumentPathIndex(path string, pos int) (int, documentPathToken, error) {
	end := strings.IndexByte(path[pos:], ']')
	if end <= 1 {
		return 0, documentPathToken{}, errors.New("invalid document path")
	}
	indexValue, err := strconv.Atoi(path[pos+1 : pos+end])
	if err != nil || indexValue < 0 {
		return 0, documentPathToken{}, errors.New("invalid document path")
	}
	return pos + end + 1, documentPathToken{index: indexValue, isIndex: true}, nil
}

func consumeDocumentPathAttr(path string, pos int) (int, documentPathToken, error) {
	start := pos
	for pos < len(path) && path[pos] != '.' && path[pos] != '[' {
		pos++
	}
	attr := strings.TrimSpace(path[start:pos])
	if attr == "" {
		return 0, documentPathToken{}, errors.New("invalid document path")
	}
	return pos, documentPathToken{attr: attr}, nil
}

func resolveDocumentPath(item map[string]attributeValue, path string) (attributeValue, bool, error) {
	tokens, err := parseDocumentPath(path)
	if err != nil {
		return attributeValue{}, false, err
	}
	current := attributeValue{M: item}
	found := true
	for _, token := range tokens {
		current, found = nextDocumentPathValue(current, found, token)
		if !found {
			return attributeValue{}, false, nil
		}
	}
	return cloneAttributeValue(current), true, nil
}

func nextDocumentPathValue(current attributeValue, found bool, token documentPathToken) (attributeValue, bool) {
	if !found {
		return attributeValue{}, false
	}
	if token.isIndex {
		if !current.hasListType() || token.index >= len(current.L) {
			return attributeValue{}, false
		}
		return current.L[token.index], true
	}
	if !current.hasMapType() {
		return attributeValue{}, false
	}
	value, ok := current.M[token.attr]
	if !ok {
		return attributeValue{}, false
	}
	return value, true
}

func setDocumentPath(item map[string]attributeValue, path string, value attributeValue) error {
	tokens, err := parseDocumentPath(path)
	if err != nil {
		return err
	}
	root, err := setDocumentPathValue(attributeValue{M: cloneAttributeValueMap(item)}, true, tokens, value)
	if err != nil {
		return err
	}
	replaceAttributeValueMap(item, root.M)
	return nil
}

func setDocumentPathValue(current attributeValue, exists bool, tokens []documentPathToken, value attributeValue) (attributeValue, error) {
	if len(tokens) == 0 {
		return cloneAttributeValue(value), nil
	}
	token := tokens[0]
	if token.isIndex {
		return setDocumentPathIndex(current, exists, token, tokens[1:], value)
	}
	return setDocumentPathAttribute(current, exists, token, tokens[1:], value)
}

func setDocumentPathIndex(current attributeValue, exists bool, token documentPathToken, rest []documentPathToken, value attributeValue) (attributeValue, error) {
	if !exists || !current.hasListType() {
		return attributeValue{}, errors.New("invalid document path")
	}
	list := cloneAttributeValueList(current.L)
	if token.index > len(list) {
		return attributeValue{}, errors.New("invalid document path")
	}
	if token.index == len(list) {
		return appendDocumentPathIndex(list, rest, value)
	}
	nextValue, err := setDocumentPathValue(list[token.index], true, rest, value)
	if err != nil {
		return attributeValue{}, err
	}
	list[token.index] = nextValue
	return attributeValue{L: list}, nil
}

func appendDocumentPathIndex(list []attributeValue, rest []documentPathToken, value attributeValue) (attributeValue, error) {
	child := value
	if len(rest) > 0 {
		var err error
		child, err = setDocumentPathValue(newDocumentContainer(rest[0]), true, rest, value)
		if err != nil {
			return attributeValue{}, err
		}
	}
	list = append(list, cloneAttributeValue(child))
	return attributeValue{L: list}, nil
}

func setDocumentPathAttribute(current attributeValue, exists bool, token documentPathToken, rest []documentPathToken, value attributeValue) (attributeValue, error) {
	var object map[string]attributeValue
	if exists {
		if !current.hasMapType() {
			return attributeValue{}, errors.New("invalid document path")
		}
		object = cloneAttributeValueMap(current.M)
	} else {
		object = map[string]attributeValue{}
	}
	child, childExists := object[token.attr]
	if !childExists && len(rest) > 0 {
		child = newDocumentContainer(rest[0])
		childExists = true
	}
	nextValue, err := setDocumentPathValue(child, childExists, rest, value)
	if err != nil {
		return attributeValue{}, err
	}
	object[token.attr] = nextValue
	return attributeValue{M: object}, nil
}

func newDocumentContainer(next documentPathToken) attributeValue {
	if next.isIndex {
		return attributeValue{L: []attributeValue{}}
	}
	return attributeValue{M: map[string]attributeValue{}}
}

func removeDocumentPath(item map[string]attributeValue, path string) error {
	tokens, err := parseDocumentPath(path)
	if err != nil {
		return err
	}
	root, err := removeDocumentPathValue(attributeValue{M: cloneAttributeValueMap(item)}, true, tokens)
	if err != nil {
		return err
	}
	replaceAttributeValueMap(item, root.M)
	return nil
}

func removeDocumentPathValue(current attributeValue, exists bool, tokens []documentPathToken) (attributeValue, error) {
	if !exists || len(tokens) == 0 {
		return current, nil
	}
	token := tokens[0]
	if token.isIndex {
		return removeDocumentPathIndex(current, token, tokens[1:])
	}
	return removeDocumentPathAttribute(current, token, tokens[1:])
}

func removeDocumentPathIndex(current attributeValue, token documentPathToken, rest []documentPathToken) (attributeValue, error) {
	if !current.hasListType() || token.index >= len(current.L) {
		return current, nil
	}
	list := cloneAttributeValueList(current.L)
	if len(rest) == 0 {
		list = append(list[:token.index], list[token.index+1:]...)
		return attributeValue{L: list}, nil
	}
	nextValue, err := removeDocumentPathValue(list[token.index], true, rest)
	if err != nil {
		return attributeValue{}, err
	}
	list[token.index] = nextValue
	return attributeValue{L: list}, nil
}

func removeDocumentPathAttribute(current attributeValue, token documentPathToken, rest []documentPathToken) (attributeValue, error) {
	if !current.hasMapType() {
		return current, nil
	}
	object := cloneAttributeValueMap(current.M)
	child, ok := object[token.attr]
	if !ok {
		return current, nil
	}
	if len(rest) == 0 {
		delete(object, token.attr)
		return attributeValue{M: object}, nil
	}
	nextValue, err := removeDocumentPathValue(child, true, rest)
	if err != nil {
		return attributeValue{}, err
	}
	object[token.attr] = nextValue
	return attributeValue{M: object}, nil
}

func replaceAttributeValueMap(dst map[string]attributeValue, src map[string]attributeValue) {
	clear(dst)
	maps.Copy(dst, src)
}

func deleteAttributeValueElements(current attributeValue, deleteValue attributeValue) (attributeValue, bool, error) {
	currentKind, _ := detectAttributeValueKind(current)
	deleteKind, _ := detectAttributeValueKind(deleteValue)
	if currentKind != deleteKind {
		return attributeValue{}, false, errors.New("DELETE supports only matching set attribute types")
	}
	switch currentKind {
	case attributeValueKindStringSet:
		return buildDeleteSetResult(attributeValue{SS: subtractStringSet(current.SS, deleteValue.SS)})
	case attributeValueKindNumberSet:
		return buildDeleteSetResult(attributeValue{NS: subtractNumberSet(current.NS, deleteValue.NS)})
	case attributeValueKindBinarySet:
		return buildDeleteSetResult(attributeValue{BS: subtractBinarySet(current.BS, deleteValue.BS)})
	case attributeValueKindInvalid,
		attributeValueKindString,
		attributeValueKindNumber,
		attributeValueKindBinary,
		attributeValueKindBool,
		attributeValueKindNull,
		attributeValueKindList,
		attributeValueKindMap:
		return attributeValue{}, false, errors.New("DELETE supports only matching set attribute types")
	}
	return attributeValue{}, false, errors.New("DELETE supports only matching set attribute types")
}

func buildDeleteSetResult(next attributeValue) (attributeValue, bool, error) {
	if next.hasStringSetType() && len(next.SS) == 0 {
		return attributeValue{}, true, nil
	}
	if next.hasNumberSetType() && len(next.NS) == 0 {
		return attributeValue{}, true, nil
	}
	if next.hasBinarySetType() && len(next.BS) == 0 {
		return attributeValue{}, true, nil
	}
	return next, false, nil
}

func subtractStringSet(current []string, remove []string) []string {
	removeSet := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		removeSet[value] = struct{}{}
	}
	out := make([]string, 0, len(current))
	for _, value := range current {
		if _, ok := removeSet[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func subtractNumberSet(current []string, remove []string) []string {
	removeSet := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		removeSet[canonicalNumberString(value)] = struct{}{}
	}
	out := make([]string, 0, len(current))
	for _, value := range current {
		if _, ok := removeSet[canonicalNumberString(value)]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func subtractBinarySet(current [][]byte, remove [][]byte) [][]byte {
	removeSet := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		removeSet[string(value)] = struct{}{}
	}
	out := make([][]byte, 0, len(current))
	for _, value := range current {
		if _, ok := removeSet[string(value)]; ok {
			continue
		}
		out = append(out, bytes.Clone(value))
	}
	return out
}

func evalUpdateValueExpression(expr string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return attributeValue{}, errors.New("invalid update expression")
	}
	if value, handled, err := evalArithmeticUpdateOperand(expr, values, item); handled {
		return value, err
	}
	if value, handled, err := evalNamedUpdateFunction(expr, values, item, "if_not_exists", evalIfNotExistsUpdateValue); handled {
		return value, err
	}
	if value, handled, err := evalNamedUpdateFunction(expr, values, item, "list_append", evalListAppendUpdateValue); handled {
		return value, err
	}
	return evalUpdateTerminalValue(expr, values, item)
}

func evalArithmeticUpdateOperand(expr string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, bool, error) {
	index, op, ok := findTopLevelArithmeticOperator(expr)
	if !ok {
		return attributeValue{}, false, nil
	}
	left, err := evalUpdateValueExpression(expr[:index], values, item)
	if err != nil {
		return attributeValue{}, true, err
	}
	right, err := evalUpdateValueExpression(expr[index+1:], values, item)
	if err != nil {
		return attributeValue{}, true, err
	}
	value, err := applyArithmeticUpdateValue(left, right, op)
	return value, true, err
}

func evalNamedUpdateFunction(
	expr string,
	values map[string]attributeValue,
	item map[string]attributeValue,
	name string,
	eval func([]string, map[string]attributeValue, map[string]attributeValue) (attributeValue, error),
) (attributeValue, bool, error) {
	args, ok, err := parseExpressionFunctionArgs(expr, name)
	if err != nil {
		return attributeValue{}, true, err
	}
	if !ok {
		return attributeValue{}, false, nil
	}
	value, err := eval(args, values, item)
	return value, true, err
}

func evalUpdateTerminalValue(expr string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	if strings.HasPrefix(expr, ":") {
		value, ok := values[expr]
		if !ok {
			return attributeValue{}, errors.New("missing value attribute")
		}
		return cloneAttributeValue(value), nil
	}
	value, found, err := resolveDocumentPath(item, expr)
	if err != nil {
		return attributeValue{}, err
	}
	if !found {
		return attributeValue{}, errors.New("missing value attribute")
	}
	return value, nil
}

func evalIfNotExistsUpdateValue(args []string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	if len(args) != updateSplitCount {
		return attributeValue{}, errors.New("invalid update expression")
	}
	current, found, err := resolveDocumentPath(item, strings.TrimSpace(args[0]))
	if err != nil {
		return attributeValue{}, err
	}
	if found {
		return current, nil
	}
	return evalUpdateValueExpression(args[1], values, item)
}

func evalListAppendUpdateValue(args []string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	if len(args) != updateSplitCount {
		return attributeValue{}, errors.New("invalid update expression")
	}
	left, err := evalUpdateValueExpression(args[0], values, item)
	if err != nil {
		return attributeValue{}, err
	}
	right, err := evalUpdateValueExpression(args[1], values, item)
	if err != nil {
		return attributeValue{}, err
	}
	if !left.hasListType() || !right.hasListType() {
		return attributeValue{}, errors.New("list_append supports only list attributes")
	}
	out := make([]attributeValue, 0, len(left.L)+len(right.L))
	for _, value := range left.L {
		out = append(out, cloneAttributeValue(value))
	}
	for _, value := range right.L {
		out = append(out, cloneAttributeValue(value))
	}
	return attributeValue{L: out}, nil
}

func applyArithmeticUpdateValue(left attributeValue, right attributeValue, op byte) (attributeValue, error) {
	if !left.hasNumberType() || !right.hasNumberType() {
		return attributeValue{}, errors.New("arithmetic update supports only number attributes")
	}
	rightValue := right.numberValue()
	if op == '-' {
		rightValue = "-" + rightValue
	}
	sum, err := addNumericAttributeValues(left.numberValue(), rightValue)
	if err != nil {
		return attributeValue{}, err
	}
	return attributeValue{N: &sum}, nil
}

func findTopLevelArithmeticOperator(expr string) (int, byte, bool) {
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		switch expr[i] {
		case '+', '-':
			if i == 0 {
				continue
			}
			return i, expr[i], true
		}
	}
	return 0, 0, false
}

func parseExpressionFunctionArgs(expr string, funcName string) ([]string, bool, error) {
	prefix := funcName + "("
	if !strings.HasPrefix(strings.ToLower(expr), strings.ToLower(prefix)) || !strings.HasSuffix(expr, ")") {
		return nil, false, nil
	}
	inner := strings.TrimSpace(expr[len(prefix) : len(expr)-1])
	parts, err := splitTopLevelByComma(inner)
	if err != nil {
		return nil, true, errors.New("invalid expression")
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts, true, nil
}

func addAttributeValue(current attributeValue, exists bool, addValue attributeValue) (attributeValue, error) {
	if addValue.hasNumberType() {
		return addNumericUpdateValue(current, exists, addValue)
	}
	if addValue.hasStringSetType() || addValue.hasNumberSetType() || addValue.hasBinarySetType() {
		return addSetUpdateValue(current, exists, addValue)
	}
	return attributeValue{}, errors.New("ADD supports only number or set attributes")
}

func addNumericUpdateValue(current attributeValue, exists bool, addValue attributeValue) (attributeValue, error) {
	if !exists {
		return cloneAttributeValue(addValue), nil
	}
	if !current.hasNumberType() {
		return attributeValue{}, errors.New("ADD supports only number attributes")
	}
	sum, err := addNumericAttributeValues(current.numberValue(), addValue.numberValue())
	if err != nil {
		return attributeValue{}, err
	}
	return attributeValue{N: &sum}, nil
}

func addSetUpdateValue(current attributeValue, exists bool, addValue attributeValue) (attributeValue, error) {
	if !exists {
		return cloneAttributeValue(addValue), nil
	}
	switch {
	case current.hasStringSetType() && addValue.hasStringSetType():
		return attributeValue{SS: mergeStringSet(current.SS, addValue.SS)}, nil
	case current.hasNumberSetType() && addValue.hasNumberSetType():
		return attributeValue{NS: mergeNumberSet(current.NS, addValue.NS)}, nil
	case current.hasBinarySetType() && addValue.hasBinarySetType():
		return attributeValue{BS: mergeBinarySet(current.BS, addValue.BS)}, nil
	default:
		return attributeValue{}, errors.New("ADD supports only matching set attribute types")
	}
}

func mergeStringSet(current []string, add []string) []string {
	out := make([]string, 0, len(current)+len(add))
	seen := make(map[string]struct{}, len(current)+len(add))
	for _, value := range append(append([]string(nil), current...), add...) {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func mergeNumberSet(current []string, add []string) []string {
	out := make([]string, 0, len(current)+len(add))
	seen := make(map[string]struct{}, len(current)+len(add))
	for _, value := range append(append([]string(nil), current...), add...) {
		key := canonicalNumberString(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func mergeBinarySet(current [][]byte, add [][]byte) [][]byte {
	out := make([][]byte, 0, len(current)+len(add))
	seen := make(map[string]struct{}, len(current)+len(add))
	for _, value := range append(cloneBinarySet(current), add...) {
		key := string(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, bytes.Clone(value))
	}
	return out
}

func validateConditionOnItem(expr string, names map[string]string, values map[string]attributeValue, item map[string]attributeValue) error {
	cond, err := replaceNames(expr, names)
	if err != nil {
		return err
	}
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return nil
	}
	ok, err := evalConditionExpression(cond, item, values)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("conditional check failed")
	}
	return nil
}

func evalConditionExpression(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	expr = trimOuterParens(strings.TrimSpace(expr))
	if expr == "" {
		return true, nil
	}
	if ok, handled, err := evalLogicalCondition(expr, "OR", item, values); handled {
		return ok, err
	}
	if ok, handled, err := evalLogicalCondition(expr, "AND", item, values); handled {
		return ok, err
	}
	if rest, ok := trimLeadingKeyword(expr, "NOT"); ok {
		ok, err := evalConditionExpression(rest, item, values)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}
	return evalAtomicCondition(expr, item, values)
}

func trimOuterParens(expr string) string {
	for {
		expr = strings.TrimSpace(expr)
		if !hasOuterParens(expr) {
			return expr
		}
		expr = expr[1 : len(expr)-1]
	}
}

func splitTopLevelByKeyword(expr string, keyword string) []string {
	if expr == "" {
		return nil
	}
	upper := strings.ToUpper(expr)
	target := strings.ToUpper(keyword)
	targetLen := len(target)
	if targetLen == 0 {
		return nil
	}
	depth := 0
	last := 0
	betweenPending := false
	parts := make([]string, 0, splitPartsInitialCapacity)
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		if nextIndex, handled, nextPending := consumeBetweenSplitState(expr, upper, keyword, i, betweenPending); handled {
			betweenPending = nextPending
			i = nextIndex
			continue
		}
		if !shouldSplitKeywordAt(expr, upper, target, targetLen, i) {
			continue
		}
		part, ok := trimmedNonEmpty(expr[last:i])
		if !ok {
			return nil
		}
		parts = append(parts, part)
		i += targetLen - 1
		last = i + 1
	}
	return finalizeKeywordSplit(expr[last:], parts)
}

func consumeBetweenSplitState(expr string, upper string, keyword string, index int, betweenPending bool) (int, bool, bool) {
	if !strings.EqualFold(keyword, "AND") {
		return index, false, betweenPending
	}
	if matchesLogicalKeyword(expr, upper, "BETWEEN", index) {
		return index + len("BETWEEN") - 1, true, true
	}
	if betweenPending && matchesLogicalKeyword(expr, upper, "AND", index) {
		return index + len("AND") - 1, true, false
	}
	return index, false, betweenPending
}

func shouldSplitKeywordAt(expr string, upper string, target string, targetLen int, index int) bool {
	return matchesKeywordTokenAt(upper, target, index) &&
		isLogicalKeywordBoundary(expr, index-1) &&
		isLogicalKeywordBoundary(expr, index+targetLen)
}

func matchesLogicalKeyword(expr string, upper string, keyword string, index int) bool {
	return matchesKeywordTokenAt(upper, keyword, index) &&
		isLogicalKeywordBoundary(expr, index-1) &&
		isLogicalKeywordBoundary(expr, index+len(keyword))
}

func evalLogicalCondition(expr string, keyword string, item map[string]attributeValue, values map[string]attributeValue) (bool, bool, error) {
	parts := splitTopLevelByKeyword(expr, keyword)
	if len(parts) == 0 {
		return false, false, nil
	}
	if strings.EqualFold(keyword, "OR") {
		ok, err := evalConditionAny(parts, item, values)
		return ok, true, err
	}
	ok, err := evalConditionAll(parts, item, values)
	return ok, true, err
}

func evalConditionAny(parts []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, part := range parts {
		ok, err := evalConditionExpression(part, item, values)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func evalConditionAll(parts []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, part := range parts {
		ok, err := evalConditionExpression(part, item, values)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evalAtomicCondition(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, handler := range conditionFunctionHandlers {
		if ok, handled, err := evalNamedConditionFunction(expr, item, values, handler); handled {
			return ok, err
		}
	}
	if ok, handled, err := evalConditionBetween(expr, item, values); handled {
		return ok, err
	}
	if ok, handled, err := evalConditionIn(expr, item, values); handled {
		return ok, err
	}
	return evalConditionComparison(expr, item, values)
}

type conditionFunctionHandler struct {
	name string
	eval func([]string, map[string]attributeValue, map[string]attributeValue) (bool, error)
}

var conditionFunctionHandlers = []conditionFunctionHandler{
	{
		name: "attribute_exists",
		eval: func(args []string, item map[string]attributeValue, _ map[string]attributeValue) (bool, error) {
			return evalAttributeExistsCondition(args, item)
		},
	},
	{
		name: "attribute_not_exists",
		eval: func(args []string, item map[string]attributeValue, _ map[string]attributeValue) (bool, error) {
			return evalAttributeNotExistsCondition(args, item)
		},
	},
	{name: "attribute_type", eval: evalAttributeTypeCondition},
	{name: "begins_with", eval: evalBeginsWithCondition},
	{name: "contains", eval: evalContainsCondition},
}

func evalNamedConditionFunction(
	expr string,
	item map[string]attributeValue,
	values map[string]attributeValue,
	handler conditionFunctionHandler,
) (bool, bool, error) {
	args, ok, err := parseExpressionFunctionArgs(expr, handler.name)
	if err != nil {
		return false, true, err
	}
	if !ok {
		return false, false, nil
	}
	value, err := handler.eval(args, item, values)
	return value, true, err
}

func evalAttributeExistsCondition(args []string, item map[string]attributeValue) (bool, error) {
	if len(args) != 1 {
		return false, errors.New("unsupported condition expression")
	}
	_, found, err := resolveDocumentPath(item, args[0])
	if err != nil {
		return false, err
	}
	return found, nil
}

func evalAttributeNotExistsCondition(args []string, item map[string]attributeValue) (bool, error) {
	ok, err := evalAttributeExistsCondition(args, item)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func evalAttributeTypeCondition(args []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	if len(args) != updateSplitCount {
		return false, errors.New("unsupported condition expression")
	}
	value, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return false, err
	}
	typeValue, ok := values[strings.TrimSpace(args[1])]
	if !ok || !typeValue.hasStringType() {
		return false, errors.New("unsupported condition expression")
	}
	return dynamoAttributeType(value) == typeValue.stringValue(), nil
}

func evalBeginsWithCondition(args []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	if len(args) != updateSplitCount {
		return false, errors.New("unsupported condition expression")
	}
	left, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return false, err
	}
	right, ok := values[strings.TrimSpace(args[1])]
	if !ok {
		return false, errors.New("missing condition value")
	}
	switch {
	case left.hasStringType() && right.hasStringType():
		return strings.HasPrefix(left.stringValue(), right.stringValue()), nil
	case left.hasBinaryType() && right.hasBinaryType():
		return bytes.HasPrefix(left.B, right.B), nil
	default:
		return false, nil
	}
}

func evalContainsCondition(args []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	if len(args) != updateSplitCount {
		return false, errors.New("unsupported condition expression")
	}
	left, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return false, err
	}
	right, ok := values[strings.TrimSpace(args[1])]
	if !ok {
		return false, errors.New("missing condition value")
	}
	return attributeValueContains(left, right), nil
}

func attributeValueContains(left attributeValue, right attributeValue) bool {
	for _, eval := range attributeValueContainsEvaluators {
		if handled, ok := eval(left, right); handled {
			return ok
		}
	}
	return false
}

type attributeValueContainsEvaluator func(attributeValue, attributeValue) (bool, bool)

var attributeValueContainsEvaluators = []attributeValueContainsEvaluator{
	containsStringAttributeValue,
	containsBinaryAttributeValue,
	containsListAttributeValue,
	containsStringSetAttributeValue,
	containsNumberSetAttributeValue,
	containsBinarySetAttributeValue,
}

func containsStringAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasStringType() || !right.hasStringType() {
		return false, false
	}
	return true, strings.Contains(left.stringValue(), right.stringValue())
}

func containsBinaryAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasBinaryType() || !right.hasBinaryType() {
		return false, false
	}
	return true, bytes.Contains(left.B, right.B)
}

func containsListAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasListType() {
		return false, false
	}
	return true, listContainsAttributeValue(left.L, right)
}

func containsStringSetAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasStringSetType() || !right.hasStringType() {
		return false, false
	}
	return true, stringSetContains(left.SS, right.stringValue())
}

func containsNumberSetAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasNumberSetType() || !right.hasNumberType() {
		return false, false
	}
	return true, numberSetContains(left.NS, right.numberValue())
}

func containsBinarySetAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasBinarySetType() || !right.hasBinaryType() {
		return false, false
	}
	return true, binarySetContains(left.BS, right.B)
}

func listContainsAttributeValue(values []attributeValue, needle attributeValue) bool {
	for _, value := range values {
		if attributeValueEqual(value, needle) {
			return true
		}
	}
	return false
}

func stringSetContains(values []string, needle string) bool {
	return slices.Contains(values, needle)
}

func numberSetContains(values []string, needle string) bool {
	for _, value := range values {
		if cmp, ok := compareNumericAttributeString(value, needle); ok && cmp == 0 {
			return true
		}
	}
	return false
}

func binarySetContains(values [][]byte, needle []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, needle) {
			return true
		}
	}
	return false
}

func evalConditionBetween(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, bool, error) {
	betweenIndex := findTopLevelKeywordIndex(expr, "BETWEEN")
	if betweenIndex < 0 {
		return false, false, nil
	}
	leftExpr := strings.TrimSpace(expr[:betweenIndex])
	rest := strings.TrimSpace(expr[betweenIndex+len("BETWEEN"):])
	andIndex := findTopLevelKeywordIndex(rest, "AND")
	if andIndex < 0 {
		return false, true, errors.New("unsupported condition expression")
	}
	lowerExpr := strings.TrimSpace(rest[:andIndex])
	upperExpr := strings.TrimSpace(rest[andIndex+len("AND"):])
	left, found, err := resolveConditionOperand(leftExpr, item, values)
	if err != nil || !found {
		return false, true, err
	}
	lower, found, err := resolveConditionOperand(lowerExpr, item, values)
	if err != nil || !found {
		return false, true, err
	}
	upper, found, err := resolveConditionOperand(upperExpr, item, values)
	if err != nil || !found {
		return false, true, err
	}
	return compareAttributeValueSortKey(left, lower) >= 0 && compareAttributeValueSortKey(left, upper) <= 0, true, nil
}

func evalConditionIn(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, bool, error) {
	inIndex := findTopLevelKeywordIndex(expr, "IN")
	if inIndex < 0 {
		return false, false, nil
	}
	left, parts, err := parseConditionInOperands(expr, inIndex, item, values)
	if err != nil {
		return false, true, err
	}
	ok, err := conditionInListContains(left, parts, item, values)
	return ok, true, err
}

func parseConditionInOperands(expr string, inIndex int, item map[string]attributeValue, values map[string]attributeValue) (attributeValue, []string, error) {
	leftExpr := strings.TrimSpace(expr[:inIndex])
	rest := strings.TrimSpace(expr[inIndex+len("IN"):])
	if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
		return attributeValue{}, nil, errors.New("unsupported condition expression")
	}
	left, found, err := resolveConditionOperand(leftExpr, item, values)
	if err != nil || !found {
		return attributeValue{}, nil, err
	}
	parts, err := splitTopLevelByComma(rest[1 : len(rest)-1])
	if err != nil {
		return attributeValue{}, nil, errors.New("unsupported condition expression")
	}
	return left, parts, nil
}

func conditionInListContains(left attributeValue, parts []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, part := range parts {
		candidate, found, err := resolveConditionOperand(part, item, values)
		if err != nil {
			return false, err
		}
		if found && attributeValueEqual(left, candidate) {
			return true, nil
		}
	}
	return false, nil
}

func evalConditionComparison(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	index, operator, ok := findTopLevelConditionComparator(expr)
	if !ok {
		return false, errors.New("unsupported condition expression")
	}
	left, right, err := resolveConditionComparisonOperands(expr, index, operator, item, values)
	if err != nil {
		return false, err
	}
	return compareConditionValues(operator, left, right)
}

func resolveConditionComparisonOperands(
	expr string,
	index int,
	operator string,
	item map[string]attributeValue,
	values map[string]attributeValue,
) (attributeValue, attributeValue, error) {
	leftExpr := strings.TrimSpace(expr[:index])
	rightExpr := strings.TrimSpace(expr[index+len(operator):])
	left, found, err := resolveConditionOperand(leftExpr, item, values)
	if err != nil || !found {
		return attributeValue{}, attributeValue{}, err
	}
	right, found, err := resolveConditionOperand(rightExpr, item, values)
	if err != nil || !found {
		return attributeValue{}, attributeValue{}, err
	}
	return left, right, nil
}

func compareConditionValues(operator string, left attributeValue, right attributeValue) (bool, error) {
	switch operator {
	case "=":
		return attributeValueEqual(left, right), nil
	case "<>":
		return !attributeValueEqual(left, right), nil
	case "<":
		return compareAttributeValueSortKey(left, right) < 0, nil
	case "<=":
		return compareAttributeValueSortKey(left, right) <= 0, nil
	case ">":
		return compareAttributeValueSortKey(left, right) > 0, nil
	case ">=":
		return compareAttributeValueSortKey(left, right) >= 0, nil
	default:
		return false, errors.New("unsupported condition expression")
	}
}

func resolveConditionOperand(expr string, item map[string]attributeValue, values map[string]attributeValue) (attributeValue, bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return attributeValue{}, false, errors.New("unsupported condition expression")
	}
	if args, ok, err := parseExpressionFunctionArgs(expr, "size"); ok || err != nil {
		if err != nil {
			return attributeValue{}, false, err
		}
		return resolveConditionSizeOperand(args, item)
	}
	if strings.HasPrefix(expr, ":") {
		value, ok := values[expr]
		if !ok {
			return attributeValue{}, false, errors.New("missing condition value")
		}
		return cloneAttributeValue(value), true, nil
	}
	value, found, err := resolveDocumentPath(item, expr)
	if err != nil {
		return attributeValue{}, false, err
	}
	return value, found, nil
}

func resolveConditionSizeOperand(args []string, item map[string]attributeValue) (attributeValue, bool, error) {
	if len(args) != 1 {
		return attributeValue{}, false, errors.New("unsupported condition expression")
	}
	value, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return attributeValue{}, false, err
	}
	size := attributeValueSize(value)
	sizeString := strconv.Itoa(size)
	return attributeValue{N: &sizeString}, true, nil
}

func attributeValueSize(value attributeValue) int {
	switch {
	case value.hasStringType():
		return len(value.stringValue())
	case value.hasBinaryType():
		return len(value.B)
	case value.hasStringSetType():
		return len(value.SS)
	case value.hasNumberSetType():
		return len(value.NS)
	case value.hasBinarySetType():
		return len(value.BS)
	case value.hasListType():
		return len(value.L)
	case value.hasMapType():
		return len(value.M)
	default:
		return 0
	}
}

func findTopLevelKeywordIndex(expr string, keyword string) int {
	upper := strings.ToUpper(expr)
	target := strings.ToUpper(keyword)
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 || !matchesKeywordTokenAt(upper, target, i) {
			continue
		}
		if !isLogicalKeywordBoundary(expr, i-1) || !isLogicalKeywordBoundary(expr, i+len(target)) {
			continue
		}
		return i
	}
	return -1
}

func trimLeadingKeyword(expr string, keyword string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(expr))
	keyword = strings.ToUpper(keyword)
	if !strings.HasPrefix(upper, keyword) {
		return "", false
	}
	trimmed := strings.TrimSpace(expr)
	if !isLogicalKeywordBoundary(trimmed, len(keyword)) {
		return "", false
	}
	return strings.TrimSpace(trimmed[len(keyword):]), true
}

func findTopLevelConditionComparator(expr string) (int, string, bool) {
	operators := []string{"<>", "<=", ">=", "=", "<", ">"}
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		for _, operator := range operators {
			if strings.HasPrefix(expr[i:], operator) {
				return i, operator, true
			}
		}
	}
	return 0, "", false
}

func dynamoAttributeType(value attributeValue) string {
	kind, count := detectAttributeValueKind(value)
	if count != 1 {
		return ""
	}
	return string(kind)
}

func hasOuterParens(expr string) bool {
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return false
	}
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth == 0 && i != len(expr)-1 {
			return false
		}
		if depth < 0 {
			return false
		}
	}
	return depth == 0
}

func nextParenDepth(depth int, ch byte) int {
	switch ch {
	case '(':
		return depth + 1
	case ')':
		return depth - 1
	default:
		return depth
	}
}

func matchesKeywordTokenAt(upperExpr string, target string, pos int) bool {
	end := pos + len(target)
	if end > len(upperExpr) {
		return false
	}
	return upperExpr[pos:end] == target
}

func isLogicalKeywordBoundary(s string, pos int) bool {
	if pos < 0 || pos >= len(s) {
		return true
	}
	ch := s[pos]
	// Keep identifier-style characters as token characters so expressions like
	// "MY_AND_VAR" or "a-OR-b" are not split at logical keyword substrings.
	if isExpressionAttributeNameByte(ch) {
		return false
	}
	return true
}

func trimmedNonEmpty(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	return trimmed, trimmed != ""
}

func finalizeKeywordSplit(tailExpr string, parts []string) []string {
	if len(parts) == 0 {
		return nil
	}
	tail, ok := trimmedNonEmpty(tailExpr)
	if !ok {
		return nil
	}
	return append(parts, tail)
}

type parsedKeyConditionTerm struct {
	attr         string
	op           queryRangeOperator
	placeholder1 string
	placeholder2 string
}

func parseKeyConditionExpression(expr string) ([]parsedKeyConditionTerm, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, errors.New("unsupported key condition expression")
	}
	parts, err := splitKeyConditionTerms(expr)
	if err != nil {
		return nil, err
	}
	if len(parts) > updateSplitCount {
		return nil, errors.New("unsupported key condition expression")
	}
	terms := make([]parsedKeyConditionTerm, 0, len(parts))
	for _, part := range parts {
		term, err := parseKeyConditionTerm(part)
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
	}
	return terms, nil
}

func splitKeyConditionTerms(expr string) ([]string, error) {
	upper := strings.ToUpper(expr)
	depth := 0
	last := 0
	betweenPending := false
	parts := make([]string, 0, splitPartsInitialCapacity)
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		keyword := keyConditionKeywordAt(expr, upper, i)
		if keyword == "" {
			continue
		}
		if keyword == "BETWEEN" {
			betweenPending = true
			i += len(keyword) - 1
			continue
		}
		if betweenPending {
			betweenPending = false
			i += len(keyword) - 1
			continue
		}
		part, ok := trimmedNonEmpty(expr[last:i])
		if !ok {
			return nil, errors.New("unsupported key condition expression")
		}
		parts = append(parts, part)
		i += len(keyword) - 1
		last = i + 1
	}
	if betweenPending {
		return nil, errors.New("unsupported key condition expression")
	}
	tail, ok := trimmedNonEmpty(expr[last:])
	if !ok {
		return nil, errors.New("unsupported key condition expression")
	}
	if len(parts) == 0 {
		return []string{tail}, nil
	}
	return append(parts, tail), nil
}

func keyConditionKeywordAt(expr string, upper string, pos int) string {
	if matchesKeywordTokenAt(upper, "BETWEEN", pos) &&
		isLogicalKeywordBoundary(expr, pos-1) &&
		isLogicalKeywordBoundary(expr, pos+len("BETWEEN")) {
		return "BETWEEN"
	}
	if matchesKeywordTokenAt(upper, "AND", pos) &&
		isLogicalKeywordBoundary(expr, pos-1) &&
		isLogicalKeywordBoundary(expr, pos+len("AND")) {
		return "AND"
	}
	return ""
}

func parseKeyConditionTerm(term string) (parsedKeyConditionTerm, error) {
	term = strings.TrimSpace(term)
	if t, ok, err := parseBeginsWithKeyConditionTerm(term); ok || err != nil {
		return t, err
	}
	if t, ok, err := parseBetweenKeyConditionTerm(term); ok || err != nil {
		return t, err
	}
	return parseComparisonKeyConditionTerm(term)
}

func parseBeginsWithKeyConditionTerm(term string) (parsedKeyConditionTerm, bool, error) {
	const prefix = "BEGINS_WITH("
	upper := strings.ToUpper(term)
	if !strings.HasPrefix(upper, prefix) {
		return parsedKeyConditionTerm{}, false, nil
	}
	if !strings.HasSuffix(term, ")") {
		return parsedKeyConditionTerm{}, true, errors.New("unsupported key condition expression")
	}
	inner := strings.TrimSpace(term[len(prefix) : len(term)-1])
	parts := strings.SplitN(inner, ",", updateSplitCount)
	if len(parts) != updateSplitCount {
		return parsedKeyConditionTerm{}, true, errors.New("unsupported key condition expression")
	}
	attrName := strings.TrimSpace(parts[0])
	placeholder := strings.TrimSpace(parts[1])
	if attrName == "" || !strings.HasPrefix(placeholder, ":") {
		return parsedKeyConditionTerm{}, true, errors.New("unsupported key condition expression")
	}
	return parsedKeyConditionTerm{
		attr:         attrName,
		op:           queryRangeOpBeginsWith,
		placeholder1: placeholder,
	}, true, nil
}

func parseBetweenKeyConditionTerm(term string) (parsedKeyConditionTerm, bool, error) {
	upper := strings.ToUpper(term)
	betweenIdx := strings.Index(upper, " BETWEEN ")
	if betweenIdx < 0 {
		return parsedKeyConditionTerm{}, false, nil
	}
	attrName := strings.TrimSpace(term[:betweenIdx])
	rest := strings.TrimSpace(term[betweenIdx+len(" BETWEEN "):])
	andIdx := strings.Index(strings.ToUpper(rest), " AND ")
	if andIdx < 0 {
		return parsedKeyConditionTerm{}, true, errors.New("unsupported key condition expression")
	}
	placeholder1 := strings.TrimSpace(rest[:andIdx])
	placeholder2 := strings.TrimSpace(rest[andIdx+len(" AND "):])
	if attrName == "" || !strings.HasPrefix(placeholder1, ":") || !strings.HasPrefix(placeholder2, ":") {
		return parsedKeyConditionTerm{}, true, errors.New("unsupported key condition expression")
	}
	return parsedKeyConditionTerm{
		attr:         attrName,
		op:           queryRangeOpBetween,
		placeholder1: placeholder1,
		placeholder2: placeholder2,
	}, true, nil
}

func parseComparisonKeyConditionTerm(term string) (parsedKeyConditionTerm, error) {
	operators := []queryRangeOperator{
		queryRangeOpLessOrEq,
		queryRangeOpGreaterEq,
		queryRangeOpLessThan,
		queryRangeOpGreater,
		queryRangeOpEqual,
	}
	for _, op := range operators {
		if t, ok := splitComparisonTerm(term, op); ok {
			return t, nil
		}
	}
	return parsedKeyConditionTerm{}, errors.New("unsupported key condition expression")
}

func splitComparisonTerm(term string, op queryRangeOperator) (parsedKeyConditionTerm, bool) {
	opStr := string(op)
	before, after, ok := strings.Cut(term, opStr)
	if !ok {
		return parsedKeyConditionTerm{}, false
	}
	left := strings.TrimSpace(before)
	right := strings.TrimSpace(after)
	if left == "" || !strings.HasPrefix(right, ":") {
		return parsedKeyConditionTerm{}, false
	}
	return parsedKeyConditionTerm{
		attr:         left,
		op:           op,
		placeholder1: right,
	}, true
}

func buildQueryCondition(keySchema dynamoKeySchema, terms []parsedKeyConditionTerm, values map[string]attributeValue) (queryCondition, error) {
	hashTerm, rangeTerm, err := classifyQueryConditionTerms(keySchema, terms)
	if err != nil {
		return queryCondition{}, err
	}
	hashValue, ok := values[hashTerm.placeholder1]
	if !ok {
		return queryCondition{}, errors.New("missing key condition value")
	}
	cond := queryCondition{
		hashAttr:  keySchema.HashKey,
		hashValue: hashValue,
	}
	if rangeTerm == nil {
		return cond, nil
	}
	value1, ok := values[rangeTerm.placeholder1]
	if !ok {
		return queryCondition{}, errors.New("missing key condition value")
	}
	rangeCond := &queryRangeCondition{
		attr:   keySchema.RangeKey,
		op:     rangeTerm.op,
		value1: value1,
	}
	if rangeTerm.op == queryRangeOpBetween {
		value2, ok := values[rangeTerm.placeholder2]
		if !ok {
			return queryCondition{}, errors.New("missing key condition value")
		}
		rangeCond.value2 = value2
	}
	cond.rangeCond = rangeCond
	return cond, nil
}

func classifyQueryConditionTerms(
	keySchema dynamoKeySchema,
	terms []parsedKeyConditionTerm,
) (parsedKeyConditionTerm, *parsedKeyConditionTerm, error) {
	if len(terms) == 0 || len(terms) > updateSplitCount {
		return parsedKeyConditionTerm{}, nil, errors.New("unsupported key condition")
	}
	hashTerm, ok := findHashConditionTerm(keySchema.HashKey, terms)
	if !ok {
		return parsedKeyConditionTerm{}, nil, errors.New("unsupported key condition")
	}
	if len(terms) == 1 {
		return hashTerm, nil, nil
	}
	rangeTerm, ok := findRangeConditionTerm(keySchema.RangeKey, terms, hashTerm)
	if !ok {
		return parsedKeyConditionTerm{}, nil, errors.New("unsupported key condition")
	}
	return hashTerm, &rangeTerm, nil
}

func findHashConditionTerm(hashKey string, terms []parsedKeyConditionTerm) (parsedKeyConditionTerm, bool) {
	var hashTerm parsedKeyConditionTerm
	found := false
	for _, term := range terms {
		if term.attr != hashKey || term.op != queryRangeOpEqual {
			continue
		}
		if found {
			return parsedKeyConditionTerm{}, false
		}
		hashTerm = term
		found = true
	}
	return hashTerm, found
}

func findRangeConditionTerm(
	rangeKey string,
	terms []parsedKeyConditionTerm,
	hashTerm parsedKeyConditionTerm,
) (parsedKeyConditionTerm, bool) {
	if strings.TrimSpace(rangeKey) == "" {
		return parsedKeyConditionTerm{}, false
	}
	for _, term := range terms {
		if term == hashTerm {
			continue
		}
		if term.attr != rangeKey {
			return parsedKeyConditionTerm{}, false
		}
		return term, true
	}
	return parsedKeyConditionTerm{}, false
}

func matchesQueryCondition(item map[string]attributeValue, cond queryCondition) bool {
	hashAttr, ok := item[cond.hashAttr]
	if !ok || !attributeValueEqual(hashAttr, cond.hashValue) {
		return false
	}
	if cond.rangeCond == nil {
		return true
	}
	rangeAttr, ok := item[cond.rangeCond.attr]
	if !ok {
		return false
	}
	return matchesQueryRangeCondition(rangeAttr, *cond.rangeCond)
}

func matchesQueryRangeCondition(attr attributeValue, cond queryRangeCondition) bool {
	if cond.op == queryRangeOpBeginsWith {
		return matchesQueryRangeBeginsWith(attr, cond.value1)
	}
	if cond.op == queryRangeOpBetween {
		return matchesQueryRangeBetween(attr, cond.value1, cond.value2)
	}
	return matchesQueryRangeCompare(attr, cond.value1, cond.op)
}

func matchesQueryRangeCompare(attr attributeValue, right attributeValue, op queryRangeOperator) bool {
	switch op {
	case queryRangeOpEqual:
		return attributeValueEqual(attr, right)
	case queryRangeOpLessThan:
		return compareAttributeValueSortKey(attr, right) < 0
	case queryRangeOpLessOrEq:
		return compareAttributeValueSortKey(attr, right) <= 0
	case queryRangeOpGreater:
		return compareAttributeValueSortKey(attr, right) > 0
	case queryRangeOpGreaterEq:
		return compareAttributeValueSortKey(attr, right) >= 0
	case queryRangeOpBetween, queryRangeOpBeginsWith:
		return false
	default:
		return false
	}
}

func matchesQueryRangeBetween(attr attributeValue, lower attributeValue, upper attributeValue) bool {
	return compareAttributeValueSortKey(attr, lower) >= 0 &&
		compareAttributeValueSortKey(attr, upper) <= 0
}

func matchesQueryRangeBeginsWith(attr attributeValue, prefixValue attributeValue) bool {
	attrKey, err := attributeValueAsKey(attr)
	if err != nil {
		return false
	}
	prefix, err := attributeValueAsKey(prefixValue)
	if err != nil {
		return false
	}
	return strings.HasPrefix(attrKey, prefix)
}

func parseCreateTableKeySchema(elems []createTableKeySchemaElement) (dynamoKeySchema, error) {
	var ks dynamoKeySchema
	for _, e := range elems {
		switch strings.ToUpper(strings.TrimSpace(e.KeyType)) {
		case "HASH":
			ks.HashKey = e.AttributeName
		case "RANGE":
			ks.RangeKey = e.AttributeName
		}
	}
	if strings.TrimSpace(ks.HashKey) == "" {
		return dynamoKeySchema{}, errors.New("missing HASH key schema")
	}
	return ks, nil
}

func (t *dynamoTableSchema) keySchemaForQuery(indexName string) (dynamoKeySchema, error) {
	if strings.TrimSpace(indexName) == "" {
		return t.PrimaryKey, nil
	}
	gsi, ok := t.GlobalSecondaryIndexes[indexName]
	if !ok {
		return dynamoKeySchema{}, errors.New("unknown index")
	}
	return gsi.KeySchema, nil
}

func (t *dynamoTableSchema) gsiProjectedAttributeSet(indexName string) (bool, map[string]struct{}, error) {
	gsi, ok := t.GlobalSecondaryIndexes[indexName]
	if !ok {
		return false, nil, errors.New("unknown index")
	}
	if strings.EqualFold(gsi.Projection.ProjectionType, "ALL") {
		return true, nil, nil
	}
	out := map[string]struct{}{
		t.PrimaryKey.HashKey:  {},
		gsi.KeySchema.HashKey: {},
	}
	if t.PrimaryKey.RangeKey != "" {
		out[t.PrimaryKey.RangeKey] = struct{}{}
	}
	if gsi.KeySchema.RangeKey != "" {
		out[gsi.KeySchema.RangeKey] = struct{}{}
	}
	for _, attr := range gsi.Projection.NonKeyAttributes {
		out[attr] = struct{}{}
	}
	return false, out, nil
}

func (t *dynamoTableSchema) projectItemForIndex(indexName string, item map[string]attributeValue) (map[string]attributeValue, error) {
	allProjected, projected, err := t.gsiProjectedAttributeSet(indexName)
	if err != nil {
		return nil, err
	}
	if allProjected {
		return cloneAttributeValueMap(item), nil
	}
	out := make(map[string]attributeValue, len(projected))
	for attr := range projected {
		if value, ok := item[attr]; ok {
			out[attr] = cloneAttributeValue(value)
		}
	}
	return out, nil
}

func (t *dynamoTableSchema) usesOrderedKeyEncoding() bool {
	return t != nil && t.KeyEncodingVersion >= dynamoOrderedKeyEncodingV2
}

func (t *dynamoTableSchema) needsLegacyKeyMigration() bool {
	return t != nil && (!t.usesOrderedKeyEncoding() || t.MigratingFromGeneration != 0)
}

func (t *dynamoTableSchema) migrationSourceSchema() *dynamoTableSchema {
	if t == nil || t.MigratingFromGeneration == 0 {
		return nil
	}
	return &dynamoTableSchema{
		TableName:              t.TableName,
		AttributeDefinitions:   t.AttributeDefinitions,
		PrimaryKey:             t.PrimaryKey,
		GlobalSecondaryIndexes: t.GlobalSecondaryIndexes,
		KeyEncodingVersion:     0,
		Generation:             t.MigratingFromGeneration,
	}
}

func (t *dynamoTableSchema) itemKeyFromAttributes(attrs map[string]attributeValue) ([]byte, error) {
	if !t.usesOrderedKeyEncoding() {
		return t.legacyItemKeyFromAttributes(attrs)
	}
	primary, err := t.primaryKeyValues(attrs)
	if err != nil {
		return nil, err
	}
	return dynamoItemKey(t.TableName, t.Generation, primary.hash, primary.rangeKey), nil
}

func (t *dynamoTableSchema) legacyItemKeyFromAttributes(attrs map[string]attributeValue) ([]byte, error) {
	hashAttr, ok := attrs[t.PrimaryKey.HashKey]
	if !ok {
		return nil, errors.New("missing hash key attribute")
	}
	hashKey, err := attributeValueAsKey(hashAttr)
	if err != nil {
		return nil, err
	}
	rangeKey := ""
	if t.PrimaryKey.RangeKey != "" {
		rangeAttr, ok := attrs[t.PrimaryKey.RangeKey]
		if !ok {
			return nil, errors.New("missing range key attribute")
		}
		rangeKey, err = attributeValueAsKey(rangeAttr)
		if err != nil {
			return nil, err
		}
	}
	return legacyDynamoItemKey(t.TableName, t.Generation, hashKey, rangeKey), nil
}

func (t *dynamoTableSchema) gsiKeyFromAttributes(indexName string, attrs map[string]attributeValue) ([]byte, bool, error) {
	if !t.usesOrderedKeyEncoding() {
		return t.legacyGSIKeyFromAttributes(indexName, attrs)
	}
	gsi, ok := t.GlobalSecondaryIndexes[indexName]
	if !ok {
		return nil, false, errors.New("global secondary index not found")
	}
	primary, err := t.primaryKeyValues(attrs)
	if err != nil {
		return nil, false, err
	}
	index, include, err := gsiKeyValues(attrs, gsi.KeySchema)
	if err != nil || !include {
		return nil, include, err
	}
	return dynamoGSIKey(t.TableName, t.Generation, indexName, index.hash, index.rangeKey, primary.hash, primary.rangeKey), true, nil
}

func (t *dynamoTableSchema) legacyGSIKeyFromAttributes(indexName string, attrs map[string]attributeValue) ([]byte, bool, error) {
	gsi, ok := t.GlobalSecondaryIndexes[indexName]
	if !ok {
		return nil, false, errors.New("global secondary index not found")
	}
	pkHash, pkRange, err := t.legacyPrimaryKeyValues(attrs)
	if err != nil {
		return nil, false, err
	}
	indexHash, indexRange, include, err := legacyGSIKeyValues(attrs, gsi.KeySchema)
	if err != nil || !include {
		return nil, include, err
	}
	return legacyDynamoGSIKey(t.TableName, t.Generation, indexName, indexHash, indexRange, pkHash, pkRange), true, nil
}

func (t *dynamoTableSchema) gsiEntryKeysForItem(attrs map[string]attributeValue) ([][]byte, error) {
	if len(t.GlobalSecondaryIndexes) == 0 || len(attrs) == 0 {
		return nil, nil
	}
	if !t.usesOrderedKeyEncoding() {
		return t.legacyGSIEntryKeysForItem(attrs)
	}
	primary, err := t.primaryKeyValues(attrs)
	if err != nil {
		return nil, err
	}
	indexNames := sortedGSIIndexNames(t.GlobalSecondaryIndexes)
	keys := make([][]byte, 0, len(indexNames))
	for _, indexName := range indexNames {
		gsi := t.GlobalSecondaryIndexes[indexName]
		index, include, err := gsiKeyValues(attrs, gsi.KeySchema)
		if err != nil {
			return nil, err
		}
		if !include {
			continue
		}
		keys = append(keys, dynamoGSIKey(t.TableName, t.Generation, indexName, index.hash, index.rangeKey, primary.hash, primary.rangeKey))
	}
	return keys, nil
}

func (t *dynamoTableSchema) legacyGSIEntryKeysForItem(attrs map[string]attributeValue) ([][]byte, error) {
	primaryHash, primaryRange, err := t.legacyPrimaryKeyValues(attrs)
	if err != nil {
		return nil, err
	}
	indexNames := sortedGSIIndexNames(t.GlobalSecondaryIndexes)
	keys := make([][]byte, 0, len(indexNames))
	for _, indexName := range indexNames {
		gsi := t.GlobalSecondaryIndexes[indexName]
		indexHash, indexRange, include, err := legacyGSIKeyValues(attrs, gsi.KeySchema)
		if err != nil {
			return nil, err
		}
		if !include {
			continue
		}
		keys = append(keys, legacyDynamoGSIKey(t.TableName, t.Generation, indexName, indexHash, indexRange, primaryHash, primaryRange))
	}
	return keys, nil
}

func (t *dynamoTableSchema) legacyPrimaryKeyValues(attrs map[string]attributeValue) (string, string, error) {
	hashAttr, ok := attrs[t.PrimaryKey.HashKey]
	if !ok {
		return "", "", errors.New("missing hash key attribute")
	}
	hash, err := attributeValueAsKey(hashAttr)
	if err != nil {
		return "", "", err
	}
	rangeKey := ""
	if t.PrimaryKey.RangeKey != "" {
		rangeAttr, ok := attrs[t.PrimaryKey.RangeKey]
		if !ok {
			return "", "", errors.New("missing range key attribute")
		}
		rangeKey, err = attributeValueAsKey(rangeAttr)
		if err != nil {
			return "", "", err
		}
	}
	return hash, rangeKey, nil
}

type dynamoEncodedKeyValues struct {
	hash     []byte
	rangeKey []byte
}

func (t *dynamoTableSchema) primaryKeyValues(attrs map[string]attributeValue) (dynamoEncodedKeyValues, error) {
	hashAttr, ok := attrs[t.PrimaryKey.HashKey]
	if !ok {
		return dynamoEncodedKeyValues{}, errors.New("missing hash key attribute")
	}
	hash, err := attributeValueAsKeySegment(hashAttr)
	if err != nil {
		return dynamoEncodedKeyValues{}, err
	}
	var rangeKey []byte
	if t.PrimaryKey.RangeKey != "" {
		rangeAttr, ok := attrs[t.PrimaryKey.RangeKey]
		if !ok {
			return dynamoEncodedKeyValues{}, errors.New("missing range key attribute")
		}
		rangeKey, err = attributeValueAsKeySegment(rangeAttr)
		if err != nil {
			return dynamoEncodedKeyValues{}, err
		}
	}
	return dynamoEncodedKeyValues{hash: hash, rangeKey: rangeKey}, nil
}

func gsiKeyValues(attrs map[string]attributeValue, ks dynamoKeySchema) (dynamoEncodedKeyValues, bool, error) {
	hashAttr, ok := attrs[ks.HashKey]
	if !ok {
		return dynamoEncodedKeyValues{}, false, nil
	}
	hash, err := attributeValueAsKeySegment(hashAttr)
	if err != nil {
		return dynamoEncodedKeyValues{}, false, err
	}
	var rangeKey []byte
	if ks.RangeKey != "" {
		rangeAttr, ok := attrs[ks.RangeKey]
		if !ok {
			return dynamoEncodedKeyValues{}, false, nil
		}
		rangeKey, err = attributeValueAsKeySegment(rangeAttr)
		if err != nil {
			return dynamoEncodedKeyValues{}, false, err
		}
	}
	return dynamoEncodedKeyValues{hash: hash, rangeKey: rangeKey}, true, nil
}

func legacyGSIKeyValues(attrs map[string]attributeValue, ks dynamoKeySchema) (string, string, bool, error) {
	hashAttr, ok := attrs[ks.HashKey]
	if !ok {
		return "", "", false, nil
	}
	hash, err := attributeValueAsKey(hashAttr)
	if err != nil {
		return "", "", false, err
	}
	rangeKey := ""
	if ks.RangeKey != "" {
		rangeAttr, ok := attrs[ks.RangeKey]
		if !ok {
			return "", "", false, nil
		}
		rangeKey, err = attributeValueAsKey(rangeAttr)
		if err != nil {
			return "", "", false, err
		}
	}
	return hash, rangeKey, true, nil
}

func sortedGSIIndexNames(indexes map[string]dynamoGlobalSecondaryIndex) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

var attributeValueKeyExtractors = map[attributeValueKind]func(attributeValue) string{
	attributeValueKindString: func(attr attributeValue) string { return attr.stringValue() },
	attributeValueKindNumber: func(attr attributeValue) string { return attr.numberValue() },
	attributeValueKindBinary: func(attr attributeValue) string { return string(attr.B) },
}

var attributeValueKeyByteExtractors = map[attributeValueKind]func(attributeValue) []byte{
	attributeValueKindString: func(attr attributeValue) []byte {
		return []byte(attr.stringValue())
	},
	attributeValueKindBinary: func(attr attributeValue) []byte {
		return bytes.Clone(attr.B)
	},
}

var attributeValueScalarEqualityComparators = map[attributeValueKind]func(attributeValue, attributeValue) bool{
	attributeValueKindString: func(left attributeValue, right attributeValue) bool { return left.stringValue() == right.stringValue() },
	attributeValueKindNumber: numberAttributeValueEqual,
	attributeValueKindBinary: func(left attributeValue, right attributeValue) bool { return bytes.Equal(left.B, right.B) },
	attributeValueKindBool:   func(left attributeValue, right attributeValue) bool { return *left.BOOL == *right.BOOL },
	attributeValueKindNull:   func(left attributeValue, right attributeValue) bool { return *left.NULL == *right.NULL },
	attributeValueKindStringSet: func(left attributeValue, right attributeValue) bool {
		return unorderedStringSlicesEqual(left.SS, right.SS)
	},
	attributeValueKindNumberSet: func(left attributeValue, right attributeValue) bool {
		return unorderedNumberSlicesEqual(left.NS, right.NS)
	},
	attributeValueKindBinarySet: func(left attributeValue, right attributeValue) bool {
		return unorderedBinarySlicesEqual(left.BS, right.BS)
	},
}

var attributeValueSortFormatters = map[attributeValueKind]func(attributeValue) string{
	attributeValueKindString:    func(attr attributeValue) string { return attr.stringValue() },
	attributeValueKindNumber:    func(attr attributeValue) string { return attr.numberValue() },
	attributeValueKindBinary:    func(attr attributeValue) string { return base64.RawURLEncoding.EncodeToString(attr.B) },
	attributeValueKindBool:      formatBoolAttributeValue,
	attributeValueKindNull:      func(attributeValue) string { return "" },
	attributeValueKindStringSet: func(attr attributeValue) string { return strings.Join(sortedStringSlice(attr.SS), "\x00") },
	attributeValueKindNumberSet: func(attr attributeValue) string { return strings.Join(sortedNumberStrings(attr.NS), "\x00") },
	attributeValueKindBinarySet: func(attr attributeValue) string { return strings.Join(sortedBinaryStrings(attr.BS), "\x00") },
}

func attributeValueAsKey(attr attributeValue) (string, error) {
	kind, count := detectAttributeValueKind(attr)
	if count != 1 {
		return "", errors.New("unsupported key attribute type")
	}
	extract, ok := attributeValueKeyExtractors[kind]
	if !ok {
		return "", errors.New("unsupported key attribute type")
	}
	return extract(attr), nil
}

func attributeValueAsKeyBytes(attr attributeValue) ([]byte, error) {
	kind, count := detectAttributeValueKind(attr)
	if count != 1 {
		return nil, errors.New("unsupported key attribute type")
	}
	if kind == attributeValueKindNumber {
		return encodeNumericKeyBytes(attr.numberValue())
	}
	extract, ok := attributeValueKeyByteExtractors[kind]
	if !ok {
		return nil, errors.New("unsupported key attribute type")
	}
	return extract(attr), nil
}

func attributeValueAsKeySegment(attr attributeValue) ([]byte, error) {
	raw, err := attributeValueAsKeyBytes(attr)
	if err != nil {
		return nil, err
	}
	return encodeDynamoKeySegment(raw), nil
}

type numericKeyParts struct {
	negative bool
	exponent int64
	digits   []byte
}

func encodeNumericKeyBytes(v string) ([]byte, error) {
	parts, err := parseNumericKeyParts(v)
	if err != nil {
		return nil, err
	}
	if len(parts.digits) == 0 {
		return []byte{0x01}, nil
	}
	body := encodeOrderedSignedInt64(parts.exponent)
	body = append(body, dynamoKeyEscapeByte)
	body = append(body, parts.digits...)
	if !parts.negative {
		return append([]byte{0x02}, body...), nil
	}
	return append([]byte{0x00}, invertBytes(body)...), nil
}

func parseNumericKeyParts(v string) (numericKeyParts, error) {
	trimmed, negative, exp10, err := parseNumericKeyLiteral(v)
	if err != nil {
		return numericKeyParts{}, err
	}
	digits, exponent, zero, err := normalizeNumericKeyParts(trimmed, exp10)
	if err != nil {
		return numericKeyParts{}, err
	}
	if zero {
		return numericKeyParts{}, nil
	}
	return numericKeyParts{
		negative: negative,
		exponent: exponent,
		digits:   digits,
	}, nil
}

func parseNumericKeyLiteral(v string) (string, bool, int64, error) {
	trimmed := strings.TrimSpace(v)
	if trimmed == "" {
		return "", false, 0, errors.New("unsupported key attribute type")
	}

	negative := false
	switch trimmed[0] {
	case '+':
		trimmed = trimmed[1:]
	case '-':
		negative = true
		trimmed = trimmed[1:]
	}
	if trimmed == "" {
		return "", false, 0, errors.New("unsupported key attribute type")
	}

	exp10 := int64(0)
	if idx := strings.IndexAny(trimmed, "eE"); idx >= 0 {
		expPart := strings.TrimSpace(trimmed[idx+1:])
		trimmed = trimmed[:idx]
		parsedExp, err := parseNumericExponent(expPart)
		if err != nil {
			return "", false, 0, err
		}
		exp10 = parsedExp
	}
	return trimmed, negative, exp10, nil
}

func parseNumericExponent(expPart string) (int64, error) {
	if expPart == "" {
		return 0, errors.New("unsupported key attribute type")
	}
	parsedExp, err := strconv.ParseInt(expPart, 10, 64)
	if err != nil {
		return 0, errors.New("unsupported key attribute type")
	}
	return parsedExp, nil
}

func normalizeNumericKeyParts(trimmed string, exp10 int64) ([]byte, int64, bool, error) {
	intPart, fracPart, err := splitNumericMantissa(trimmed)
	if err != nil {
		return nil, 0, false, err
	}
	combined := intPart + fracPart
	leadingZeros := leadingZeroCount(combined)
	if leadingZeros == len(combined) {
		return nil, 0, true, nil
	}
	digits := []byte(strings.TrimRight(combined[leadingZeros:], "0"))
	if len(digits) == 0 {
		return nil, 0, true, nil
	}
	exponent := int64(len(intPart)) + exp10 - int64(leadingZeros)
	return digits, exponent, false, nil
}

func splitNumericMantissa(trimmed string) (string, string, error) {
	if strings.Count(trimmed, ".") > 1 {
		return "", "", errors.New("unsupported key attribute type")
	}
	intPart := trimmed
	fracPart := ""
	if before, after, ok := strings.Cut(trimmed, "."); ok {
		intPart = before
		fracPart = after
	}
	if intPart == "" && fracPart == "" {
		return "", "", errors.New("unsupported key attribute type")
	}
	if !decimalDigitsOnly(intPart) || !decimalDigitsOnly(fracPart) {
		return "", "", errors.New("unsupported key attribute type")
	}
	return intPart, fracPart, nil
}

func leadingZeroCount(v string) int {
	count := 0
	for count < len(v) && v[count] == '0' {
		count++
	}
	return count
}

func decimalDigitsOnly(v string) bool {
	for i := range v {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

func encodeOrderedSignedInt64(v int64) []byte {
	switch {
	case v < 0:
		return append([]byte{0x00}, invertBytes(encodeOrderedUint64(signedMagnitude(v)))...)
	case v == 0:
		return []byte{0x01}
	default:
		return append([]byte{0x02}, encodeOrderedUint64(uint64(v))...)
	}
}

func signedMagnitude(v int64) uint64 {
	if v >= 0 {
		return uint64(v)
	}
	abs := big.NewInt(v)
	abs.Abs(abs)
	return abs.Uint64()
}

var orderedUint64LengthPrefix = [...]byte{0, 1, 2, 3, 4, 5, 6, 7, 8}

func encodeOrderedUint64(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	start := 0
	for start < len(buf)-1 && buf[start] == 0 {
		start++
	}
	width := len(buf) - start
	out := make([]byte, 0, width+1)
	out = append(out, orderedUint64LengthPrefix[width])
	out = append(out, buf[start:]...)
	return out
}

func invertBytes(in []byte) []byte {
	out := make([]byte, len(in))
	for i := range in {
		out[i] = ^in[i]
	}
	return out
}

func encodeDynamoKeySegment(raw []byte) []byte {
	out := encodeDynamoKeySegmentPrefix(raw)
	out = append(out, dynamoKeyEscapeByte, dynamoKeyTerminatorByte)
	return out
}

func encodeDynamoKeySegmentPrefix(raw []byte) []byte {
	return appendEscapedDynamoKeyBytes(make([]byte, 0, len(raw)+dynamoKeySegmentOverhead), raw)
}

func appendEscapedDynamoKeyBytes(dst []byte, raw []byte) []byte {
	for _, b := range raw {
		if b == dynamoKeyEscapeByte {
			dst = append(dst, dynamoKeyEscapeByte, dynamoKeyEscapedZeroByte)
			continue
		}
		dst = append(dst, b)
	}
	return dst
}

func attributeValueEqual(left attributeValue, right attributeValue) bool {
	leftKind, leftCount := detectAttributeValueKind(left)
	rightKind, rightCount := detectAttributeValueKind(right)
	if leftCount == 0 && rightCount == 0 {
		return true
	}
	if leftCount != 1 || rightCount != 1 || leftKind != rightKind {
		return false
	}
	if leftKind == attributeValueKindMap {
		return mapAttributeValueEqual(left, right)
	}
	if leftKind == attributeValueKindList {
		return listAttributeValueEqual(left, right)
	}
	compare, ok := attributeValueScalarEqualityComparators[leftKind]
	if !ok {
		return false
	}
	return compare(left, right)
}

func numberAttributeValueEqual(left attributeValue, right attributeValue) bool {
	cmp, ok := compareNumericAttributeString(left.numberValue(), right.numberValue())
	if !ok {
		return left.numberValue() == right.numberValue()
	}
	return cmp == 0
}

func mapAttributeValueEqual(left attributeValue, right attributeValue) bool {
	if len(left.M) != len(right.M) {
		return false
	}
	for key, leftValue := range left.M {
		rightValue, ok := right.M[key]
		if !ok || !attributeValueEqual(leftValue, rightValue) {
			return false
		}
	}
	return true
}

func listAttributeValueEqual(left attributeValue, right attributeValue) bool {
	if len(left.L) != len(right.L) {
		return false
	}
	for i := range left.L {
		if !attributeValueEqual(left.L[i], right.L[i]) {
			return false
		}
	}
	return true
}

func compareAttributeValueSortKey(left attributeValue, right attributeValue) int {
	if left.hasNumberType() && right.hasNumberType() {
		if cmp, ok := compareNumericAttributeString(left.numberValue(), right.numberValue()); ok {
			return cmp
		}
	}
	if left.hasBinaryType() && right.hasBinaryType() {
		return bytes.Compare(left.B, right.B)
	}
	return strings.Compare(attributeValueSortFallback(left), attributeValueSortFallback(right))
}

func compareNumericAttributeString(left string, right string) (int, bool) {
	leftRat := &big.Rat{}
	rightRat := &big.Rat{}
	if _, ok := leftRat.SetString(strings.TrimSpace(left)); !ok {
		return 0, false
	}
	if _, ok := rightRat.SetString(strings.TrimSpace(right)); !ok {
		return 0, false
	}
	return leftRat.Cmp(rightRat), true
}

func attributeValueSortFallback(attr attributeValue) string {
	kind, count := detectAttributeValueKind(attr)
	if count != 1 {
		return ""
	}
	format, ok := attributeValueSortFormatters[kind]
	if !ok {
		return ""
	}
	return format(attr)
}

func formatBoolAttributeValue(attr attributeValue) string {
	if *attr.BOOL {
		return "1"
	}
	return "0"
}

func unorderedStringSlicesEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	lv := sortedStringSlice(left)
	rv := sortedStringSlice(right)
	for i := range lv {
		if lv[i] != rv[i] {
			return false
		}
	}
	return true
}

func unorderedNumberSlicesEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	lv := sortedNumberStrings(left)
	rv := sortedNumberStrings(right)
	for i := range lv {
		if lv[i] != rv[i] {
			return false
		}
	}
	return true
}

func unorderedBinarySlicesEqual(left [][]byte, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	lv := sortedBinaryStrings(left)
	rv := sortedBinaryStrings(right)
	for i := range lv {
		if lv[i] != rv[i] {
			return false
		}
	}
	return true
}

func sortedStringSlice(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func sortedNumberStrings(in []string) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = canonicalNumberString(in[i])
	}
	sort.Strings(out)
	return out
}

func sortedBinaryStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i := range in {
		out[i] = base64.RawURLEncoding.EncodeToString(in[i])
	}
	sort.Strings(out)
	return out
}

func canonicalNumberString(v string) string {
	rat := &big.Rat{}
	if _, ok := rat.SetString(strings.TrimSpace(v)); !ok {
		return strings.TrimSpace(v)
	}
	return rat.RatString()
}

func reverseItems(items []map[string]attributeValue) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func (d *DynamoDBServer) lockItemUpdate(lockKey string) func() {
	idx := stripeIndex(lockKey, itemUpdateLockStripeCount)
	d.itemUpdateLocks[idx].Lock()
	return d.itemUpdateLocks[idx].Unlock
}

func (d *DynamoDBServer) lockTableOperations(tableNames []string) func() {
	if len(tableNames) == 0 {
		return func() {}
	}
	idxs := make([]int, 0, len(tableNames))
	seen := map[int]struct{}{}
	for _, tableName := range tableNames {
		idx := stripeIndex(tableName, tableLockStripeCount)
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	for _, idx := range idxs {
		d.tableLocks[idx].Lock()
	}
	return func() {
		for i := len(idxs) - 1; i >= 0; i-- {
			d.tableLocks[idxs[i]].Unlock()
		}
	}
}

func stripeIndex(key string, stripeCount uint32) int {
	if stripeCount == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % stripeCount)
}

func dynamoItemUpdateLockKey(tableName string, key map[string]attributeValue) (string, error) {
	parts := make([]string, 0, len(key))
	for name := range key {
		parts = append(parts, name)
	}
	sort.Strings(parts)
	var b strings.Builder
	b.WriteString(tableName)
	b.WriteByte('|')
	for _, name := range parts {
		val, err := attributeValueAsKey(key[name])
		if err != nil {
			return "", errors.WithStack(err)
		}
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(val)
		b.WriteByte('|')
	}
	return b.String(), nil
}

func describeTableShape(t *dynamoTableSchema) map[string]any {
	attrDefs := make([]map[string]string, 0, len(t.AttributeDefinitions))
	for name, typ := range t.AttributeDefinitions {
		attrDefs = append(attrDefs, map[string]string{
			"AttributeName": name,
			"AttributeType": typ,
		})
	}
	sort.Slice(attrDefs, func(i, j int) bool {
		return attrDefs[i]["AttributeName"] < attrDefs[j]["AttributeName"]
	})

	keySchema := []map[string]string{{
		"AttributeName": t.PrimaryKey.HashKey,
		"KeyType":       "HASH",
	}}
	if t.PrimaryKey.RangeKey != "" {
		keySchema = append(keySchema, map[string]string{
			"AttributeName": t.PrimaryKey.RangeKey,
			"KeyType":       "RANGE",
		})
	}

	resp := map[string]any{
		"TableName":            t.TableName,
		"TableStatus":          "ACTIVE",
		"KeySchema":            keySchema,
		"AttributeDefinitions": attrDefs,
	}

	if len(t.GlobalSecondaryIndexes) > 0 {
		gsis := make([]map[string]any, 0, len(t.GlobalSecondaryIndexes))
		indexNames := make([]string, 0, len(t.GlobalSecondaryIndexes))
		for name := range t.GlobalSecondaryIndexes {
			indexNames = append(indexNames, name)
		}
		sort.Strings(indexNames)
		for _, name := range indexNames {
			gsi := t.GlobalSecondaryIndexes[name]
			ks := gsi.KeySchema
			projection := map[string]any{
				"ProjectionType": gsi.Projection.ProjectionType,
			}
			if len(gsi.Projection.NonKeyAttributes) > 0 {
				projection["NonKeyAttributes"] = append([]string(nil), gsi.Projection.NonKeyAttributes...)
			}
			idxKeySchema := []map[string]string{{
				"AttributeName": ks.HashKey,
				"KeyType":       "HASH",
			}}
			if ks.RangeKey != "" {
				idxKeySchema = append(idxKeySchema, map[string]string{
					"AttributeName": ks.RangeKey,
					"KeyType":       "RANGE",
				})
			}
			indexDesc := map[string]any{
				"IndexName":   name,
				"IndexStatus": "ACTIVE",
				"KeySchema":   idxKeySchema,
				"Projection":  projection,
			}
			gsis = append(gsis, indexDesc)
		}
		resp["GlobalSecondaryIndexes"] = gsis
	}

	return resp
}

func cloneAttributeValueMap(in map[string]attributeValue) map[string]attributeValue {
	if in == nil {
		return nil
	}
	out := make(map[string]attributeValue, len(in))
	for k, v := range in {
		out[k] = cloneAttributeValue(v)
	}
	return out
}

func cloneAttributeValueList(in []attributeValue) []attributeValue {
	if in == nil {
		return nil
	}
	out := make([]attributeValue, 0, len(in))
	for _, value := range in {
		out = append(out, cloneAttributeValue(value))
	}
	return out
}

func cloneAttributeValue(in attributeValue) attributeValue {
	out := attributeValue{}
	if in.S != nil {
		s := *in.S
		out.S = &s
	}
	if in.N != nil {
		n := *in.N
		out.N = &n
	}
	if in.B != nil {
		out.B = bytes.Clone(in.B)
	}
	if in.BOOL != nil {
		b := *in.BOOL
		out.BOOL = &b
	}
	if in.NULL != nil {
		n := *in.NULL
		out.NULL = &n
	}
	out.SS = cloneStringSlice(in.SS)
	out.NS = cloneStringSlice(in.NS)
	out.BS = cloneBinarySet(in.BS)
	if in.L != nil {
		out.L = make([]attributeValue, len(in.L))
		for i := range in.L {
			out.L[i] = cloneAttributeValue(in.L[i])
		}
	}
	if in.M != nil {
		out.M = cloneAttributeValueMap(in.M)
	}
	return out
}

func (d *DynamoDBServer) nextTxnReadTS() uint64 {
	maxTS := uint64(0)
	if d.store != nil {
		maxTS = d.store.LastCommitTS()
	}

	clock := d.coordinator.Clock()
	if clock == nil {
		if maxTS == ^uint64(0) {
			return maxTS
		}
		return maxTS + 1
	}
	if maxTS > 0 {
		clock.Observe(maxTS)
	}
	return clock.Next()
}

func (d *DynamoDBServer) pinReadTS(ts uint64) *kv.ActiveTimestampToken {
	if d == nil || d.readTracker == nil {
		return &kv.ActiveTimestampToken{}
	}
	return d.readTracker.Pin(ts)
}

func (d *DynamoDBServer) loadTableSchema(ctx context.Context, tableName string) (*dynamoTableSchema, bool, error) {
	return d.loadTableSchemaAt(ctx, tableName, snapshotTS(d.coordinator.Clock(), d.store))
}

func (d *DynamoDBServer) loadTableSchemaAt(ctx context.Context, tableName string, ts uint64) (*dynamoTableSchema, bool, error) {
	b, err := d.store.GetAt(ctx, dynamoTableMetaKey(tableName), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	schema, err := decodeStoredDynamoTableSchema(b)
	if err != nil {
		return nil, false, err
	}
	d.observeTables(ctx, schema.TableName)
	return schema, true, nil
}

func (d *DynamoDBServer) loadTableGenerationAt(ctx context.Context, tableName string, ts uint64) (uint64, error) {
	b, err := d.store.GetAt(ctx, dynamoTableGenerationKey(tableName), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	gen, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return gen, nil
}

func (d *DynamoDBServer) readItemAtKeyAt(ctx context.Context, key []byte, ts uint64) (map[string]attributeValue, bool, error) {
	b, err := d.store.GetAt(ctx, key, ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	item, err := decodeStoredDynamoItem(b)
	if err != nil {
		return nil, false, err
	}
	return item, true, nil
}

func (d *DynamoDBServer) readLogicalItemAt(
	ctx context.Context,
	schema *dynamoTableSchema,
	key map[string]attributeValue,
	ts uint64,
) (*dynamoItemLocation, bool, error) {
	itemKey, err := schema.itemKeyFromAttributes(key)
	if err != nil {
		return nil, false, err
	}
	item, found, err := d.readItemAtKeyAt(ctx, itemKey, ts)
	if err != nil {
		return nil, false, err
	}
	if found {
		return &dynamoItemLocation{schema: schema, key: itemKey, item: item}, true, nil
	}
	sourceSchema := schema.migrationSourceSchema()
	if sourceSchema == nil {
		return nil, false, nil
	}
	sourceKey, err := sourceSchema.itemKeyFromAttributes(key)
	if err != nil {
		return nil, false, err
	}
	item, found, err = d.readItemAtKeyAt(ctx, sourceKey, ts)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return &dynamoItemLocation{schema: sourceSchema, key: sourceKey, item: item}, true, nil
}

func (d *DynamoDBServer) ensureLegacyTableMigration(ctx context.Context, tableName string) error {
	unlock := d.lockTableOperations([]string{tableName})
	defer unlock()
	return d.ensureLegacyTableMigrationLocked(ctx, tableName)
}

func (d *DynamoDBServer) ensureLegacyTableMigrationLocked(ctx context.Context, tableName string) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := d.nextTxnReadTS()
		schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists || !schema.needsLegacyKeyMigration() {
			return nil
		}
		if !schema.usesOrderedKeyEncoding() {
			err = d.startLegacyTableKeyMigration(ctx, schema, readTS)
		} else {
			err = d.migrateLegacyTableGeneration(ctx, schema)
		}
		if err == nil {
			continue
		}
		if !isRetryableTransactWriteError(err) {
			return err
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "legacy table migration retry attempts exhausted")
}

func (d *DynamoDBServer) startLegacyTableKeyMigration(
	ctx context.Context,
	schema *dynamoTableSchema,
	readTS uint64,
) error {
	if schema == nil || schema.usesOrderedKeyEncoding() {
		return nil
	}
	nextGeneration, err := d.nextTableGenerationAt(ctx, schema.TableName, readTS)
	if err != nil {
		return err
	}
	req, err := makeCreateTableRequest(&dynamoTableSchema{
		TableName:               schema.TableName,
		AttributeDefinitions:    schema.AttributeDefinitions,
		PrimaryKey:              schema.PrimaryKey,
		GlobalSecondaryIndexes:  schema.GlobalSecondaryIndexes,
		KeyEncodingVersion:      dynamoOrderedKeyEncodingV2,
		MigratingFromGeneration: schema.Generation,
	}, nextGeneration)
	if err != nil {
		return err
	}
	req.StartTS = readTS
	if _, err := d.coordinator.Dispatch(ctx, req); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DynamoDBServer) migrateLegacyTableGeneration(ctx context.Context, schema *dynamoTableSchema) error {
	sourceSchema := schema.migrationSourceSchema()
	if sourceSchema == nil {
		return nil
	}
	sourceReadTS := snapshotTS(d.coordinator.Clock(), d.store)
	if err := d.migrateLegacySourceItems(ctx, schema, sourceSchema, sourceReadTS); err != nil {
		return err
	}
	empty, err := d.isTableGenerationEmpty(ctx, schema.TableName, sourceSchema.Generation)
	if err != nil {
		return err
	}
	if !empty {
		return nil
	}
	return d.finalizeLegacyTableMigration(ctx, schema)
}

func (d *DynamoDBServer) migrateLegacySourceItems(
	ctx context.Context,
	targetSchema *dynamoTableSchema,
	sourceSchema *dynamoTableSchema,
	readTS uint64,
) error {
	readPin := d.pinReadTS(readTS)
	defer readPin.Release()

	prefix := dynamoItemPrefixForTable(targetSchema.TableName, sourceSchema.Generation)
	upper := prefixScanEnd(prefix)
	cursor := bytes.Clone(prefix)
	for {
		kvs, err := d.scanLegacyMigrationPage(ctx, cursor, upper, readTS)
		if err != nil {
			return err
		}
		nextCursor, done, err := d.migrateLegacySourcePage(ctx, targetSchema, sourceSchema, prefix, upper, kvs)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		cursor = nextCursor
	}
}

func (d *DynamoDBServer) migrateLegacyItem(
	ctx context.Context,
	targetSchema *dynamoTableSchema,
	sourceSchema *dynamoTableSchema,
	sourceKey []byte,
	sourceItem map[string]attributeValue,
) error {
	lockKey, targetKey, err := resolveLegacyMigrationTarget(targetSchema, sourceItem)
	if err != nil {
		return err
	}
	unlock := d.lockItemUpdate(lockKey)
	defer unlock()

	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := d.nextTxnReadTS()
		req, done, err := d.buildLegacyMigrationRequest(ctx, targetSchema, sourceSchema, targetKey, sourceKey, readTS)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		req.StartTS = readTS
		if _, err := d.coordinator.Dispatch(ctx, req); err == nil {
			return nil
		} else if !isRetryableTransactWriteError(err) {
			return errors.WithStack(err)
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "legacy item migration retry attempts exhausted")
}

func (d *DynamoDBServer) scanLegacyMigrationPage(
	ctx context.Context,
	cursor []byte,
	upper []byte,
	readTS uint64,
) ([]*store.KVPair, error) {
	kvs, err := d.store.ScanAt(ctx, cursor, upper, dynamoScanPageLimit, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return kvs, nil
}

func (d *DynamoDBServer) migrateLegacySourcePage(
	ctx context.Context,
	targetSchema *dynamoTableSchema,
	sourceSchema *dynamoTableSchema,
	prefix []byte,
	upper []byte,
	kvs []*store.KVPair,
) ([]byte, bool, error) {
	if len(kvs) == 0 {
		return nil, true, nil
	}
	for _, kvp := range kvs {
		if !bytes.HasPrefix(kvp.Key, prefix) {
			return nil, true, nil
		}
		item, err := decodeStoredDynamoItem(kvp.Value)
		if err != nil {
			return nil, false, err
		}
		if err := d.migrateLegacyItem(ctx, targetSchema, sourceSchema, kvp.Key, item); err != nil {
			return nil, false, err
		}
	}
	if len(kvs) < dynamoScanPageLimit {
		return nil, true, nil
	}
	cursor := nextScanCursor(kvs[len(kvs)-1].Key)
	if upper != nil && bytes.Compare(cursor, upper) >= 0 {
		return nil, true, nil
	}
	return cursor, false, nil
}

func resolveLegacyMigrationTarget(targetSchema *dynamoTableSchema, sourceItem map[string]attributeValue) (string, []byte, error) {
	keyAttrs, err := primaryKeyAttributes(targetSchema.PrimaryKey, sourceItem)
	if err != nil {
		return "", nil, err
	}
	lockKey, err := dynamoItemUpdateLockKey(targetSchema.TableName, keyAttrs)
	if err != nil {
		return "", nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	targetKey, err := targetSchema.itemKeyFromAttributes(keyAttrs)
	if err != nil {
		return "", nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return lockKey, targetKey, nil
}

func (d *DynamoDBServer) buildLegacyMigrationRequest(
	ctx context.Context,
	targetSchema *dynamoTableSchema,
	sourceSchema *dynamoTableSchema,
	targetKey []byte,
	sourceKey []byte,
	readTS uint64,
) (*kv.OperationGroup[kv.OP], bool, error) {
	_, targetFound, err := d.readItemAtKeyAt(ctx, targetKey, readTS)
	if err != nil {
		return nil, false, err
	}
	currentSource, sourceFound, err := d.readItemAtKeyAt(ctx, sourceKey, readTS)
	if err != nil {
		return nil, false, err
	}
	if !sourceFound {
		return nil, true, nil
	}
	currentLocation := &dynamoItemLocation{
		schema: sourceSchema,
		key:    sourceKey,
		item:   currentSource,
	}
	if targetFound {
		req, err := buildItemDeleteRequestWithSource(currentLocation)
		return req, false, err
	}
	req, _, err := buildItemWriteRequestWithSource(targetSchema, targetKey, currentSource, currentLocation)
	return req, false, err
}

func (d *DynamoDBServer) isTableGenerationEmpty(ctx context.Context, tableName string, generation uint64) (bool, error) {
	prefix := dynamoItemPrefixForTable(tableName, generation)
	kvs, err := d.store.ScanAt(ctx, prefix, prefixScanEnd(prefix), 1, snapshotTS(d.coordinator.Clock(), d.store))
	if err != nil {
		return false, errors.WithStack(err)
	}
	for _, kvp := range kvs {
		if bytes.HasPrefix(kvp.Key, prefix) {
			return false, nil
		}
	}
	return true, nil
}

func (d *DynamoDBServer) finalizeLegacyTableMigration(ctx context.Context, schema *dynamoTableSchema) error {
	if schema == nil || schema.MigratingFromGeneration == 0 {
		return nil
	}
	oldGeneration := schema.MigratingFromGeneration
	finalized := *schema
	finalized.MigratingFromGeneration = 0
	body, err := encodeStoredDynamoTableSchema(&finalized)
	if err != nil {
		return errors.WithStack(err)
	}
	readTS := d.nextTxnReadTS()
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: readTS,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: dynamoTableMetaKey(schema.TableName), Value: body},
		},
	}
	if _, err := d.coordinator.Dispatch(ctx, req); err != nil {
		return errors.WithStack(err)
	}
	d.launchDeletedTableCleanup(schema.TableName, oldGeneration)
	return nil
}

func (d *DynamoDBServer) scanAllByPrefix(ctx context.Context, prefix []byte) ([]*store.KVPair, error) {
	return d.scanAllByPrefixAt(ctx, prefix, snapshotTS(d.coordinator.Clock(), d.store))
}

func (d *DynamoDBServer) scanAllByPrefixAt(ctx context.Context, prefix []byte, readTS uint64) ([]*store.KVPair, error) {
	readPin := d.pinReadTS(readTS)
	defer readPin.Release()

	end := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)

	out := make([]*store.KVPair, 0, dynamoScanPageLimit)
	for {
		kvs, err := d.store.ScanAt(ctx, start, end, dynamoScanPageLimit, readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(kvs) == 0 {
			break
		}
		for _, kvp := range kvs {
			if !bytes.HasPrefix(kvp.Key, prefix) {
				return out, nil
			}
			out = append(out, kvp)
		}
		if len(kvs) < dynamoScanPageLimit {
			break
		}
		start = nextScanCursor(kvs[len(kvs)-1].Key)
		if end != nil && bytes.Compare(start, end) > 0 {
			break
		}
	}
	return out, nil
}

func nextScanCursor(lastKey []byte) []byte {
	next := make([]byte, len(lastKey)+1)
	copy(next, lastKey)
	return next
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func dynamoTableMetaKey(tableName string) []byte {
	return []byte(dynamoTableMetaPrefix + encodeDynamoSegment(tableName))
}

func dynamoTableGenerationKey(tableName string) []byte {
	return []byte(dynamoTableGenerationPrefix + encodeDynamoSegment(tableName))
}

func dynamoItemPrefixForTable(tableName string, generation uint64) []byte {
	return []byte(dynamoItemPrefix + encodeDynamoSegment(tableName) + "|" + strconv.FormatUint(generation, 10) + "|")
}

func dynamoItemHashPrefixForTable(tableName string, generation uint64, hashKey []byte) []byte {
	base := dynamoItemPrefixForTable(tableName, generation)
	return append(base, hashKey...)
}

func legacyDynamoItemHashPrefixForTable(tableName string, generation uint64, hashKey string) []byte {
	return []byte(
		dynamoItemPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(hashKey) + "|",
	)
}

func dynamoGSIPrefixForTable(tableName string, generation uint64) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|",
	)
}

func dynamoGSIIndexPrefixForTable(tableName string, generation uint64, indexName string) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(indexName) + "|",
	)
}

func dynamoGSIHashPrefixForTable(tableName string, generation uint64, indexName string, hashKey []byte) []byte {
	base := dynamoGSIIndexPrefixForTable(tableName, generation, indexName)
	return append(base, hashKey...)
}

func legacyDynamoGSIHashPrefixForTable(tableName string, generation uint64, indexName string, hashKey string) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(indexName) + "|" +
			encodeDynamoSegment(hashKey) + "|",
	)
}

func dynamoGSIKey(
	tableName string,
	generation uint64,
	indexName string,
	indexHash []byte,
	indexRange []byte,
	pkHash []byte,
	pkRange []byte,
) []byte {
	key := dynamoGSIIndexPrefixForTable(tableName, generation, indexName)
	key = append(key, indexHash...)
	key = append(key, indexRange...)
	key = append(key, pkHash...)
	key = append(key, pkRange...)
	return key
}

func legacyDynamoGSIKey(tableName string, generation uint64, indexName string, indexHash string, indexRange string, pkHash string, pkRange string) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(indexName) + "|" +
			encodeDynamoSegment(indexHash) + "|" +
			encodeDynamoSegment(indexRange) + "|" +
			encodeDynamoSegment(pkHash) + "|" +
			encodeDynamoSegment(pkRange),
	)
}

func dynamoItemKey(tableName string, generation uint64, hashKey []byte, rangeKey []byte) []byte {
	key := dynamoItemPrefixForTable(tableName, generation)
	key = append(key, hashKey...)
	key = append(key, rangeKey...)
	return key
}

func legacyDynamoItemKey(tableName string, generation uint64, hashKey, rangeKey string) []byte {
	return []byte(
		dynamoItemPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(hashKey) + "|" +
			encodeDynamoSegment(rangeKey),
	)
}

func encodeDynamoSegment(v string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(v))
}

func decodeDynamoSegment(v string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(b), nil
}

func tableNameFromMetaKey(key []byte) (string, bool) {
	enc, ok := strings.CutPrefix(string(key), dynamoTableMetaPrefix)
	if !ok || enc == "" {
		return "", false
	}
	name, err := decodeDynamoSegment(enc)
	if err != nil {
		return "", false
	}
	return name, true
}

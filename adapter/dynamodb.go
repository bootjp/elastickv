package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"hash/fnv"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/kv"
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
	getItemTarget            = targetPrefix + "GetItem"
	queryTarget              = targetPrefix + "Query"
	updateItemTarget         = targetPrefix + "UpdateItem"
	transactWriteItemsTarget = targetPrefix + "TransactWriteItems"
)

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
	queryDefaultLimit           = 100
	tableCleanupDeleteBatchSize = 256

	dynamoTableMetaPrefix       = "!ddb|meta|table|"
	dynamoTableGenerationPrefix = "!ddb|meta|gen|"
	dynamoItemPrefix            = "!ddb|item|"
	dynamoGSIPrefix             = "!ddb|gsi|"
	dynamoScanPageLimit         = 1024
)

const (
	dynamoErrValidation        = "ValidationException"
	dynamoErrInternal          = "InternalServerError"
	dynamoErrConditionalFailed = "ConditionalCheckFailedException"
	dynamoErrResourceNotFound  = "ResourceNotFoundException"
	dynamoErrResourceInUse     = "ResourceInUseException"
)

type DynamoDBServer struct {
	listen          net.Listener
	store           store.MVCCStore
	coordinator     kv.Coordinator
	httpServer      *http.Server
	targetHandlers  map[string]func(http.ResponseWriter, *http.Request)
	itemUpdateLocks [itemUpdateLockStripeCount]sync.Mutex
	tableLocks      [tableLockStripeCount]sync.Mutex
}

func NewDynamoDBServer(listen net.Listener, st store.MVCCStore, coordinate kv.Coordinator) *DynamoDBServer {
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
		getItemTarget:            d.getItem,
		queryTarget:              d.query,
		updateItemTarget:         d.updateItem,
		transactWriteItemsTarget: d.transactWriteItems,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.handle)
	d.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
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
	target := r.Header.Get("X-Amz-Target")
	if !d.dispatchByTarget(target, w, r) {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, "unsupported operation")
	}
}

func (d *DynamoDBServer) dispatchByTarget(target string, w http.ResponseWriter, r *http.Request) bool {
	handler, ok := d.targetHandlers[target]
	if !ok {
		return false
	}
	handler(w, r)
	return true
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
	IndexName string                        `json:"IndexName"`
	KeySchema []createTableKeySchemaElement `json:"KeySchema"`
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
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
	ScanIndexForward          *bool                     `json:"ScanIndexForward"`
	Limit                     *int32                    `json:"Limit"`
	ExclusiveStartKey         map[string]attributeValue `json:"ExclusiveStartKey"`
}

type getItemInput struct {
	TableName string                    `json:"TableName"`
	Key       map[string]attributeValue `json:"Key"`
}

type updateItemInput struct {
	TableName                 string                    `json:"TableName"`
	Key                       map[string]attributeValue `json:"Key"`
	UpdateExpression          string                    `json:"UpdateExpression"`
	ConditionExpression       string                    `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]attributeValue `json:"ExpressionAttributeValues"`
}

type dynamoKeySchema struct {
	HashKey  string `json:"hash_key"`
	RangeKey string `json:"range_key,omitempty"`
}

type dynamoTableSchema struct {
	TableName              string                     `json:"table_name"`
	AttributeDefinitions   map[string]string          `json:"attribute_definitions,omitempty"`
	PrimaryKey             dynamoKeySchema            `json:"primary_key"`
	GlobalSecondaryIndexes map[string]dynamoKeySchema `json:"global_secondary_indexes,omitempty"`
	Generation             uint64                     `json:"generation"`
}

func (d *DynamoDBServer) createTable(w http.ResponseWriter, r *http.Request) {
	in, err := decodeCreateTableInput(r.Body)
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
	gsis := make(map[string]dynamoKeySchema, len(in.GlobalSecondaryIndexes))
	for _, gsi := range in.GlobalSecondaryIndexes {
		if strings.TrimSpace(gsi.IndexName) == "" {
			return nil, errors.New("invalid global secondary index")
		}
		ks, err := parseCreateTableKeySchema(gsi.KeySchema)
		if err != nil {
			return nil, err
		}
		gsis[gsi.IndexName] = ks
	}
	return &dynamoTableSchema{
		TableName:              in.TableName,
		AttributeDefinitions:   attrDefs,
		PrimaryKey:             primary,
		GlobalSecondaryIndexes: gsis,
	}, nil
}

func (d *DynamoDBServer) createTableWithRetry(ctx context.Context, tableName string, baseSchema *dynamoTableSchema) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for attempt := 0; attempt < transactRetryMaxAttempts; attempt++ {
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
		TableName:              baseSchema.TableName,
		AttributeDefinitions:   baseSchema.AttributeDefinitions,
		PrimaryKey:             baseSchema.PrimaryKey,
		GlobalSecondaryIndexes: baseSchema.GlobalSecondaryIndexes,
		Generation:             nextGeneration,
	}
	schemaBytes, err := json.Marshal(schema)
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
	in, err := decodeDeleteTableInput(r.Body)
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
	for attempt := 0; attempt < transactRetryMaxAttempts; attempt++ {
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
		end := start + tableCleanupDeleteBatchSize
		if end > len(keys) {
			end = len(keys)
		}
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
	body, err := io.ReadAll(r.Body)
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
	in, err := decodeListTablesInput(r.Body)
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
	end := start + limit
	if end > len(names) {
		end = len(names)
	}
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	var in putItemInput
	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}
	if strings.TrimSpace(in.TableName) == "" {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, "missing table name")
		return
	}
	if err := d.putItemWithRetry(r.Context(), in); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	writeDynamoJSON(w, map[string]any{})
}

func (d *DynamoDBServer) putItemWithRetry(ctx context.Context, in putItemInput) error {
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
}

func (d *DynamoDBServer) retryItemWriteWithGeneration(
	ctx context.Context,
	tableName string,
	exhaustedMessage string,
	prepare func(readTS uint64) (*itemWritePlan, error),
) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for attempt := 0; attempt < transactRetryMaxAttempts; attempt++ {
		readTS := d.nextTxnReadTS()
		plan, err := prepare(readTS)
		if err != nil {
			return err
		}
		if err = d.commitItemWrite(ctx, plan.req); err != nil {
			if !isRetryableTransactWriteError(err) {
				return errors.WithStack(err)
			}
		} else {
			retry, verifyErr := d.handleGenerationFenceResult(
				ctx,
				d.verifyTableGeneration(ctx, tableName, plan.generation),
				plan.cleanup,
			)
			if verifyErr != nil {
				return verifyErr
			}
			if !retry {
				return nil
			}
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, exhaustedMessage)
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
	current, found, err := d.readItemAtKeyAt(ctx, itemKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !found {
		current = nil
	}
	req, cleanup, err := buildItemWriteRequest(schema, itemKey, current, in.Item)
	if err != nil {
		return nil, err
	}
	return &itemWritePlan{
		req:        req,
		generation: schema.Generation,
		cleanup:    cleanup,
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
	body, err := io.ReadAll(r.Body)
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

	schema, exists, err := d.loadTableSchema(r.Context(), in.TableName)
	if err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return
	}
	if !exists {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
		return
	}

	itemKey, err := schema.itemKeyFromAttributes(in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, dynamoErrValidation, err.Error())
		return
	}

	item, found, err := d.readItemAtKey(r.Context(), itemKey)
	if err != nil {
		writeDynamoError(w, http.StatusInternalServerError, dynamoErrInternal, err.Error())
		return
	}
	if !found {
		writeDynamoJSON(w, map[string]any{})
		return
	}
	writeDynamoJSON(w, map[string]any{"Item": item})
}

func (d *DynamoDBServer) updateItem(w http.ResponseWriter, r *http.Request) {
	in, err := decodeUpdateItemInput(r.Body)
	if err != nil {
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
	if err := d.updateItemWithRetry(r.Context(), in); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	writeDynamoJSON(w, map[string]any{})
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
	return in, nil
}

func (d *DynamoDBServer) updateItemWithRetry(ctx context.Context, in updateItemInput) error {
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
	current, found, err := d.readItemAtKeyAt(ctx, itemKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !found {
		current = map[string]attributeValue{}
	}
	nextItem, err := buildUpdatedItem(schema, in, current)
	if err != nil {
		return nil, err
	}
	req, cleanup, err := buildItemWriteRequest(schema, itemKey, current, nextItem)
	if err != nil {
		return nil, err
	}
	return &itemWritePlan{
		req:        req,
		generation: schema.Generation,
		cleanup:    cleanup,
	}, nil
}

func buildUpdatedItem(schema *dynamoTableSchema, in updateItemInput, current map[string]attributeValue) (map[string]attributeValue, error) {
	if err := validateConditionOnItem(in.ConditionExpression, in.ExpressionAttributeNames, in.ExpressionAttributeValues, current); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	nextItem := cloneAttributeValueMap(current)
	for k, v := range in.Key {
		nextItem[k] = v
	}
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

func buildItemWriteRequest(schema *dynamoTableSchema, itemKey []byte, current map[string]attributeValue, nextItem map[string]attributeValue) (*kv.OperationGroup[kv.OP], [][]byte, error) {
	payload, err := json.Marshal(nextItem)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: itemKey, Value: payload},
	}
	cleanup := [][]byte{itemKey}
	delKeys, putKeys, err := gsiDeltaKeys(schema, current, nextItem)
	if err != nil {
		return nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	for _, key := range delKeys {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}
	for _, key := range putKeys {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: itemKey})
		cleanup = append(cleanup, key)
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: 0,
		Elems:   elems,
	}, cleanup, nil
}

func gsiDeltaKeys(schema *dynamoTableSchema, current map[string]attributeValue, next map[string]attributeValue) ([][]byte, [][]byte, error) {
	oldKeys, err := schema.gsiEntryKeysForItem(current)
	if err != nil {
		return nil, nil, err
	}
	newKeys, err := schema.gsiEntryKeysForItem(next)
	if err != nil {
		return nil, nil, err
	}
	oldSet := bytesToSet(oldKeys)
	newSet := bytesToSet(newKeys)
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

func bytesToSet(keys [][]byte) map[string][]byte {
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		out[string(key)] = key
	}
	return out
}

func (d *DynamoDBServer) query(w http.ResponseWriter, r *http.Request) {
	in, err := decodeQueryInput(r.Body)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	out, err := d.queryItems(r.Context(), in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	resp := map[string]any{
		"Items":        out.items,
		"Count":        len(out.items),
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
	return in, nil
}

type queryOutput struct {
	items            []map[string]attributeValue
	scannedCount     int
	lastEvaluatedKey map[string]attributeValue
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
	schema, exists, err := d.loadTableSchema(ctx, in.TableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	keySchema, cond, err := resolveQueryCondition(in, schema)
	if err != nil {
		return nil, err
	}
	items, scannedCount, err := d.queryItemsByKeyCondition(ctx, in, schema, keySchema, cond)
	if err != nil {
		return nil, err
	}
	orderQueryItems(items, keySchema.RangeKey, in.ScanIndexForward)
	if items, err = applyQueryExclusiveStartKey(schema, in.ExclusiveStartKey, items); err != nil {
		return nil, err
	}
	limit := resolveQueryLimit(in.Limit)
	pagedItems, lastKey := paginateQueryItems(schema.PrimaryKey, items, limit)
	return &queryOutput{
		items:            pagedItems,
		scannedCount:     scannedCount,
		lastEvaluatedKey: lastKey,
	}, nil
}

func resolveQueryCondition(in queryInput, schema *dynamoTableSchema) (dynamoKeySchema, queryCondition, error) {
	keySchema, err := schema.keySchemaForQuery(in.IndexName)
	if err != nil {
		return dynamoKeySchema{}, queryCondition{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	parsed, err := parseKeyConditionExpression(replaceNames(in.KeyConditionExpression, in.ExpressionAttributeNames))
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
		item := map[string]attributeValue{}
		if err := json.Unmarshal(kvp.Value, &item); err != nil {
			return nil, errors.WithStack(err)
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

func (d *DynamoDBServer) queryItemsByKeyCondition(
	ctx context.Context,
	in queryInput,
	schema *dynamoTableSchema,
	keySchema dynamoKeySchema,
	cond queryCondition,
) ([]map[string]attributeValue, int, error) {
	if strings.TrimSpace(in.IndexName) != "" {
		return d.queryItemsByGSI(ctx, in, schema, cond)
	}
	scanPrefix, err := queryScanPrefix(schema, in, keySchema, cond.hashValue)
	if err != nil {
		return nil, 0, err
	}
	kvs, err := d.scanAllByPrefix(ctx, scanPrefix)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	items, err := filterQueryItems(kvs, cond)
	if err != nil {
		return nil, 0, err
	}
	return items, len(kvs), nil
}

func (d *DynamoDBServer) queryItemsByGSI(
	ctx context.Context,
	in queryInput,
	schema *dynamoTableSchema,
	cond queryCondition,
) ([]map[string]attributeValue, int, error) {
	hashKey, err := attributeValueAsKey(cond.hashValue)
	if err != nil {
		return nil, 0, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	prefix := dynamoGSIHashPrefixForTable(in.TableName, schema.Generation, in.IndexName, hashKey)
	kvs, err := d.scanAllByPrefix(ctx, prefix)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	readTS := snapshotTS(d.coordinator.Clock(), d.store)
	itemKeys := uniqueGSIItemKeys(kvs)
	items, err := d.readItemsForGSIQuery(ctx, itemKeys, readTS, cond)
	if err != nil {
		return nil, 0, err
	}
	return items, len(kvs), nil
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
	if len(itemKeys) == 0 {
		return nil, nil
	}
	workerCount := resolveGSIReadWorkerCount(len(itemKeys))
	jobs := make(chan gsiReadJob)
	results := make(chan gsiReadResult, len(itemKeys))
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	d.startGSIReadWorkers(&wg, workerCount, workerCtx, readTS, cond, jobs, results, cancel)
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
	cond queryCondition,
	jobs <-chan gsiReadJob,
	results chan<- gsiReadResult,
	cancel context.CancelFunc,
) {
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.gsiReadWorker(ctx, readTS, cond, jobs, results, cancel)
		}()
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
	for i := 0; i < len(itemKeys); i++ {
		if item := indexed[i]; item != nil {
			items = append(items, item)
		}
	}
	return items, nil
}

func (d *DynamoDBServer) gsiReadWorker(
	ctx context.Context,
	readTS uint64,
	cond queryCondition,
	jobs <-chan gsiReadJob,
	results chan<- gsiReadResult,
	cancel context.CancelFunc,
) {
	for {
		job, ok := nextGSIReadJob(ctx, jobs)
		if !ok {
			return
		}
		item, emit, err := d.executeGSIReadJob(ctx, readTS, cond, job.key)
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
	cond queryCondition,
	key []byte,
) (map[string]attributeValue, bool, error) {
	item, found, err := d.readItemAtKeyAt(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if !matchesQueryCondition(item, cond) {
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
	if keySchema.HashKey != schema.PrimaryKey.HashKey {
		return dynamoItemPrefixForTable(in.TableName, schema.Generation), nil
	}
	hashKey, err := attributeValueAsKey(hashValue)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return dynamoItemHashPrefixForTable(in.TableName, schema.Generation, hashKey), nil
}

func applyQueryExclusiveStartKey(schema *dynamoTableSchema, startKey map[string]attributeValue, items []map[string]attributeValue) ([]map[string]attributeValue, error) {
	if len(startKey) == 0 {
		return items, nil
	}
	startItemKey, err := schema.itemKeyFromAttributes(startKey)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "invalid ExclusiveStartKey")
	}
	startIndex := -1
	for i, item := range items {
		itemKey, err := schema.itemKeyFromAttributes(item)
		if err != nil {
			continue
		}
		if bytes.Equal(itemKey, startItemKey) {
			startIndex = i
			break
		}
	}
	if startIndex < 0 {
		return items, nil
	}
	return items[startIndex+1:], nil
}

func resolveQueryLimit(limit *int32) int {
	if limit == nil || *limit <= 0 {
		return queryDefaultLimit
	}
	return int(*limit)
}

func paginateQueryItems(keySchema dynamoKeySchema, items []map[string]attributeValue, limit int) ([]map[string]attributeValue, map[string]attributeValue) {
	if limit <= 0 || len(items) <= limit {
		return items, nil
	}
	out := items[:limit]
	return out, makeLastEvaluatedKey(keySchema, out[len(out)-1])
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

func (d *DynamoDBServer) transactWriteItems(w http.ResponseWriter, r *http.Request) {
	in, err := decodeTransactWriteItemsInput(r.Body)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if _, err := collectTransactWriteTableNames(in); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	if err := d.transactWriteItemsWithRetry(r.Context(), in); err != nil {
		writeDynamoErrorFromErr(w, err)
		return
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
	for attempt := 0; attempt < transactRetryMaxAttempts; attempt++ {
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
	current, found, err := d.readItemAtKeyAt(ctx, itemKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !found {
		current = nil
	}
	req, cleanup, err := buildItemWriteRequest(schema, itemKey, current, in.Item)
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
	current, found, err := d.readItemAtKeyAt(ctx, itemKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !found {
		current = map[string]attributeValue{}
	}
	updateIn := updateItemInput(in)
	nextItem, err := buildUpdatedItem(schema, updateIn, current)
	if err != nil {
		return nil, err
	}
	req, cleanup, err := buildItemWriteRequest(schema, itemKey, current, nextItem)
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
	itemKey, err := schema.itemKeyFromAttributes(in.Key)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	current, found, err := d.readItemAtKeyAt(ctx, itemKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
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
	req, err := buildItemDeleteRequest(schema, itemKey, current)
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
	current, found, err := d.readItemAtKeyAt(ctx, itemKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := validateConditionOnItem(
		in.ConditionExpression,
		in.ExpressionAttributeNames,
		in.ExpressionAttributeValues,
		valueOrEmptyMap(current, found),
	); err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrConditionalFailed, err.Error())
	}
	lockReq, lockCleanup, err := buildConditionCheckLockRequest(itemKey, current, found)
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

func buildItemDeleteRequest(
	schema *dynamoTableSchema,
	itemKey []byte,
	current map[string]attributeValue,
) (*kv.OperationGroup[kv.OP], error) {
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: itemKey},
	}
	delKeys, _, err := gsiDeltaKeys(schema, current, nil)
	if err != nil {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	for _, key := range delKeys {
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
		payload, err := json.Marshal(current)
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
	delay := backoff
	if delay > remaining {
		delay = remaining
	}
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

func replaceNames(expr string, names map[string]string) string {
	if expr == "" || len(names) == 0 {
		return expr
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
	return strings.NewReplacer(args...).Replace(expr)
}

func applyUpdateExpression(expr string, names map[string]string, values map[string]attributeValue, item map[string]attributeValue) error {
	updExpr := strings.TrimSpace(replaceNames(expr, names))
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
		return applyDeleteUpdateAction(section.body, item)
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
	attrName := strings.TrimSpace(parts[0])
	if attrName == "" {
		return errors.New("invalid update expression attribute")
	}
	valPlaceholder := strings.TrimSpace(parts[1])
	valAttr, ok := values[valPlaceholder]
	if !ok {
		return errors.New("missing value attribute")
	}
	item[attrName] = valAttr
	return nil
}

func applyRemoveUpdateAction(body string, item map[string]attributeValue) error {
	attrs, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, attr := range attrs {
		attrName := strings.TrimSpace(attr)
		if attrName == "" {
			return errors.New("invalid update expression attribute")
		}
		delete(item, attrName)
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
	attrName := strings.TrimSpace(parts[0])
	placeholder := strings.TrimSpace(parts[1])
	if attrName == "" || !strings.HasPrefix(placeholder, ":") {
		return errors.New("invalid update expression")
	}
	addValue, ok := values[placeholder]
	if !ok {
		return errors.New("missing value attribute")
	}
	if !addValue.hasNumberType() {
		return errors.New("ADD supports only number attributes")
	}
	current, exists := item[attrName]
	if !exists {
		item[attrName] = addValue
		return nil
	}
	if !current.hasNumberType() {
		return errors.New("ADD supports only number attributes")
	}
	sum, err := addNumericAttributeValues(current.numberValue(), addValue.numberValue())
	if err != nil {
		return err
	}
	item[attrName] = attributeValue{N: &sum}
	return nil
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

func applyDeleteUpdateAction(body string, item map[string]attributeValue) error {
	terms, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, term := range terms {
		attrName := deleteTermAttributeName(term)
		if attrName == "" {
			return errors.New("invalid update expression")
		}
		delete(item, attrName)
	}
	return nil
}

func deleteTermAttributeName(term string) string {
	fields := strings.Fields(strings.TrimSpace(term))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
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

func validateConditionOnItem(expr string, names map[string]string, values map[string]attributeValue, item map[string]attributeValue) error {
	cond := strings.TrimSpace(replaceNames(expr, names))
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
	parts := make([]string, 0, splitPartsInitialCapacity)
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 || !matchesKeywordTokenAt(upper, target, i) {
			continue
		}
		if !isLogicalKeywordBoundary(expr, i-1) || !isLogicalKeywordBoundary(expr, i+targetLen) {
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
	if attr, ok := parseConditionFuncAttr(expr, "attribute_exists"); ok {
		_, exists := item[attr]
		return exists, nil
	}
	if attr, ok := parseConditionFuncAttr(expr, "attribute_not_exists"); ok {
		_, exists := item[attr]
		return !exists, nil
	}
	return evalConditionEquals(expr, item, values)
}

func parseConditionFuncAttr(expr string, funcName string) (string, bool) {
	prefix := funcName + "("
	if !strings.HasPrefix(expr, prefix) || !strings.HasSuffix(expr, ")") {
		return "", false
	}
	return strings.TrimSpace(expr[len(prefix) : len(expr)-1]), true
}

func evalConditionEquals(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	parts := strings.SplitN(expr, "=", updateSplitCount)
	if len(parts) != updateSplitCount {
		return false, errors.New("unsupported condition expression")
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])
	if !strings.HasPrefix(right, ":") {
		return false, errors.New("unsupported condition expression")
	}
	lhs, ok := item[left]
	if !ok {
		return false, nil
	}
	rhs, ok := values[right]
	if !ok {
		return false, errors.New("missing condition value")
	}
	return attributeValueEqual(lhs, rhs), nil
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
	// Keep ASCII letters/digits/underscore as identifier token characters so
	// expressions like "MY_AND_VAR" are not split at the "AND" substring.
	if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
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
	idx := strings.Index(term, opStr)
	if idx < 0 {
		return parsedKeyConditionTerm{}, false
	}
	left := strings.TrimSpace(term[:idx])
	right := strings.TrimSpace(term[idx+len(opStr):])
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
	ks, ok := t.GlobalSecondaryIndexes[indexName]
	if !ok {
		return dynamoKeySchema{}, errors.New("unknown index")
	}
	return ks, nil
}

func (t *dynamoTableSchema) itemKeyFromAttributes(attrs map[string]attributeValue) ([]byte, error) {
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
	return dynamoItemKey(t.TableName, t.Generation, hashKey, rangeKey), nil
}

func (t *dynamoTableSchema) gsiEntryKeysForItem(attrs map[string]attributeValue) ([][]byte, error) {
	if len(t.GlobalSecondaryIndexes) == 0 || len(attrs) == 0 {
		return nil, nil
	}
	pkHash, pkRange, err := t.primaryKeyValues(attrs)
	if err != nil {
		return nil, err
	}
	indexNames := sortedGSIIndexNames(t.GlobalSecondaryIndexes)
	keys := make([][]byte, 0, len(indexNames))
	for _, indexName := range indexNames {
		ks := t.GlobalSecondaryIndexes[indexName]
		indexHash, indexRange, include, err := gsiKeyValues(attrs, ks)
		if err != nil {
			return nil, err
		}
		if !include {
			continue
		}
		keys = append(keys, dynamoGSIKey(t.TableName, t.Generation, indexName, indexHash, indexRange, pkHash, pkRange))
	}
	return keys, nil
}

func (t *dynamoTableSchema) primaryKeyValues(attrs map[string]attributeValue) (string, string, error) {
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

func gsiKeyValues(attrs map[string]attributeValue, ks dynamoKeySchema) (string, string, bool, error) {
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

func sortedGSIIndexNames(indexes map[string]dynamoKeySchema) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func attributeValueAsKey(attr attributeValue) (string, error) {
	switch {
	case attr.hasListType() || attr.hasMapType() || attr.BOOL != nil || attr.NULL != nil:
		return "", errors.New("unsupported key attribute type")
	case attr.hasNumberType():
		return attr.numberValue(), nil
	case attr.hasStringType():
		return attr.stringValue(), nil
	default:
		return "", errors.New("unsupported key attribute type")
	}
}

func attributeValueEqual(left attributeValue, right attributeValue) bool {
	if handled, equal := compareBoolAttribute(left, right); handled {
		return equal
	}
	if handled, equal := compareNullAttribute(left, right); handled {
		return equal
	}
	if handled, equal := compareMapAttribute(left, right); handled {
		return equal
	}
	if handled, equal := compareListAttribute(left, right); handled {
		return equal
	}
	if handled, equal := compareNumberAttribute(left, right); handled {
		return equal
	}
	if !left.hasStringType() && !right.hasStringType() {
		return true
	}
	if !left.hasStringType() || !right.hasStringType() {
		return false
	}
	return left.stringValue() == right.stringValue()
}

func compareBoolAttribute(left attributeValue, right attributeValue) (bool, bool) {
	if left.BOOL == nil && right.BOOL == nil {
		return false, false
	}
	if left.BOOL == nil || right.BOOL == nil {
		return true, false
	}
	return true, *left.BOOL == *right.BOOL
}

func compareNullAttribute(left attributeValue, right attributeValue) (bool, bool) {
	if left.NULL == nil && right.NULL == nil {
		return false, false
	}
	if left.NULL == nil || right.NULL == nil {
		return true, false
	}
	return true, *left.NULL == *right.NULL
}

func compareMapAttribute(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasMapType() && !right.hasMapType() {
		return false, false
	}
	if !left.hasMapType() || !right.hasMapType() {
		return true, false
	}
	if len(left.M) != len(right.M) {
		return true, false
	}
	for k, lv := range left.M {
		rv, ok := right.M[k]
		if !ok || !attributeValueEqual(lv, rv) {
			return true, false
		}
	}
	return true, true
}

func compareListAttribute(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasListType() && !right.hasListType() {
		return false, false
	}
	if !left.hasListType() || !right.hasListType() {
		return true, false
	}
	if len(left.L) != len(right.L) {
		return true, false
	}
	for i := range left.L {
		if !attributeValueEqual(left.L[i], right.L[i]) {
			return true, false
		}
	}
	return true, true
}

func compareNumberAttribute(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasNumberType() && !right.hasNumberType() {
		return false, false
	}
	if !left.hasNumberType() || !right.hasNumberType() {
		return true, false
	}
	return true, left.numberValue() == right.numberValue()
}

func compareAttributeValueSortKey(left attributeValue, right attributeValue) int {
	if left.hasNumberType() && right.hasNumberType() {
		if cmp, ok := compareNumericAttributeString(left.numberValue(), right.numberValue()); ok {
			return cmp
		}
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
	switch {
	case attr.hasNumberType():
		return attr.numberValue()
	case attr.BOOL != nil:
		if *attr.BOOL {
			return "1"
		}
		return "0"
	case attr.NULL != nil:
		return ""
	case attr.hasMapType() || attr.hasListType():
		return ""
	case attr.hasStringType():
		return attr.stringValue()
	default:
		return ""
	}
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
			ks := t.GlobalSecondaryIndexes[name]
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
				"Projection": map[string]string{
					"ProjectionType": "ALL",
				},
			}
			gsis = append(gsis, indexDesc)
		}
		resp["GlobalSecondaryIndexes"] = gsis
	}

	return resp
}

func cloneAttributeValueMap(in map[string]attributeValue) map[string]attributeValue {
	out := make(map[string]attributeValue, len(in))
	for k, v := range in {
		out[k] = v
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
	schema := &dynamoTableSchema{}
	if err := json.Unmarshal(b, schema); err != nil {
		return nil, false, errors.WithStack(err)
	}
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

func (d *DynamoDBServer) readItemAtKey(ctx context.Context, key []byte) (map[string]attributeValue, bool, error) {
	return d.readItemAtKeyAt(ctx, key, snapshotTS(d.coordinator.Clock(), d.store))
}

func (d *DynamoDBServer) readItemAtKeyAt(ctx context.Context, key []byte, ts uint64) (map[string]attributeValue, bool, error) {
	b, err := d.store.GetAt(ctx, key, ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	item := map[string]attributeValue{}
	if err := json.Unmarshal(b, &item); err != nil {
		return nil, false, errors.WithStack(err)
	}
	return item, true, nil
}

func (d *DynamoDBServer) scanAllByPrefix(ctx context.Context, prefix []byte) ([]*store.KVPair, error) {
	readTS := snapshotTS(d.coordinator.Clock(), d.store)
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

func dynamoTableMetaKey(tableName string) []byte {
	return []byte(dynamoTableMetaPrefix + encodeDynamoSegment(tableName))
}

func dynamoTableGenerationKey(tableName string) []byte {
	return []byte(dynamoTableGenerationPrefix + encodeDynamoSegment(tableName))
}

func dynamoItemPrefixForTable(tableName string, generation uint64) []byte {
	return []byte(dynamoItemPrefix + encodeDynamoSegment(tableName) + "|" + strconv.FormatUint(generation, 10) + "|")
}

func dynamoItemHashPrefixForTable(tableName string, generation uint64, hashKey string) []byte {
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

func dynamoGSIHashPrefixForTable(tableName string, generation uint64, indexName string, hashKey string) []byte {
	return []byte(
		dynamoGSIPrefix +
			encodeDynamoSegment(tableName) + "|" +
			strconv.FormatUint(generation, 10) + "|" +
			encodeDynamoSegment(indexName) + "|" +
			encodeDynamoSegment(hashKey) + "|",
	)
}

func dynamoGSIKey(tableName string, generation uint64, indexName string, indexHash string, indexRange string, pkHash string, pkRange string) []byte {
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

func dynamoItemKey(tableName string, generation uint64, hashKey, rangeKey string) []byte {
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

package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"hash/fnv"
	"io"
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
	transactRetryMaxAttempts    = 128
	transactRetryMaxDuration    = 2 * time.Second
	transactRetryInitialBackoff = 1 * time.Millisecond
	transactRetryMaxBackoff     = 10 * time.Millisecond
	transactRetryBackoffFactor  = 2
	itemUpdateLockStripeCount   = 256
	tableLockStripeCount        = 128
	queryDefaultLimit           = 100
	maxReplaceNameDepth         = 32
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
	listen           net.Listener
	store            store.MVCCStore
	coordinator      kv.Coordinator
	dynamoTranscoder *dynamodbTranscoder
	httpServer       *http.Server
	targetHandlers   map[string]func(http.ResponseWriter, *http.Request)
	itemUpdateLocks  [itemUpdateLockStripeCount]sync.Mutex
	tableLocks       [tableLockStripeCount]sync.Mutex
}

func NewDynamoDBServer(listen net.Listener, st store.MVCCStore, coordinate kv.Coordinator) *DynamoDBServer {
	d := &DynamoDBServer{
		listen:           listen,
		store:            st,
		coordinator:      coordinate,
		dynamoTranscoder: newDynamoDBTranscoder(),
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
			if err := d.cleanupDeletedTableGeneration(ctx, tableName, schema.Generation); err != nil {
				return errors.WithStack(err)
			}
			return nil
		}

		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "delete table retry attempts exhausted")
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
	unlock := d.lockTableOperations([]string{in.TableName})
	defer unlock()
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
	unlockTable := d.lockTableOperations([]string{in.TableName})
	defer unlockTable()
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

func (d *DynamoDBServer) queryItems(ctx context.Context, in queryInput) (*queryOutput, error) {
	schema, exists, err := d.loadTableSchema(ctx, in.TableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
	}
	attrName, expected, keySchema, err := resolveQueryCondition(in, schema)
	if err != nil {
		return nil, err
	}
	items, scannedCount, err := d.queryItemsByKeyCondition(ctx, in, schema, keySchema, attrName, expected)
	if err != nil {
		return nil, err
	}
	if items, err = applyQueryExclusiveStartKey(schema, in.ExclusiveStartKey, items); err != nil {
		return nil, err
	}
	orderQueryItems(items, keySchema.RangeKey, in.ScanIndexForward)
	limit := resolveQueryLimit(in.Limit)
	pagedItems, lastKey := paginateQueryItems(schema.PrimaryKey, items, limit)
	return &queryOutput{
		items:            pagedItems,
		scannedCount:     scannedCount,
		lastEvaluatedKey: lastKey,
	}, nil
}

func resolveQueryCondition(in queryInput, schema *dynamoTableSchema) (string, attributeValue, dynamoKeySchema, error) {
	attrName, placeholder, err := parseKeyConditionExpression(replaceNames(in.KeyConditionExpression, in.ExpressionAttributeNames))
	if err != nil {
		return "", attributeValue{}, dynamoKeySchema{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	expected, ok := in.ExpressionAttributeValues[placeholder]
	if !ok {
		return "", attributeValue{}, dynamoKeySchema{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing key condition value")
	}
	keySchema, err := schema.keySchemaForQuery(in.IndexName)
	if err != nil {
		return "", attributeValue{}, dynamoKeySchema{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if attrName != keySchema.HashKey {
		return "", attributeValue{}, dynamoKeySchema{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported key condition")
	}
	return attrName, expected, keySchema, nil
}

func filterQueryItems(kvs []*store.KVPair, attrName string, expected attributeValue) ([]map[string]attributeValue, error) {
	items := make([]map[string]attributeValue, 0, len(kvs))
	for _, kvp := range kvs {
		item := map[string]attributeValue{}
		if err := json.Unmarshal(kvp.Value, &item); err != nil {
			return nil, errors.WithStack(err)
		}
		attr, ok := item[attrName]
		if !ok || !attributeValueEqual(attr, expected) {
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
	attrName string,
	expected attributeValue,
) ([]map[string]attributeValue, int, error) {
	if strings.TrimSpace(in.IndexName) != "" {
		return d.queryItemsByGSI(ctx, in, schema, attrName, expected)
	}
	scanPrefix, err := queryScanPrefix(schema, in, keySchema, expected)
	if err != nil {
		return nil, 0, err
	}
	kvs, err := d.scanAllByPrefix(ctx, scanPrefix)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	items, err := filterQueryItems(kvs, attrName, expected)
	if err != nil {
		return nil, 0, err
	}
	return items, len(kvs), nil
}

func (d *DynamoDBServer) queryItemsByGSI(
	ctx context.Context,
	in queryInput,
	schema *dynamoTableSchema,
	attrName string,
	expected attributeValue,
) ([]map[string]attributeValue, int, error) {
	hashKey, err := attributeValueAsKey(expected)
	if err != nil {
		return nil, 0, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	prefix := dynamoGSIHashPrefixForTable(in.TableName, schema.Generation, in.IndexName, hashKey)
	kvs, err := d.scanAllByPrefix(ctx, prefix)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	readTS := snapshotTS(d.coordinator.Clock(), d.store)
	items := make([]map[string]attributeValue, 0, len(kvs))
	for _, kvp := range kvs {
		item, found, err := d.readItemAtKeyAt(ctx, kvp.Value, readTS)
		if err != nil {
			return nil, 0, err
		}
		if !found {
			continue
		}
		attr, ok := item[attrName]
		if !ok || !attributeValueEqual(attr, expected) {
			continue
		}
		items = append(items, item)
	}
	return items, len(kvs), nil
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
	tables, err := collectTransactWriteTableNames(in)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	unlock := d.lockTableOperations(tables)
	defer unlock()
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
		if item.Put == nil {
			return nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported transact item")
		}
		tableName := strings.TrimSpace(item.Put.TableName)
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

func (d *DynamoDBServer) transactWriteItemsWithRetry(ctx context.Context, in transactWriteItemsInput) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for attempt := 0; attempt < transactRetryMaxAttempts; attempt++ {
		reqs, generations, cleanupKeys, err := d.buildTransactWriteItemsRequest(ctx, in)
		if err != nil {
			return err
		}
		if _, err = d.dispatchTransactWriteItemsWithRetry(ctx, reqs); err != nil {
			if !isRetryableTransactWriteError(err) {
				return err
			}
		} else {
			retry, verifyErr := d.handleGenerationFenceResult(
				ctx,
				d.verifyTableGenerations(ctx, generations),
				cleanupKeys,
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
	return newDynamoAPIError(http.StatusInternalServerError, dynamoErrInternal, "transact write retry attempts exhausted")
}

func (d *DynamoDBServer) buildTransactWriteItemsRequest(ctx context.Context, in transactWriteItemsInput) (*kv.OperationGroup[kv.OP], map[string]uint64, [][]byte, error) {
	readTS := d.nextTxnReadTS()
	reqs := &kv.OperationGroup[kv.OP]{IsTxn: true}
	schemaCache := make(map[string]*dynamoTableSchema)
	tableGenerations := make(map[string]uint64)
	cleanup := make([][]byte, 0, len(in.TransactItems))
	for _, item := range in.TransactItems {
		if item.Put == nil {
			return nil, nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported transact item")
		}
		tableName := strings.TrimSpace(item.Put.TableName)
		if tableName == "" {
			return nil, nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing table name")
		}
		schema, err := d.resolveTransactTableSchema(ctx, schemaCache, tableName, readTS)
		if err != nil {
			return nil, nil, nil, err
		}
		itemKey, err := schema.itemKeyFromAttributes(item.Put.Item)
		if err != nil {
			return nil, nil, nil, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
		}
		current, found, err := d.readItemAtKeyAt(ctx, itemKey, readTS)
		if err != nil {
			return nil, nil, nil, errors.WithStack(err)
		}
		if !found {
			current = nil
		}
		itemReq, itemCleanup, err := buildItemWriteRequest(schema, itemKey, current, item.Put.Item)
		if err != nil {
			return nil, nil, nil, err
		}
		reqs.Elems = append(reqs.Elems, itemReq.Elems...)
		tableGenerations[tableName] = schema.Generation
		cleanup = append(cleanup, itemCleanup...)
	}
	return reqs, tableGenerations, cleanup, nil
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

func (d *DynamoDBServer) dispatchTransactWriteItemsWithRetry(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	backoff := transactRetryInitialBackoff
	var lastErr error
	startedAt := time.Now()
	deadline := startedAt.Add(transactRetryMaxDuration)

	for attempt := 0; attempt < transactRetryMaxAttempts; attempt++ {
		// Retry with a fresh transaction start timestamp.
		reqs.StartTS = 0
		resp, err := d.coordinator.Dispatch(ctx, reqs)
		if err == nil {
			return resp, nil
		}
		lastErr = errors.WithStack(err)
		if !isRetryableTransactWriteError(err) {
			return nil, lastErr
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, errors.Wrapf(lastErr, "transact write retry timeout after %s (attempts: %d)", transactRetryMaxDuration, attempt+1)
		}
		retryDelay := backoff
		if retryDelay > remaining {
			retryDelay = remaining
		}
		if err := waitTransactRetryBackoff(ctx, retryDelay); err != nil {
			combined := errors.Join(err, lastErr)
			return nil, errors.Wrap(combined, "transact write retry canceled")
		}
		backoff = nextTransactRetryBackoff(backoff)
	}

	return nil, errors.Wrapf(lastErr, "transact write retry attempts exhausted after %s (attempts: %d)", time.Since(startedAt), transactRetryMaxAttempts)
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
	for i := 0; i < maxReplaceNameDepth; i++ {
		next := expr
		for _, key := range keys {
			next = strings.ReplaceAll(next, key, names[key])
		}
		if next == expr {
			return next
		}
		expr = next
	}
	return expr
}

func applyUpdateExpression(expr string, names map[string]string, values map[string]attributeValue, item map[string]attributeValue) error {
	updExpr := strings.TrimSpace(replaceNames(expr, names))
	if !strings.HasPrefix(strings.ToUpper(updExpr), "SET ") {
		return errors.New("unsupported update expression")
	}
	assign := strings.TrimSpace(updExpr[len("SET "):])
	parts := strings.SplitN(assign, "=", updateSplitCount)
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

func parseKeyConditionExpression(expr string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(expr), "=", updateSplitCount)
	if len(parts) != updateSplitCount {
		return "", "", errors.New("unsupported key condition expression")
	}
	attrName := strings.TrimSpace(parts[0])
	placeholder := strings.TrimSpace(parts[1])
	if attrName == "" || !strings.HasPrefix(placeholder, ":") {
		return "", "", errors.New("unsupported key condition expression")
	}
	return attrName, placeholder, nil
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
		return attr.N, nil
	case attr.hasStringType():
		return attr.S, nil
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
	return left.S == right.S
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
	return true, left.N == right.N
}

func compareAttributeValueSortKey(left attributeValue, right attributeValue) int {
	if left.hasNumberType() && right.hasNumberType() {
		if cmp, ok := compareNumericAttributeString(left.N, right.N); ok {
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
		return attr.N
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
		return attr.S
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

package adapter

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

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

type transactGetItemsInput struct {
	TransactItems []transactGetItem `json:"TransactItems"`
}

type transactGetItem struct {
	Get *transactGetItemGet `json:"Get"`
}

type transactGetItemGet struct {
	TableName                string                    `json:"TableName"`
	Key                      map[string]attributeValue `json:"Key"`
	ProjectionExpression     string                    `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames map[string]string         `json:"ExpressionAttributeNames,omitempty"`
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

// transactGetItems implements TransactGetItems: reads multiple items atomically
// at a single snapshot timestamp, guaranteeing a consistent view across all keys.
func (d *DynamoDBServer) transactGetItems(w http.ResponseWriter, r *http.Request) {
	in, err := decodeTransactGetItemsInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}

	// Lease-check every shard this transaction will read BEFORE the single
	// snapshot timestamp is resolved, so the quorum-freshness bound is
	// established without changing the single-snapshot-ts semantics. The
	// timestamp below is still sampled once and shared by all items.
	if !d.leaseCheckTransactGetItems(w, r, in) {
		return
	}

	// Acquire a single read timestamp for all items to guarantee a consistent snapshot.
	readTS := d.nextTxnReadTS()
	pin := d.pinReadTS(readTS)
	defer pin.Release()

	responses, tableMetrics, err := d.buildTransactGetItemsResponses(r.Context(), in, readTS)
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	for table, m := range tableMetrics {
		d.observeReadMetrics(r.Context(), table, m.found, m.requested)
	}
	writeDynamoJSON(w, map[string]any{"Responses": responses})
}

func decodeTransactGetItemsInput(bodyReader io.Reader) (transactGetItemsInput, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return transactGetItemsInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	var in transactGetItemsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return transactGetItemsInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	if len(in.TransactItems) == 0 {
		return transactGetItemsInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing transact items")
	}
	if len(in.TransactItems) > transactGetItemsMaxItems {
		return transactGetItemsInput{}, newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation,
			"Too many items in TransactGetItems: "+strconv.Itoa(len(in.TransactItems))+" (max "+strconv.Itoa(transactGetItemsMaxItems)+")")
	}
	return in, nil
}

// collectTransactGetTableNames returns a deduplicated list of table names referenced
// in the TransactGetItems input. Used to run ensureLegacyTableMigration once per table.
func collectTransactGetTableNames(in transactGetItemsInput) []string {
	seen := make(map[string]struct{}, len(in.TransactItems))
	names := make([]string, 0, len(in.TransactItems))
	for _, item := range in.TransactItems {
		if item.Get == nil {
			continue
		}
		t := item.Get.TableName
		if _, exists := seen[t]; exists {
			continue
		}
		seen[t] = struct{}{}
		names = append(names, t)
	}
	return names
}

// transactGetItemsMetrics holds per-table counts for metrics reporting.
type transactGetItemsMetrics struct {
	requested int
	found     int
}

// buildTransactGetItemsResponses reads each requested item at the given readTS
// and returns the ordered response list and a per-table metrics map.
// schemaCache avoids redundant storage reads when multiple items share the same table.
// seenItemKeys enforces the DynamoDB rule that a transaction may not reference the
// same item more than once.
// ensureLegacyTableMigration is called once per unique table before any item is read.
func (d *DynamoDBServer) buildTransactGetItemsResponses(ctx context.Context, in transactGetItemsInput, readTS uint64) ([]map[string]any, map[string]*transactGetItemsMetrics, error) {
	tableNames := collectTransactGetTableNames(in)
	for _, tableName := range tableNames {
		if err := d.ensureLegacyTableMigration(ctx, tableName); err != nil {
			return nil, nil, err
		}
	}
	schemaCache := make(map[string]*dynamoTableSchema)
	seenItemKeys := make(map[transactGetSeenKey]struct{}, len(in.TransactItems))
	tableMetrics := make(map[string]*transactGetItemsMetrics)
	responses := make([]map[string]any, 0, len(in.TransactItems))
	for _, item := range in.TransactItems {
		entry, itemFound, tableName, err := d.readTransactGetItem(ctx, item, schemaCache, seenItemKeys, readTS)
		if err != nil {
			return nil, nil, err
		}
		responses = append(responses, entry)
		m := tableMetrics[tableName]
		if m == nil {
			m = &transactGetItemsMetrics{}
			tableMetrics[tableName] = m
		}
		m.requested++
		if itemFound {
			m.found++
		}
	}
	return responses, tableMetrics, nil
}

// transactGetSeenKey is the map key used for duplicate-item detection in
// TransactGetItems. Using a struct avoids separator-collision risks from
// string concatenation and is more idiomatic Go.
type transactGetSeenKey struct {
	tableName string
	keyStr    string
}

// readTransactGetItem validates and reads a single item in a TransactGetItems request.
// ensureLegacyTableMigration must be called for g.TableName before invoking this function.
// Returns the response entry, whether the item was found, the table name, and any error.
// Returning the table name avoids the caller having to re-access item.Get after the call.
func (d *DynamoDBServer) readTransactGetItem(ctx context.Context, item transactGetItem, schemaCache map[string]*dynamoTableSchema, seenItemKeys map[transactGetSeenKey]struct{}, readTS uint64) (map[string]any, bool, string, error) {
	if item.Get == nil {
		return nil, false, "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "TransactGetItems only supports Get operations")
	}
	g := item.Get
	if strings.TrimSpace(g.TableName) == "" {
		return nil, false, "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing TableName in Get")
	}
	schema, err := d.resolveTransactTableSchema(ctx, schemaCache, g.TableName, readTS)
	if err != nil {
		return nil, false, "", err
	}
	// Reject duplicate item keys to match real DynamoDB behavior.
	// canonicalPrimaryKeyStr reads only hash/range key attributes from g.Key
	// by schema name, so extra attributes in the map are safely ignored —
	// no separate primaryKeyAttributes extraction is needed.
	keyStr, err := canonicalPrimaryKeyStr(schema.PrimaryKey, g.Key)
	if err != nil {
		return nil, false, "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	seenKey := transactGetSeenKey{tableName: g.TableName, keyStr: keyStr}
	if _, dup := seenItemKeys[seenKey]; dup {
		return nil, false, "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation,
			"Transaction request cannot include multiple operations on one item")
	}
	seenItemKeys[seenKey] = struct{}{}
	loc, found, err := d.readLogicalItemAt(ctx, schema, g.Key, readTS)
	if err != nil {
		// Return the error as-is: storage errors from readItemAtKeyAt surface as
		// InternalServerError (500) via writeDynamoErrorFromErr in the HTTP handler.
		return nil, false, "", err
	}
	entry := map[string]any{}
	if found {
		projected, err := projectItem(loc.item, g.ProjectionExpression, g.ExpressionAttributeNames)
		if err != nil {
			return nil, false, "", err
		}
		entry["Item"] = projected
	}
	return entry, found, g.TableName, nil
}

// canonicalPrimaryKeyStr returns a collision-free canonical string of primary
// key attributes for duplicate-item detection in TransactGetItems and
// TransactWriteItems. Shared between both operations to avoid duplicated logic.
//
// Takes the table's keySchema so it can write hash key then range key in a
// fixed schema-defined order, avoiding a slice allocation and sort — DynamoDB
// primary keys have at most two attributes, so direct lookup beats sorting.
//
// Format per attribute: "<name>=<type>:<len>:<value>", separated by \x1f.
// The length prefix makes the format collision-free: a string value that
// contains \x1f cannot be confused with the inter-attribute separator because
// the decoder knows exactly how many bytes belong to each value.
// Numeric values are normalised; binary values are base64-encoded.
func canonicalPrimaryKeyStr(keySchema dynamoKeySchema, key map[string]attributeValue) (string, error) {
	var buf strings.Builder
	hashVal, ok := key[keySchema.HashKey]
	if !ok {
		return "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing hash key attribute")
	}
	buf.WriteString(keySchema.HashKey)
	buf.WriteByte('=')
	if err := writeCanonicalAttrValue(&buf, hashVal); err != nil {
		return "", err
	}
	if keySchema.RangeKey != "" {
		rangeVal, ok := key[keySchema.RangeKey]
		if !ok {
			return "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "missing range key attribute")
		}
		buf.WriteByte('\x1f')
		buf.WriteString(keySchema.RangeKey)
		buf.WriteByte('=')
		if err := writeCanonicalAttrValue(&buf, rangeVal); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

// writeCanonicalAttrValue appends a length-prefixed typed value for a single
// primary key attribute to buf. Format: "<type>:<len>:<value>".
// Supports S (string), N (normalised number), and B (base64-encoded binary).
// The length prefix prevents collisions when string values contain \x1f.
func writeCanonicalAttrValue(buf *strings.Builder, v attributeValue) error {
	switch {
	case v.S != nil:
		buf.WriteString("S:")
		buf.WriteString(strconv.Itoa(len(*v.S)))
		buf.WriteByte(':')
		buf.WriteString(*v.S)
	case v.N != nil:
		n := canonicalNumberString(*v.N)
		buf.WriteString("N:")
		buf.WriteString(strconv.Itoa(len(n)))
		buf.WriteByte(':')
		buf.WriteString(n)
	case v.B != nil:
		encoded := base64.StdEncoding.EncodeToString(v.B)
		buf.WriteString("B:")
		buf.WriteString(strconv.Itoa(len(encoded)))
		buf.WriteByte(':')
		buf.WriteString(encoded)
	default:
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported key attribute type for duplicate detection")
	}
	return nil
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
	// seenItemKeys tracks (tableName, primaryKey) pairs to detect duplicates.
	// Real DynamoDB rejects requests with multiple operations on the same item.
	seenItemKeys := make(map[string]struct{}, len(in.TransactItems))
	for _, item := range in.TransactItems {
		if err := d.processTransactWriteItem(ctx, item, readTS, reqs, schemaCache, seenItemKeys, tableGenerations, &cleanup); err != nil {
			return nil, nil, nil, err
		}
	}
	return reqs, tableGenerations, cleanup, nil
}

// processTransactWriteItem validates and plans a single item within a
// TransactWriteItems request, appending the resulting ops to reqs and cleanup.
func (d *DynamoDBServer) processTransactWriteItem(
	ctx context.Context,
	item transactWriteItem,
	readTS uint64,
	reqs *kv.OperationGroup[kv.OP],
	schemaCache map[string]*dynamoTableSchema,
	seenItemKeys map[string]struct{},
	tableGenerations map[string]uint64,
	cleanup *[][]byte,
) error {
	tableName, err := transactWriteItemTableName(item)
	if err != nil {
		return err
	}
	schema, err := d.resolveTransactTableSchema(ctx, schemaCache, tableName, readTS)
	if err != nil {
		return err
	}
	// Reject duplicate item keys to match real DynamoDB behavior.
	itemKeyStr, err := transactWriteItemPrimaryKeyStr(schema, item)
	if err != nil {
		return err
	}
	compositeKey := tableName + "\x00" + itemKeyStr
	if _, dup := seenItemKeys[compositeKey]; dup {
		return newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation,
			"Transaction request cannot include multiple operations on one item")
	}
	seenItemKeys[compositeKey] = struct{}{}
	plan, err := d.buildTransactWriteItemPlan(ctx, schema, item, readTS)
	if err != nil {
		// Real DynamoDB wraps per-item condition failures in
		// TransactionCanceledException rather than surfacing the raw
		// ConditionalCheckFailedException to the caller.
		var apiErr *dynamoAPIError
		if errors.As(err, &apiErr) && apiErr.errorType == dynamoErrConditionalFailed {
			return newDynamoAPIError(http.StatusBadRequest, dynamoErrTransactionCanceled, apiErr.message)
		}
		return err
	}
	reqs.Elems = append(reqs.Elems, plan.elems...)
	reqs.ReadKeys = append(reqs.ReadKeys, plan.readKeys...)
	if !plan.writes {
		return nil
	}
	tableGenerations[tableName] = schema.Generation
	*cleanup = append(*cleanup, plan.cleanup...)
	return nil
}

// transactWriteItemPrimaryKeyStr returns a canonical string of the item's
// primary key attributes, used to detect duplicate-item violations in
// TransactWriteItems (real DynamoDB returns ValidationException for these).
// Delegates to canonicalPrimaryKeyStr for the actual serialization.
// primaryKeyAttributes is applied uniformly across all operation types so that
// only hash/range key fields are used for duplicate detection, regardless of
// whether the operation carries a full Item (Put) or a Key-only map (Update/Delete/ConditionCheck).
func transactWriteItemPrimaryKeyStr(schema *dynamoTableSchema, item transactWriteItem) (string, error) {
	var rawAttrs map[string]attributeValue
	switch {
	case item.Update != nil:
		rawAttrs = item.Update.Key
	case item.Delete != nil:
		rawAttrs = item.Delete.Key
	case item.ConditionCheck != nil:
		rawAttrs = item.ConditionCheck.Key
	case item.Put != nil:
		rawAttrs = item.Put.Item
	default:
		return "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, "unsupported transact item")
	}
	keyAttrs, err := primaryKeyAttributes(schema.PrimaryKey, rawAttrs)
	if err != nil {
		// primaryKeyAttributes already returns a dynamoAPIError; return it directly
		// to preserve its status code and error type.
		return "", err
	}
	keyStr, err := canonicalPrimaryKeyStr(schema.PrimaryKey, keyAttrs)
	if err != nil {
		return "", newDynamoAPIError(http.StatusBadRequest, dynamoErrValidation, err.Error())
	}
	return keyStr, nil
}

type transactWriteItemPlan struct {
	elems   []*kv.Elem[kv.OP]
	cleanup [][]byte
	writes  bool
	// readKeys contains the raw storage keys that were read during plan
	// construction.  They are propagated into OperationGroup.ReadKeys so the
	// FSM can validate read-write conflicts atomically at commit time,
	// preventing lost-update and G0 anomalies on concurrent transactions that
	// read the same item at a stale timestamp.
	readKeys [][]byte
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
		elems:    req.Elems,
		cleanup:  cleanup,
		writes:   true,
		readKeys: [][]byte{itemKey},
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
		elems:    req.Elems,
		cleanup:  cleanup,
		writes:   true,
		readKeys: [][]byte{itemKey},
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
		// Item does not exist at readTS. Track the key so a concurrent create
		// after our snapshot can be detected if the overall transaction
		// includes write elems and therefore reaches FSM validation. In a
		// pure no-op transaction, these read keys may not be validated.
		return &transactWriteItemPlan{readKeys: [][]byte{itemKey}}, nil
	}
	req, err := buildItemDeleteRequestWithSource(currentLocation)
	if err != nil {
		return nil, err
	}
	return &transactWriteItemPlan{
		elems:    req.Elems,
		writes:   true,
		readKeys: [][]byte{itemKey},
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
		elems:    lockReq.Elems,
		cleanup:  lockCleanup,
		writes:   true,
		readKeys: [][]byte{itemKey},
	}, nil
}

func valueOrEmptyMap(item map[string]attributeValue, found bool) map[string]attributeValue {
	if found {
		return item
	}
	return map[string]attributeValue{}
}

func buildConditionCheckLockRequest(
	itemKey []byte,
	current map[string]attributeValue,
	found bool,
) (*kv.OperationGroup[kv.OP], [][]byte, error) {
	if !found {
		// Item does not exist: no write is needed.
		// Include itemKey in ReadKeys only so OCC conflict detection fires
		// if a concurrent writer commits to this key between our startTS and commitTS.
		// Writing a Del tombstone here would shadow any concurrently committed Put
		// at a higher timestamp, causing G-single-item-realtime anomalies.
		// Return nil cleanup since nothing was written by this condition check.
		return &kv.OperationGroup[kv.OP]{
				IsTxn:   true,
				StartTS: 0,
				Elems:   nil,
			},
			nil,
			nil
	}
	payload, err := encodeStoredDynamoItem(current)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: itemKey, Value: payload}}
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
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked) || errors.Is(err, kv.ErrRouteWriteFenced)
}

func isIgnorableTransactRaceError(err error) bool {
	// Route fences reject before applying the write. They must be retried or
	// propagated, not swallowed as "another worker already completed it".
	return errors.Is(err, store.ErrWriteConflict) || errors.Is(err, kv.ErrTxnLocked)
}

func shouldPreserveTransactWriteAttempt(err error) bool {
	return isRetryableTransactWriteError(err) && !errors.Is(err, kv.ErrRouteWriteFenced)
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
	// Use consistentReadLatestTS to always read the latest committed schema.
	// Using a stale snapshotTS can cause false "table not found" results when
	// this node's LastCommitTS is behind the table creation timestamp, which
	// would erroneously trigger cleanupCommittedKeys and delete live item data.
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, consistentReadLatestTS)
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

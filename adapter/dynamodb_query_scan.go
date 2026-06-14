package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

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

func (d *DynamoDBServer) query(w http.ResponseWriter, r *http.Request) {
	in, err := decodeQueryInput(maxDynamoBodyReader(w, r))
	if err != nil {
		writeDynamoErrorFromErr(w, err)
		return
	}
	// Lease-check the shard the Query reads BEFORE queryItems samples
	// readTS, so the quorum-freshness bound is established without
	// changing read-snapshot semantics (sampling readTS only after the
	// confirmation keeps any commit that landed before it visible). A
	// base-table Query on a single partition key reads exactly one
	// hash-key prefix, which routes to one shard group, so the check is
	// routed by that prefix in a multi-group deployment. GSI queries and
	// queries whose prefix cannot be resolved fall back to the keyless
	// check, which spans every shard the range can touch.
	if !d.leaseCheckQuery(w, r, in) {
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
	// A Scan reads the whole table and therefore spans every shard that
	// holds any of its items. leaseCheckScan establishes the quorum-freshness
	// bound across every group BEFORE scanItems samples readTS — but only for
	// a request that passes the cheap table/GSI validation, so a scan that
	// will deterministically 4xx is not masked by a degraded-lease 500
	// (codex #952 P2-A).
	if !d.leaseCheckScan(w, r, in) {
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

// projectionInvalid returns true when the ProjectionExpression cannot be
// parsed against the given ExpressionAttributeNames. resolveProjectionAttributes
// is the same validator newReadPageState runs before the iterator reads from
// the store, so a true result means the read path WILL reject the request with
// a deterministic ValidationException without touching data. Pre-pass uses
// this to skip leasing in that case (codex #952 P2 round-4 lines 2346, 2492).
// An empty ProjectionExpression is the common "project everything" case and
// returns false (no validation needed).
func projectionInvalid(projectionExpression string, names map[string]string) bool {
	if strings.TrimSpace(projectionExpression) == "" {
		return false
	}
	_, err := resolveProjectionAttributes(projectionExpression, names)
	return err != nil
}

// scanExclusiveStartKeyInvalid returns true when in.ExclusiveStartKey cannot be
// decoded against the table's primary key (Scan with no IndexName) or the named
// GSI (Scan with IndexName). It mirrors the validation resolveTableReadBounds /
// resolveGSIReadBounds run in scanItems so the lease pre-pass can route the
// invalid case to the same skip-lease path as table-not-found etc. A nil schema
// is treated as "not invalid" because multiShardReadLeasePlan already classified
// the request as queryLeaseSkip in that case and we never reach here.
func scanExclusiveStartKeyInvalid(schema *dynamoTableSchema, in scanInput) bool {
	if schema == nil || len(in.ExclusiveStartKey) == 0 {
		return false
	}
	if strings.TrimSpace(in.IndexName) == "" {
		_, err := schema.itemKeyFromAttributes(in.ExclusiveStartKey)
		return err != nil
	}
	_, _, err := schema.gsiKeyFromAttributes(in.IndexName, in.ExclusiveStartKey)
	return err != nil
}

// queryExclusiveStartKeyInvalid mirrors the validation
// resolveQueryExclusiveStartKey runs inside queryItems' read-bounds resolution
// (`adapter/dynamodb.go` resolveQueryExclusiveStartKey / resolveTableReadBounds /
// resolveGSIReadBounds): a malformed ExclusiveStartKey produces a deterministic
// ValidationException without touching any store. Returning true routes the
// lease pre-pass to queryLeaseSkip so a degraded-shard 500 cannot mask that 4xx
// (codex #952 P2 round-3). Mirrors scanExclusiveStartKeyInvalid for the
// Query path — kept separate because the GSI vs base-table dispatch differs
// from the Scan input.
func queryExclusiveStartKeyInvalid(schema *dynamoTableSchema, in queryInput) bool {
	if schema == nil || len(in.ExclusiveStartKey) == 0 {
		return false
	}
	if strings.TrimSpace(in.IndexName) == "" {
		_, err := schema.itemKeyFromAttributes(in.ExclusiveStartKey)
		return err != nil
	}
	_, _, err := schema.gsiKeyFromAttributes(in.IndexName, in.ExclusiveStartKey)
	return err != nil
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

// consistentReadLatestTS is a read timestamp used for ConsistentRead=true reads.
// The value is far above any realistic HLC timestamp (~Unix-nanosecond range,
// ≪ 10^19), so reading at this TS from the leader's Pebble store returns the
// most recently committed version of any key.  It avoids the noStartTS
// sentinel (^uint64(0)) used by the coordinator.
//
// This sentinel is used on BOTH the leader and followers:
//   - On a follower, the read is proxied to the leader via proxyRawGet with
//     ts=consistentReadLatestTS, so the leader reads the absolute latest version.
//   - On the leader, the LeaderRoutedStore performs a linearizable read fence
//     (ensuring all committed Raft entries are applied) and then reads locally
//     at consistentReadLatestTS, returning the latest committed version.
//
// Using store.LastCommitTS() instead would introduce a TOCTOU race: the
// timestamp is captured before the linearizable fence, so a write committed
// after LastCommitTS() but applied during the fence would be missed.
const consistentReadLatestTS = ^uint64(0) - 1

func (d *DynamoDBServer) resolveDynamoReadTS(consistentRead *bool) uint64 {
	if consistentRead != nil && *consistentRead {
		return consistentReadLatestTS
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.gsiReadWorker(ctx, readTS, filter, jobs, results, cancel)
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

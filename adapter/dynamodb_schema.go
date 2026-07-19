package adapter

import (
	"bytes"
	"context"
	"io"
	"log/slog"
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
		readTimestamp, err := d.beginTxnReadTimestamp(ctx, "dynamodb create table: begin read timestamp")
		if err != nil {
			return errors.WithStack(err)
		}
		readTS := readTimestamp.Timestamp()
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
		req.StartTS = readTS
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
		readTimestamp, err := d.beginTxnReadTimestamp(ctx, "dynamodb delete table: begin read timestamp")
		if err != nil {
			return errors.WithStack(err)
		}
		readTS := readTimestamp.Timestamp()
		schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists {
			return newDynamoAPIError(http.StatusBadRequest, dynamoErrResourceNotFound, "table not found")
		}

		req := &kv.OperationGroup[kv.OP]{
			IsTxn:   true,
			StartTS: readTS,
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
	// Dispatch a single DEL_PREFIX operation per prefix. The FSM on each node
	// scans and writes tombstones locally, avoiding the enumerate-then-batch-
	// delete loop that previously required many Raft proposals.
	for _, prefix := range prefixes {
		_, err := d.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.DelPrefix, Key: prefix},
			},
		})
		if err != nil {
			return errors.WithStack(err)
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

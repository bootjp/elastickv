package adapter

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (d *DynamoDBServer) ensureLegacyTableMigration(ctx context.Context, tableName string) error {
	unlock := d.lockTableOperations([]string{tableName})
	defer unlock()
	return d.ensureLegacyTableMigrationLocked(ctx, tableName)
}

func (d *DynamoDBServer) ensureLegacyTableMigrationLocked(ctx context.Context, tableName string) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTimestamp, schema, migrationRequired, err := d.legacyMigrationSnapshot(ctx, tableName)
		if err != nil {
			return errors.WithStack(err)
		}
		if !migrationRequired {
			return nil
		}
		// Admin read-only callers (AdminScanTable) must not trigger
		// migration writes. Their own pre-check at the admin readTS
		// already rejects needs-migration tables, but the schema can
		// transition between that check and this one (Codex r8 P2 on
		// PR #805) — refuse rather than racing into write-path code.
		if isAdminReadOnlyContext(ctx) {
			return errors.Wrap(ErrAdminDynamoValidation,
				"table requires a one-time legacy-key migration before admin read endpoints are available; migrate via the SigV4 surface first")
		}
		if !schema.usesOrderedKeyEncoding() {
			err = d.startLegacyTableKeyMigration(ctx, schema, readTimestamp)
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

func (d *DynamoDBServer) legacyMigrationSnapshot(
	ctx context.Context,
	tableName string,
) (kv.ReadTimestamp, *dynamoTableSchema, bool, error) {
	readTimestamp, err := d.beginTxnReadTimestamp(ctx, "dynamodb legacy-table migration: begin read timestamp")
	if err != nil {
		return kv.ReadTimestamp{}, nil, false, errors.WithStack(err)
	}
	schema, exists, err := d.loadTableSchemaAt(ctx, tableName, readTimestamp.Timestamp())
	if err != nil {
		return kv.ReadTimestamp{}, nil, false, errors.WithStack(err)
	}
	return readTimestamp, schema, exists && schema.needsLegacyKeyMigration(), nil
}

func (d *DynamoDBServer) startLegacyTableKeyMigration(
	ctx context.Context,
	schema *dynamoTableSchema,
	readTimestamp kv.ReadTimestamp,
) error {
	if schema == nil || schema.usesOrderedKeyEncoding() {
		return nil
	}
	readTS := readTimestamp.Timestamp()
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
		readTimestamp, err := d.beginTxnReadTimestamp(ctx, "dynamodb legacy-item migration: begin read timestamp")
		if err != nil {
			return errors.WithStack(err)
		}
		readTS := readTimestamp.Timestamp()
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
	readTimestamp, err := d.beginTxnReadTimestamp(ctx, "dynamodb finalize migration: begin read timestamp")
	if err != nil {
		return errors.WithStack(err)
	}
	readTS := readTimestamp.Timestamp()
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

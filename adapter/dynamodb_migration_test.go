package adapter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/kv/kvtest"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// localAdapterCoordinator applies dispatches to an in-memory store through
// kvtest.LocalCoordinator while keeping stubAdapterCoordinator's configurable
// leadership surface. Both share one HLC. DEL_PREFIX is applied only as a
// standalone, non-transactional group, mirroring the production coordinator's
// dispatch-time rejection rules (Codex P1 on PR #695: a txn mixing Del and
// DelPrefix must fail here as it does in production).
type localAdapterCoordinator struct {
	stubAdapterCoordinator
	store store.MVCCStore
	local *kvtest.LocalCoordinator
}

func newLocalAdapterCoordinator(st store.MVCCStore) *localAdapterCoordinator {
	return newLocalAdapterCoordinatorWithOptions(st, kvtest.LocalOptions{DelPrefix: kvtest.PrefixApply})
}

func newLocalAdapterCoordinatorWithOptions(st store.MVCCStore, options kvtest.LocalOptions) *localAdapterCoordinator {
	clock := kv.NewHLC()
	options.Clock = clock
	return &localAdapterCoordinator{
		stubAdapterCoordinator: stubAdapterCoordinator{clock: clock},
		store:                  st,
		local:                  kvtest.NewLocalCoordinator(st, options),
	}
}

func (c *localAdapterCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := c.local.Dispatch(ctx, req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return resp, nil
}

func newLegacyMigrationTestServer(
	t *testing.T,
	withGSI bool,
	rangeType string,
) (*dynamoTableSchema, *DynamoDBServer, store.MVCCStore) {
	t.Helper()

	schema := &dynamoTableSchema{
		TableName:          "t",
		Generation:         1,
		KeyEncodingVersion: 0,
		AttributeDefinitions: map[string]string{
			"pk": "S",
			"sk": rangeType,
		},
		PrimaryKey: dynamoKeySchema{
			HashKey:  "pk",
			RangeKey: "sk",
		},
	}
	if withGSI {
		schema.AttributeDefinitions["status"] = "S"
		schema.GlobalSecondaryIndexes = map[string]dynamoGlobalSecondaryIndex{
			"status-index": {
				KeySchema: dynamoKeySchema{
					HashKey:  "status",
					RangeKey: "sk",
				},
				Projection: dynamoGSIProjection{
					ProjectionType: "ALL",
				},
			},
		}
	}

	st := store.NewMVCCStore()
	server := NewDynamoDBServer(nil, st, newLocalAdapterCoordinator(st))
	return schema, server, st
}

func TestDynamoDB_EnsureLegacyTableMigration_MigratesLegacyGeneration(t *testing.T) {
	t.Parallel()

	legacySchema, server, st := newLegacyMigrationTestServer(t, true, "N")
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(legacySchema)
	for _, seq := range []string{"10", "2", "100"} {
		writer.writeItem(legacySchema, map[string]attributeValue{
			"pk":     newStringAttributeValue("tenant"),
			"sk":     newNumberAttributeValue(seq),
			"status": newStringAttributeValue("open"),
			"value":  newStringAttributeValue(fmt.Sprintf("v-%s", seq)),
		})
	}

	ctx := context.Background()
	require.NoError(t, server.ensureLegacyTableMigration(ctx, legacySchema.TableName))

	schema, exists, err := server.loadTableSchema(ctx, legacySchema.TableName)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, schema.usesOrderedKeyEncoding())
	require.Zero(t, schema.MigratingFromGeneration)
	require.Equal(t, uint64(2), schema.Generation)

	out, err := server.queryItems(ctx, queryInput{
		TableName:              legacySchema.TableName,
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
		Limit: int32Ptr(1),
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, newNumberAttributeValue("2"), out.items[0]["sk"])

	reverseOut, err := server.queryItems(ctx, queryInput{
		TableName:              legacySchema.TableName,
		IndexName:              "status-index",
		KeyConditionExpression: "status = :status",
		ExpressionAttributeValues: map[string]attributeValue{
			":status": newStringAttributeValue("open"),
		},
		Limit:            int32Ptr(1),
		ScanIndexForward: boolPtr(false),
	})
	require.NoError(t, err)
	require.Len(t, reverseOut.items, 1)
	require.Equal(t, newNumberAttributeValue("100"), reverseOut.items[0]["sk"])

	require.Eventually(t, func() bool {
		oldItems, err := server.scanAllByPrefix(ctx, dynamoItemPrefixForTable(legacySchema.TableName, legacySchema.Generation))
		if err != nil {
			return false
		}
		oldGSI, err := server.scanAllByPrefix(ctx, dynamoGSIPrefixForTable(legacySchema.TableName, legacySchema.Generation))
		if err != nil {
			return false
		}
		return len(oldItems) == 0 && len(oldGSI) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestDynamoDB_EnsureLegacyTableMigration_PrefersExistingTargetItems(t *testing.T) {
	t.Parallel()

	legacySchema, server, st := newLegacyMigrationTestServer(t, false, "N")
	writer := newDynamoFixtureWriter(t, st)
	migratingSchema := &dynamoTableSchema{
		TableName:               legacySchema.TableName,
		Generation:              2,
		KeyEncodingVersion:      dynamoOrderedKeyEncodingV2,
		MigratingFromGeneration: legacySchema.Generation,
		AttributeDefinitions:    legacySchema.AttributeDefinitions,
		PrimaryKey:              legacySchema.PrimaryKey,
	}
	writer.writeSchema(migratingSchema)
	writer.writeItem(legacySchema, map[string]attributeValue{
		"pk":    newStringAttributeValue("tenant"),
		"sk":    newNumberAttributeValue("2"),
		"value": newStringAttributeValue("old"),
	})
	writer.writeItem(legacySchema, map[string]attributeValue{
		"pk":    newStringAttributeValue("tenant"),
		"sk":    newNumberAttributeValue("5"),
		"value": newStringAttributeValue("migrated"),
	})
	writer.writeItem(migratingSchema, map[string]attributeValue{
		"pk":    newStringAttributeValue("tenant"),
		"sk":    newNumberAttributeValue("2"),
		"value": newStringAttributeValue("new"),
	})

	ctx := context.Background()
	require.NoError(t, server.ensureLegacyTableMigration(ctx, legacySchema.TableName))

	schema, exists, err := server.loadTableSchema(ctx, legacySchema.TableName)
	require.NoError(t, err)
	require.True(t, exists)
	require.Zero(t, schema.MigratingFromGeneration)

	out, err := server.queryItems(ctx, queryInput{
		TableName:              legacySchema.TableName,
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.items, 2)
	require.Equal(t, newStringAttributeValue("new"), out.items[0]["value"])
	require.Equal(t, newNumberAttributeValue("2"), out.items[0]["sk"])
	require.Equal(t, newNumberAttributeValue("5"), out.items[1]["sk"])
}

func TestDynamoDB_TransactWriteItemsWithRetry_MigratesLegacyTables(t *testing.T) {
	t.Parallel()

	legacySchema, server, st := newLegacyMigrationTestServer(t, false, "S")
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(legacySchema)
	writer.writeItem(legacySchema, map[string]attributeValue{
		"pk":    newStringAttributeValue("tenant"),
		"sk":    newStringAttributeValue("item"),
		"value": newStringAttributeValue("old"),
	})

	ctx := context.Background()
	err := server.transactWriteItemsWithRetry(ctx, transactWriteItemsInput{
		TransactItems: []transactWriteItem{
			{
				Update: &transactUpdateInput{
					TableName:        legacySchema.TableName,
					Key:              map[string]attributeValue{"pk": newStringAttributeValue("tenant"), "sk": newStringAttributeValue("item")},
					UpdateExpression: "SET #value = :value",
					ExpressionAttributeNames: map[string]string{
						"#value": "value",
					},
					ExpressionAttributeValues: map[string]attributeValue{
						":value": newStringAttributeValue("new"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	schema, exists, err := server.loadTableSchema(ctx, legacySchema.TableName)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, schema.usesOrderedKeyEncoding())
	require.Zero(t, schema.MigratingFromGeneration)

	current, found, err := server.readLogicalItemAt(ctx, schema, map[string]attributeValue{
		"pk": newStringAttributeValue("tenant"),
		"sk": newStringAttributeValue("item"),
	}, snapshotTS(server.coordinator.Clock(), server.store))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, newStringAttributeValue("new"), current.item["value"])
}

// TestDynamoDB_EnsureLegacyTableMigration_RefusesAdminReadOnlyContext
// pins Codex r8 P2 on PR #805: an admin read-only call (tagged via
// withAdminReadOnlyContext) must refuse to trigger Raft writes from
// inside the migration path, even when the table genuinely needs
// migration. This closes the TOCTOU window between AdminScanTable's
// entry-point legacy check and scanItems' internal migration
// trigger.
func TestDynamoDB_EnsureLegacyTableMigration_RefusesAdminReadOnlyContext(t *testing.T) {
	t.Parallel()

	legacySchema, server, st := newLegacyMigrationTestServer(t, false, "S")
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(legacySchema)
	writer.writeItem(legacySchema, map[string]attributeValue{
		"pk": newStringAttributeValue("p"),
		"sk": newStringAttributeValue("s"),
	})

	// Without the admin tag, ensureLegacyTableMigration drives the
	// migration through to completion (control: confirm the table
	// genuinely needs migration in this setup).
	ctx := context.Background()
	require.NoError(t, server.loadAndVerifyNeedsMigration(ctx, legacySchema.TableName, t))

	// With the admin read-only context tag set, the same path
	// MUST refuse with ErrAdminDynamoValidation rather than running
	// the write-path migration.
	adminCtx := withAdminReadOnlyContext(ctx)
	err := server.ensureLegacyTableMigration(adminCtx, legacySchema.TableName)
	require.True(t, errors.Is(err, ErrAdminDynamoValidation),
		"want ErrAdminDynamoValidation from admin read-only migration trigger; got %v", err)

	// The schema must not have been migrated (write-path skipped).
	schema, exists, err := server.loadTableSchema(ctx, legacySchema.TableName)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, schema.needsLegacyKeyMigration(),
		"schema must still need migration after refused admin call (no writes occurred)")
}

// loadAndVerifyNeedsMigration is a tiny test helper: load the
// schema and confirm needsLegacyKeyMigration() == true. Returns
// an error so the caller can require.NoError it.
func (d *DynamoDBServer) loadAndVerifyNeedsMigration(ctx context.Context, tableName string, t *testing.T) error {
	t.Helper()
	schema, exists, err := d.loadTableSchema(ctx, tableName)
	if err != nil {
		return err
	}
	require.True(t, exists, "table must exist")
	require.True(t, schema.needsLegacyKeyMigration(), "table must need migration in this setup")
	return nil
}

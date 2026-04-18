package adapter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type localAdapterCoordinator struct {
	stubAdapterCoordinator
	store store.MVCCStore
}

func newLocalAdapterCoordinator(st store.MVCCStore) *localAdapterCoordinator {
	return &localAdapterCoordinator{
		stubAdapterCoordinator: stubAdapterCoordinator{clock: kv.NewHLC()},
		store:                  st,
	}
}

func (c *localAdapterCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req == nil {
		return &kv.CoordinateResponse{}, nil
	}
	commitTS, err := c.commitTSForRequest(req)
	if err != nil {
		return nil, err
	}
	if err := c.applyElems(ctx, req.Elems, commitTS); err != nil {
		return nil, err
	}
	return &kv.CoordinateResponse{}, nil
}

func (c *localAdapterCoordinator) commitTSForRequest(req *kv.OperationGroup[kv.OP]) (uint64, error) {
	if req == nil {
		return 0, nil
	}
	commitTS := req.CommitTS
	if commitTS == 0 {
		commitTS = c.Clock().Next()
		if req.IsTxn && commitTS <= req.StartTS {
			c.Clock().Observe(req.StartTS)
			commitTS = c.Clock().Next()
		}
	} else {
		c.Clock().Observe(commitTS)
	}
	if req.IsTxn && commitTS <= req.StartTS {
		return 0, kv.ErrTxnCommitTSRequired
	}
	return commitTS, nil
}

func (c *localAdapterCoordinator) applyElems(ctx context.Context, elems []*kv.Elem[kv.OP], commitTS uint64) error {
	for _, elem := range elems {
		if err := c.applyElem(ctx, elem, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func (c *localAdapterCoordinator) applyElem(ctx context.Context, elem *kv.Elem[kv.OP], commitTS uint64) error {
	if elem == nil {
		return nil
	}
	switch elem.Op {
	case kv.Put:
		return c.store.PutAt(ctx, elem.Key, elem.Value, commitTS, 0)
	case kv.Del:
		return c.store.DeleteAt(ctx, elem.Key, commitTS)
	case kv.DelPrefix:
		return c.store.DeletePrefixAt(ctx, elem.Key, nil, commitTS)
	default:
		return nil
	}
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

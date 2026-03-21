package adapter

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestDynamoDB_QueryItems_LegacyJSONItemDoesNotRewriteStorage(t *testing.T) {
	t.Parallel()

	schema, server, st := newStorageFormatMigrationTestServer(t)
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(schema)

	item := map[string]attributeValue{
		"pk":    newStringAttributeValue("tenant"),
		"sk":    newStringAttributeValue("item"),
		"value": newStringAttributeValue("old"),
	}
	itemKey, rawBefore := writeLegacyJSONItemFixture(t, writer, schema, item)

	out, err := server.queryItems(context.Background(), queryInput{
		TableName:              schema.TableName,
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, newStringAttributeValue("old"), out.items[0]["value"])

	rawAfter := mustReadRawValue(t, st, itemKey)
	require.False(t, hasStoredDynamoPrefix(rawBefore, storedDynamoItemProtoPrefix))
	require.False(t, hasStoredDynamoPrefix(rawAfter, storedDynamoItemProtoPrefix))
	require.True(t, bytes.Equal(rawBefore, rawAfter))
}

func TestDynamoDB_UpdateItemWithRetry_RewritesLegacyJSONItemToProto(t *testing.T) {
	t.Parallel()

	schema, server, st := newStorageFormatMigrationTestServer(t)
	writer := newDynamoFixtureWriter(t, st)
	writer.writeSchema(schema)

	item := map[string]attributeValue{
		"pk":    newStringAttributeValue("tenant"),
		"sk":    newStringAttributeValue("item"),
		"value": newStringAttributeValue("old"),
	}
	itemKey, rawBefore := writeLegacyJSONItemFixture(t, writer, schema, item)

	plan, err := server.updateItemWithRetry(context.Background(), updateItemInput{
		TableName:        schema.TableName,
		Key:              map[string]attributeValue{"pk": newStringAttributeValue("tenant"), "sk": newStringAttributeValue("item")},
		UpdateExpression: "SET #value = :value",
		ExpressionAttributeNames: map[string]string{
			"#value": "value",
		},
		ExpressionAttributeValues: map[string]attributeValue{
			":value": newStringAttributeValue("new"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, newStringAttributeValue("new"), plan.next["value"])

	rawAfter := mustReadRawValue(t, st, itemKey)
	require.False(t, hasStoredDynamoPrefix(rawBefore, storedDynamoItemProtoPrefix))
	require.True(t, hasStoredDynamoPrefix(rawAfter, storedDynamoItemProtoPrefix))

	decoded, err := decodeStoredDynamoItem(rawAfter)
	require.NoError(t, err)
	require.Equal(t, newStringAttributeValue("new"), decoded["value"])
}

func TestDynamoDB_EnsureLegacyTableMigration_RewritesLegacyJSONSchemaToProto(t *testing.T) {
	t.Parallel()

	legacySchema, server, st := newLegacyMigrationTestServer(t, false, "S")
	writer := newDynamoFixtureWriter(t, st)

	body, err := json.Marshal(legacySchema)
	require.NoError(t, err)
	writer.put(dynamoTableMetaKey(legacySchema.TableName), body)
	writer.put(dynamoTableGenerationKey(legacySchema.TableName), fmt.Appendf(nil, "%d", legacySchema.Generation))

	require.NoError(t, server.ensureLegacyTableMigration(context.Background(), legacySchema.TableName))

	raw := mustReadRawValue(t, st, dynamoTableMetaKey(legacySchema.TableName))
	require.True(t, hasStoredDynamoPrefix(raw, storedDynamoSchemaProtoPrefix))

	schema, err := decodeStoredDynamoTableSchema(raw)
	require.NoError(t, err)
	require.True(t, schema.usesOrderedKeyEncoding())
	require.Zero(t, schema.MigratingFromGeneration)
	require.Equal(t, uint64(2), schema.Generation)
}

func newStorageFormatMigrationTestServer(t *testing.T) (*dynamoTableSchema, *DynamoDBServer, store.MVCCStore) {
	t.Helper()

	schema := &dynamoTableSchema{
		TableName:          "t",
		Generation:         1,
		KeyEncodingVersion: dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{
			"pk":    "S",
			"sk":    "S",
			"value": "S",
		},
		PrimaryKey: dynamoKeySchema{
			HashKey:  "pk",
			RangeKey: "sk",
		},
	}

	st := store.NewMVCCStore()
	server := NewDynamoDBServer(nil, st, newLocalAdapterCoordinator(st))
	return schema, server, st
}

func writeLegacyJSONItemFixture(
	t *testing.T,
	writer *dynamoFixtureWriter,
	schema *dynamoTableSchema,
	item map[string]attributeValue,
) ([]byte, []byte) {
	t.Helper()

	itemKey, err := schema.itemKeyFromAttributes(item)
	require.NoError(t, err)

	body, err := json.Marshal(item)
	require.NoError(t, err)

	writer.put(itemKey, body)
	gsiKeys, err := schema.gsiEntryKeysForItem(item)
	require.NoError(t, err)
	for _, gsiKey := range gsiKeys {
		writer.put(gsiKey, itemKey)
	}

	return itemKey, body
}

func mustReadRawValue(t *testing.T, st store.MVCCStore, key []byte) []byte {
	t.Helper()

	raw, err := st.GetAt(context.Background(), key, st.LastCommitTS())
	require.NoError(t, err)
	return raw
}

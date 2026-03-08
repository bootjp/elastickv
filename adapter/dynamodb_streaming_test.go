package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type dynamoTrackingStore struct {
	store.MVCCStore
	scanCalls        int
	reverseScanCalls int
	getCalls         int
	scanLimits       []int
	reverseLimits    []int
}

func (s *dynamoTrackingStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.scanCalls++
	s.scanLimits = append(s.scanLimits, limit)
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func (s *dynamoTrackingStore) ReverseScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.reverseScanCalls++
	s.reverseLimits = append(s.reverseLimits, limit)
	return s.MVCCStore.ReverseScanAt(ctx, start, end, limit, ts)
}

func (s *dynamoTrackingStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.getCalls++
	return s.MVCCStore.GetAt(ctx, key, ts)
}

type dynamoFixtureWriter struct {
	t      *testing.T
	store  store.MVCCStore
	nextTS uint64
}

func newDynamoFixtureWriter(t *testing.T, st store.MVCCStore) *dynamoFixtureWriter {
	return &dynamoFixtureWriter{t: t, store: st, nextTS: 1}
}

func (w *dynamoFixtureWriter) put(key []byte, value []byte) {
	w.t.Helper()
	err := w.store.PutAt(context.Background(), key, value, w.nextTS, 0)
	require.NoError(w.t, err)
	w.nextTS++
}

func (w *dynamoFixtureWriter) writeSchema(schema *dynamoTableSchema) {
	w.t.Helper()
	body, err := json.Marshal(schema)
	require.NoError(w.t, err)
	w.put(dynamoTableMetaKey(schema.TableName), body)
	w.put(dynamoTableGenerationKey(schema.TableName), []byte(fmt.Sprintf("%d", schema.Generation)))
}

func (w *dynamoFixtureWriter) writeItem(schema *dynamoTableSchema, item map[string]attributeValue) {
	w.t.Helper()
	itemKey, err := schema.itemKeyFromAttributes(item)
	require.NoError(w.t, err)
	body, err := json.Marshal(item)
	require.NoError(w.t, err)
	w.put(itemKey, body)
	gsiKeys, err := schema.gsiEntryKeysForItem(item)
	require.NoError(w.t, err)
	for _, gsiKey := range gsiKeys {
		w.put(gsiKey, itemKey)
	}
}

func TestDynamoDB_QueryItems_StreamsPrimaryPrefixPages(t *testing.T) {
	t.Parallel()

	schema, server, tracking := newStreamingTestServer(t, false)
	writer := newDynamoFixtureWriter(t, tracking)
	writer.writeSchema(schema)
	for i := 0; i < dynamoScanPageLimit*2+1; i++ {
		writer.writeItem(schema, map[string]attributeValue{
			"pk": newStringAttributeValue("tenant"),
			"sk": newStringAttributeValue(fmt.Sprintf("%04d", i)),
		})
	}

	out, err := server.queryItems(context.Background(), queryInput{
		TableName:              schema.TableName,
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
		Limit: int32Ptr(1),
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, 1, out.scannedCount)
	require.Equal(t, 1, tracking.scanCalls)
	require.Equal(t, []int{2}, tracking.scanLimits)
}

func TestDynamoDB_QueryItems_StreamsNumericPrimaryPages(t *testing.T) {
	t.Parallel()

	schema, server, tracking := newStreamingTestServerWithRangeType(t, false, "N")
	writer := newDynamoFixtureWriter(t, tracking)
	writer.writeSchema(schema)
	for _, seq := range []string{"10", "2", "100"} {
		writer.writeItem(schema, map[string]attributeValue{
			"pk": newStringAttributeValue("tenant"),
			"sk": newNumberAttributeValue(seq),
		})
	}

	out, err := server.queryItems(context.Background(), queryInput{
		TableName:              schema.TableName,
		KeyConditionExpression: "pk = :pk",
		ExpressionAttributeValues: map[string]attributeValue{
			":pk": newStringAttributeValue("tenant"),
		},
		Limit: int32Ptr(1),
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, newNumberAttributeValue("2"), out.items[0]["sk"])
	require.Equal(t, 1, tracking.scanCalls)
	require.Zero(t, tracking.reverseScanCalls)
	require.Equal(t, []int{2}, tracking.scanLimits)
}

func TestDynamoDB_ScanItems_StreamsPrimaryPrefixPages(t *testing.T) {
	t.Parallel()

	schema, server, tracking := newStreamingTestServer(t, false)
	writer := newDynamoFixtureWriter(t, tracking)
	writer.writeSchema(schema)
	for i := 0; i < dynamoScanPageLimit*2+1; i++ {
		writer.writeItem(schema, map[string]attributeValue{
			"pk": newStringAttributeValue(fmt.Sprintf("pk-%04d", i)),
			"sk": newStringAttributeValue("row"),
		})
	}

	out, err := server.scanItems(context.Background(), scanInput{
		TableName: schema.TableName,
		Limit:     int32Ptr(1),
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, 1, out.scannedCount)
	require.Equal(t, 1, tracking.scanCalls)
	require.Equal(t, []int{2}, tracking.scanLimits)
}

func TestDynamoDB_QueryItems_StreamsGSIIndexPages(t *testing.T) {
	t.Parallel()

	schema, server, tracking := newStreamingTestServer(t, true)
	writer := newDynamoFixtureWriter(t, tracking)
	writer.writeSchema(schema)
	for i := 0; i < dynamoScanPageLimit*2+1; i++ {
		writer.writeItem(schema, map[string]attributeValue{
			"pk":     newStringAttributeValue(fmt.Sprintf("pk-%04d", i)),
			"sk":     newStringAttributeValue(fmt.Sprintf("%04d", i)),
			"status": newStringAttributeValue("open"),
		})
	}

	out, err := server.queryItems(context.Background(), queryInput{
		TableName:              schema.TableName,
		IndexName:              "status-index",
		KeyConditionExpression: "status = :status",
		ExpressionAttributeValues: map[string]attributeValue{
			":status": newStringAttributeValue("open"),
		},
		Limit: int32Ptr(1),
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, 1, out.scannedCount)
	require.Equal(t, 1, tracking.scanCalls)
	require.Equal(t, []int{2}, tracking.scanLimits)
	require.Equal(t, 3, tracking.getCalls)
}

func TestDynamoDB_QueryItems_StreamsReverseGSIIndexPages(t *testing.T) {
	t.Parallel()

	schema, server, tracking := newStreamingTestServer(t, true)
	writer := newDynamoFixtureWriter(t, tracking)
	writer.writeSchema(schema)
	for i := 0; i < dynamoScanPageLimit*2+1; i++ {
		writer.writeItem(schema, map[string]attributeValue{
			"pk":     newStringAttributeValue(fmt.Sprintf("pk-%04d", i)),
			"sk":     newStringAttributeValue(fmt.Sprintf("%04d", i)),
			"status": newStringAttributeValue("open"),
		})
	}

	out, err := server.queryItems(context.Background(), queryInput{
		TableName:              schema.TableName,
		IndexName:              "status-index",
		KeyConditionExpression: "status = :status",
		ExpressionAttributeValues: map[string]attributeValue{
			":status": newStringAttributeValue("open"),
		},
		Limit:            int32Ptr(1),
		ScanIndexForward: boolPtr(false),
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, newStringAttributeValue(fmt.Sprintf("%04d", dynamoScanPageLimit*2)), out.items[0]["sk"])
	require.Equal(t, 1, tracking.reverseScanCalls)
	require.Zero(t, tracking.scanCalls)
	require.Equal(t, []int{2}, tracking.reverseLimits)
	require.Equal(t, 3, tracking.getCalls)
}

func TestDynamoDB_ScanItems_StreamsGSIIndexPages(t *testing.T) {
	t.Parallel()

	schema, server, tracking := newStreamingTestServer(t, true)
	writer := newDynamoFixtureWriter(t, tracking)
	writer.writeSchema(schema)
	for i := 0; i < dynamoScanPageLimit*2+1; i++ {
		writer.writeItem(schema, map[string]attributeValue{
			"pk":     newStringAttributeValue(fmt.Sprintf("pk-%04d", i)),
			"sk":     newStringAttributeValue(fmt.Sprintf("%04d", i)),
			"status": newStringAttributeValue("open"),
		})
	}

	out, err := server.scanItems(context.Background(), scanInput{
		TableName: schema.TableName,
		IndexName: "status-index",
		Limit:     int32Ptr(1),
	})
	require.NoError(t, err)
	require.Len(t, out.items, 1)
	require.Equal(t, 1, out.scannedCount)
	require.Equal(t, 1, tracking.scanCalls)
	require.Equal(t, []int{2}, tracking.scanLimits)
	require.Equal(t, 3, tracking.getCalls)
}

func newStreamingTestServer(t *testing.T, withGSI bool) (*dynamoTableSchema, *DynamoDBServer, *dynamoTrackingStore) {
	t.Helper()
	return newStreamingTestServerWithRangeType(t, withGSI, "S")
}

func newStreamingTestServerWithRangeType(
	t *testing.T,
	withGSI bool,
	rangeType string,
) (*dynamoTableSchema, *DynamoDBServer, *dynamoTrackingStore) {
	t.Helper()

	schema := &dynamoTableSchema{
		TableName:          "t",
		Generation:         1,
		KeyEncodingVersion: dynamoOrderedKeyEncodingV2,
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

	tracking := &dynamoTrackingStore{MVCCStore: store.NewMVCCStore()}
	server := NewDynamoDBServer(nil, tracking, &stubAdapterCoordinator{})
	return schema, server, tracking
}

func int32Ptr(v int32) *int32 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func newNumberAttributeValue(v string) attributeValue {
	return attributeValue{N: &v}
}

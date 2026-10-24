package adapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// staleTransactGetStore models a new leader whose best-effort watermark still
// trails a previously acknowledged write. A ReadIndex-fenced floor sees it.
type staleTransactGetStore struct {
	store.MVCCStore
	floor    uint64
	floorErr error
}

func (s *staleTransactGetStore) GlobalLastCommitTS(context.Context) uint64 {
	return 2
}

func (s *staleTransactGetStore) CommittedTimestampFloorForKeys(_ context.Context, _ [][]byte) (uint64, error) {
	return s.floor, s.floorErr
}

func TestDynamoDB_TransactGetItems_FailsClosedWhenSnapshotFenceUnavailable(t *testing.T) {
	t.Parallel()
	underlying := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, underlying)
	schema := &dynamoTableSchema{
		TableName:            "t",
		Generation:           1,
		KeyEncodingVersion:   dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{"pk": "S"},
		PrimaryKey:           dynamoKeySchema{HashKey: "pk"},
	}
	writer.writeSchema(schema)
	writer.writeItem(schema, map[string]attributeValue{"pk": newStringAttributeValue("k")})
	server := NewDynamoDBServer(nil, &staleTransactGetStore{
		MVCCStore: underlying,
		floorErr:  errors.New("leader changed"),
	}, &stubAdapterCoordinator{})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(`{"TransactItems":[{"Get":{"TableName":"t","Key":{"pk":{"S":"k"}}}}]}`))
	rec := httptest.NewRecorder()
	server.transactGetItems(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "leader changed")
}

func TestDynamoDB_TransactGetItems_FencesSnapshotAfterLeaderChange(t *testing.T) {
	t.Parallel()
	underlying := store.NewMVCCStore()
	writer := newDynamoFixtureWriter(t, underlying)
	schema := &dynamoTableSchema{
		TableName:            "t",
		Generation:           1,
		KeyEncodingVersion:   dynamoOrderedKeyEncodingV2,
		AttributeDefinitions: map[string]string{"pk": "S"},
		PrimaryKey:           dynamoKeySchema{HashKey: "pk"},
	}
	writer.writeSchema(schema)
	writer.writeItem(schema, map[string]attributeValue{
		"pk": newStringAttributeValue("k"),
		"v":  newStringAttributeValue("fresh"),
	})

	server := NewDynamoDBServer(nil, &staleTransactGetStore{MVCCStore: underlying, floor: underlying.LastCommitTS()}, &stubAdapterCoordinator{})
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(`{"TransactItems":[{"Get":{"TableName":"t","Key":{"pk":{"S":"k"}}}}]}`))
	rec := httptest.NewRecorder()
	server.transactGetItems(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"fresh"`)
}

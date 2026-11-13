package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type dynamoReadMarkerCoordinator struct {
	stubAdapterCoordinator
	marks []uint64
}

func (c *dynamoReadMarkerCoordinator) MarkUnattributableClientRead(routeID uint64) {
	c.marks = append(c.marks, routeID)
}

func TestDynamoReadObservationMarksServedReads(t *testing.T) {
	coord := &dynamoReadMarkerCoordinator{}
	server := &DynamoDBServer{
		coordinator: coord,
		targetHandlers: map[string]func(http.ResponseWriter, *http.Request){
			getItemTarget: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) },
			putItemTarget: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) },
		},
	}
	for _, target := range []string{getItemTarget, putItemTarget} {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", target)
		server.handle(httptest.NewRecorder(), req)
	}
	require.Equal(t, []uint64{0}, coord.marks)
}

func TestDynamoReadObservationAttributesFailedPointReads(t *testing.T) {
	coord := &redisReadObservationCoordinator{}
	d := &DynamoDBServer{coordinator: coord, store: store.NewMVCCStore()}
	ctx := context.Background()
	_, found, err := d.loadTableSchemaAt(ctx, "table", 1)
	require.NoError(t, err)
	require.False(t, found)
	_, err = d.loadTableGenerationAt(ctx, "table", 1)
	require.NoError(t, err)
	_, found, err = d.readItemAtKeyAt(ctx, []byte("item"), 1)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, [][]byte{
		dynamoTableMetaKey("table"), dynamoTableGenerationKey("table"), []byte("item"),
	}, coord.keys)
}

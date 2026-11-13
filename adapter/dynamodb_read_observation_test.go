package adapter

import (
	"net/http"
	"net/http/httptest"
	"testing"

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

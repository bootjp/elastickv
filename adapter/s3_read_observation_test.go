package adapter

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3ReadObservationMarksServedReads(t *testing.T) {
	coord := &dynamoReadMarkerCoordinator{}
	coord.leaderSet = true
	coord.leader = true
	server := NewS3Server(nil, "", store.NewMVCCStore(), coord, nil)
	t.Cleanup(server.cancelCleanups)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req := httptest.NewRequest(method, "/", nil)
		server.handle(httptest.NewRecorder(), req)
	}
	require.Equal(t, []uint64{0}, coord.marks)
}

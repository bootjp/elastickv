package adapter

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSQSReadObservationMarksJSONAndQueryStorageAccess(t *testing.T) {
	coord := &dynamoReadMarkerCoordinator{}
	server := NewSQSServer(nil, store.NewMVCCStore(), coord)
	t.Cleanup(server.Stop)
	server.targetHandlers[sqsGetQueueAttributesTarget] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	server.targetHandlers[sqsSendMessageTarget] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	server.targetHandlers[sqsDeleteMessageTarget] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
	for _, target := range []string{sqsGetQueueAttributesTarget, sqsSendMessageTarget, sqsDeleteMessageTarget} {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", target)
		server.handle(httptest.NewRecorder(), req)
	}
	require.Equal(t, []uint64{0, 0, 0}, coord.marks)

	server.handle(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/?Action=ReceiveMessage", nil))
	server.handle(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/?Action=DeleteMessage", nil))
	require.Equal(t, []uint64{0, 0, 0, 0, 0}, coord.marks)
}

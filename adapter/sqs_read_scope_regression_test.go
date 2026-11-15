package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSQSReceiveRetiresPreludeAndPollingSnapshots(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	server := newDelayTestServer(t, base)
	createDelayTestQueue(t, server, 1, 0)
	observed := &requestScopeStore{MVCCStore: server.store}
	server.store = observed
	tracker := kv.NewActiveTimestampTracker()
	server.readTracker = tracker
	parent := kv.NewReadRequest(t.Context(), server.coordinator, server.store, tracker)
	defer parent.Close()
	messages, err := server.receiveMessageCore(parent.Context(), fifoDelayTestQueue, sqsReceiveMessageInput{})
	require.NoError(t, err)
	require.Empty(t, messages)
	require.Zero(t, tracker.Oldest(), "receive prelude must not retain a snapshot while polling")
	assertRetiredBlockingReadScopes(t, parent, observed.observed)
	observed.observed = nil
	for i := range 8 {
		require.NoError(t, base.PutAt(parent.Context(), []byte("unrelated"), []byte{byte(i)}, base.LastCommitTS()+1, 0))
		messages, err = server.scanAndDeliverOnce(parent.Context(), fifoDelayTestQueue, sqsReceiveOptions{Max: 1})
		require.NoError(t, err)
		require.Empty(t, messages)
		require.Zero(t, tracker.Oldest())
		assertRetiredBlockingReadScopes(t, parent, observed.observed)
		observed.observed = nil
		require.NoError(t, parent.Context().Err())
	}
}

package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestRedisLeaderRoutedListRangeCapability(t *testing.T) {
	server, owner := redisPropagationServer(t)
	server.coordinator = &readScopeSingleGroupCoordinator{Coordinator: server.coordinator}
	server.store = kv.NewLeaderRoutedStore(server.store, server.coordinator)
	redisPropagationCommand(t, server, cmdRPush, "list", "a", "b")
	reply := redisPropagationCommand(t, server, cmdLRange, "list", "0", "-1")
	require.Equal(t, "*2\r\n$1\r\na\r\n$1\r\nb\r\n", reply.raw.String())
	request := kv.NewReadRequest(owner.Context(), server.coordinator, server.store, nil)
	defer request.Close()
	snapshot, err := request.LatestTargets(request.Context(), "list range", []kv.ReadFenceTarget{{GroupID: 1, Key: []byte("list")}})
	require.NoError(t, err)
	values, err := server.rangeListAt(request.Context(), []byte("list"), []byte("0"), []byte("-1"), snapshot.Timestamp())
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, values)
}

func TestRedisXReadRetiresDollarAndPollingSnapshots(t *testing.T) {
	server, owner := redisPropagationServer(t)
	redisPropagationCommand(t, server, cmdXAdd, "stream", "1-0", "field", "value")
	observed := &requestScopeStore{MVCCStore: server.store}
	server.store = observed
	tracker := kv.NewActiveTimestampTracker()
	server.readTracker = tracker
	parent := kv.NewReadRequest(owner.Context(), server.coordinator, server.store, tracker)
	defer parent.Close()
	req := xreadRequest{keys: [][]byte{[]byte("stream")}, afterIDs: []string{"$"}, count: 1}
	require.NoError(t, server.resolveXReadAfterIDs(parent.Context(), &req))
	require.Equal(t, "1-0", req.afterIDs[0])
	require.Zero(t, tracker.Oldest(), "completed dollar resolution must release its snapshot")
	assertRetiredXReadScopes(t, parent, observed.observed)
	observed.observed = nil
	for i := range 16 {
		watermark := server.store.LastCommitTS() + 1
		require.NoError(t, server.store.PutAt(parent.Context(), []byte("unrelated"), []byte{byte(i)}, watermark, 0))
		results, err := server.xreadOnce(parent.Context(), req)
		require.NoError(t, err)
		require.Empty(t, results)
		assertRetiredXReadScopes(t, parent, observed.observed)
		observed.observed = nil
		require.Zero(t, tracker.Oldest(), "completed poll %d must release its snapshot", i)
		require.NoError(t, parent.Context().Err(), "poll retirement must preserve the command owner")
	}
	req.afterIDs[0] = "invalid"
	_, err := server.xreadOnce(parent.Context(), req)
	require.Error(t, err)
	require.Zero(t, tracker.Oldest(), "failed polling reads must release their snapshots")
	assertRetiredXReadScopes(t, parent, observed.observed)
	require.NoError(t, parent.Context().Err())

}

type readScopeSingleGroupCoordinator struct{ kv.Coordinator }

func (*readScopeSingleGroupCoordinator) EngineGroupIDForKey([]byte) uint64 { return 1 }

func assertRetiredXReadScopes(t *testing.T, parent *kv.ReadRequest, scopes []*kv.ReadRequest) {
	t.Helper()
	require.NotEmpty(t, scopes)
	for _, scope := range scopes {
		require.NotSame(t, parent, scope)
		require.ErrorIs(t, scope.Context().Err(), context.Canceled)
		_, err := scope.Issued(scope.Context(), 1)
		require.ErrorIs(t, err, kv.ErrReadRequestClosed)
	}
}

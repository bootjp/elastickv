package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type physicalScopeTestStore struct {
	store.MVCCStore
	calls int
}

func (s *physicalScopeTestStore) ScanAtPhysicalLimit(context.Context, []byte, []byte, int, int, uint64) ([]*store.KVPair, bool, error) {
	s.calls++
	return nil, false, nil
}
func (s *physicalScopeTestStore) ReverseScanAtPhysicalLimit(context.Context, []byte, []byte, int, int, uint64) ([]*store.KVPair, bool, error) {
	s.calls++
	return nil, false, nil
}
func TestLuaPhysicalScansRequireIssuedLiveCapabilities(t *testing.T) {
	base := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	st := &physicalScopeTestStore{MVCCStore: base}
	request := kv.NewReadRequest(t.Context(), newLocalAdapterCoordinator(st), st, nil)
	defer request.Close()
	script := &luaScriptContext{ctx: request.Context(), startTS: 42, server: &RedisServer{store: st}}
	_, _, err := script.scanAtPhysicalLimit(nil, nil, 10)
	require.ErrorIs(t, err, kv.ErrReadSnapshotUnissued)
	_, _, err = script.reverseScanAtPhysicalLimit(nil, nil, 10)
	require.ErrorIs(t, err, kv.ErrReadSnapshotUnissued)
	require.Zero(t, st.calls)
	snapshot, err := request.Latest(request.Context(), "lua")
	require.NoError(t, err)
	script.startTS = snapshot.Timestamp()
	_, _, err = script.scanAtPhysicalLimit(nil, nil, 10)
	require.NoError(t, err)
	_, _, err = script.reverseScanAtPhysicalLimit(nil, nil, 10)
	require.NoError(t, err)
	request.Close()
	_, _, err = script.scanAtPhysicalLimit(nil, nil, 10)
	require.ErrorIs(t, err, kv.ErrReadRequestClosed)
	_, _, err = script.reverseScanAtPhysicalLimit(nil, nil, 10)
	require.ErrorIs(t, err, kv.ErrReadRequestClosed)
	require.Equal(t, 2, st.calls)
}

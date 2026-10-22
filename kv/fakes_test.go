package kv

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/stretchr/testify/require"
)

// nopCoordinator is the in-package counterpart of kvtest.NopCoordinator.
type nopCoordinator struct {
	once  sync.Once
	clock *HLC
}

func (*nopCoordinator) Dispatch(context.Context, *OperationGroup[OP]) (*CoordinateResponse, error) {
	return &CoordinateResponse{}, nil
}
func (*nopCoordinator) IsLeader() bool                                   { return true }
func (*nopCoordinator) VerifyLeader(context.Context) error               { return nil }
func (*nopCoordinator) LinearizableRead(context.Context) (uint64, error) { return 0, nil }
func (*nopCoordinator) RaftLeader() string                               { return "local" }
func (*nopCoordinator) IsLeaderForKey([]byte) bool                       { return true }
func (*nopCoordinator) VerifyLeaderForKey(context.Context, []byte) error { return nil }
func (*nopCoordinator) RaftLeaderForKey([]byte) string                   { return "local" }
func (c *nopCoordinator) Clock() *HLC {
	c.once.Do(func() { c.clock = NewHLC() })
	return c.clock
}

type testEngine = raftenginetest.StubEngine

var _ Coordinator = (*nopCoordinator)(nil)

func TestSharedFakesPreserveFallbacks(t *testing.T) {
	c := &nopCoordinator{}
	require.True(t, c.IsLeader())
	require.Same(t, c.Clock(), c.Clock())
	_, optional := any(c).(LeaseReadableCoordinator)
	require.False(t, optional)
	engine := &testEngine{StaticLeaderView: raftenginetest.StaticLeaderView{StateValue: raftengine.StateLeader, ReadIndex: 7}}
	idx, err := engine.LinearizableRead(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(7), idx)
	_, optional = any(engine).(raftengine.LeaseProvider)
	require.False(t, optional)
}

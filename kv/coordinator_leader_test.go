package kv

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCoordinateVerifyLeader_LeaderReturnsNil(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "coord-leader", NewKvFSM(st))
	t.Cleanup(stop)

	c := NewCoordinator(&stubTransactional{}, r)
	require.NoError(t, c.VerifyLeader())
}

func TestCoordinateVerifyLeader_NilRaftReturnsLeaderNotFound(t *testing.T) {
	t.Parallel()

	c := &Coordinate{}
	require.ErrorIs(t, c.VerifyLeader(), ErrLeaderNotFound)
}

package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCoordinateVerifyLeader_LeaderReturnsNil(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	r, stop := newSingleRaft(t, "coord-leader", NewKvFSMWithHLC(st, NewHLC()))
	t.Cleanup(stop)

	c := NewCoordinatorWithEngine(&stubTransactional{}, r)
	require.NoError(t, c.VerifyLeader(context.Background()))
}

func TestCoordinateVerifyLeader_NilRaftReturnsLeaderNotFound(t *testing.T) {
	t.Parallel()

	c := &Coordinate{}
	require.ErrorIs(t, c.VerifyLeader(context.Background()), ErrLeaderNotFound)
}

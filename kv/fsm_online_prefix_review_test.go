package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestOnlineMixedPointPrefixBatch(t *testing.T) {
	for _, prefix := range []string{"a/", "n"} {
		t.Run(prefix, func(t *testing.T) {
			testOnlineMixedPointPrefixBatch(t, prefix)
		})
	}
}

func testOnlineMixedPointPrefixBatch(t *testing.T, prefix string) {
	t.Helper()
	st := g17OpenStore(t, t.TempDir())
	require.NoError(t, st.PutAt(t.Context(), []byte("a/old"), []byte("old"), 10, 0))
	f := g17FSM(t, st, WithRouteHistory(nil, 1))
	installSourceOwnershipForTest(t, f, 0)
	session := onlineRegressionSession()
	session.Identity.JobID, session.Identity.Index = 11, 12
	session.StartIndex, session.LastIndex = 12, 12
	applier, ok := st.(store.RaftEntryApplier)
	require.True(t, ok)
	require.NoError(t, applier.ApplyRaftEntry(t.Context(), 12, func(ctx context.Context) error {
		return store.StartOnlineMigrationSession(ctx, st, session)
	}))
	command, err := proto.Marshal(&pb.RaftCommand{Requests: []*pb.Request{
		{Ts: 20, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("point")}}},
		{Ts: 21, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("a/new"), Value: []byte("new")}}},
		{Ts: 22, ObservedRouteVersion: EncodeObservedRouteVersion(1), Mutations: []*pb.Mutation{{Op: pb.Op_DEL_PREFIX, Key: []byte(prefix)}}},
	}})
	require.NoError(t, err)
	f.SetApplyIndex(13)
	response := f.Apply(command)
	require.NoError(t, haltApplyOf(response))
	require.Nil(t, response)
	index, _ := g17AppliedIndex(t, st)
	require.Equal(t, uint64(13), index)
	saved, err := store.ReadOnlineMigrationSession(t.Context(), st, 11)
	require.NoError(t, err)
	if prefix == "a/" {
		require.Empty(t, saved.Invalid)
		require.Equal(t, uint64(1), saved.Sequence)
		frame, readErr := store.ReadOnlineMigrationFrame(t.Context(), st, saved, 1)
		require.NoError(t, readErr)
		require.Len(t, frame.Outcomes, 3)
		require.Len(t, frame.Effects, 1)
		require.Equal(t, []byte("n"), frame.Effects[0].Key)
		value, readErr := st.GetAt(t.Context(), []byte("n"), 30)
		require.NoError(t, readErr)
		require.Equal(t, []byte("point"), value)
		for _, key := range []string{"a/old", "a/new"} {
			_, readErr = st.GetAt(t.Context(), []byte(key), 30)
			require.ErrorIs(t, readErr, store.ErrKeyNotFound)
		}
		f.SetApplyIndex(13)
		require.Nil(t, f.Apply(command), "replay preserves the complete atomic result")
	} else {
		require.NotEmpty(t, saved.Invalid)
		require.Zero(t, saved.Sequence)
		_, readErr := st.GetAt(t.Context(), []byte("n"), 30)
		require.ErrorIs(t, readErr, store.ErrKeyNotFound)
	}
}

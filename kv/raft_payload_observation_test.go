package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftPayloadWrapObservationRejectsReplacement(t *testing.T) {
	t.Parallel()
	g := &ShardGroup{}
	wrap := func(p []byte) ([]byte, error) { return p, nil }
	first := g.SetObservedRaftPayloadWrap(wrap)
	require.True(t, g.HasRaftPayloadWrap(first))
	g.SetRaftPayloadWrap(nil)
	require.False(t, g.HasRaftPayloadWrap(first))
	second := g.SetObservedRaftPayloadWrap(wrap)
	require.NotEqual(t, first, second)
	require.False(t, g.HasRaftPayloadWrap(first))
	require.True(t, g.HasRaftPayloadWrap(second))
	g.SetRaftPayloadWrap(wrap)
	require.False(t, g.HasRaftPayloadWrap(second))
	require.False(t, g.HasRaftPayloadWrap(RaftPayloadWrapObservation{}))
}

func TestRaftPayloadWrapInvalidationPrecedesActualPublication(t *testing.T) {
	t.Parallel()
	g := &ShardGroup{}
	wrap := func(p []byte) ([]byte, error) { return p, nil }
	first := g.SetObservedRaftPayloadWrap(wrap)
	calls := 0
	g.SetRaftPayloadWrapInvalidator(func() func() {
		calls++
		require.True(t, g.HasRaftPayloadWrap(first), "old wrapper must remain installed until callback closes authority")
		return func() { require.False(t, g.HasRaftPayloadWrap(first)) }
	})
	second := g.SetObservedRaftPayloadWrap(wrap)
	require.Equal(t, 1, calls)
	require.NotEqual(t, first.Identity(), second.Identity())
	g.SetRaftPayloadWrapInvalidator(func() func() {
		calls++
		require.True(t, g.HasRaftPayloadWrap(second))
		return func() { require.Nil(t, g.RaftPayloadWrap()) }
	})
	g.SetRaftPayloadWrap(nil)
	require.Equal(t, 2, calls)
	require.Nil(t, g.RaftPayloadWrap())
}

func TestRaftPayloadMutationCannotBeReusedAfterSettlement(t *testing.T) {
	t.Parallel()
	g := &ShardGroup{}
	wrap := func(p []byte) ([]byte, error) { return p, nil }
	mutation := BeginRaftPayloadWrapMutation(func() func() { return func() {} })
	first := g.SetObservedRaftPayloadWrapInMutation(wrap, mutation)
	mutation.Close()
	require.Panics(t, func() { g.SetObservedRaftPayloadWrapInMutation(nil, mutation) })
	require.True(t, g.HasRaftPayloadWrap(first))
	g.SetRaftPayloadWrapInvalidator(func() func() { return nil })
	require.Panics(t, func() { g.SetRaftPayloadWrap(nil) })
	require.True(t, g.HasRaftPayloadWrap(first))
}

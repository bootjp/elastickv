package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeObservedRouteVersionZeroGatedForRollingUpgrade(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(0), EncodeObservedRouteVersion(0))

	decoded, pinned := DecodeObservedRouteVersion(0)
	require.Equal(t, uint64(0), decoded)
	require.False(t, pinned)

	decoded, pinned = DecodeObservedRouteVersion(ObservedRouteVersionZero)
	require.Equal(t, uint64(0), decoded)
	require.True(t, pinned)
}

func TestEncodeObservedRouteVersionNonZeroPassesThrough(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(7), EncodeObservedRouteVersion(7))
	decoded, pinned := DecodeObservedRouteVersion(7)
	require.Equal(t, uint64(7), decoded)
	require.True(t, pinned)
}

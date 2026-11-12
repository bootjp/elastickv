package main

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/stretchr/testify/require"
)

func TestDurationToTicks(t *testing.T) {
	t.Parallel()

	t.Run("zero tick returns min", func(t *testing.T) {
		require.Equal(t, 5, durationToTicks(200*time.Millisecond, 0, 5))
	})

	t.Run("exact division", func(t *testing.T) {
		require.Equal(t, 20, durationToTicks(200*time.Millisecond, 10*time.Millisecond, 1))
	})

	t.Run("remainder rounds up", func(t *testing.T) {
		require.Equal(t, 21, durationToTicks(205*time.Millisecond, 10*time.Millisecond, 1))
	})

	t.Run("below min returns min", func(t *testing.T) {
		require.Equal(t, 10, durationToTicks(50*time.Millisecond, 10*time.Millisecond, 10))
	})
}

func TestNewRaftFactory_UnsupportedEngine(t *testing.T) {
	t.Parallel()
	_, err := newRaftFactory(noderuntime.RaftEngineType("unknown"), nil)
	require.ErrorIs(t, err, noderuntime.ErrUnsupportedRaftEngine)
}

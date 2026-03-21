package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActiveTimestampTrackerOldest(t *testing.T) {
	tracker := NewActiveTimestampTracker()

	first := tracker.Pin(30)
	second := tracker.Pin(20)
	third := tracker.Pin(40)
	defer first.Release()
	defer second.Release()
	defer third.Release()

	require.Equal(t, uint64(20), tracker.Oldest())

	second.Release()
	require.Equal(t, uint64(30), tracker.Oldest())
}

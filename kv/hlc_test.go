package kv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHLCNextIsMonotonic(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	last := h.Next()
	for i := 0; i < 1000; i++ {
		next := h.Next()
		require.Greater(t, next, last)
		last = next
	}
}

func TestHLCObserveDoesNotRegress(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	base := h.Next()

	h.Observe(base - 1)
	require.Equal(t, base, h.Current())

	higher := base + 100
	h.Observe(higher)
	require.Equal(t, higher, h.Current())
}

func TestHLCNextAfterObserveLogicalOverflow(t *testing.T) {
	t.Parallel()

	h := NewHLC()
	wall := nonNegativeUint64(time.Now().UnixMilli())
	observed := (wall << hlcLogicalBits) | hlcLogicalMask
	h.Observe(observed)

	next := h.Next()
	require.Greater(t, next, observed)
}

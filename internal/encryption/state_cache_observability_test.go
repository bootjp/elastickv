package encryption_test

import (
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/stretchr/testify/require"
)

// TestStateCacheMirrorsRaftSlotAndSidecarIndex pins the §9.2
// observability mirrors. They are refreshed by the same
// RefreshFromSidecar call that maintains the decision mirrors, so a
// refresh that updated one and not the other would let the metrics
// report stale sidecar state indefinitely.
func TestStateCacheMirrorsRaftSlotAndSidecarIndex(t *testing.T) {
	t.Parallel()

	cache := encryption.NewStateCache()

	// Pre-bootstrap posture.
	id, ok := cache.ActiveRaftKeyID()
	require.Zero(t, id)
	require.False(t, ok)
	require.Zero(t, cache.SidecarRaftAppliedIndex())

	cache.RefreshFromSidecar(&encryption.Sidecar{
		Version:          1,
		RaftAppliedIndex: 9182,
		Active:           encryption.ActiveKeys{Storage: 7, Raft: 8},
	})

	id, ok = cache.ActiveRaftKeyID()
	require.Equal(t, uint32(8), id)
	require.True(t, ok)
	require.Equal(t, uint64(9182), cache.SidecarRaftAppliedIndex())

	// The storage mirror must still track its own slot.
	storageID, ok := cache.ActiveStorageKeyID()
	require.Equal(t, uint32(7), storageID)
	require.True(t, ok)

	// A later refresh must advance the index, not latch it.
	cache.RefreshFromSidecar(&encryption.Sidecar{
		Version:          1,
		RaftAppliedIndex: 9200,
		Active:           encryption.ActiveKeys{Storage: 7, Raft: 8},
	})
	require.Equal(t, uint64(9200), cache.SidecarRaftAppliedIndex())
}

func TestStateCacheObservabilityMirrorsAreNilSafe(t *testing.T) {
	t.Parallel()

	var cache *encryption.StateCache
	id, ok := cache.ActiveRaftKeyID()
	require.Zero(t, id)
	require.False(t, ok)
	require.Zero(t, cache.SidecarRaftAppliedIndex())
}

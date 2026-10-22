package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// forEachMVCCStore runs the same contract against the test double and the
// production backend. Each call to newStore owns an independent fixture.
func forEachMVCCStore(t *testing.T, test func(t *testing.T, newStore func(*testing.T) MVCCStore)) {
	t.Helper()
	backends := []struct {
		name string
		open func(*testing.T) MVCCStore
	}{
		{"memory", func(t *testing.T) MVCCStore {
			t.Helper()
			s := NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			return s
		}},
		{"pebble", func(t *testing.T) MVCCStore {
			t.Helper()
			s, err := NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			return s
		}},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) { test(t, backend.open) })
	}
}

func countConformanceVersions(t *testing.T, s MVCCStore) int {
	t.Helper()
	if memory, ok := s.(*mvccStore); ok {
		memory.mtx.RLock()
		defer memory.mtx.RUnlock()
		count := 0
		for _, value := range memory.tree.Values() {
			versions, ok := value.([]VersionedValue)
			require.True(t, ok)
			count += len(versions)
		}
		return count
	}
	return countPebbleVersions(t, s)
}

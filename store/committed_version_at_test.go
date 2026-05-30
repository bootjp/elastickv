package store

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// committedVersionAtCase drives the exact-timestamp existence probe used by
// the one-phase transaction idempotency design (option 2). The same table
// runs against both the in-memory mvccStore and the Pebble-backed store so
// their semantics stay identical.
func runCommittedVersionAtSuite(t *testing.T, newStore func(t *testing.T) MVCCStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("exact hit on a put version", func(t *testing.T) {
		st := newStore(t)
		require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), 100, 0))

		ok, err := st.CommittedVersionAt(ctx, []byte("k"), 100)
		require.NoError(t, err)
		require.True(t, ok, "version committed at exactly 100 must be found")
	})

	t.Run("miss on an absent key", func(t *testing.T) {
		st := newStore(t)
		ok, err := st.CommittedVersionAt(ctx, []byte("absent"), 100)
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("exactness: a newer version does not satisfy an earlier ts", func(t *testing.T) {
		// This is the load-bearing case. A loose `latestTS >= ts` check
		// (as applyCommitWithIdempotencyFallback uses) would report true
		// for ts=200 because 300 >= 200, misclassifying a different txn's
		// commit as our own prior attempt. The exact probe must not.
		st := newStore(t)
		require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 100, 0))
		require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v3"), 300, 0))

		got100, err := st.CommittedVersionAt(ctx, []byte("k"), 100)
		require.NoError(t, err)
		require.True(t, got100, "version at exactly 100 exists")

		got200, err := st.CommittedVersionAt(ctx, []byte("k"), 200)
		require.NoError(t, err)
		require.False(t, got200, "no version at exactly 200; a >=300 match must not leak through")

		got300, err := st.CommittedVersionAt(ctx, []byte("k"), 300)
		require.NoError(t, err)
		require.True(t, got300, "version at exactly 300 exists")
	})

	t.Run("tombstone counts as a landed version", func(t *testing.T) {
		// The previous attempt landed even if it committed a delete; the
		// probe must report present so the retry deduplicates rather than
		// re-applying.
		st := newStore(t)
		require.NoError(t, st.DeleteAt(ctx, []byte("k"), 100))

		ok, err := st.CommittedVersionAt(ctx, []byte("k"), 100)
		require.NoError(t, err)
		require.True(t, ok, "a tombstone committed at exactly 100 must count as landed")
	})

	t.Run("different key isolation", func(t *testing.T) {
		st := newStore(t)
		require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("v"), 100, 0))

		ok, err := st.CommittedVersionAt(ctx, []byte("b"), 100)
		require.NoError(t, err)
		require.False(t, ok, "a version on key a must not satisfy a probe on key b")
	})
}

func TestCommittedVersionAt_MVCCStore(t *testing.T) {
	runCommittedVersionAtSuite(t, func(t *testing.T) MVCCStore {
		t.Helper()
		return NewMVCCStore()
	})
}

func TestCommittedVersionAt_PebbleStore(t *testing.T) {
	runCommittedVersionAtSuite(t, func(t *testing.T) MVCCStore {
		t.Helper()
		dir, err := os.MkdirTemp("", "committed-version-at")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		s, err := NewPebbleStore(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

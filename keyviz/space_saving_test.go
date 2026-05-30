package keyviz

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestSpaceSavingTracksUpToCapacity(t *testing.T) {
	t.Parallel()
	s := newSpaceSaving(4)
	for _, k := range [][]byte{{0x01}, {0x02}, {0x03}, {0x04}} {
		s.observe(k)
	}
	require.Equal(t, 4, s.len())
	snap := s.snapshot()
	require.Len(t, snap, 4)
	for _, e := range snap {
		require.Equal(t, uint64(1), e.Count)
	}
}

func TestSpaceSavingIncrementsExisting(t *testing.T) {
	t.Parallel()
	s := newSpaceSaving(4)
	for i := 0; i < 5; i++ {
		s.observe([]byte{0xAA})
	}
	require.Equal(t, 1, s.len())
	require.Equal(t, uint64(5), s.snapshot()[0].Count)
}

func TestSpaceSavingEvictsMinAndIncrements(t *testing.T) {
	t.Parallel()
	s := newSpaceSaving(2)
	s.observe([]byte("a"))   // a:1
	s.observe([]byte("b"))   // b:1
	for i := 0; i < 5; i++ { // b:6
		s.observe([]byte("b"))
	}
	// New key c evicts the minimum (a:1) and takes count = 1 + 1 = 2.
	s.observe([]byte("c"))
	require.Equal(t, 2, s.len())
	got := map[string]uint64{}
	for _, e := range s.snapshot() {
		got[string(e.Key)] = e.Count
	}
	require.Equal(t, uint64(6), got["b"])
	require.Equal(t, uint64(2), got["c"], "Space-Saving overestimate-on-insert: min_counter+1")
	require.NotContains(t, got, "a", "evicted entry must be gone")
}

func TestSpaceSavingResetClearsEntries(t *testing.T) {
	t.Parallel()
	s := newSpaceSaving(4)
	for _, k := range [][]byte{{0x01}, {0x02}, {0x03}} {
		s.observe(k)
	}
	s.reset()
	require.Equal(t, 0, s.len())
	require.Nil(t, s.snapshot())
	// Capacity preserved after reset.
	for _, k := range [][]byte{{0x10}, {0x11}, {0x12}, {0x13}, {0x14}} {
		s.observe(k)
	}
	require.Equal(t, 4, s.len(), "capacity preserved across reset")
}

// TestSpaceSavingSnapshotDeepCopy pins the Gemini-medium concern: a
// published snapshot must not alias the live sketch's storage, so a
// subsequent reset/eviction cannot mutate the snapshot a reader holds.
func TestSpaceSavingSnapshotDeepCopy(t *testing.T) {
	t.Parallel()
	s := newSpaceSaving(2)
	s.observe([]byte("hello"))
	snap := s.snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, []byte("hello"), snap[0].Key)
	// Mutating the live sketch must not touch the snapshot.
	s.reset()
	for i := 0; i < 100; i++ {
		s.observe([]byte("world"))
	}
	require.Equal(t, []byte("hello"), snap[0].Key, "snapshot must be deep-copied")
	require.Equal(t, uint64(1), snap[0].Count)
}

// TestSpaceSavingObservedBufferOwnedIndependently pins the design's
// pool-vs-SS ownership rule: the caller's key buffer can be safely
// reused or mutated after observe returns, because SS makes its own
// copy on insert.
func TestSpaceSavingObservedBufferOwnedIndependently(t *testing.T) {
	t.Parallel()
	s := newSpaceSaving(4)
	buf := []byte{'a', 'b', 'c'}
	s.observe(buf)
	// Mutate the caller buffer post-observe.
	buf[0] = 'X'
	buf[1] = 'Y'
	buf[2] = 'Z'
	snap := s.snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, []byte("abc"), snap[0].Key, "SS must own a copy independent of the caller buffer")
}

// TestSpaceSavingHeavyHitterGuarantee is the canonical Misra–Gries
// guarantee: a key with true frequency f > N/m is GUARANTEED in the
// tracked set after the stream is consumed.
func TestSpaceSavingHeavyHitterGuarantee(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		m := rapid.IntRange(2, 32).Draw(rt, "m")
		// Generate a stream of length N over a key alphabet of size
		// noisyAlphabet. Pick one "hot" key with frequency strictly
		// greater than N/m and assert it survives.
		nNoisy := rapid.IntRange(0, 200).Draw(rt, "noisyCount")
		hotFreq := rapid.IntRange(1, 200).Draw(rt, "hotFreq")
		// Total stream length:
		n := nNoisy + hotFreq
		if hotFreq*m <= n {
			// hotFreq must be > N/m for the guarantee. Skip cases that
			// don't satisfy the precondition (Chernoff-style filter).
			rt.SkipNow()
		}

		s := newSpaceSaving(m)
		// Interleave hot and noisy observations deterministically.
		alphabet := []byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80}
		hot := []byte("HOT")
		hi, ni := 0, 0
		for hi < hotFreq || ni < nNoisy {
			if hi < hotFreq && (ni >= nNoisy || (hi+ni)%2 == 0) {
				s.observe(hot)
				hi++
			} else {
				k := []byte{alphabet[ni%len(alphabet)]}
				s.observe(k)
				ni++
			}
		}

		// Heavy hitter MUST be present.
		found := false
		for _, e := range s.snapshot() {
			if bytes.Equal(e.Key, hot) {
				found = true
				// hotFreq is from rapid.IntRange(1, 200) — strictly
				// positive — so the conversion is wrap-safe; the
				// guard keeps gosec G115 silent without a //nolint.
				wantAtLeast := uint64(0)
				if hotFreq > 0 {
					wantAtLeast = uint64(hotFreq)
				}
				require.GreaterOrEqual(rt, e.Count, wantAtLeast,
					"SS counter is an upper bound on true frequency, so >= true count")
				break
			}
		}
		require.True(rt, found, "key with f > N/m must be tracked (m=%d, N=%d, hotFreq=%d)", m, n, hotFreq)
	})
}

// TestSpaceSavingSnapshotSortedByCountDescending is just a sanity for
// the drill-down handler: it relies on snapshot's contents but sorts
// them itself by descending count. This test pins the raw snapshot
// contract (unsorted, all entries returned).
func TestSpaceSavingSnapshotReturnsAllEntries(t *testing.T) {
	t.Parallel()
	s := newSpaceSaving(8)
	counts := []int{10, 5, 1, 20, 3, 7}
	for i, c := range counts {
		k := []byte{byte(i)}
		for j := 0; j < c; j++ {
			s.observe(k)
		}
	}
	snap := s.snapshot()
	require.Len(t, snap, len(counts))
	sort.Slice(snap, func(i, j int) bool { return snap[i].Count > snap[j].Count })
	require.Equal(t, uint64(20), snap[0].Count)
	require.Equal(t, uint64(1), snap[len(snap)-1].Count)
}

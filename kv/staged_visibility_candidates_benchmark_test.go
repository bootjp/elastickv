package kv

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// seedSparseCandidateRange builds the shape latestCandidateVersionsAt is worst
// at: a handful of candidate keys spread across a range that is otherwise dense
// with versions nobody asked for.
func seedSparseCandidateRange(tb testing.TB, st store.MVCCStore, candidates, fillerPerGap, versionsPerFiller int) [][]byte {
	tb.Helper()

	ctx := context.Background()
	keys := make([][]byte, 0, candidates)
	commitTS := uint64(1)
	for c := range candidates {
		key := []byte(fmt.Sprintf("k%06d", c*(fillerPerGap+1)))
		require.NoError(tb, st.PutAt(ctx, key, []byte("candidate"), commitTS, 0))
		commitTS++
		keys = append(keys, key)
		for f := 1; f <= fillerPerGap; f++ {
			filler := []byte(fmt.Sprintf("k%06d", c*(fillerPerGap+1)+f))
			for v := range versionsPerFiller {
				_ = v
				require.NoError(tb, st.PutAt(ctx, filler, []byte("filler-value-padding"), commitTS, 0))
				commitTS++
			}
		}
	}
	return keys
}

// BenchmarkLatestCandidateVersionsAt measures the candidate resolution a staged
// visibility scan page performs. The sparse case is the one the scan budget
// exists for: the enclosing range holds far more versions than the candidate
// set, and an unbounded export decodes all of them.
func BenchmarkLatestCandidateVersionsAt(b *testing.B) {
	for _, tc := range []struct {
		name              string
		candidates        int
		fillerPerGap      int
		versionsPerFiller int
	}{
		{name: "dense", candidates: 64, fillerPerGap: 0, versionsPerFiller: 0},
		{name: "sparse", candidates: 64, fillerPerGap: 64, versionsPerFiller: 16},
	} {
		b.Run(tc.name, func(b *testing.B) {
			st := store.NewMVCCStore()
			b.Cleanup(func() { _ = st.Close() })
			keys := seedSparseCandidateRange(b, st, tc.candidates, tc.fillerPerGap, tc.versionsPerFiller)
			ctx := context.Background()
			readTS := ^uint64(0) >> 1

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				got, err := latestCandidateVersionsAt(ctx, st, keys, readTS)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != len(keys) {
					b.Fatalf("resolved %d of %d candidates", len(got), len(keys))
				}
			}
		})
	}
}

// The bounded export must still resolve every candidate: whatever it does not
// reach is probed by exact key, so the result is the same set either way.
func TestLatestCandidateVersionsAtResolvesSparseCandidates(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	keys := seedSparseCandidateRange(t, st, 32, 64, 16)

	got, err := latestCandidateVersionsAt(context.Background(), st, keys, ^uint64(0)>>1)
	require.NoError(t, err)
	require.Len(t, got, len(keys))
	for _, key := range keys {
		version, ok := got[string(key)]
		require.True(t, ok, "candidate %q must resolve", key)
		require.Equal(t, []byte("candidate"), version.Value)
	}
}

// A candidate with no visible version stays absent rather than being invented
// by the probe fallback.
func TestLatestCandidateVersionsAtOmitsAbsentCandidates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	keys := seedSparseCandidateRange(t, st, 4, 64, 8)
	missing := []byte("k999999")
	keys = append(keys, missing)

	got, err := latestCandidateVersionsAt(ctx, st, keys, ^uint64(0)>>1)
	require.NoError(t, err)
	require.Len(t, got, len(keys)-1)
	_, ok := got[string(missing)]
	require.False(t, ok)
}

// exportRecordingStore captures the options every ExportVersions call carries.
type exportRecordingStore struct {
	store.MVCCStore
	scannedBudgets []uint64
	endKeys        [][]byte
	exports        int
}

func (s *exportRecordingStore) ExportVersions(
	ctx context.Context,
	opts store.ExportVersionsOptions,
) (store.ExportVersionsResult, error) {
	s.exports++
	s.scannedBudgets = append(s.scannedBudgets, opts.MaxScannedBytes)
	s.endKeys = append(s.endKeys, bytes.Clone(opts.EndKey))
	return s.MVCCStore.ExportVersions(ctx, opts)
}

// The candidate export spans from the smallest candidate through the largest
// and filters for the candidate set, so an unbounded scan budget decodes every
// intervening version -- tombstones and dense MVCC history included -- for a
// page that wants at most a bounded number of exact keys. A sparse route could
// make one ordinary scan page consume unbounded I/O on the serving leader.
func TestLatestCandidateVersionsAtBoundsTheRangeScan(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	t.Cleanup(func() { _ = base.Close() })
	keys := seedSparseCandidateRange(t, base, 32, 64, 16)
	recording := &exportRecordingStore{MVCCStore: base}

	got, err := latestCandidateVersionsAt(context.Background(), recording, keys, ^uint64(0)>>1)
	require.NoError(t, err)
	require.Len(t, got, len(keys), "every candidate still resolves")

	require.NotEmpty(t, recording.scannedBudgets)
	require.Equal(t, uint64(stagedVisibilityCandidateScanBudget), recording.scannedBudgets[0],
		"the range pass must carry a finite scan budget")
	require.Greater(t, recording.exports, 1,
		"a range pass that hit the budget must fall back to exact-key probes")
}

// The probe fallback must cover exactly the candidate key. prefixScanEnd(key)
// covers every key that has it as a prefix, so probing an absent "a" would walk
// "ab", "az" and all of their versions before concluding "a" is not there --
// unbounded work that the scan budget only splits into repeated exports, once
// per unresolved candidate.
func TestLatestCandidateVersionsAtProbesExactKeysOnly(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	t.Cleanup(func() { _ = base.Close() })
	keys := seedSparseCandidateRange(t, base, 32, 64, 16)
	// A candidate with no version of its own, but with neighbours that share it
	// as a prefix: a prefix-bounded probe would scan all of them.
	absent := []byte("k000000-absent")
	ctx := context.Background()
	for i := range 64 {
		require.NoError(t, base.PutAt(ctx, []byte(fmt.Sprintf("%s-%03d", absent, i)), []byte("neighbour"), 1, 0))
	}
	keys = append(keys, absent)

	recording := &exportRecordingStore{MVCCStore: base}
	got, err := latestCandidateVersionsAt(ctx, recording, keys, ^uint64(0)>>1)
	require.NoError(t, err)
	_, ok := got[string(absent)]
	require.False(t, ok, "the absent candidate has no visible version")

	require.Contains(t, recording.endKeys, exactKeyScanEnd(absent),
		"the probe must be bounded to the candidate key alone")
	for _, end := range recording.endKeys[1:] {
		require.NotEqual(t, prefixScanEnd(absent), end,
			"no probe may use a prefix bound")
	}
}

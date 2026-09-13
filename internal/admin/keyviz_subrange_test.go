package admin

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

// TestPivotKeyVizColumnsSubRowsDoNotCollide is the Gemini-HIGH
// regression: sub-rows of one route share its RouteID, so a pivot keyed
// on bare RouteID would collapse them into one row and drop all but the
// last. Keying on BucketID (which embeds #subIdx when SubBucketCount>1)
// keeps each sub-bucket a distinct output row. See design §5.1.
func TestPivotKeyVizColumnsSubRowsDoNotCollide(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_000, 0)
	cols := []keyviz.MatrixColumn{
		{
			At: at,
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte{0x00}, End: []byte{0x10}, SubBucket: 0, SubBucketCount: 2, Writes: 5},
				{RouteID: 1, Start: []byte{0x10}, End: []byte{0x20}, SubBucket: 1, SubBucketCount: 2, Writes: 9},
			},
		},
	}
	m := pivotKeyVizColumns(cols, keyVizSeriesWrites, keyVizRowBudgetCap)
	require.Len(t, m.Rows, 2, "two sub-rows of the same route must not collide")

	byID := map[string]KeyVizRow{}
	for _, r := range m.Rows {
		byID[r.BucketID] = r
	}
	require.Contains(t, byID, "route:1#0")
	require.Contains(t, byID, "route:1#1")
	require.Equal(t, []uint64{5}, byID["route:1#0"].Values)
	require.Equal(t, []uint64{9}, byID["route:1#1"].Values)
}

// TestPivotKeyVizColumnsK1KeepsLegacyBucketID pins backward
// compatibility: a non-sub-divided row (SubBucketCount<=1) keeps the
// exact "route:<id>" BucketID with no #suffix.
func TestPivotKeyVizColumnsK1KeepsLegacyBucketID(t *testing.T) {
	t.Parallel()
	cols := []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{RouteID: 7, Start: []byte{0x00}, End: []byte{0xFF}, SubBucket: 0, SubBucketCount: 1, Writes: 3},
			},
		},
	}
	m := pivotKeyVizColumns(cols, keyVizSeriesWrites, keyVizRowBudgetCap)
	require.Len(t, m.Rows, 1)
	require.Equal(t, "route:7", m.Rows[0].BucketID)
}

func TestPivotKeyVizColumnsLabelsDoNotCollide(t *testing.T) {
	t.Parallel()
	at := time.Unix(1_700_000_000, 0)
	cols := []keyviz.MatrixColumn{
		{
			At: at,
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Label: keyviz.LabelDynamo, Start: []byte("a"), End: []byte("z"), SubBucketCount: 1, Writes: 5},
				{RouteID: 1, Label: keyviz.LabelRedis, Start: []byte("a"), End: []byte("z"), SubBucketCount: 1, Writes: 9},
			},
		},
	}

	m := pivotKeyVizColumns(cols, keyVizSeriesWrites, keyVizRowBudgetCap)
	require.Len(t, m.Rows, 2)
	byID := map[string]KeyVizRow{}
	for _, row := range m.Rows {
		byID[row.BucketID] = row
	}
	require.Equal(t, "dynamo", byID["route:1:dynamo"].Label)
	require.Equal(t, []uint64{5}, byID["route:1:dynamo"].Values)
	require.Equal(t, "redis", byID["route:1:redis"].Label)
	require.Equal(t, []uint64{9}, byID["route:1:redis"].Values)
}

// TestMergeKeyVizMatricesMixedKCoexist pins the §9 decision 2: in a
// mixed-K cluster a K=1 peer emits "route:1" while a K=2 peer emits
// "route:1#0"/"route:1#1". Because the merge dedupes by BucketID these
// do NOT merge — they coexist as distinct rows (the K=1 row does not
// absorb the sub-rows or vice versa).
func TestMergeKeyVizMatricesMixedKCoexist(t *testing.T) {
	t.Parallel()
	col := []int64{1_700_000_000_000}
	k1Peer := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Start: []byte{0x00}, End: []byte{0x20}, Values: []uint64{40}},
		},
	}
	k2Peer := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:1#0", Start: []byte{0x00}, End: []byte{0x10}, Values: []uint64{25}},
			{BucketID: "route:1#1", Start: []byte{0x10}, End: []byte{0x20}, Values: []uint64{15}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{k1Peer, k2Peer}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 3, "K=1 row and the two K=2 sub-rows must coexist, not merge")

	byID := map[string][]uint64{}
	for _, r := range merged.Rows {
		byID[r.BucketID] = r.Values
		require.False(t, r.Conflict, "distinct-bucket coexistence is not a conflict")
	}
	require.Equal(t, []uint64{40}, byID["route:1"])
	require.Equal(t, []uint64{25}, byID["route:1#0"])
	require.Equal(t, []uint64{15}, byID["route:1#1"])
}

// TestMergeKeyVizMatricesCarriesSubRangeMetadata is the fan-out regression.
//
// rowMergeAcc retained neither SubBucket nor SubBucketCount, so every merged
// response serialized both as omitted: subRangeLabel returned null and the
// sub-range label plus the narrowed Start/End captions disappeared specifically
// in the cluster-wide view — the one view where an operator is most likely to be
// looking at sub-divided routes.
func TestMergeKeyVizMatricesCarriesSubRangeMetadata(t *testing.T) {
	t.Parallel()

	col := []int64{1_700_000_000_000}
	subRow := func(id string, sub int, values []uint64) KeyVizRow {
		return KeyVizRow{
			BucketID: id, Start: []byte{byte(sub * 0x10)}, End: []byte{byte((sub + 1) * 0x10)},
			SubBucket: sub, SubBucketCount: 2, Values: values,
		}
	}
	peerA := KeyVizMatrix{
		ColumnUnixMs: col, Series: keyVizSeriesWrites,
		Rows: []KeyVizRow{subRow("route:1#0", 0, []uint64{5}), subRow("route:1#1", 1, []uint64{7})},
	}
	peerB := KeyVizMatrix{
		ColumnUnixMs: col, Series: keyVizSeriesWrites,
		Rows: []KeyVizRow{subRow("route:1#0", 0, []uint64{3}), subRow("route:1#1", 1, []uint64{2})},
	}

	merged := mergeKeyVizMatrices([]KeyVizMatrix{peerA, peerB}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 2)

	byID := map[string]KeyVizRow{}
	for _, r := range merged.Rows {
		byID[r.BucketID] = r
	}
	require.Equal(t, 0, byID["route:1#0"].SubBucket)
	require.Equal(t, 2, byID["route:1#0"].SubBucketCount,
		"a merged row must keep its sub-range identity, or the SPA renders no label")
	require.Equal(t, 1, byID["route:1#1"].SubBucket)
	require.Equal(t, 2, byID["route:1#1"].SubBucketCount)
}

// TestMergeKeyVizMatricesTakesSubRangeMetadataFromAnyPeer covers the rolling
// upgrade: during one, a bucket is reported by peers that omit the fields and
// peers that populate them. Taking the first peer's zero would blank the row for
// the whole cluster, so a nonzero value from any peer wins.
func TestMergeKeyVizMatricesTakesSubRangeMetadataFromAnyPeer(t *testing.T) {
	t.Parallel()

	col := []int64{1_700_000_000_000}
	// The legacy peer is FIRST, so the accumulator is seeded from it.
	legacyPeer := KeyVizMatrix{
		ColumnUnixMs: col, Series: keyVizSeriesWrites,
		Rows: []KeyVizRow{{
			BucketID: "route:1#1", Start: []byte{0x10}, End: []byte{0x20}, Values: []uint64{4},
		}},
	}
	upgradedPeer := KeyVizMatrix{
		ColumnUnixMs: col, Series: keyVizSeriesWrites,
		Rows: []KeyVizRow{{
			BucketID: "route:1#1", Start: []byte{0x10}, End: []byte{0x20},
			SubBucket: 1, SubBucketCount: 2, Values: []uint64{6},
		}},
	}

	merged := mergeKeyVizMatrices([]KeyVizMatrix{legacyPeer, upgradedPeer}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, 1, merged.Rows[0].SubBucket)
	require.Equal(t, 2, merged.Rows[0].SubBucketCount,
		"one legacy peer must not blank the sub-range identity the upgraded peers report")
}

// A route that is not sub-divided must stay byte-identical on the wire, so an
// older SPA sees no change.
func TestMergeKeyVizMatricesOmitsSubRangeMetadataWhenAbsent(t *testing.T) {
	t.Parallel()

	col := []int64{1_700_000_000_000}
	peer := func(v uint64) KeyVizMatrix {
		return KeyVizMatrix{
			ColumnUnixMs: col, Series: keyVizSeriesWrites,
			Rows: []KeyVizRow{{
				BucketID: "route:1", Start: []byte{0x00}, End: []byte{0x20}, Values: []uint64{v},
			}},
		}
	}

	merged := mergeKeyVizMatrices([]KeyVizMatrix{peer(1), peer(2)}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 1)
	require.Zero(t, merged.Rows[0].SubBucket)
	require.Zero(t, merged.Rows[0].SubBucketCount)
}

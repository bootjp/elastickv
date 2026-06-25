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

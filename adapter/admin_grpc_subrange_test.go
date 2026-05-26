package adapter

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/stretchr/testify/require"
)

// TestMatrixToProtoSubRowsDoNotCollide is the adapter-side twin of the
// admin pivot-collision regression: sub-rows of one route share its
// RouteID, so the proto pivot must key on BucketId (which embeds
// #subIdx) rather than RouteID, or it would collapse them. The proto
// KeyVizRow needs no new field — BucketId distinguishes the sub-rows.
// See design §5.1.
func TestMatrixToProtoSubRowsDoNotCollide(t *testing.T) {
	t.Parallel()
	pick := func(r keyviz.MatrixRow) uint64 { return r.Writes }
	cols := []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte{0x00}, End: []byte{0x10}, SubBucket: 0, SubBucketCount: 2, Writes: 5},
				{RouteID: 1, Start: []byte{0x10}, End: []byte{0x20}, SubBucket: 1, SubBucketCount: 2, Writes: 9},
			},
		},
	}
	resp := matrixToProto(cols, pick, 1024)
	require.Len(t, resp.Rows, 2, "two sub-rows of the same route must not collide")

	byID := map[string][]uint64{}
	for _, r := range resp.Rows {
		byID[r.BucketId] = r.Values
	}
	require.Equal(t, []uint64{5}, byID["route:1#0"])
	require.Equal(t, []uint64{9}, byID["route:1#1"])
}

// TestNewKeyVizRowFromAggregateZeroTotalFallback pins the adapter/handler
// harmonization (Claude round-2 follow-up): an aggregate row whose
// MemberRoutesTotal is still 0 (a just-coalesced bucket serialized before
// its count is set) reports route_count = len(MemberRoutes) rather than
// the nonsensical 0 — matching the JSON pivot's defensive fallback.
func TestNewKeyVizRowFromAggregateZeroTotalFallback(t *testing.T) {
	t.Parallel()
	mr := keyviz.MatrixRow{
		RouteID:           99,
		Aggregate:         true,
		MemberRoutes:      []uint64{1, 2, 3},
		MemberRoutesTotal: 0, // not yet set
	}
	row := newKeyVizRowFrom(mr, 1)
	require.Equal(t, uint64(3), row.RouteCount, "aggregate with total=0 must fall back to len(MemberRoutes)")
}

// TestMatrixToProtoK1KeepsLegacyBucketID pins that a non-sub-divided row
// keeps the exact legacy "route:<id>" BucketId (no #suffix).
func TestMatrixToProtoK1KeepsLegacyBucketID(t *testing.T) {
	t.Parallel()
	pick := func(r keyviz.MatrixRow) uint64 { return r.Writes }
	cols := []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{RouteID: 7, Start: []byte{0x00}, End: []byte{0xFF}, SubBucket: 0, SubBucketCount: 1, Writes: 3},
			},
		},
	}
	resp := matrixToProto(cols, pick, 1024)
	require.Len(t, resp.Rows, 1)
	require.Equal(t, "route:7", resp.Rows[0].BucketId)
}

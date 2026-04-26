package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeKeyVizSampler is a deterministic in-memory KeyVizSampler so
// AdminServer tests don't need to drive a real keyviz.MemSampler with
// goroutines and time. Snapshot returns a fresh deep copy of the
// configured columns so the test mirrors the real sampler's contract.
type fakeKeyVizSampler struct {
	cols []keyviz.MatrixColumn
}

func (f *fakeKeyVizSampler) Snapshot(_, _ time.Time) []keyviz.MatrixColumn {
	out := make([]keyviz.MatrixColumn, len(f.cols))
	for i, c := range f.cols {
		rows := make([]keyviz.MatrixRow, len(c.Rows))
		for j, r := range c.Rows {
			rows[j] = r
			rows[j].Start = append([]byte(nil), r.Start...)
			rows[j].End = append([]byte(nil), r.End...)
			if len(r.MemberRoutes) > 0 {
				rows[j].MemberRoutes = append([]uint64(nil), r.MemberRoutes...)
			}
		}
		out[i] = keyviz.MatrixColumn{At: c.At, Rows: rows}
	}
	return out
}

// TestGetKeyVizMatrixReturnsUnavailableWhenSamplerNotRegistered pins
// the failure mode operators should see when keyviz is disabled on
// a node — Unavailable rather than a successful empty response.
func TestGetKeyVizMatrixReturnsUnavailableWhenSamplerNotRegistered(t *testing.T) {
	t.Parallel()
	srv := NewAdminServer(NodeIdentity{NodeID: "node-a"}, nil)
	_, err := srv.GetKeyVizMatrix(context.Background(), &pb.GetKeyVizMatrixRequest{})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

// TestGetKeyVizMatrixPivotsColumnsToRows pins the row-major proto
// layout: one KeyVizRow per RouteID with values aligned to the
// parallel column_unix_ms slice. Drives a fake sampler with two
// columns and two routes (one of which reports zero in column 1).
func TestGetKeyVizMatrixPivotsColumnsToRows(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_700_000_000, 0)
	t1 := t0.Add(time.Minute)
	srv := newAdminServerWithFakeSampler(t, twoColumnTwoRouteCols(t0, t1))

	resp, err := srv.GetKeyVizMatrix(context.Background(), &pb.GetKeyVizMatrixRequest{
		Series: pb.KeyVizSeries_KEYVIZ_SERIES_READS,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{t0.UnixMilli(), t1.UnixMilli()}, resp.ColumnUnixMs)
	require.Len(t, resp.Rows, 2)
	// Sorted by Start: route 1 ("a") then route 2 ("m").
	r1, r2 := resp.Rows[0], resp.Rows[1]
	require.Equal(t, "route:1", r1.BucketId)
	require.Equal(t, "route:2", r2.BucketId)
	require.Equal(t, []byte("a"), r1.Start)
	require.Equal(t, []byte("m"), r1.End)
	require.False(t, r1.Aggregate)
	require.False(t, r2.Aggregate)
	require.Equal(t, []uint64{4, 9}, r1.Values)
	// Route 2 is absent in column 1 — zero by default.
	require.Equal(t, []uint64{7, 0}, r2.Values)
}

func twoColumnTwoRouteCols(t0, t1 time.Time) []keyviz.MatrixColumn {
	return []keyviz.MatrixColumn{
		{
			At: t0,
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte("a"), End: []byte("m"), Reads: 4, Writes: 1},
				{RouteID: 2, Start: []byte("m"), End: []byte("z"), Reads: 7, Writes: 0},
			},
		},
		{
			At: t1,
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte("a"), End: []byte("m"), Reads: 9, Writes: 3},
			},
		},
	}
}

func newAdminServerWithFakeSampler(t *testing.T, cols []keyviz.MatrixColumn) *AdminServer {
	t.Helper()
	srv := NewAdminServer(NodeIdentity{NodeID: "node-a"}, nil)
	srv.RegisterSampler(&fakeKeyVizSampler{cols: cols})
	return srv
}

// TestGetKeyVizMatrixSeriesSelection pins the request.Series →
// MatrixRow counter mapping including the UNSPECIFIED → Reads default.
func TestGetKeyVizMatrixSeriesSelection(t *testing.T) {
	t.Parallel()
	row := keyviz.MatrixRow{
		RouteID:    1,
		Start:      []byte("a"),
		End:        []byte("z"),
		Reads:      11,
		Writes:     22,
		ReadBytes:  333,
		WriteBytes: 4444,
	}
	srv := newAdminServerWithFakeSampler(t, []keyviz.MatrixColumn{
		{At: time.Unix(1_700_000_000, 0), Rows: []keyviz.MatrixRow{row}},
	})

	for _, tc := range []struct {
		name   string
		series pb.KeyVizSeries
		want   uint64
	}{
		{"unspecified defaults to reads", pb.KeyVizSeries_KEYVIZ_SERIES_UNSPECIFIED, 11},
		{"reads", pb.KeyVizSeries_KEYVIZ_SERIES_READS, 11},
		{"writes", pb.KeyVizSeries_KEYVIZ_SERIES_WRITES, 22},
		{"read_bytes", pb.KeyVizSeries_KEYVIZ_SERIES_READ_BYTES, 333},
		{"write_bytes", pb.KeyVizSeries_KEYVIZ_SERIES_WRITE_BYTES, 4444},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := srv.GetKeyVizMatrix(context.Background(), &pb.GetKeyVizMatrixRequest{Series: tc.series})
			require.NoError(t, err)
			require.Len(t, resp.Rows, 1)
			require.Equal(t, []uint64{tc.want}, resp.Rows[0].Values)
		})
	}
}

// TestGetKeyVizMatrixEncodesAggregateBucket pins the proto layout
// for virtual buckets: bucket_id prefixed "virtual:", aggregate=true,
// route_ids carries the visible MemberRoutes list, and route_count
// reports the TRUE total (MemberRoutesTotal) — including past-cap
// folded routes — so consumers always know how many routes
// contributed.
func TestGetKeyVizMatrixEncodesAggregateBucket(t *testing.T) {
	t.Parallel()
	srv := newAdminServerWithFakeSampler(t, []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{
					RouteID:           ^uint64(0), // synthetic virtual-bucket ID
					Start:             []byte("c"),
					End:               []byte("d"),
					Aggregate:         true,
					MemberRoutes:      []uint64{2, 3, 4},
					MemberRoutesTotal: 3,
					Reads:             50,
				},
			},
		},
	})

	resp, err := srv.GetKeyVizMatrix(context.Background(), &pb.GetKeyVizMatrixRequest{
		Series: pb.KeyVizSeries_KEYVIZ_SERIES_READS,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)
	r := resp.Rows[0]
	require.True(t, r.Aggregate)
	require.Equal(t, "virtual:18446744073709551615", r.BucketId)
	require.Equal(t, uint64(3), r.RouteCount)
	require.False(t, r.RouteIdsTruncated)
	require.Equal(t, []uint64{2, 3, 4}, r.RouteIds)
}

// TestGetKeyVizMatrixSurfacesRouteCountTruncation pins Codex round-1
// P2 on PR #646: when the sampler caps MemberRoutes at
// MaxMemberRoutesPerSlot, route_count must still report the TRUE
// total (MemberRoutesTotal) and route_ids_truncated must flip true so
// consumers know the visible list is a prefix.
func TestGetKeyVizMatrixSurfacesRouteCountTruncation(t *testing.T) {
	t.Parallel()
	srv := newAdminServerWithFakeSampler(t, []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{
					RouteID:           ^uint64(0),
					Start:             []byte("c"),
					End:               []byte("d"),
					Aggregate:         true,
					MemberRoutes:      []uint64{2, 3}, // visible cap=2
					MemberRoutesTotal: 9,              // 7 more folded past the cap
					Reads:             100,
				},
			},
		},
	})

	resp, err := srv.GetKeyVizMatrix(context.Background(), &pb.GetKeyVizMatrixRequest{
		Series: pb.KeyVizSeries_KEYVIZ_SERIES_READS,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)
	r := resp.Rows[0]
	require.Equal(t, uint64(9), r.RouteCount, "route_count must reflect MemberRoutesTotal")
	require.True(t, r.RouteIdsTruncated, "route_ids_truncated must signal capped membership")
	require.Equal(t, []uint64{2, 3}, r.RouteIds)
}

// TestGetKeyVizMatrixHonorsRowsBudget pins Codex round-1 P1 on
// PR #646: a request with rows=N must return at most N rows. We
// stage 4 routes with distinct activity totals and request rows=2;
// the response must contain only the two highest-activity routes,
// sorted by Start.
func TestGetKeyVizMatrixHonorsRowsBudget(t *testing.T) {
	t.Parallel()
	srv := newAdminServerWithFakeSampler(t, []keyviz.MatrixColumn{
		{
			At: time.Unix(1_700_000_000, 0),
			Rows: []keyviz.MatrixRow{
				{RouteID: 1, Start: []byte("a"), End: []byte("b"), Reads: 1},
				{RouteID: 2, Start: []byte("b"), End: []byte("c"), Reads: 100},
				{RouteID: 3, Start: []byte("c"), End: []byte("d"), Reads: 5},
				{RouteID: 4, Start: []byte("d"), End: []byte("e"), Reads: 50},
			},
		},
	})

	resp, err := srv.GetKeyVizMatrix(context.Background(), &pb.GetKeyVizMatrixRequest{
		Series: pb.KeyVizSeries_KEYVIZ_SERIES_READS,
		Rows:   2,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 2, "rows budget must cap response size")
	// Top 2 by activity = routes 2 (100) and 4 (50); sorted by Start
	// gives "b" then "d".
	require.Equal(t, "route:2", resp.Rows[0].BucketId)
	require.Equal(t, "route:4", resp.Rows[1].BucketId)
}

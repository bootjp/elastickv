package admin

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// TestMergeKeyVizMatricesReadsSum pins the §4.1 rule: read counters
// from distinct nodes are independent local serves and add. The
// merged matrix has one column entry per timestamp seen anywhere
// and the row's Values for that column equal the sum of all node
// inputs at that column.
func TestMergeKeyVizMatricesReadsSum(t *testing.T) {
	t.Parallel()
	col := []int64{1_700_000_000_000}
	a := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{10}},
		},
	}
	b := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{25}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{a, b}, keyVizSeriesReads)
	require.Equal(t, []int64{1_700_000_000_000}, merged.ColumnUnixMs)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, []uint64{35}, merged.Rows[0].Values)
	require.False(t, merged.Rows[0].Conflict, "reads must never raise conflict")
}

// TestMergeKeyVizMatricesWritesMaxStableLeader pins the §4.2 happy
// path: under stable leadership exactly one node reports non-zero
// writes. The merge picks the non-zero value without raising
// conflict.
func TestMergeKeyVizMatricesWritesMaxStableLeader(t *testing.T) {
	t.Parallel()
	col := []int64{1_700_000_000_000, 1_700_000_001_000}
	leader := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:7", Values: []uint64{42, 17}},
		},
	}
	follower := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:7", Values: []uint64{0, 0}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{follower, leader}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, []uint64{42, 17}, merged.Rows[0].Values)
	require.False(t, merged.Rows[0].Conflict, "stable-leader merge must not raise conflict")
}

// TestMergeKeyVizMatricesWritesMaxLeadershipFlip pins §4.2 under a
// mid-window flip: two nodes report non-zero, disagreeing values
// for the same cell. The merge keeps the larger value and raises
// the row-level conflict flag so the SPA can hatch the row.
func TestMergeKeyVizMatricesWritesMaxLeadershipFlip(t *testing.T) {
	t.Parallel()
	col := []int64{1_700_000_000_000}
	exLeader := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:9", Values: []uint64{30}},
		},
	}
	newLeader := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:9", Values: []uint64{55}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{exLeader, newLeader}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, []uint64{55}, merged.Rows[0].Values, "max-merge must keep the larger value")
	require.True(t, merged.Rows[0].Conflict, "leadership flip must raise the row conflict flag")
}

// TestMergeKeyVizMatricesUnionColumns pins the §4.5 rule: a column
// present in only some nodes still gets a slot in the merged matrix;
// missing values fill in as zero.
func TestMergeKeyVizMatricesUnionColumns(t *testing.T) {
	t.Parallel()
	a := KeyVizMatrix{
		ColumnUnixMs: []int64{100, 200},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{1, 2}},
		},
	}
	b := KeyVizMatrix{
		ColumnUnixMs: []int64{200, 300},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{10, 20}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{a, b}, keyVizSeriesReads)
	require.Equal(t, []int64{100, 200, 300}, merged.ColumnUnixMs)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, []uint64{1, 12, 20}, merged.Rows[0].Values,
		"missing columns must read as zero on the side that does not have them")
}

// TestMergeKeyVizMatricesDistinctRowsPreserveOrder pins the §4.4
// row-identity rule: rows with distinct BucketIDs land in the
// merged matrix in first-seen-order, preserving the per-node row
// order so a single-node fan-out is byte-identical to the local
// matrix.
func TestMergeKeyVizMatricesDistinctRowsPreserveOrder(t *testing.T) {
	t.Parallel()
	a := KeyVizMatrix{
		ColumnUnixMs: []int64{100},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:5", Values: []uint64{1}},
			{BucketID: "route:1", Values: []uint64{2}},
		},
	}
	b := KeyVizMatrix{
		ColumnUnixMs: []int64{100},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:9", Values: []uint64{4}},
			{BucketID: "route:5", Values: []uint64{8}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{a, b}, keyVizSeriesReads)
	require.Len(t, merged.Rows, 3)
	require.Equal(t, "route:5", merged.Rows[0].BucketID)
	require.Equal(t, "route:1", merged.Rows[1].BucketID)
	require.Equal(t, "route:9", merged.Rows[2].BucketID)
}

// TestKeyVizFanoutRunSinglePeerOK exercises the end-to-end happy
// path: one peer responds with a parseable matrix; the aggregator
// merges it with the local view and reports both nodes ok.
func TestKeyVizFanoutRunSinglePeerOK(t *testing.T) {
	t.Parallel()
	peerMatrix := KeyVizMatrix{
		ColumnUnixMs: []int64{1_700_000_000_000},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{7}},
		},
	}
	peer := newKeyVizPeerStub(t, http.StatusOK, peerMatrix)
	defer peer.Close()

	f := NewKeyVizFanout("self:8080", []string{peer.URL}).WithHTTPClient(peer.Client())
	local := KeyVizMatrix{
		ColumnUnixMs: []int64{1_700_000_000_000},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{3}},
		},
	}

	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, local)
	require.Equal(t, []uint64{10}, merged.Rows[0].Values, "reads must sum across local + peer")
	require.NotNil(t, merged.Fanout)
	require.Equal(t, 2, merged.Fanout.Expected)
	require.Equal(t, 2, merged.Fanout.Responded)
	require.Len(t, merged.Fanout.Nodes, 2)
	require.Equal(t, "self:8080", merged.Fanout.Nodes[0].Node)
	require.True(t, merged.Fanout.Nodes[0].OK)
	require.Equal(t, peer.URL, merged.Fanout.Nodes[1].Node)
	require.True(t, merged.Fanout.Nodes[1].OK)
}

// TestKeyVizFanoutRunPeerHTTPError pins the §2.1 degraded-mode
// contract: a peer that returns 5xx contributes ok=false with the
// status surfaced; the local matrix still ships and Responded
// reflects the partial success.
func TestKeyVizFanoutRunPeerHTTPError(t *testing.T) {
	t.Parallel()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "boom")
	}))
	defer peer.Close()

	f := NewKeyVizFanout("self:8080", []string{peer.URL}).WithHTTPClient(peer.Client())
	local := KeyVizMatrix{
		ColumnUnixMs: []int64{1_700_000_000_000},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{3}},
		},
	}

	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, local)
	require.Equal(t, []uint64{3}, merged.Rows[0].Values, "5xx peer must not perturb local counts")
	require.NotNil(t, merged.Fanout)
	require.Equal(t, 2, merged.Fanout.Expected)
	require.Equal(t, 1, merged.Fanout.Responded)
	require.False(t, merged.Fanout.Nodes[1].OK)
	require.Contains(t, merged.Fanout.Nodes[1].Error, "500")
}

// TestKeyVizFanoutRunPeerTimeout pins the design 9 timeout: a
// peer that hangs past the per-call ceiling contributes ok=false
// and the request still completes promptly.
func TestKeyVizFanoutRunPeerTimeout(t *testing.T) {
	t.Parallel()
	hang := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-hang:
		}
	}))
	defer peer.Close()
	defer close(hang)

	f := NewKeyVizFanout("self:8080", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithTimeout(50 * time.Millisecond)

	start := time.Now()
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, KeyVizMatrix{Series: keyVizSeriesReads})
	require.Less(t, time.Since(start), 1*time.Second, "fan-out must not wait beyond its per-peer timeout")
	require.NotNil(t, merged.Fanout)
	require.Equal(t, 1, merged.Fanout.Responded)
	require.False(t, merged.Fanout.Nodes[1].OK)
}

// TestKeyVizFanoutRunNoPeers exercises the single-node fallback:
// when peers is empty, Run returns the local matrix with a Fanout
// block reporting Expected=1, Responded=1.
func TestKeyVizFanoutRunNoPeers(t *testing.T) {
	t.Parallel()
	f := NewKeyVizFanout("self:8080", nil)
	local := KeyVizMatrix{
		ColumnUnixMs: []int64{1_700_000_000_000},
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{99}},
		},
	}
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesWrites, rows: 1024}, local)
	require.Equal(t, []uint64{99}, merged.Rows[0].Values)
	require.Equal(t, 1, merged.Fanout.Expected)
	require.Equal(t, 1, merged.Fanout.Responded)
	require.Equal(t, "self:8080", merged.Fanout.Nodes[0].Node)
}

// TestBuildKeyVizPeerURLForwardsParams pins the §6 contract that
// the per-peer URL is rebuilt from the parsed parameters, so the
// peer always gets a deterministic query string regardless of the
// upstream client's encoding quirks.
func TestBuildKeyVizPeerURLForwardsParams(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		peer   string
		params keyVizParams
		want   string
	}{
		{
			name:   "with scheme",
			peer:   "http://10.0.0.2:8080",
			params: keyVizParams{series: keyVizSeriesWrites, rows: 256},
			want:   "http://10.0.0.2:8080/admin/api/v1/keyviz/matrix?rows=256&series=writes",
		},
		{
			name:   "host only (no scheme)",
			peer:   "10.0.0.2:8080",
			params: keyVizParams{series: keyVizSeriesReads, rows: 1024},
			want:   "http://10.0.0.2:8080/admin/api/v1/keyviz/matrix?rows=1024&series=reads",
		},
		{
			name: "with time bounds",
			peer: "http://node-a",
			params: keyVizParams{
				series: keyVizSeriesReads,
				rows:   8,
				from:   time.UnixMilli(1_700_000_000_000),
				to:     time.UnixMilli(1_700_000_900_000),
			},
			want: "http://node-a/admin/api/v1/keyviz/matrix?from_unix_ms=1700000000000&rows=8&series=reads&to_unix_ms=1700000900000",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildKeyVizPeerURL(tc.peer, tc.params)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestKeyVizFanoutRunPeerOrder pins that the per-node status array
// follows the operator-supplied peer order. Self is always first.
func TestKeyVizFanoutRunPeerOrder(t *testing.T) {
	t.Parallel()
	matrix := KeyVizMatrix{Series: keyVizSeriesReads}
	first := newKeyVizPeerStub(t, http.StatusOK, matrix)
	defer first.Close()
	second := newKeyVizPeerStub(t, http.StatusOK, matrix)
	defer second.Close()

	f := NewKeyVizFanout("self:8080", []string{first.URL, second.URL}).WithHTTPClient(first.Client())
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, matrix)
	require.Equal(t, []FanoutNodeStatus{
		{Node: "self:8080", OK: true},
		{Node: first.URL, OK: true},
		{Node: second.URL, OK: true},
	}, merged.Fanout.Nodes)
}

// newKeyVizPeerStub spins up an httptest.Server that answers
// /admin/api/v1/keyviz/matrix with a fixed JSON body. Anything
// else returns 404 — which surfaces as "peer status 404" in the
// aggregator and lets a future test assert the path verbatim.
func newKeyVizPeerStub(t *testing.T, status int, body KeyVizMatrix) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/admin/api/v1/keyviz/matrix") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Logf("encode peer body: %v", err)
		}
	}))
	return srv
}

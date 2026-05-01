package admin

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// TestMergeKeyVizMatricesPreservesRaftIdentity pins the Phase 2-C+
// per-cell wire extension on the fan-out merge path: mergeRowInto
// stamps the destination row's per-cell (RaftGroupIDs[idx],
// LeaderTerms[idx]) from whichever source contributed the value
// kept at that cell. Both sources reporting the same identity
// for a writes-max merge is the steady-state shape — merged
// identity matches regardless of source order. (Gemini HIGH on
// PR #720 resolved by going per-cell; row-level scalars would
// only capture the first column's identity and break the per-cell
// dedupe goal.)
func TestMergeKeyVizMatricesPreservesRaftIdentity(t *testing.T) {
	t.Parallel()
	col := []int64{1_700_000_000_000}
	a := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:5", Values: []uint64{30}, RaftGroupIDs: []uint64{7}, LeaderTerms: []uint64{42}},
		},
	}
	b := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:5", Values: []uint64{0}, RaftGroupIDs: []uint64{7}, LeaderTerms: []uint64{42}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{a, b}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, []uint64{7}, merged.Rows[0].RaftGroupIDs, "RaftGroupIDs must survive mergeRowInto")
	require.Equal(t, []uint64{42}, merged.Rows[0].LeaderTerms, "LeaderTerms must survive mergeRowInto")
}

// TestMergeKeyVizMatricesPerCellIdentityMatchesValueOwner pins the
// Gemini MEDIUM fix: when maxMerge picks a value from one source,
// the identity at that cell must come from the SAME source — not
// from whoever happened to be processed first. Drives a leadership
// flip across two consecutive columns with each leader winning
// one cell.
func TestMergeKeyVizMatricesPerCellIdentityMatchesValueOwner(t *testing.T) {
	t.Parallel()
	col := []int64{1_700_000_000_000, 1_700_000_001_000}
	exLeader := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:9", Values: []uint64{50, 0}, RaftGroupIDs: []uint64{7, 7}, LeaderTerms: []uint64{42, 42}},
		},
	}
	newLeader := KeyVizMatrix{
		ColumnUnixMs: col,
		Series:       keyVizSeriesWrites,
		Rows: []KeyVizRow{
			{BucketID: "route:9", Values: []uint64{0, 80}, RaftGroupIDs: []uint64{7, 7}, LeaderTerms: []uint64{43, 43}},
		},
	}
	merged := mergeKeyVizMatrices([]KeyVizMatrix{exLeader, newLeader}, keyVizSeriesWrites)
	require.Len(t, merged.Rows, 1)
	require.Equal(t, []uint64{50, 80}, merged.Rows[0].Values, "writes max-merge picks the larger per cell")
	require.Equal(t, []uint64{7, 7}, merged.Rows[0].RaftGroupIDs, "groupID stays 7 across both cells")
	require.Equal(t, []uint64{42, 43}, merged.Rows[0].LeaderTerms,
		"col0's identity comes from exLeader (term 42, won 50 vs 0); col1's identity comes from newLeader (term 43, won 80 vs 0)")
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

// TestKeyVizFanoutRunForwardsCookies pins the auth bug fix that
// brought all peers up from 401 to 200: the inbound user's session
// cookies are forwarded on every peer request so the receiving
// node's SessionAuth middleware sees a valid principal. Without
// this, peers reject every fan-out call with 401 missing-session-
// cookie and the cluster heatmap collapses to "1 of N nodes
// responded".
//
// Pins both halves of the cookie contract:
//   - admin_session and admin_csrf are forwarded with their values
//     verbatim.
//   - Unrelated cookies present on the inbound request are
//     **dropped** rather than leaked to peer nodes (Gemini
//     security-medium on PR #692).
//   - The peer call carries the X-Admin-Fanout-Peer header so the
//     receiving handler can short-circuit its own fan-out (Claude
//     bot P1 on PR #692; the receiving check is in
//     KeyVizHandler.ServeHTTP and is exercised by
//     TestKeyVizHandlerSkipsFanoutForPeerCall in
//     keyviz_handler_test.go).
func TestKeyVizFanoutRunForwardsCookies(t *testing.T) {
	t.Parallel()
	var seenRequests []*http.Request
	var seenMu sync.Mutex
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/admin/api/v1/keyviz/matrix") {
			http.NotFound(w, r)
			return
		}
		seenMu.Lock()
		seenRequests = append(seenRequests, r.Clone(context.Background()))
		seenMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(KeyVizMatrix{Series: keyVizSeriesReads})
	}))
	defer peer.Close()

	f := NewKeyVizFanout("self:8080", []string{peer.URL}).WithHTTPClient(peer.Client())
	cookies := []*http.Cookie{
		{Name: "admin_session", Value: "session-token-abc"},
		{Name: "admin_csrf", Value: "csrf-token-def"},
		{Name: "unrelated_app_session", Value: "should-not-leak"},
	}
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, KeyVizMatrix{Series: keyVizSeriesReads}, cookies)
	require.NotNil(t, merged.Fanout, "fan-out result must be present")
	require.Len(t, merged.Fanout.Nodes, 2, "self + 1 peer = 2 nodes (Claude bot P2 on PR #692)")
	require.True(t, merged.Fanout.Nodes[1].OK)

	seenMu.Lock()
	defer seenMu.Unlock()
	require.Len(t, seenRequests, 1)
	got := seenRequests[0].Cookies()
	names := make(map[string]string, len(got))
	for _, c := range got {
		names[c.Name] = c.Value
	}
	require.Equal(t, "session-token-abc", names["admin_session"], "session cookie must be forwarded verbatim")
	require.Equal(t, "csrf-token-def", names["admin_csrf"], "csrf cookie forwarded; harmless on a GET but kept for parity")
	require.NotContains(t, names, "unrelated_app_session",
		"unrelated cookies must be dropped; only admin_session/admin_csrf are whitelisted")
	require.Equal(t, "1", seenRequests[0].Header.Get(keyVizFanoutPeerHeader),
		"peer marker header must be set so the receiver short-circuits its own fan-out")
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
	peer := newKeyVizPeerStub(t, peerMatrix)
	defer peer.Close()

	f := NewKeyVizFanout("self:8080", []string{peer.URL}).WithHTTPClient(peer.Client())
	local := KeyVizMatrix{
		ColumnUnixMs: []int64{1_700_000_000_000},
		Series:       keyVizSeriesReads,
		Rows: []KeyVizRow{
			{BucketID: "route:1", Values: []uint64{3}},
		},
	}

	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, local, nil)
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

	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, local, nil)
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
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, KeyVizMatrix{Series: keyVizSeriesReads}, nil)
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
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesWrites, rows: 1024}, local, nil)
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
	first := newKeyVizPeerStub(t, matrix)
	defer first.Close()
	second := newKeyVizPeerStub(t, matrix)
	defer second.Close()

	f := NewKeyVizFanout("self:8080", []string{first.URL, second.URL}).WithHTTPClient(first.Client())
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, matrix, nil)
	require.Equal(t, []FanoutNodeStatus{
		{Node: "self:8080", OK: true},
		{Node: first.URL, OK: true},
		{Node: second.URL, OK: true},
	}, merged.Fanout.Nodes)
}

// TestKeyVizFanoutRunPeerExceedsBodyLimit pins the over-cap path
// (Claude bot round-2 on PR #686). Lowering the per-peer limit to a
// small test value lets us drive the path without serving 64 MiB.
// The peer streams a body deliberately larger than the cap; the
// aggregator's countingReader must:
//   - Bound how many bytes are pulled off the wire (the LimitReader
//     enforces the security property).
//   - Detect the overshoot reliably even when the json.Decoder
//     buffers the trailing bytes internally.
//
// What we assert: the call returns within the test timeout (no hang),
// the per-node status surfaces, and the response carries the
// expected number of node entries. The warning log is best-effort
// and not asserted directly — the reliability of the byte-counting
// is the load-bearing invariant.
func TestKeyVizFanoutRunPeerExceedsBodyLimit(t *testing.T) {
	t.Parallel()
	bigRow := KeyVizRow{
		BucketID: "route:overshoot",
		Values:   []uint64{1, 2, 3, 4},
	}
	rows := make([]KeyVizRow, 256)
	for i := range rows {
		rows[i] = bigRow
	}
	body := KeyVizMatrix{
		ColumnUnixMs: []int64{1_700_000_000_000, 1_700_000_001_000, 1_700_000_002_000, 1_700_000_003_000},
		Rows:         rows,
		Series:       keyVizSeriesReads,
	}
	peer := newKeyVizPeerStub(t, body)
	defer peer.Close()

	const testCap int64 = 1024
	f := NewKeyVizFanout("self:8080", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithResponseBodyLimit(testCap)

	start := time.Now()
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, KeyVizMatrix{Series: keyVizSeriesReads}, nil)
	require.Less(t, time.Since(start), 5*time.Second, "decode must respect the size cap and complete promptly")
	require.NotNil(t, merged.Fanout)
	require.Equal(t, 2, merged.Fanout.Expected)
	require.Len(t, merged.Fanout.Nodes, 2)
	require.Equal(t, "self:8080", merged.Fanout.Nodes[0].Node)
	require.True(t, merged.Fanout.Nodes[0].OK, "self always reports ok")
	// Peer status: decode either errored on the truncated body
	// (ok=false) or succeeded on a partial matrix (ok=true). Either
	// is fine — what we are pinning is the bound, not the outcome.
}

// TestKeyVizFanoutRunPeerNearCapSucceedsWithWarning pins the
// warning-fires path Claude bot round-2 flagged on PR #686. A body
// whose JSON ends within the cap but whose total length (with
// trailing whitespace) overruns the cap exercises the case where
// the decoder returns success but countingReader.n > cap. The
// peer entry surfaces ok=true; the warning log is emitted by the
// aggregator (best-effort, not asserted from the test).
//
// Construction: minimal JSON envelope (~30 B) + 256 B of trailing
// whitespace, against a 100 B cap. json.Decoder reads in bufio
// chunks, so the LimitReader hands it cap+1 = 101 bytes; the
// decoder sees the complete object and returns nil, leaving
// cr.n == 101 > 100 → the warning condition is true.
func TestKeyVizFanoutRunPeerNearCapSucceedsWithWarning(t *testing.T) {
	t.Parallel()
	tiny := KeyVizMatrix{Series: keyVizSeriesReads}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/admin/api/v1/keyviz/matrix") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(tiny); err != nil {
			t.Fatalf("encode tiny: %v", err)
		}
		_, _ = io.WriteString(w, strings.Repeat(" ", 256))
	}))
	defer peer.Close()

	const testCap int64 = 100
	f := NewKeyVizFanout("self:8080", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithResponseBodyLimit(testCap)

	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, KeyVizMatrix{Series: keyVizSeriesReads}, nil)
	require.NotNil(t, merged.Fanout)
	require.Len(t, merged.Fanout.Nodes, 2)
	require.True(t, merged.Fanout.Nodes[0].OK, "self always reports ok")
	require.True(t, merged.Fanout.Nodes[1].OK,
		"near-cap success path: small JSON with trailing whitespace must decode despite the cap; got error %q",
		merged.Fanout.Nodes[1].Error)
}

// TestKeyVizFanoutRunPeerOverlargeBody pins the security-high
// review item on PR #686: a peer that streams more than
// keyVizPeerResponseBodyLimit bytes must not pin a goroutine on the
// JSON decoder or balloon memory. The aggregator caps the decode at
// the configured limit and surfaces a warning log rather than
// silently accepting a truncated matrix.
func TestKeyVizFanoutRunPeerOverlargeBody(t *testing.T) {
	t.Parallel()
	// Build a JSON payload whose `rows` array is enormous: many
	// rows of 4096 zeroed values. We do not actually need to exceed
	// the production cap (64 MiB) — we just need to assert that the
	// decode completes promptly and that the peer call ends up
	// reporting OK with a row count that matches what was on the wire
	// up to the cap.
	hugeRow := KeyVizRow{
		BucketID: "route:big",
		Values:   make([]uint64, 4096),
	}
	rows := make([]KeyVizRow, 64)
	for i := range rows {
		rows[i] = hugeRow
	}
	body := KeyVizMatrix{
		ColumnUnixMs: make([]int64, 4096),
		Rows:         rows,
		Series:       keyVizSeriesReads,
	}
	peer := newKeyVizPeerStub(t, body)
	defer peer.Close()

	f := NewKeyVizFanout("self:8080", []string{peer.URL}).WithHTTPClient(peer.Client())

	start := time.Now()
	merged := f.Run(context.Background(), keyVizParams{series: keyVizSeriesReads, rows: 1024}, KeyVizMatrix{Series: keyVizSeriesReads}, nil)
	require.Less(t, time.Since(start), 5*time.Second, "decode must respect the size cap and complete promptly")
	require.NotNil(t, merged.Fanout)
	require.True(t, merged.Fanout.Nodes[1].OK, "in-cap response must succeed; cap is 64 MiB and the synthetic body is well under that")
}

// newKeyVizPeerStub spins up an httptest.Server that answers
// /admin/api/v1/keyviz/matrix with a fixed 200 JSON body. Anything
// else returns 404 — which surfaces as "peer status 404" in the
// aggregator and lets a future test assert the path verbatim.
//
// Tests that need a non-200 response build their handler inline
// (see TestKeyVizFanoutRunPeerHTTPError); this helper covers the
// common happy-path stub.
func newKeyVizPeerStub(t *testing.T, body KeyVizMatrix) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/admin/api/v1/keyviz/matrix") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Logf("encode peer body: %v", err)
		}
	}))
	return srv
}

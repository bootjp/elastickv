package admin

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// silentLogger discards every record so tests don't pollute -v output
// with warnings the test is intentionally provoking (timeouts, decode
// failures, body-cap breaches).
func silentLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(&strings.Builder{}, nil))
}

// TestHotKeysFanout_NoPeers_EchoesLocalWithSelfStatus pins the
// single-node fast path: Run with an empty peer list returns the
// local response unchanged plus a Fanout block showing the local
// node as the only responder.
func TestHotKeysFanout_NoPeers_EchoesLocalWithSelfStatus(t *testing.T) {
	t.Parallel()
	f := NewKeyVizHotKeysFanout("nodeA", nil)
	local := hotKeyResponse{
		RouteID:    7,
		Series:     "writes",
		SampleRate: 16,
		SampledN:   100,
		Keys: []hotKeyResponseEntry{
			{KeyB64: b64("hot"), Count: 64},
		},
		SnapshotAt: time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC),
	}
	out := f.Run(context.Background(), hotKeysParams{routeID: 7, top: 20}, local, nil)
	require.NotNil(t, out.Fanout)
	require.Equal(t, 1, out.Fanout.Responded)
	require.Equal(t, 1, out.Fanout.Expected)
	require.Equal(t, "nodeA", out.Fanout.Nodes[0].Node)
	require.True(t, out.Fanout.Nodes[0].OK)
	require.Equal(t, local.Keys, out.Keys)
	require.Equal(t, local.SampledN, out.SampledN)
}

// TestMergeHotKeyResponses_DesignSection6 covers each merge rule in
// the design §6 table: SUM on counts/bounds/sampled_n/drops/skips,
// MAX on sample_rate, OR on degraded, latest on snapshot_at, with the
// top-K truncation and the tie-break sort applied.
func TestMergeHotKeyResponses_DesignSection6(t *testing.T) {
	t.Parallel()
	tcases := []struct {
		name     string
		params   hotKeysParams
		inputs   []hotKeyResponse
		expected hotKeyResponse
	}{
		{
			name:   "two peers, distinct keys → all preserved",
			params: hotKeysParams{routeID: 1, series: "writes", top: 20},
			inputs: []hotKeyResponse{
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					Keys: []hotKeyResponseEntry{{KeyB64: b64("a"), Count: 100}},
				},
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					Keys: []hotKeyResponseEntry{{KeyB64: b64("b"), Count: 50}},
				},
			},
			expected: hotKeyResponse{
				RouteID: 1, Series: "writes", SampleRate: 16,
				Approximate: true,
				Keys: []hotKeyResponseEntry{
					{KeyB64: b64("a"), Count: 100},
					{KeyB64: b64("b"), Count: 50},
				},
			},
		},
		{
			name:   "two peers, overlapping key → SUM counts",
			params: hotKeysParams{routeID: 1, series: "writes", top: 20},
			inputs: []hotKeyResponse{
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					Keys: []hotKeyResponseEntry{{KeyB64: b64("hot"), Count: 80}},
				},
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					Keys: []hotKeyResponseEntry{{KeyB64: b64("hot"), Count: 30}},
				},
			},
			expected: hotKeyResponse{
				RouteID: 1, Series: "writes", SampleRate: 16,
				Approximate: true,
				Keys: []hotKeyResponseEntry{
					{KeyB64: b64("hot"), Count: 110},
				},
			},
		},
		{
			name:   "sample_rate MAX, sampled_n SUM, error_bound SUM",
			params: hotKeysParams{routeID: 1, series: "writes", top: 20},
			inputs: []hotKeyResponse{
				{
					RouteID: 1, Series: "writes",
					SampleRate: 16, SampledN: 1000, ErrorBound: 250,
					Keys: []hotKeyResponseEntry{{KeyB64: b64("a"), Count: 100}},
				},
				{
					RouteID: 1, Series: "writes",
					SampleRate: 64, SampledN: 500, ErrorBound: 2000,
					Keys: []hotKeyResponseEntry{{KeyB64: b64("a"), Count: 50}},
				},
			},
			expected: hotKeyResponse{
				RouteID: 1, Series: "writes",
				Approximate: true,
				SampleRate:  64, // MAX
				SampledN:    1500,
				ErrorBound:  2250,
				Keys: []hotKeyResponseEntry{
					{KeyB64: b64("a"), Count: 150},
				},
			},
		},
		{
			name:   "degraded ORs across drops + skips",
			params: hotKeysParams{routeID: 1, series: "writes", top: 20},
			inputs: []hotKeyResponse{
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					DroppedSamples: 7,
					Keys:           []hotKeyResponseEntry{{KeyB64: b64("a"), Count: 100}},
				},
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					SkippedLongKeys: 3,
					Keys:            []hotKeyResponseEntry{{KeyB64: b64("a"), Count: 50}},
				},
			},
			expected: hotKeyResponse{
				RouteID: 1, Series: "writes", SampleRate: 16,
				Approximate:     true,
				DroppedSamples:  7,
				SkippedLongKeys: 3,
				Degraded:        true, // drops OR skips
				Keys: []hotKeyResponseEntry{
					{KeyB64: b64("a"), Count: 150},
				},
			},
		},
		{
			name:   "snapshot_at MAX",
			params: hotKeysParams{routeID: 1, series: "writes", top: 20},
			inputs: []hotKeyResponse{
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					SnapshotAt: time.Date(2026, 5, 29, 9, 0, 0, 0, time.UTC),
					Keys:       []hotKeyResponseEntry{{KeyB64: b64("a"), Count: 1}},
				},
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					SnapshotAt: time.Date(2026, 5, 29, 9, 0, 5, 0, time.UTC),
					Keys:       []hotKeyResponseEntry{{KeyB64: b64("a"), Count: 1}},
				},
			},
			expected: hotKeyResponse{
				RouteID: 1, Series: "writes", SampleRate: 16,
				Approximate: true,
				SnapshotAt:  time.Date(2026, 5, 29, 9, 0, 5, 0, time.UTC),
				Keys: []hotKeyResponseEntry{
					{KeyB64: b64("a"), Count: 2},
				},
			},
		},
		{
			name:   "top truncates after merge",
			params: hotKeysParams{routeID: 1, series: "writes", top: 2},
			inputs: []hotKeyResponse{
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					Keys: []hotKeyResponseEntry{
						{KeyB64: b64("a"), Count: 5},
						{KeyB64: b64("b"), Count: 50},
						{KeyB64: b64("c"), Count: 100},
					},
				},
			},
			expected: hotKeyResponse{
				RouteID: 1, Series: "writes", SampleRate: 16,
				Approximate: true,
				Keys: []hotKeyResponseEntry{
					{KeyB64: b64("c"), Count: 100},
					{KeyB64: b64("b"), Count: 50},
				},
			},
		},
		{
			name:   "tie-break by KeyB64 ascending for deterministic output",
			params: hotKeysParams{routeID: 1, series: "writes", top: 20},
			inputs: []hotKeyResponse{
				{
					RouteID: 1, Series: "writes", SampleRate: 16,
					Keys: []hotKeyResponseEntry{
						{KeyB64: b64("zzz"), Count: 100},
						{KeyB64: b64("aaa"), Count: 100},
					},
				},
			},
			expected: hotKeyResponse{
				RouteID: 1, Series: "writes", SampleRate: 16,
				Approximate: true,
				Keys: []hotKeyResponseEntry{
					{KeyB64: b64("aaa"), Count: 100},
					{KeyB64: b64("zzz"), Count: 100},
				},
			},
		},
	}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mergeHotKeyResponses(tc.inputs, tc.params)
			// Fanout is set by Run, not merge; tests do not assert on it here.
			got.Fanout = nil
			require.Equal(t, tc.expected, got)
		})
	}
}

// TestBuildKeyVizHotKeysPeerURL covers the URL builder's three
// behaviours: host:port shorthand → http://, full URL pass-through,
// and the query-parameter forwarding (parsed values, not raw query).
func TestBuildKeyVizHotKeysPeerURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		peer   string
		params hotKeysParams
		want   string
	}{
		{
			name:   "host:port shorthand defaults to http://",
			peer:   "10.0.0.2:8080",
			params: hotKeysParams{routeID: 7, series: "writes", top: 20},
			want:   "http://10.0.0.2:8080/admin/api/v1/keyviz/hotkeys?route_id=7&series=writes&top=20",
		},
		{
			name:   "full URL preserved",
			peer:   "https://node-b.internal:9443",
			params: hotKeysParams{routeID: 7, series: "writes", top: 20},
			want:   "https://node-b.internal:9443/admin/api/v1/keyviz/hotkeys?route_id=7&series=writes&top=20",
		},
		{
			name: "sub_bucket + from/to forwarded",
			peer: "10.0.0.2:8080",
			params: hotKeysParams{
				routeID: 3, subBucket: 5, subBucketSet: true,
				series: "writes", top: 50,
				fromUnixMs: 1_700_000_000_000,
				toUnixMs:   1_700_000_010_000,
			},
			want: "http://10.0.0.2:8080/admin/api/v1/keyviz/hotkeys?from_unix_ms=1700000000000&route_id=3&series=writes&sub_bucket=5&to_unix_ms=1700000010000&top=50",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildKeyVizHotKeysPeerURL(tc.peer, tc.params)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestHotKeysFanout_AntiRecursionHeader pins the recursion-guard
// invariant: when a peer's fetchPeer fires the request, the receiving
// handler must see the X-Admin-Fanout-Peer header and skip its own
// fan-out. The test runs an httptest peer that ASSERTS the header is
// present.
func TestHotKeysFanout_AntiRecursionHeader(t *testing.T) {
	t.Parallel()
	peerHits := 0
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerHits++
		require.Equal(t, "1", r.Header.Get(keyVizFanoutPeerHeader),
			"fan-out fetchPeer must mark the peer call with %s", keyVizFanoutPeerHeader)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{
			RouteID: 1, Series: "writes", SampleRate: 16,
			Keys: []hotKeyResponseEntry{{KeyB64: b64("p"), Count: 10}},
		})
	}))
	defer peer.Close()

	f := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	local := hotKeyResponse{
		RouteID: 1, Series: "writes", SampleRate: 16,
		Keys: []hotKeyResponseEntry{{KeyB64: b64("local"), Count: 5}},
	}
	out := f.Run(context.Background(), hotKeysParams{routeID: 1, series: "writes", top: 20}, local, nil)
	require.Equal(t, 1, peerHits, "fan-out should call the peer exactly once")
	require.Equal(t, 2, out.Fanout.Expected, "self + 1 peer")
	require.Equal(t, 2, out.Fanout.Responded)
}

// TestHotKeysFanout_PeerErrorRecordedNotFatal verifies that a peer
// returning a non-OK status surfaces as ok=false in the per-node
// status but does not abort aggregation — the merged response still
// includes the local contribution.
func TestHotKeysFanout_PeerErrorRecordedNotFatal(t *testing.T) {
	t.Parallel()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Simulates a peer that has --keyvizEnabled but not
		// --keyvizHotKeysEnabled (mixed-K topology). Design §6
		// expects this peer's contribution to be omitted, not the
		// whole drill-down to fail.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "hotkeys_disabled", "message": "hot-keys drill-down is not enabled on this node",
		})
	}))
	defer peer.Close()

	f := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	local := hotKeyResponse{
		RouteID: 1, Series: "writes", SampleRate: 16,
		Keys: []hotKeyResponseEntry{{KeyB64: b64("local"), Count: 5}},
	}
	out := f.Run(context.Background(), hotKeysParams{routeID: 1, series: "writes", top: 20}, local, nil)
	require.Equal(t, 2, out.Fanout.Expected)
	require.Equal(t, 1, out.Fanout.Responded, "peer 503 records ok=false")
	require.False(t, out.Fanout.Nodes[1].OK)
	require.Contains(t, out.Fanout.Nodes[1].Error, "503")
	require.Len(t, out.Keys, 1, "local still contributes its key")
	require.Equal(t, b64("local"), out.Keys[0].KeyB64)
}

// TestHotKeysHandler_FanoutIntegration drives the full handler with
// an httptest peer attached so the wiring (ServeHTTP → fan-out → merge
// → response) is exercised end-to-end. The peer's response is merged
// with the local snapshot before being written to the wire.
func TestHotKeysHandler_FanoutIntegration(t *testing.T) {
	t.Parallel()
	snapshotAt := time.Date(2026, 5, 29, 10, 0, 0, 0, time.UTC)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{
			RouteID: 1, Series: "writes", Approximate: true,
			SampleRate: 16, SampledN: 800, ErrorBound: 100,
			Keys: []hotKeyResponseEntry{
				{KeyB64: b64("shared"), Count: 200},
				{KeyB64: b64("peer-only"), Count: 80},
			},
			SnapshotAt: snapshotAt,
		})
	}))
	defer peer.Close()

	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID: 1, SampledN: 400, SampleRate: 16, Capacity: 64,
		SnapshotAt: snapshotAt,
		Entries: []keyviz.KeyvizHotKeyEntry{
			{Key: []byte("shared"), Count: 10},    // 10 × 16 = 160 scaled
			{Key: []byte("local-only"), Count: 5}, // 5 × 16 = 80 scaled
		},
	}
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&top=10", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var got hotKeyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.NotNil(t, got.Fanout)
	require.Equal(t, 2, got.Fanout.Expected)
	require.Equal(t, 2, got.Fanout.Responded)
	keysByB64 := map[string]uint64{}
	for _, k := range got.Keys {
		keysByB64[k.KeyB64] = k.Count
	}
	// shared: local 160 + peer 200 = 360
	require.Equal(t, uint64(360), keysByB64[b64("shared")])
	// local-only: 80
	require.Equal(t, uint64(80), keysByB64[b64("local-only")])
	// peer-only: 80
	require.Equal(t, uint64(80), keysByB64[b64("peer-only")])
	// SampledN summed across self + peer
	require.Equal(t, uint64(400+800), got.SampledN)
}

// TestHotKeysHandler_FanoutSkippedOnPeerHeader pins the
// anti-recursion path inside the handler: when an inbound request
// already carries X-Admin-Fanout-Peer, ServeHTTP must NOT call
// h.fanout.Run, so the peer does not in turn call back into us.
func TestHotKeysHandler_FanoutSkippedOnPeerHeader(t *testing.T) {
	t.Parallel()
	// Spin up an httptest peer that records hits so we can prove the
	// handler did not fan out a second time. If the recursion guard
	// is missing, this peer would be called.
	peerHits := 0
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerHits++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{RouteID: 1, Series: "writes", SampleRate: 16})
	}))
	defer peer.Close()

	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID: 1, SampledN: 10, SampleRate: 16, Capacity: 64,
		SnapshotAt: time.Now().UTC(),
		Entries:    []keyviz.KeyvizHotKeyEntry{{Key: []byte("k"), Count: 1}},
	}
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	req := httptest.NewRequestWithContext(context.Background(), "GET",
		"/admin/api/v1/keyviz/hotkeys?route_id=1", nil)
	req.Header.Set(keyVizFanoutPeerHeader, "1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 0, peerHits, "fan-out must be suppressed when X-Admin-Fanout-Peer is set on the inbound request")
	var got hotKeyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Nil(t, got.Fanout, "no fan-out → no Fanout block in the response")
}

// b64 is the test-side base64 encoder mirror of the server's
// encoding in buildHotKeysResponse; keeps the test fixtures readable
// without sprinkling base64.StdEncoding.EncodeToString everywhere.
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

package admin

import (
	"context"
	"encoding/base64"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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
			want:   "http://10.0.0.2:8080/admin/api/v1/keyviz/hotkeys?route_id=7&series=writes&top=256",
		},
		{
			name:   "full URL preserved",
			peer:   "https://node-b.internal:9443",
			params: hotKeysParams{routeID: 7, series: "writes", top: 20},
			want:   "https://node-b.internal:9443/admin/api/v1/keyviz/hotkeys?route_id=7&series=writes&top=256",
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
			want: "http://10.0.0.2:8080/admin/api/v1/keyviz/hotkeys?from_unix_ms=1700000000000&route_id=3&series=writes&sub_bucket=5&to_unix_ms=1700000010000&top=256",
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
	// peerHits is written from the httptest goroutine and read from
	// the test goroutine; atomic.Int32 keeps the access correctly
	// synchronised so a future -race sweep cannot surface a false
	// negative (claude bot round-1 minor).
	var peerHits atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerHits.Add(1)
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
	require.Equal(t, int32(1), peerHits.Load(), "fan-out should call the peer exactly once")
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
	// ErrorBound: local scaledErrorBound(400 sampledN, 16 R, 64 cap)
	// = 16*400/64 = 100; peer reported 100; merged = SUM = 200.
	// Pins the SUM-not-MAX choice end-to-end (claude bot round-1).
	require.Equal(t, uint64(200), got.ErrorBound)
}

// TestHotKeysHandler_FanoutFillsNilLocalFromPeer pins the gemini
// HIGH fix: when this node has no snapshot for the requested route
// but a peer does, the handler MUST fan out and surface the peer's
// data rather than 404 before consulting peers. Without the fix, a
// route that lives only on a peer would 404 depending on which node
// the SPA happened to hit.
func TestHotKeysHandler_FanoutFillsNilLocalFromPeer(t *testing.T) {
	t.Parallel()
	snapshotAt := time.Date(2026, 5, 29, 11, 0, 0, 0, time.UTC)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{
			RouteID: 9, Series: "writes", Approximate: true,
			SampleRate: 16, SampledN: 500, ErrorBound: 125,
			Keys: []hotKeyResponseEntry{
				{KeyB64: b64("peer-hot"), Count: 320},
			},
			SnapshotAt: snapshotAt,
		})
	}))
	defer peer.Close()

	// stub deliberately leaves snapshots[9] unset → snap == nil.
	src := newStubSource()
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=9", nil)
	require.Equal(t, http.StatusOK, rec.Code, "nil local + peer-with-data must serve 200, not the pre-fix 404")
	var got hotKeyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Keys, 1)
	require.Equal(t, b64("peer-hot"), got.Keys[0].KeyB64)
	require.Equal(t, uint64(320), got.Keys[0].Count)
	require.Equal(t, uint64(500), got.SampledN, "peer's SampledN survives the empty-local merge")
	require.NotNil(t, got.Fanout)
	require.Equal(t, 2, got.Fanout.Responded)
}

// TestHotKeysHandler_FanoutNilLocalAllPeersEmpty pins the back-half
// of the gemini HIGH fix: extending past the early 404 must not
// turn an unanimously-empty cluster into a 200 with empty keys —
// the post-fan-out check must downgrade it back to 404 so the SPA's
// "no snapshot" branch still fires.
func TestHotKeysHandler_FanoutNilLocalAllPeersEmpty(t *testing.T) {
	t.Parallel()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Peer also has no snapshot for the route. Send the same 404
		// shape the real handler emits; the fan-out records ok=false
		// and the merger sees no contribution from this peer.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "no_snapshot", "message": "no hot-keys snapshot",
		})
	}))
	defer peer.Close()

	src := newStubSource() // empty snapshots
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=9", nil)
	require.Equal(t, http.StatusNotFound, rec.Code,
		"unanimously empty cluster must still 404, not 200-with-empty-keys")
	require.Contains(t, rec.Body.String(), "no_snapshot")
}

// TestHotKeysHandler_FanoutNilLocalPeerHasEmptySnapshot pins the
// codex P2 round-4 fix: when this node has no snapshot but a peer
// returns a VALID empty snapshot (a tracked route that's quiet in
// the latest window — keys=[], sampled_n=0, snapshot_at non-zero),
// the merged response must be 200 with empty keys, not 404. The
// single-node path returns 200 for a non-nil snapshot even when
// quiet; the multi-node path must match. Without the fix the
// `len(Keys)==0 && SampledN==0` post-check would downgrade to 404
// and the response would depend on which node the operator hit.
func TestHotKeysHandler_FanoutNilLocalPeerHasEmptySnapshot(t *testing.T) {
	t.Parallel()
	snapshotAt := time.Date(2026, 5, 29, 15, 0, 0, 0, time.UTC)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Peer's route is tracked but quiet: valid 200 with an
		// empty Keys slice, SampledN=0, but a real SnapshotAt.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{
			RouteID: 9, Series: "writes", Approximate: true,
			SampleRate: 16, SampledN: 0,
			Keys:       []hotKeyResponseEntry{},
			SnapshotAt: snapshotAt,
		})
	}))
	defer peer.Close()

	src := newStubSource() // no local snapshot for route 9
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=9", nil)
	require.Equal(t, http.StatusOK, rec.Code,
		"valid empty peer snapshot must be 200 with empty keys, not 404 (matches single-node path)")
	var got hotKeyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Empty(t, got.Keys)
	require.Equal(t, snapshotAt, got.SnapshotAt.UTC(),
		"peer's SnapshotAt must survive the merge so the SPA sees 'tracked but quiet' rather than 'no snapshot'")
	require.NotNil(t, got.Fanout)
	require.Equal(t, 2, got.Fanout.Responded)
}

// TestHotKeysHandler_FanoutSuppressedOnPeerHeaderEvenWithNilLocal
// guards the recursion invariant under the gemini HIGH fix: when an
// inbound request already carries X-Admin-Fanout-Peer, the handler
// must continue to 404 on a nil local snapshot rather than fan out
// (which would recurse). Without this gate, every peer receiving a
// no_snapshot probe would fan out to every other peer and the
// O(N²) blast pattern would return.
func TestHotKeysHandler_FanoutSuppressedOnPeerHeaderEvenWithNilLocal(t *testing.T) {
	t.Parallel()
	var peerHits atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{RouteID: 9, Series: "writes", SampleRate: 16})
	}))
	defer peer.Close()

	src := newStubSource() // empty
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	req := httptest.NewRequestWithContext(context.Background(), "GET",
		"/admin/api/v1/keyviz/hotkeys?route_id=9", nil)
	req.Header.Set(keyVizFanoutPeerHeader, "1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code,
		"peer-header request with nil local must 404, not recursively fan out")
	require.Equal(t, int32(0), peerHits.Load(),
		"recursion guard must prevent any outgoing peer call")
}

// TestHotKeysHandler_MixedKLocalDisabledPeerEnabled pins the codex
// P2 round-3 fix: when this node has KeyViz sampling on but
// --keyvizHotKeysEnabled=false, the cluster-wide hot-keys endpoint
// must still fan out and surface a peer's data instead of returning
// 503 unconditionally. Per design §6, mixed-K topologies omit the
// local non-sampling contribution and merge whatever peers report.
// Without the fix, the response would depend on which admin node the
// operator (or load balancer) happens to hit.
func TestHotKeysHandler_MixedKLocalDisabledPeerEnabled(t *testing.T) {
	t.Parallel()
	snapshotAt := time.Date(2026, 5, 29, 14, 0, 0, 0, time.UTC)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{
			RouteID: 1, Series: "writes", Approximate: true,
			SampleRate: 16, SampledN: 250, ErrorBound: 62,
			Keys:       []hotKeyResponseEntry{{KeyB64: b64("peer-hot"), Count: 480}},
			SnapshotAt: snapshotAt,
		})
	}))
	defer peer.Close()

	// Local source has KeyViz on but hot-keys deliberately off.
	src := newStubSource()
	src.enabled = false
	src.capacity = 0 // sampler reports 0 capacity when hot-keys is off
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1", nil)
	require.Equal(t, http.StatusOK, rec.Code,
		"mixed-K (local !enabled + peer enabled with data) must serve 200, not 503")
	var got hotKeyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Keys, 1)
	require.Equal(t, b64("peer-hot"), got.Keys[0].KeyB64)
	require.Equal(t, uint64(480), got.Keys[0].Count)
	require.Equal(t, uint64(250), got.SampledN, "peer's SampledN survives the empty-local merge")
	require.Equal(t, uint64(62), got.ErrorBound, "peer's ErrorBound survives the empty-local merge")
	require.NotNil(t, got.Fanout)
	require.Equal(t, 2, got.Fanout.Responded)
}

// TestHotKeysHandler_MixedKLocalDisabledNoFanoutStill503 confirms
// the codex P2 fix only relaxes the 503 when fan-out CAN reach
// peers. With no fan-out configured a hot-keys-off node still
// returns 503 hotkeys_disabled — there is no peer to consult, and
// silently 404-ing would hide the configuration error from the
// operator.
func TestHotKeysHandler_MixedKLocalDisabledNoFanoutStill503(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.enabled = false
	src.capacity = 0
	h := newHandler(t, src) // no fan-out

	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1", nil)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "hotkeys_disabled")
}

// TestHotKeysHandler_MixedKPeerCallStill503 pins the anti-recursion
// invariant under mixed-K: an inbound request carrying the
// X-Admin-Fanout-Peer header must NOT trigger fan-out even on a
// disabled-local node. Without this guard, a sampling peer asking
// this node for its contribution would cause us to fan back out to
// every other peer the operator listed — exactly the O(N²) blast
// the recursion guard exists to prevent.
func TestHotKeysHandler_MixedKPeerCallStill503(t *testing.T) {
	t.Parallel()
	var peerHits atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{RouteID: 1, Series: "writes", SampleRate: 16})
	}))
	defer peer.Close()

	src := newStubSource()
	src.enabled = false
	src.capacity = 0
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

	require.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"peer-header request on disabled-local must 503, not fan out")
	require.Equal(t, int32(0), peerHits.Load(),
		"recursion guard must prevent any outgoing peer call even under mixed-K")
}

// TestHotKeysFanout_PeerTopRequestsSketchCeiling pins the Codex P1
// round-1 fix: peer URLs MUST request the SS sketch ceiling (the max
// possible per-route capacity) instead of forwarding the operator's
// `top`. Otherwise each peer truncates to its own top-K before merge
// and a key whose true cluster rank is high but per-peer rank falls
// below the cut is silently dropped.
func TestHotKeysFanout_PeerTopRequestsSketchCeiling(t *testing.T) {
	t.Parallel()
	url, err := buildKeyVizHotKeysPeerURL("10.0.0.2:8080",
		hotKeysParams{routeID: 1, series: "writes", top: 1})
	require.NoError(t, err)
	require.Contains(t, url, "top="+itoa(keyviz.MaxHotKeysPerRoute),
		"peer URL must request the sketch ceiling, not the operator's top")
}

// TestHotKeysHandler_LocalTopDeferredUntilAfterFanoutMerge pins the
// Codex P1 round-2 fix: even after peer URLs request the sketch
// ceiling, the LOCAL node's collectEntries must ALSO defer its `top`
// truncation until after the merge. Otherwise a key whose true
// cluster rank is high but whose per-NODE local rank falls below
// `top` is silently dropped before peers' counts get a chance to
// promote it to cluster top-K.
//
// Concrete scenario (top=1):
//   - local snapshot:  x=100, z=99
//   - peer snapshot:   z=99 only
//
// True cluster top is z = 99 (local) + 99 (peer) = 198, not
// x = 100. Without the round-2 fix the merger would see only x
// locally (z pre-truncated) and report x. With the fix the local
// contribution carries BOTH candidates so the merge sums z
// correctly.
func TestHotKeysHandler_LocalTopDeferredUntilAfterFanoutMerge(t *testing.T) {
	t.Parallel()
	snapshotAt := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hotKeyResponse{
			RouteID: 1, Series: "writes", Approximate: true,
			SampleRate: 16, SampledN: 100,
			Keys:       []hotKeyResponseEntry{{KeyB64: b64("z"), Count: 99}},
			SnapshotAt: snapshotAt,
		})
	}))
	defer peer.Close()

	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID: 1, SampledN: 100, SampleRate: 16, Capacity: 64,
		SnapshotAt: snapshotAt,
		Entries: []keyviz.KeyvizHotKeyEntry{
			{Key: []byte("x"), Count: 100}, // scaled 1600
			{Key: []byte("z"), Count: 99},  // scaled 1584
		},
	}
	fanout := NewKeyVizHotKeysFanout("self", []string{peer.URL}).
		WithHTTPClient(peer.Client()).
		WithLogger(silentLogger())
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(silentLogger()).
		WithFanout(fanout)

	// top=1. Cluster sums: z = 1584 + 99 = 1683, x = 1600.
	// Without the round-2 fix the local would discard z before
	// the merger ever sees the peer's contribution and report x.
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&top=1", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var got hotKeyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Keys, 1, "operator asked for top=1; merged response must be size 1")
	require.Equal(t, b64("z"), got.Keys[0].KeyB64,
		"merged top-1 must be z (cluster sum 1683) not x (cluster sum 1600); local pre-truncation must NOT discard z before the merge sees the peer's contribution")
	require.Equal(t, uint64(1683), got.Keys[0].Count)
}

// itoa is a tiny strconv.Itoa wrapper that keeps the test imports
// minimal (the file already pulls in goccy/go-json for one decoder
// and stdlib strconv would otherwise be the only direct stdlib add).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TestHotKeysHandler_FanoutSkippedOnPeerHeader pins the
// anti-recursion path inside the handler: when an inbound request
// already carries X-Admin-Fanout-Peer, ServeHTTP must NOT call
// h.fanout.Run, so the peer does not in turn call back into us.
func TestHotKeysHandler_FanoutSkippedOnPeerHeader(t *testing.T) {
	t.Parallel()
	// Spin up an httptest peer that records hits so we can prove the
	// handler did not fan out a second time. If the recursion guard
	// is missing, this peer would be called. atomic.Int32 keeps the
	// cross-goroutine read race-free under -race.
	var peerHits atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerHits.Add(1)
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
	require.Equal(t, int32(0), peerHits.Load(), "fan-out must be suppressed when X-Admin-Fanout-Peer is set on the inbound request")
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

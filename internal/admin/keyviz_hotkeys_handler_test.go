package admin

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
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

// stubHotKeysSource mocks the sampler so handler tests don't have to
// drive a real *keyviz.MemSampler (clock + goroutine + tick). Setting
// disabled=true reproduces the --keyvizHotKeysEnabled=false path.
type stubHotKeysSource struct {
	enabled    bool
	capacity   int
	sampleRate int
	maxKeyLen  int
	snapshots  map[uint64]*keyviz.KeyvizHotKeysSnapshot
	bounds     map[boundsKey]boundsValue
}

type boundsKey struct {
	routeID   uint64
	label     keyviz.Label
	subBucket int
}

type boundsValue struct {
	lo, hi []byte
	ok     bool
}

func (s *stubHotKeysSource) HotKeysOptions() (bool, int, int, int) {
	return s.enabled, s.capacity, s.sampleRate, s.maxKeyLen
}

func (s *stubHotKeysSource) HotKeysSnapshot(routeID uint64, label keyviz.Label) *keyviz.KeyvizHotKeysSnapshot {
	snap := s.snapshots[routeID]
	if snap == nil || snap.Label != label {
		return nil
	}
	return snap
}

func (s *stubHotKeysSource) SubBucketBoundsFor(routeID uint64, label keyviz.Label, subBucket int) ([]byte, []byte, bool) {
	v, ok := s.bounds[boundsKey{routeID: routeID, label: label, subBucket: subBucket}]
	if !ok {
		return nil, nil, false
	}
	return v.lo, v.hi, v.ok
}

func newStubSource() *stubHotKeysSource {
	return &stubHotKeysSource{
		enabled:    true,
		capacity:   64,
		sampleRate: 16,
		maxKeyLen:  1024,
		snapshots:  map[uint64]*keyviz.KeyvizHotKeysSnapshot{},
		bounds:     map[boundsKey]boundsValue{},
	}
}

func newHandler(t *testing.T, src KeyVizHotKeysSource) http.Handler {
	t.Helper()
	h := NewKeyVizHotKeysHandler(src).
		WithLogger(slog.New(slog.NewJSONHandler(&strings.Builder{}, nil)))
	return h
}

func TestHotKeysHandler_DisabledKeyVizReturns503(t *testing.T) {
	t.Parallel()
	h := newHandler(t, nil)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1", nil)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "keyviz_disabled")
}

func TestHotKeysHandler_DisabledHotKeysReturns503(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.enabled = false
	h := newHandler(t, src)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1", nil)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "hotkeys_disabled")
}

func TestHotKeysHandler_MissingRouteIDReturns400(t *testing.T) {
	t.Parallel()
	h := newHandler(t, newStubSource())
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "route_id is required")
}

func TestHotKeysHandler_InvalidRouteIDReturns400(t *testing.T) {
	t.Parallel()
	h := newHandler(t, newStubSource())
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=not-a-number", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHotKeysHandler_NoSnapshotReturns404(t *testing.T) {
	t.Parallel()
	src := newStubSource() // no snapshots inserted
	h := newHandler(t, src)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=42", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "no_snapshot")
}

func TestHotKeysHandler_WholeRouteReturnsScaledTopN(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID:    1,
		SampledN:   100,
		SampleRate: 16,
		Capacity:   64,
		SnapshotAt: time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Entries: []keyviz.KeyvizHotKeyEntry{
			{Key: []byte("a"), Count: 1},
			{Key: []byte("hot"), Count: 50},
			{Key: []byte("middling"), Count: 5},
		},
	}
	h := newHandler(t, src)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&top=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp hotKeyResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, uint64(1), resp.RouteID)
	require.Nil(t, resp.SubBucket)
	require.Equal(t, "writes", resp.Series)
	require.True(t, resp.Approximate)
	require.Equal(t, 16, resp.SampleRate)
	require.Equal(t, uint64(100), resp.SampledN)
	require.False(t, resp.Degraded)
	// error_bound = 16 × 100 / 64 = 25
	require.Equal(t, uint64(25), resp.ErrorBound)

	require.Len(t, resp.Keys, 2, "top=2 truncates")
	// Sorted descending by count. counts scaled by sample_rate (16):
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("hot")), resp.Keys[0].KeyB64)
	require.Equal(t, uint64(50*16), resp.Keys[0].Count, "count must be scaled by sample_rate")
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("middling")), resp.Keys[1].KeyB64)
	require.Equal(t, uint64(5*16), resp.Keys[1].Count)
}

func TestHotKeysHandler_LabelParamSelectsLabeledSnapshot(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID:    1,
		Label:      keyviz.LabelRedis,
		SampledN:   1,
		SampleRate: 1,
		Capacity:   8,
		SnapshotAt: time.Now().UTC(),
		Entries:    []keyviz.KeyvizHotKeyEntry{{Key: []byte("redis-key"), Count: 1}},
	}
	h := newHandler(t, src)

	unlabeled := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1", nil)
	require.Equal(t, http.StatusNotFound, unlabeled.Code)

	labeled := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&label=redis", nil)
	require.Equal(t, http.StatusOK, labeled.Code)
	var resp hotKeyResponse
	require.NoError(t, json.NewDecoder(labeled.Body).Decode(&resp))
	require.Equal(t, "redis", resp.Label)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("redis-key")), resp.Keys[0].KeyB64)
}

func TestHotKeysHandler_SubBucketFiltersByBytes(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID:    1,
		SampledN:   10,
		SampleRate: 1,
		Capacity:   8,
		SnapshotAt: time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Entries: []keyviz.KeyvizHotKeyEntry{
			{Key: []byte("aaa"), Count: 1},  // outside (below lo)
			{Key: []byte("mmm"), Count: 10}, // inside  [m, t)
			{Key: []byte("rrr"), Count: 3},  // inside
			{Key: []byte("zzz"), Count: 7},  // outside (>= hi)
			{Key: []byte("ttt"), Count: 99}, // outside (>= hi)
		},
	}
	// Sub-bucket 3 spans [m, t).
	src.bounds[boundsKey{routeID: 1, label: keyviz.LabelLegacy, subBucket: 3}] = boundsValue{lo: []byte("m"), hi: []byte("t"), ok: true}
	h := newHandler(t, src)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&sub_bucket=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp hotKeyResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.NotNil(t, resp.SubBucket)
	require.Equal(t, 3, *resp.SubBucket)
	require.Len(t, resp.Keys, 2, "only mmm and rrr fall in [m, t)")
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("mmm")), resp.Keys[0].KeyB64)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("rrr")), resp.Keys[1].KeyB64)
}

func TestHotKeysHandler_SubBucketUnboundedTailFilter(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID:    1,
		SampledN:   10,
		SampleRate: 1,
		Capacity:   8,
		SnapshotAt: time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Entries: []keyviz.KeyvizHotKeyEntry{
			{Key: []byte("aaa"), Count: 1},  // outside (below lo)
			{Key: []byte("xxx"), Count: 10}, // inside (≥ lo, hi=nil)
			{Key: []byte("zzz"), Count: 5},
		},
	}
	src.bounds[boundsKey{routeID: 1, label: keyviz.LabelLegacy, subBucket: 7}] = boundsValue{lo: []byte("m"), hi: nil, ok: true}
	h := newHandler(t, src)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&sub_bucket=7", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp hotKeyResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Len(t, resp.Keys, 2)
}

func TestHotKeysHandler_SubBucketOutOfRange(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{RouteID: 1, SnapshotAt: time.Now().UTC(), SampleRate: 1, Capacity: 8}
	// bounds map has nothing for (1, 99) → ok=false
	h := newHandler(t, src)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&sub_bucket=99", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "sub_bucket is out of range")
}

func TestHotKeysHandler_DegradedFlagOR(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		dropped, skipped uint64
		wantDegraded     bool
	}{
		{"both zero", 0, 0, false},
		{"only drops", 7, 0, true},
		{"only long-key skips", 0, 11, true},
		{"both", 1, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := newStubSource()
			src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
				RouteID:         1,
				SampledN:        10,
				DroppedSamples:  tc.dropped,
				SkippedLongKeys: tc.skipped,
				SampleRate:      1,
				Capacity:        8,
				SnapshotAt:      time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
			}
			h := newHandler(t, src)
			rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var resp hotKeyResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			require.Equal(t, tc.wantDegraded, resp.Degraded)
			require.Equal(t, tc.dropped, resp.DroppedSamples)
			require.Equal(t, tc.skipped, resp.SkippedLongKeys)
		})
	}
}

func TestHotKeysHandler_OutOfSnapshotWindow(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	snapAt := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID: 1, SampledN: 10, SampleRate: 1, Capacity: 8, SnapshotAt: snapAt,
	}
	h := NewKeyVizHotKeysHandler(src).WithSnapshotWindow(time.Minute)

	// Request a window 10 minutes in the past → outside [snapAt-1m, snapAt].
	past := snapAt.Add(-10 * time.Minute).UnixMilli()
	url := "/admin/api/v1/keyviz/hotkeys?route_id=1&from_unix_ms=" + intStr(past) +
		"&to_unix_ms=" + intStr(snapAt.Add(-9*time.Minute).UnixMilli())
	rec := serve(t, h, "GET", url, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "out_of_snapshot_window")

	// A window inside the snapshot window must pass.
	url2 := "/admin/api/v1/keyviz/hotkeys?route_id=1&from_unix_ms=" +
		intStr(snapAt.Add(-30*time.Second).UnixMilli()) +
		"&to_unix_ms=" + intStr(snapAt.UnixMilli())
	rec2 := serve(t, h, "GET", url2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
}

// TestHotKeysHandler_InvertedTimeWindowReturns400 pins the
// parseTimeWindowParams from > to rejection (round-2 reviewer 🟡): an
// inverted [from, to] window must be rejected at the parameter layer
// with 400 invalid_query so the snapshot-window check never gets to
// evaluate an upside-down interval.
func TestHotKeysHandler_InvertedTimeWindowReturns400(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{
		RouteID: 1, SampledN: 10, SampleRate: 1, Capacity: 8,
		SnapshotAt: time.Now().UTC(),
	}
	h := newHandler(t, src)
	// from = 5000ms, to = 1000ms (to < from).
	url := "/admin/api/v1/keyviz/hotkeys?route_id=1&from_unix_ms=5000&to_unix_ms=1000"
	rec := serve(t, h, "GET", url, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "invalid_query")
	require.Contains(t, body, "from_unix_ms")
	require.Contains(t, body, "to_unix_ms")
}

func TestHotKeysHandler_UnknownSeriesReturns400(t *testing.T) {
	t.Parallel()
	src := newStubSource()
	src.snapshots[1] = &keyviz.KeyvizHotKeysSnapshot{RouteID: 1, SnapshotAt: time.Now().UTC(), SampleRate: 1, Capacity: 8}
	h := newHandler(t, src)
	rec := serve(t, h, "GET", "/admin/api/v1/keyviz/hotkeys?route_id=1&series=reads", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "writes")
}

func TestHotKeysHandler_NonGetReturns405(t *testing.T) {
	t.Parallel()
	h := newHandler(t, newStubSource())
	rec := serve(t, h, "POST", "/admin/api/v1/keyviz/hotkeys?route_id=1", bytes.NewReader([]byte(`{}`)))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// serve fires a single request through h and returns the recorded
// response. body may be nil for GETs.
func serve(t *testing.T, h http.Handler, method, urlStr string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), method, urlStr, body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// intStr is strconv.FormatInt(x, 10) without importing strconv into every
// test that wants a single conversion.
func intStr(v int64) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

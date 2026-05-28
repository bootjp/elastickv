package admin

import (
	"bytes"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	"github.com/goccy/go-json"
)

// KeyVizHotKeysSource is the narrow contract the hot-keys drill-down
// handler depends on. Production wires a *keyviz.MemSampler; tests use a
// stub that returns canned snapshots / bounds.
//
// HotKeysSnapshot must be safe to call concurrently with the
// aggregator's publish path (atomic.Pointer.Load — see keyviz/hot_keys.go).
// SubBucketBoundsFor returns ok=false when the route is unknown, an
// aggregate, or the index is out of range; nil hi means unbounded tail.
type KeyVizHotKeysSource interface {
	HotKeysOptions() (enabled bool, capacity, sampleRate, maxKeyLen int)
	HotKeysSnapshot(routeID uint64) *keyviz.KeyvizHotKeysSnapshot
	SubBucketBoundsFor(routeID uint64, subBucket int) (lo, hi []byte, ok bool)
}

// KeyVizHotKeysHandler serves GET /admin/api/v1/keyviz/hotkeys.
//
// The route is mounted on the admin auth chain (SessionAuth + audit) and
// further gated by the full-access role at the router — design §7
// (privacy/auth) requires both because the response carries actual user
// key bytes. Returns 503 keyviz_disabled / hotkeys_disabled separately
// so the SPA can render a precise "feature off" message rather than a
// generic 404.
type KeyVizHotKeysHandler struct {
	source KeyVizHotKeysSource
	// snapshotWindow is the ±duration around SnapshotAt that the
	// out_of_snapshot_window check accepts (Codex P2 round-5 L224).
	// Defaults to keyviz.DefaultStep; tests can override via WithSnapshotWindow.
	snapshotWindow time.Duration
	now            func() time.Time
	logger         *slog.Logger
}

func NewKeyVizHotKeysHandler(source KeyVizHotKeysSource) *KeyVizHotKeysHandler {
	return &KeyVizHotKeysHandler{
		source:         source,
		snapshotWindow: keyviz.DefaultStep,
		now:            func() time.Time { return time.Now().UTC() },
		logger:         slog.Default(),
	}
}

// WithLogger overrides the slog destination so main.go can attach a
// component tag without changing the constructor signature. Nil is a no-op.
func (h *KeyVizHotKeysHandler) WithLogger(l *slog.Logger) *KeyVizHotKeysHandler {
	if l == nil {
		return h
	}
	h.logger = l
	return h
}

// WithClock lets tests inject a deterministic SnapshotAt for the
// out-of-window check. Nil is a no-op.
func (h *KeyVizHotKeysHandler) WithClock(now func() time.Time) *KeyVizHotKeysHandler {
	if now == nil {
		return h
	}
	h.now = now
	return h
}

// WithSnapshotWindow overrides the default keyvizStep used for the
// out_of_snapshot_window 400 (design §5). Non-positive is treated as a
// no-op so the default stays in force.
func (h *KeyVizHotKeysHandler) WithSnapshotWindow(d time.Duration) *KeyVizHotKeysHandler {
	if d <= 0 {
		return h
	}
	h.snapshotWindow = d
	return h
}

const (
	hotKeysDefaultTop  = 20
	hotKeysMaxTopParam = 1024
)

// hotKeyResponse mirrors the design §5 wire shape. Counts are
// scaled-to-true-frequency estimates (sampled-counter × sample_rate),
// `degraded` is true iff drops > 0 OR skipped_long_keys > 0 (§6 fan-out
// merge OR), and `error_bound` is the per-node Space-Saving bound in
// scaled units (`sample_rate × sampled_n / m`).
type hotKeyResponse struct {
	RouteID         uint64                `json:"route_id"`
	SubBucket       *int                  `json:"sub_bucket,omitempty"`
	Series          string                `json:"series"`
	Keys            []hotKeyResponseEntry `json:"keys"`
	Approximate     bool                  `json:"approximate"`
	SampleRate      int                   `json:"sample_rate"`
	SampledN        uint64                `json:"sampled_n"`
	DroppedSamples  uint64                `json:"dropped_samples"`
	SkippedLongKeys uint64                `json:"skipped_long_keys"`
	Degraded        bool                  `json:"degraded"`
	ErrorBound      uint64                `json:"error_bound"`
	SnapshotAt      time.Time             `json:"snapshot_at"`
}

type hotKeyResponseEntry struct {
	KeyB64 string `json:"key_b64"`
	Count  uint64 `json:"count"`
}

func (h *KeyVizHotKeysHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET")
		return
	}
	if h.source == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "keyviz_disabled",
			"key visualizer sampler is not configured on this node")
		return
	}
	enabled, capacity, _, _ := h.source.HotKeysOptions()
	if !enabled {
		writeJSONError(w, http.StatusServiceUnavailable, "hotkeys_disabled",
			"hot-keys drill-down is not enabled on this node (--keyvizHotKeysEnabled)")
		return
	}

	params, err := parseHotKeysParams(r, capacity)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}

	snap := h.source.HotKeysSnapshot(params.routeID)
	if snap == nil {
		writeJSONError(w, http.StatusNotFound, "no_snapshot",
			"no hot-keys snapshot is available for this route yet (it may be an aggregate, or no traffic has flushed since the feature was enabled)")
		return
	}

	if errCode := h.checkSnapshotWindow(params, snap); errCode != "" {
		writeJSONError(w, http.StatusBadRequest, errCode,
			"requested time window does not overlap the current snapshot; v1 returns only the current keyvizStep window")
		return
	}

	entries, errCode, errMsg := h.collectEntries(params, snap)
	if errCode != "" {
		writeJSONError(w, http.StatusBadRequest, errCode, errMsg)
		return
	}

	out := buildHotKeysResponse(params, snap, entries)
	h.audit(r, params, len(out.Keys), out.Degraded)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		h.logger.LogAttrs(r.Context(), slog.LevelWarn, "admin keyviz hotkeys response encode failed",
			slog.String("error", err.Error()),
		)
	}
}

// checkSnapshotWindow returns "" when the requested [from,to] window is
// acceptable, or the error_code string to send a 400 with otherwise. v1
// rejects a window that does not overlap [SnapshotAt - snapshotWindow,
// SnapshotAt] (design §5; Codex P2 round-5 L224). Omitted bounds default
// to the snapshot window so a request with no time params always passes.
func (h *KeyVizHotKeysHandler) checkSnapshotWindow(p hotKeysParams, snap *keyviz.KeyvizHotKeysSnapshot) string {
	if p.fromUnixMs == 0 && p.toUnixMs == 0 {
		return ""
	}
	windowHigh := snap.SnapshotAt
	windowLow := windowHigh.Add(-h.snapshotWindow)
	reqLow := windowLow
	reqHigh := windowHigh
	if p.fromUnixMs != 0 {
		reqLow = time.UnixMilli(p.fromUnixMs).UTC()
	}
	if p.toUnixMs != 0 {
		reqHigh = time.UnixMilli(p.toUnixMs).UTC()
	}
	if reqHigh.Before(windowLow) || reqLow.After(windowHigh) {
		return "out_of_snapshot_window"
	}
	return ""
}

// collectEntries filters the snapshot entries to the requested sub-range
// (when sub_bucket is set), sorts descending by count, truncates to top.
// Returns (entries, "", "") on success or ("", errCode, errMsg) on a
// validation error (e.g. out-of-range sub_bucket).
func (h *KeyVizHotKeysHandler) collectEntries(p hotKeysParams, snap *keyviz.KeyvizHotKeysSnapshot) ([]keyviz.KeyvizHotKeyEntry, string, string) {
	entries := snap.Entries
	if p.subBucketSet {
		lo, hi, ok := h.source.SubBucketBoundsFor(p.routeID, p.subBucket)
		if !ok {
			return nil, "invalid_query", "sub_bucket is out of range for this route"
		}
		filtered := make([]keyviz.KeyvizHotKeyEntry, 0, len(entries))
		for _, e := range entries {
			if bytes.Compare(e.Key, lo) < 0 {
				continue
			}
			if hi != nil && bytes.Compare(e.Key, hi) >= 0 {
				continue
			}
			filtered = append(filtered, e)
		}
		entries = filtered
	} else {
		// Copy the snapshot's slice header so the sort below does not
		// reorder the immutable published snapshot in place (a reader
		// holding the snapshot reasonably expects it not to mutate
		// under their feet).
		entries = append([]keyviz.KeyvizHotKeyEntry(nil), entries...)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Count > entries[j].Count })
	if len(entries) > p.top {
		entries = entries[:p.top]
	}
	return entries, "", ""
}

// buildHotKeysResponse formats the wire body. count is scaled by the
// snapshot's sample_rate; error_bound is the same scaled
// (sample_rate × sampled_n / m) per-node bound from design §5.
func buildHotKeysResponse(p hotKeysParams, snap *keyviz.KeyvizHotKeysSnapshot, entries []keyviz.KeyvizHotKeyEntry) hotKeyResponse {
	out := hotKeyResponse{
		RouteID:         p.routeID,
		Series:          p.series,
		Keys:            make([]hotKeyResponseEntry, len(entries)),
		Approximate:     true,
		SampleRate:      snap.SampleRate,
		SampledN:        snap.SampledN,
		DroppedSamples:  snap.DroppedSamples,
		SkippedLongKeys: snap.SkippedLongKeys,
		Degraded:        snap.DroppedSamples > 0 || snap.SkippedLongKeys > 0,
		ErrorBound:      scaledErrorBound(snap),
		SnapshotAt:      snap.SnapshotAt.UTC(),
	}
	if p.subBucketSet {
		sb := p.subBucket
		out.SubBucket = &sb
	}
	scale := positiveAsUint64(snap.SampleRate)
	for i, e := range entries {
		out.Keys[i] = hotKeyResponseEntry{
			KeyB64: base64.StdEncoding.EncodeToString(e.Key),
			Count:  e.Count * scale,
		}
	}
	return out
}

// scaledErrorBound is sample_rate × sampled_n / m, in scaled
// (true-frequency-estimate) units. Capacity is clamped at >=1 by
// NewMemSampler, so the division is safe; the guards below are
// belt-and-suspenders for snapshots produced by a future code path.
func scaledErrorBound(snap *keyviz.KeyvizHotKeysSnapshot) uint64 {
	sr := positiveAsUint64(snap.SampleRate)
	cap := positiveAsUint64(snap.Capacity)
	if cap == 0 {
		return 0
	}
	return sr * snap.SampledN / cap
}

// positiveAsUint64 converts an int that is known to be non-negative
// (sampler-clamped to ≥1 in practice) to uint64 with a defensive guard
// so gosec G115 has no negative-wrap path to flag.
func positiveAsUint64(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

func (h *KeyVizHotKeysHandler) audit(r *http.Request, p hotKeysParams, returned int, degraded bool) {
	attrs := []slog.Attr{
		slog.String("component", "admin.keyviz.hotkeys"),
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Uint64("route_id", p.routeID),
		slog.Int("top", p.top),
		slog.String("series", p.series),
		slog.Int("returned", returned),
		slog.Bool("degraded", degraded),
	}
	if p.subBucketSet {
		attrs = append(attrs, slog.Int("sub_bucket", p.subBucket))
	}
	if principal, ok := PrincipalFromContext(r.Context()); ok {
		attrs = append(attrs, slog.String("principal", principal.AccessKey))
	}
	h.logger.LogAttrs(r.Context(), slog.LevelInfo, "admin_audit", attrs...)
}

type hotKeysParams struct {
	routeID      uint64
	subBucket    int
	subBucketSet bool
	series       string
	top          int
	fromUnixMs   int64
	toUnixMs     int64
}

func parseHotKeysParams(r *http.Request, capacity int) (hotKeysParams, error) {
	q := r.URL.Query()
	routeID, err := parseRouteIDParam(q.Get("route_id"))
	if err != nil {
		return hotKeysParams{}, err
	}
	p := hotKeysParams{routeID: routeID, series: "writes", top: hotKeysDefaultTop}
	if err := parseSubBucketParam(q.Get("sub_bucket"), &p); err != nil {
		return hotKeysParams{}, err
	}
	if err := parseSeriesParam(q.Get("series"), &p); err != nil {
		return hotKeysParams{}, err
	}
	if err := parseTopParam(q.Get("top"), capacity, &p); err != nil {
		return hotKeysParams{}, err
	}
	if err := parseTimeWindowParams(q.Get("from_unix_ms"), q.Get("to_unix_ms"), &p); err != nil {
		return hotKeysParams{}, err
	}
	return p, nil
}

func parseRouteIDParam(raw string) (uint64, error) {
	if raw == "" {
		return 0, errors.New("route_id is required")
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, errors.New("route_id must be an unsigned 64-bit integer")
	}
	return v, nil
}

func parseSubBucketParam(raw string, p *hotKeysParams) error {
	if raw == "" {
		return nil
	}
	sb, err := strconv.Atoi(raw)
	if err != nil || sb < 0 {
		return errors.New("sub_bucket must be a non-negative integer")
	}
	p.subBucket = sb
	p.subBucketSet = true
	return nil
}

func parseSeriesParam(raw string, p *hotKeysParams) error {
	if raw == "" {
		return nil
	}
	if raw != "writes" {
		return errors.New("series: v1 supports only 'writes' (design §5)")
	}
	p.series = raw
	return nil
}

func parseTopParam(raw string, capacity int, p *hotKeysParams) error {
	if raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return errors.New("top must be a positive integer")
		}
		if n > hotKeysMaxTopParam {
			n = hotKeysMaxTopParam
		}
		p.top = n
	}
	if capacity > 0 && p.top > capacity {
		p.top = capacity
	}
	return nil
}

func parseTimeWindowParams(fromRaw, toRaw string, p *hotKeysParams) error {
	if fromRaw != "" {
		v, err := strconv.ParseInt(fromRaw, 10, 64)
		if err != nil {
			return errors.New("from_unix_ms must be an integer (unix milliseconds)")
		}
		p.fromUnixMs = v
	}
	if toRaw != "" {
		v, err := strconv.ParseInt(toRaw, 10, 64)
		if err != nil {
			return errors.New("to_unix_ms must be an integer (unix milliseconds)")
		}
		p.toUnixMs = v
	}
	return nil
}

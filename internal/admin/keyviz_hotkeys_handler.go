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
	// fanout is non-nil when the operator configured
	// --keyvizFanoutNodes and the local node has at least one peer
	// (after self-filtering). When set, ServeHTTP runs the per-peer
	// fan-out after building the local response and replaces the
	// reply with the merged cluster-wide view (design §6).
	fanout *KeyVizHotKeysFanout
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

// WithFanout enables cluster-wide aggregation (design §6). When the
// supplied fan-out is non-nil the handler also gates on the
// keyVizFanoutPeerHeader on the inbound request so a peer-originated
// fan-out call does not recursively fan out (matches the matrix
// fan-out's anti-recursion semantics in keyviz_handler.go).
func (h *KeyVizHotKeysHandler) WithFanout(f *KeyVizHotKeysFanout) *KeyVizHotKeysHandler {
	h.fanout = f
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
	// Fanout is set when the handler aggregated peer responses (Phase
	// 2-A++ §6). Absent when fan-out is not configured or when the
	// request itself originated from another node's aggregator (the
	// X-Admin-Fanout-Peer header suppresses the recursive call).
	Fanout *FanoutResult `json:"fanout,omitempty"`
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
	out, errStatus, errCode, errMsg := h.buildLocalResponse(r, params, snap)
	if errCode != "" {
		writeJSONError(w, errStatus, errCode, errMsg)
		return
	}

	out = h.maybeFanout(r, params, out)

	// After fan-out, if no source (local OR any peer) produced data
	// for this route, return the same 404 the single-node path would.
	// Without this post-fan-out check, a route that exists on no node
	// would 200 with empty keys instead of the documented 404, and
	// the SPA's "no snapshot" branch would never fire.
	if snap == nil && len(out.Keys) == 0 && out.SampledN == 0 {
		writeJSONError(w, http.StatusNotFound, "no_snapshot",
			"no hot-keys snapshot is available for this route on any node in the cluster")
		return
	}

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

// buildLocalResponse turns this node's snapshot (or its absence)
// into a hotKeyResponse ready to feed into the fan-out merger.
// Returns (response, errStatus, errCode, errMsg); errCode is "" on
// success, non-empty when the caller must short-circuit with the
// returned status (4xx).
//
// Three branches matter:
//
//   - snap != nil: run the existing pre-fan-out checks
//     (checkSnapshotWindow / collectEntries) and return the populated
//     local response.
//   - snap == nil AND fan-out configured AND not a peer call:
//     return an empty placeholder so the merger can overlay peer data
//     — without this, a route that lives only on a peer would 404
//     before the cluster was ever consulted (gemini HIGH).
//   - snap == nil otherwise (no fan-out configured, OR this request
//     is itself a peer fan-out call): 404 no_snapshot, same as the
//     pre-PR shape; consulting peers would either be impossible or
//     would recurse.
//
// The post-fan-out check in ServeHTTP downgrades back to 404 when
// even peers had nothing, so a client never sees a "200 with empty
// keys" body for a route that exists nowhere.
func (h *KeyVizHotKeysHandler) buildLocalResponse(r *http.Request, params hotKeysParams, snap *keyviz.KeyvizHotKeysSnapshot) (hotKeyResponse, int, string, string) {
	if snap == nil {
		if h.fanout != nil && r.Header.Get(keyVizFanoutPeerHeader) == "" {
			return emptyHotKeyResponse(params), 0, "", ""
		}
		return hotKeyResponse{}, http.StatusNotFound, "no_snapshot",
			"no hot-keys snapshot is available for this route yet (it may be an aggregate, or no traffic has flushed since the feature was enabled)"
	}
	if errCode := h.checkSnapshotWindow(params, snap); errCode != "" {
		return hotKeyResponse{}, http.StatusBadRequest, errCode,
			"requested time window does not overlap the current snapshot; v1 returns only the current keyvizStep window"
	}
	// When fan-out will run after this call, defer the operator's
	// `top` truncation to the merger. Symmetric with the peer-URL
	// fix: a peer returns every locally tracked candidate so the
	// merger can compute the correct sum-then-top-K; the local
	// contribution must do the same. Otherwise, a key whose true
	// cluster rank is high but whose per-node rank falls below `top`
	// is dropped by this side's pre-truncation before the merge ever
	// sees it. Example (top=1): local x=100,z=99; peer z=99. With
	// local pre-truncation, merge sees only x locally → reports
	// x=100 even though z's cluster total is 198 (Codex P1 round-2).
	localParams := params
	if h.fanout != nil && r.Header.Get(keyVizFanoutPeerHeader) == "" {
		localParams.top = keyviz.MaxHotKeysPerRoute
	}
	entries, errCode, errMsg := h.collectEntries(localParams, snap)
	if errCode != "" {
		return hotKeyResponse{}, http.StatusBadRequest, errCode, errMsg
	}
	return buildHotKeysResponse(params, snap, entries), 0, "", ""
}

// emptyHotKeyResponse is the placeholder local response used when
// this node has no snapshot for the requested route but the fan-out
// is configured (so a peer may still have data). SnapshotAt stays
// the zero value so the merger's MAX pick lets any peer's timestamp
// win; SampleRate stays 0 so it never wins the MAX comparison; keys
// is non-nil so the JSON encoder emits `keys: []` rather than `null`
// even when no peer contributes either.
func emptyHotKeyResponse(params hotKeysParams) hotKeyResponse {
	out := hotKeyResponse{
		RouteID:     params.routeID,
		Series:      params.series,
		Approximate: true,
		Keys:        []hotKeyResponseEntry{},
	}
	if params.subBucketSet {
		sb := params.subBucket
		out.SubBucket = &sb
	}
	return out
}

// maybeFanout runs the configured fan-out aggregator iff this
// request is the operator-originated entry point (no peer header).
// Returning early when no fan-out is configured (or when the request
// is itself a peer call) keeps ServeHTTP within the cyclop limit
// while preserving the design §6 anti-recursion rule. Forwarding the
// inbound cookies lets each peer's SessionAuth see a valid
// principal; nil cookies surface as ok=false in the per-node status.
func (h *KeyVizHotKeysHandler) maybeFanout(r *http.Request, params hotKeysParams, out hotKeyResponse) hotKeyResponse {
	if h.fanout == nil || r.Header.Get(keyVizFanoutPeerHeader) != "" {
		return out
	}
	return h.fanout.Run(r.Context(), params, out, r.Cookies())
}

// scaledErrorBound is sample_rate × sampled_n / m, in scaled
// (true-frequency-estimate) units. Capacity is clamped at >=1 by
// NewMemSampler, so the division is safe; the guards below are
// belt-and-suspenders for snapshots produced by a future code path.
// (Local m, not `cap`, to avoid shadowing the builtin per gocritic.)
func scaledErrorBound(snap *keyviz.KeyvizHotKeysSnapshot) uint64 {
	sr := positiveAsUint64(snap.SampleRate)
	m := positiveAsUint64(snap.Capacity)
	if m == 0 {
		return 0
	}
	return sr * snap.SampledN / m
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
	// Reject an inverted window before checkSnapshotWindow ever runs:
	// a `to < from` interval makes the overlap check ambiguous and
	// papers over what is almost certainly a client bug (gemini medium
	// / claude 🔵 on PR #854).
	if p.fromUnixMs != 0 && p.toUnixMs != 0 && p.fromUnixMs > p.toUnixMs {
		return errors.New("from_unix_ms must be <= to_unix_ms")
	}
	return nil
}

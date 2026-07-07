package admin

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/keyviz"
	pkgerrors "github.com/cockroachdb/errors"
)

// KeyVizHotKeysFanout aggregates this node's local hot-keys response
// with responses fetched from a static peer list. Mirrors the
// KeyVizFanout pattern: parallel per-peer fetch, cookie forwarding,
// anti-recursion via the keyVizFanoutPeerHeader, per-peer body cap,
// per-node status surfaced in the response.
//
// Merge semantics differ from the matrix fan-out — see
// docs/design/2026_05_28_implemented_keyviz_hot_key_topk.md §6:
//
//   - count per key:      SUM across peers (responses already carry
//     scaled-to-true estimates from buildHotKeysResponse).
//   - error_bound:        SUM (per-peer bounds compose additively
//     for the sum-of-independent-estimates).
//   - sample_rate:        MAX (the coarsest peer dominates).
//   - sampled_n:          SUM (informational).
//   - dropped_samples:    SUM.
//   - skipped_long_keys:  SUM.
//   - degraded:           OR (any peer with drops > 0 OR skipped > 0).
//   - approximate:        OR — SS sketches are always approximate.
//   - snapshot_at:        MAX (latest publish across the cluster).
//   - keys:               re-sort by summed count, take top.
//
// Reporting the per-peer error_bound as a MAX (rather than SUM) would
// understate the noise floor by up to a factor of peer_count and
// mislead any client using the bound as a confidence interval. Either
// degradation source alone (drops OR skips) is enough to flip
// degraded — including the case where a peer drained its queue cleanly
// but skipped a too-long key that would have been the cluster's
// hottest entry.
type KeyVizHotKeysFanout struct {
	self      string
	peers     []string
	client    *http.Client
	timeout   time.Duration
	logger    *slog.Logger
	bodyLimit int64 // 0 falls back to keyVizPeerResponseBodyLimit.
}

// NewKeyVizHotKeysFanout wires the aggregator. self is the local
// node's identity for the FanoutResult.Nodes entry. peers is the list
// of HTTP base URLs (or host:port shorthand) to query — typically the
// operator's --keyvizFanoutNodes list with the local entry filtered
// out at construction (see main_admin.go::buildKeyVizHotKeysFanout).
func NewKeyVizHotKeysFanout(self string, peers []string) *KeyVizHotKeysFanout {
	return &KeyVizHotKeysFanout{
		self:    self,
		peers:   append([]string(nil), peers...),
		client:  &http.Client{Timeout: keyVizFanoutDefaultTimeout},
		timeout: keyVizFanoutDefaultTimeout,
		logger:  slog.Default(),
	}
}

// WithLogger overrides the slog destination so main can attach a
// component tag. Nil is a no-op.
func (f *KeyVizHotKeysFanout) WithLogger(l *slog.Logger) *KeyVizHotKeysFanout {
	if l == nil || f == nil {
		return f
	}
	f.logger = l
	return f
}

// WithHTTPClient swaps the HTTP client. Tests inject an httptest
// server's Client(). nil resets to the default.
func (f *KeyVizHotKeysFanout) WithHTTPClient(c *http.Client) *KeyVizHotKeysFanout {
	if f == nil {
		return f
	}
	if c == nil {
		f.client = &http.Client{Timeout: f.timeout}
		return f
	}
	f.client = c
	return f
}

// WithResponseBodyLimit overrides the per-peer JSON decode cap.
// Values <= 0 reset to the default.
func (f *KeyVizHotKeysFanout) WithResponseBodyLimit(n int64) *KeyVizHotKeysFanout {
	if f == nil {
		return f
	}
	if n <= 0 {
		f.bodyLimit = 0
		return f
	}
	f.bodyLimit = n
	return f
}

// WithTimeout sets the per-peer timeout (and updates the http.Client
// timeout when it has not been replaced via WithHTTPClient). Values
// <= 0 leave the existing timeout unchanged.
func (f *KeyVizHotKeysFanout) WithTimeout(d time.Duration) *KeyVizHotKeysFanout {
	if f == nil || d <= 0 {
		return f
	}
	f.timeout = d
	if f.client != nil {
		f.client.Timeout = d
	}
	return f
}

// Run merges local with peer responses and returns the combined
// hot-keys response plus per-node status. On an empty peer list Run
// echoes local with a FanoutResult that reports Expected=1,
// Responded=1.
//
// cookies are attached to every peer request so the receiving node's
// SessionAuth middleware sees a valid admin session. Production
// passes the inbound request's cookies; nil disables cookie
// forwarding (peers will 401 unless they have their own bypass).
// All cluster nodes must share the same --adminSessionSigningKey for
// the cookie minted by node A to be verifiable on node B.
//
// Run never returns an error: peer-level failures surface in the
// FanoutResult; aggregation is best-effort. A peer that 503s with
// hotkeys_disabled (i.e. mixed-K) is recorded as ok=false with the
// status message and its contribution is simply omitted from the
// merge, matching design §6's "peers that don't sample omit their
// contribution".
func (f *KeyVizHotKeysFanout) Run(ctx context.Context, params hotKeysParams, local hotKeyResponse, cookies []*http.Cookie) hotKeyResponse {
	if f == nil || len(f.peers) == 0 {
		local.Fanout = &FanoutResult{
			Nodes:     []FanoutNodeStatus{{Node: f.selfName(), OK: true}},
			Responded: 1,
			Expected:  1,
		}
		return local
	}

	results := f.fetchPeersParallel(ctx, params, cookies)

	responses := []hotKeyResponse{local}
	statuses := []FanoutNodeStatus{{Node: f.selfName(), OK: true}}
	for _, r := range results {
		if r.err != nil {
			statuses = append(statuses, FanoutNodeStatus{
				Node: r.node, OK: false, Error: r.err.Error(),
			})
			continue
		}
		responses = append(responses, *r.response)
		statuses = append(statuses, FanoutNodeStatus{Node: r.node, OK: true})
	}
	merged := mergeHotKeyResponses(responses, params)
	merged.Fanout = &FanoutResult{
		Nodes:     statuses,
		Responded: countOK(statuses),
		Expected:  len(statuses),
	}
	return merged
}

func (f *KeyVizHotKeysFanout) selfName() string {
	if f == nil || f.self == "" {
		return "self"
	}
	return f.self
}

// hotKeysPeerResult is the per-peer outcome the goroutine pool
// collects before the synchronous merge phase. Either response is
// non-nil or err is non-nil; never both.
type hotKeysPeerResult struct {
	node     string
	response *hotKeyResponse
	err      error
}

func (f *KeyVizHotKeysFanout) fetchPeersParallel(ctx context.Context, params hotKeysParams, cookies []*http.Cookie) []hotKeysPeerResult {
	// Cap per-peer wall time so a single slow node cannot hold the
	// SPA poll open beyond the configured timeout. The parent
	// context is preserved as the cancellation root so an early
	// client disconnect short-circuits every in-flight peer call.
	callCtx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	results := make([]hotKeysPeerResult, len(f.peers))
	var wg sync.WaitGroup
	for i, peer := range f.peers {
		wg.Add(1)
		go func(i int, peer string) {
			defer wg.Done()
			resp, err := f.fetchPeer(callCtx, peer, params, cookies)
			results[i] = hotKeysPeerResult{node: peer, response: resp, err: err}
		}(i, peer)
	}
	wg.Wait()
	return results
}

func (f *KeyVizHotKeysFanout) fetchPeer(ctx context.Context, peer string, params hotKeysParams, cookies []*http.Cookie) (*hotKeyResponse, error) {
	target, err := buildKeyVizHotKeysPeerURL(peer, params)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "build peer url")
	}
	var hk hotKeyResponse
	if err := fetchPeerJSON(ctx, f.client, peer, target, cookies, f.responseBodyLimit(), f.logger, &hk); err != nil {
		return nil, err
	}
	return &hk, nil
}

// responseBodyLimit returns the per-peer JSON body cap. The hot-keys
// response is much smaller than the matrix (default Top-20 keys, each
// at most a few hundred bytes), but we share the matrix's 64 MiB cap
// so a misbehaving peer cannot get more leeway here than there.
func (f *KeyVizHotKeysFanout) responseBodyLimit() int64 {
	if f.bodyLimit > 0 {
		return f.bodyLimit
	}
	return keyVizPeerResponseBodyLimit
}

// buildKeyVizHotKeysPeerURL forwards the parsed hot-keys query
// parameters from the upstream request, NOT the raw query string.
// Forwarding parsed values prevents an upstream from injecting
// parameters we do not recognise and keeps the per-peer URL
// deterministic for tests.
//
// The peer string accepts both shapes the matrix fan-out accepts:
//
//   - Full URL: http://10.0.0.2:8080
//   - host:port: 10.0.0.2:8080 (interpreted as http://host:port)
func buildKeyVizHotKeysPeerURL(peer string, params hotKeysParams) (string, error) {
	raw := peer
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	base, err := url.Parse(raw)
	if err != nil {
		return "", pkgerrors.Wrapf(err, "parse peer base url %q", peer)
	}
	if base.Host == "" {
		return "", fmt.Errorf("%w: peer base url %q has no host", errKeyVizPeer, peer)
	}
	base.Path = pathKeyVizHotKeys
	q := base.Query()
	q.Set("route_id", strconv.FormatUint(params.routeID, 10))
	if params.subBucketSet {
		q.Set("sub_bucket", strconv.Itoa(params.subBucket))
	}
	q.Set("series", params.series)
	// Always request the per-route sketch ceiling from each peer
	// instead of forwarding the operator's `top`. Forwarding `top`
	// would have each peer truncate to its own top-K before merge,
	// hiding a key whose true cluster-wide rank is high but whose
	// per-peer rank falls below the requested cut. Example
	// (top=1):
	//   peer A: x=100, z=99 (z hidden)
	//   peer B: z=99
	//   merge would report x=100 even though z's cluster total is
	//   198. Requesting MaxHotKeysPerRoute pulls every locally
	//   tracked candidate so the merger can compute the correct
	//   sum-then-top-K (Codex P1 round-1).
	q.Set("top", strconv.Itoa(keyviz.MaxHotKeysPerRoute))
	if params.fromUnixMs != 0 {
		q.Set("from_unix_ms", strconv.FormatInt(params.fromUnixMs, 10))
	}
	if params.toUnixMs != 0 {
		q.Set("to_unix_ms", strconv.FormatInt(params.toUnixMs, 10))
	}
	base.RawQuery = q.Encode()
	return base.String(), nil
}

// foldHotKeyResponse folds one source response into the running
// merge accumulators. Split out from mergeHotKeyResponses so the
// outer function stays under cyclop's cyclomatic-complexity bound;
// the per-response work is the bulk of the merge cost so isolating
// it also makes the merge profile easier to read.
func foldHotKeyResponse(r *hotKeyResponse, out *hotKeyResponse, sums map[string]uint64, keyOrder *[]string) {
	for _, k := range r.Keys {
		if _, ok := sums[k.KeyB64]; !ok {
			*keyOrder = append(*keyOrder, k.KeyB64)
		}
		sums[k.KeyB64] += k.Count
	}
	out.SampledN += r.SampledN
	out.DroppedSamples += r.DroppedSamples
	out.SkippedLongKeys += r.SkippedLongKeys
	out.ErrorBound += r.ErrorBound
	if r.SampleRate > out.SampleRate {
		out.SampleRate = r.SampleRate
	}
	if r.SnapshotAt.After(out.SnapshotAt) {
		out.SnapshotAt = r.SnapshotAt
	}
}

// mergeHotKeyResponses applies the design §6 merge rules across all
// peer responses (local at index 0). The first response anchors the
// non-aggregated identity fields (RouteID, SubBucket, Series); they
// are not re-derived because the receiving peer echoes the request
// params back unchanged, so every responding peer carries identical
// values for them.
func mergeHotKeyResponses(responses []hotKeyResponse, params hotKeysParams) hotKeyResponse {
	if len(responses) == 0 {
		// Defensive: callers always pass at least the local
		// response, but return a well-typed zero so a future caller
		// cannot panic the merge step.
		return hotKeyResponse{
			RouteID:     params.routeID,
			Series:      params.series,
			Approximate: true,
			Keys:        []hotKeyResponseEntry{},
		}
	}
	base := responses[0]
	// Seed the MAX fields (SampleRate, SnapshotAt) from responses[0]
	// so the fold loop below can stay uniform across all responses
	// including index 0. The MAX comparisons against the seed are
	// no-ops for index 0; for the SUM fields (SampledN,
	// DroppedSamples, SkippedLongKeys, ErrorBound) the zero-init is
	// the SUM identity and the loop folds responses[0] cleanly.
	out := hotKeyResponse{
		RouteID:     base.RouteID,
		SubBucket:   base.SubBucket,
		Series:      base.Series,
		Approximate: true, // SS sketches are always approximate.
		SampleRate:  base.SampleRate,
		SnapshotAt:  base.SnapshotAt,
	}
	// keyOrder preserves first-seen so two peers contributing the
	// same key in different positions still produce a deterministic
	// (not map-iteration-order) post-sort result for tests.
	sums := make(map[string]uint64, len(base.Keys))
	keyOrder := make([]string, 0, len(base.Keys))
	for i := range responses {
		foldHotKeyResponse(&responses[i], &out, sums, &keyOrder)
	}
	out.Degraded = out.DroppedSamples > 0 || out.SkippedLongKeys > 0

	merged := make([]hotKeyResponseEntry, 0, len(keyOrder))
	for _, k := range keyOrder {
		merged = append(merged, hotKeyResponseEntry{KeyB64: k, Count: sums[k]})
	}
	// Sort descending by count; tie-break by KeyB64 ascending so the
	// merge is total-ordered (no map-iteration nondeterminism leaks
	// past the keyOrder hint when two peers report identical counts
	// for distinct keys).
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Count != merged[j].Count {
			return merged[i].Count > merged[j].Count
		}
		return merged[i].KeyB64 < merged[j].KeyB64
	})
	if len(merged) > params.top {
		merged = merged[:params.top]
	}
	out.Keys = merged
	return out
}

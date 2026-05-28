package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pkgerrors "github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
)

// errKeyVizPeer is the sentinel wrapped by every fan-out failure so
// callers can errors.Is() against it without parsing strings. Each
// concrete failure adds its own %w-wrapped detail.
var errKeyVizPeer = errors.New("keyviz fan-out peer error")

// keyVizFanoutDefaultTimeout matches the design 9 open-question 2
// proposed default: 2 seconds per peer call. Operators on weird
// networks can override via WithTimeout.
const keyVizFanoutDefaultTimeout = 2 * time.Second

// keyVizPeerErrorBodyLimit caps how many bytes of a peer's non-OK
// response body we splice into the error message. 512 is enough to
// surface a typical structured error envelope without letting a
// misbehaving peer flood operator logs.
const keyVizPeerErrorBodyLimit = 512

// keyVizFanoutPeerHeader marks a request as originating from another
// node's fan-out aggregator. The receiving handler skips its own
// fan-out step when this header is set, breaking the
// every-node-fans-to-every-node recursion that would otherwise turn
// a single browser poll into O(N²) peer HTTP calls in a symmetric
// cluster (Claude bot P1 on PR #692).
const keyVizFanoutPeerHeader = "X-Admin-Fanout-Peer"

// keyVizPeerResponseBodyLimit caps the JSON body we are willing to
// decode from a peer. A misbehaving or compromised peer that streams
// gigabytes back at us would otherwise pin a goroutine on
// json.Decode and balloon memory.
//
// Sizing: 1024 rows × 4096 columns = ~4M uint64 cells. JSON encoding
// of a uint64 ranges from 1 byte ("0") to 20 bytes (max uint64), with
// realistic heatmap traffic skewing low (most cells are 0 or small).
// At a worst-case 20 bytes/value the raw values alone would reach
// ~80 MiB, slightly over the 64 MiB cap. That is intentional: the
// operator-visible failure mode is "warning logged, matrix may be
// truncated", not "DoS". Operators on extreme-traffic deployments
// who hit the cap should override via a future flag once the need
// is real.
const keyVizPeerResponseBodyLimit int64 = 64 << 20 // 64 MiB

// keyVizMergeBucketHint is a hand-tuned starting capacity for the
// merge phase's bucket map / order slice. Most fan-out responses
// are well under 1024 rows; 64 lets a small cluster avoid the
// initial map grow while keeping the worst-case overhead trivial
// against the 1024-row budget.
const keyVizMergeBucketHint = 64

// FanoutResult is the per-response fan-out summary attached to
// KeyVizMatrix.Fanout when fan-out is enabled. Nodes is ordered by
// the operator-supplied node list (self first) so the SPA can render
// a stable row order; Responded counts ok=true entries; Expected is
// the configured peer count plus self.
//
// See docs/design/2026_04_27_proposed_keyviz_cluster_fanout.md 5.
type FanoutResult struct {
	Nodes     []FanoutNodeStatus `json:"nodes"`
	Responded int                `json:"responded"`
	Expected  int                `json:"expected"`
}

// FanoutNodeStatus is one node's contribution status for a single
// fan-out request. OK=true means the node returned a parseable
// matrix; OK=false carries the reason (timeout, refused, 5xx body,
// JSON decode failure). The local node always reports OK=true: its
// matrix is computed in-process and cannot fail in this layer.
type FanoutNodeStatus struct {
	Node  string `json:"node"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// KeyVizFanout aggregates this node's local matrix with matrices
// fetched from a static peer list. The contract:
//
//   - peers must NOT include self; the handler computes the local
//     matrix and passes it to Run alongside the peer set.
//   - Each peer is queried in parallel via HTTP GET on the same
//     /admin/api/v1/keyviz/matrix path. The query string is rebuilt
//     from the parsed parameters so a peer running an older or newer
//     server does not receive an unrecognised parameter we never
//     intended to forward.
//   - A peer that times out, errors, or returns a non-OK status
//     contributes a FanoutNodeStatus{OK: false, Error: ...} but does
//     not abort the request. Aggregation proceeds with whatever
//     succeeded.
//
// The merge rules are documented in 4 of the design doc:
//
//   - Reads / read_bytes: sum across nodes (each node served distinct
//     follower reads).
//   - Writes / write_bytes: §9.1 canonical (raftGroupID, leaderTerm)
//     dedupe; when two sources disagree within the same (group, term,
//     column) we set Conflicts[column]=true (and the row-level OR
//     Conflict) so the SPA can hatch the affected cell.
type KeyVizFanout struct {
	self      string
	peers     []string
	client    *http.Client
	timeout   time.Duration
	logger    *slog.Logger
	bodyLimit int64 // per-peer JSON cap; 0 falls back to keyVizPeerResponseBodyLimit.
}

// NewKeyVizFanout wires the aggregator. self is the local node's
// identity for the FanoutResult.Nodes entry (does not have to match
// any peer URL). peers is the list of HTTP base URLs to query
// (e.g. http://10.0.0.2:8080) — typically the operator's
// --keyvizFanoutNodes list with the local entry filtered out.
//
// The default per-peer timeout is 2 seconds, matching the design 9
// open question 2 default. The default HTTP client has no
// connection pool tuning beyond stdlib defaults; intra-cluster
// admin traffic does not yet justify a custom transport.
func NewKeyVizFanout(self string, peers []string) *KeyVizFanout {
	return &KeyVizFanout{
		self:   self,
		peers:  append([]string(nil), peers...),
		client: &http.Client{Timeout: keyVizFanoutDefaultTimeout},
		// timeout shadows client.Timeout so tests can shorten the
		// per-call ceiling without rebuilding the http.Client.
		timeout: keyVizFanoutDefaultTimeout,
		logger:  slog.Default(),
	}
}

// WithLogger overrides the slog destination so main.go can attach a
// component tag. nil leaves the existing logger.
func (f *KeyVizFanout) WithLogger(l *slog.Logger) *KeyVizFanout {
	if l == nil || f == nil {
		return f
	}
	f.logger = l
	return f
}

// WithHTTPClient swaps the HTTP client. Tests inject an httptest
// server's Client(); operators may want a custom transport in the
// future. nil resets to the default.
func (f *KeyVizFanout) WithHTTPClient(c *http.Client) *KeyVizFanout {
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
// Production leaves this unset; tests use it to drive the over-cap
// path with a small synthetic body. Values <= 0 reset to the
// default.
func (f *KeyVizFanout) WithResponseBodyLimit(n int64) *KeyVizFanout {
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
func (f *KeyVizFanout) WithTimeout(d time.Duration) *KeyVizFanout {
	if f == nil || d <= 0 {
		return f
	}
	f.timeout = d
	if f.client != nil {
		f.client.Timeout = d
	}
	return f
}

// attachAdminCookies forwards only the admin session and CSRF
// cookies to a peer request. We whitelist rather than passing every
// inbound cookie through: a logged-in operator may have unrelated
// cookies for other services on the same domain (analytics, feature
// flags, dev-tooling sessions), and the fan-out should not blast
// those across the cluster's internal network. The peer's
// SessionAuth middleware only inspects admin_session, and the CSRF
// double-submit cookie pairs with the X-Admin-CSRF header (which
// fan-out doesn't send because all peer calls are GETs); the cookie
// is forwarded for parity with browser-issued requests but not
// load-bearing. (Gemini security-medium on PR #692.)
func attachAdminCookies(req *http.Request, cookies []*http.Cookie) {
	for _, c := range cookies {
		if c.Name == sessionCookieName || c.Name == csrfCookieName {
			req.AddCookie(c)
		}
	}
}

// peerResult is the per-peer outcome the goroutine pool collects
// before the synchronous merge phase. Either matrix is non-nil or
// err is non-nil; never both.
type peerResult struct {
	node   string
	matrix *KeyVizMatrix
	err    error
}

// Run merges local with peer responses and returns the combined
// matrix plus per-node status. local is the matrix the handler
// already computed against the in-process sampler; on a single-node
// cluster (peers empty) Run returns local with a Fanout block that
// reports Expected=1, Responded=1.
//
// cookies are attached to every peer request so the receiving node's
// SessionAuth middleware sees a valid admin session. Production
// passes the inbound request's cookies; nil disables cookie
// forwarding (peers will 401 unless they have their own bypass).
// All cluster nodes must share the same --adminSessionSigningKey for
// the cookie minted by node A to be verifiable on node B; the
// existing HA setup already requires this.
//
// Run never returns an error: peer-level failures surface in the
// FanoutResult; aggregation is best-effort.
func (f *KeyVizFanout) Run(ctx context.Context, params keyVizParams, local KeyVizMatrix, cookies []*http.Cookie) KeyVizMatrix {
	if f == nil || len(f.peers) == 0 {
		merged := local
		merged.Fanout = &FanoutResult{
			Nodes:     []FanoutNodeStatus{{Node: f.selfName(), OK: true}},
			Responded: 1,
			Expected:  1,
		}
		return merged
	}

	results := f.fetchPeersParallel(ctx, params, cookies)

	matrices := []KeyVizMatrix{local}
	statuses := []FanoutNodeStatus{{Node: f.selfName(), OK: true}}
	for _, r := range results {
		if r.err != nil {
			statuses = append(statuses, FanoutNodeStatus{
				Node: r.node, OK: false, Error: r.err.Error(),
			})
			continue
		}
		matrices = append(matrices, *r.matrix)
		statuses = append(statuses, FanoutNodeStatus{Node: r.node, OK: true})
	}
	merged := mergeKeyVizMatrices(matrices, params.series)
	merged.Fanout = &FanoutResult{
		Nodes:     statuses,
		Responded: countOK(statuses),
		Expected:  len(statuses),
	}
	merged.Series = local.Series
	merged.GeneratedAt = local.GeneratedAt
	return merged
}

func (f *KeyVizFanout) selfName() string {
	if f == nil || f.self == "" {
		return "self"
	}
	return f.self
}

func countOK(statuses []FanoutNodeStatus) int {
	n := 0
	for _, s := range statuses {
		if s.OK {
			n++
		}
	}
	return n
}

func (f *KeyVizFanout) fetchPeersParallel(ctx context.Context, params keyVizParams, cookies []*http.Cookie) []peerResult {
	// Cap per-peer wall time so a single slow node cannot hold the
	// SPA poll open beyond the configured timeout. The parent
	// context is preserved as the cancellation root so an early
	// client disconnect short-circuits every in-flight peer call.
	callCtx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	results := make([]peerResult, len(f.peers))
	var wg sync.WaitGroup
	for i, peer := range f.peers {
		wg.Add(1)
		go func(i int, peer string) {
			defer wg.Done()
			matrix, err := f.fetchPeer(callCtx, peer, params, cookies)
			results[i] = peerResult{node: peer, matrix: matrix, err: err}
		}(i, peer)
	}
	wg.Wait()
	return results
}

func (f *KeyVizFanout) fetchPeer(ctx context.Context, peer string, params keyVizParams, cookies []*http.Cookie) (*KeyVizMatrix, error) {
	target, err := buildKeyVizPeerURL(peer, params)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "build peer url")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, http.NoBody)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "new request")
	}
	req.Header.Set("Accept", "application/json")
	// Mark this request as a peer fan-out call so the receiving
	// handler does not recursively fan out to every other peer —
	// without this header, a symmetric cluster (every node lists
	// every other node) generates O(N²) peer HTTP calls per
	// browser poll. The check on the receiving side is in
	// KeyVizHandler.ServeHTTP. (Claude bot P1 on PR #692.)
	req.Header.Set(keyVizFanoutPeerHeader, "1")
	attachAdminCookies(req, cookies)
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "peer request")
	}
	defer func() {
		// A peer that hangs on body close can wedge our goroutine
		// against the deadline; log and move on rather than blocking.
		if cerr := resp.Body.Close(); cerr != nil {
			f.logger.LogAttrs(ctx, slog.LevelDebug, "keyviz fan-out: peer body close failed",
				slog.String("peer", peer),
				slog.String("error", cerr.Error()),
			)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		// Read a bounded prefix of the body so the error message is
		// useful without letting a misbehaving peer flood our logs.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, keyVizPeerErrorBodyLimit))
		return nil, fmt.Errorf("%w: status %d: %s", errKeyVizPeer, resp.StatusCode, string(body))
	}
	// Bound the JSON decode so a peer that streams gigabytes cannot
	// pin a goroutine and balloon memory. The countingReader wraps a
	// LimitReader so:
	//   - The hard cap is enforced by io.LimitReader (security
	//     bound: at most cap+1 bytes ever pulled off the wire).
	//   - The byte counter is incremented on every Read, including
	//     the chunks json.NewDecoder buffers internally — so the
	//     post-decode `n > cap` check fires reliably even when the
	//     decoder consumed the trailing byte itself rather than
	//     leaving it for an external probe (Claude bot round-2 on
	//     PR #686 flagged the bufio false-negative).
	cr := &countingReader{r: io.LimitReader(resp.Body, f.responseBodyLimit()+1)}
	var m KeyVizMatrix
	if err := json.NewDecoder(cr).Decode(&m); err != nil {
		return nil, pkgerrors.Wrap(err, "decode peer response")
	}
	if cr.n > f.responseBodyLimit() {
		f.logger.LogAttrs(ctx, slog.LevelWarn, "keyviz fan-out: peer response exceeded size limit; truncated decode",
			slog.String("peer", peer),
			slog.Int64("limit_bytes", f.responseBodyLimit()),
			slog.Int64("read_bytes", cr.n),
		)
	}
	return &m, nil
}

// countingReader wraps an io.Reader and tracks total bytes read.
// It is the only reliable way to detect that a JSON decoder
// consumed past a LimitReader cap, since json.NewDecoder uses
// internal buffering and an external one-byte probe of the
// LimitReader can return EOF even when the decoder pulled past
// the cap into its own buffer.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	// CLAUDE.md says "avoid //nolint — refactor instead", but the
	// io.Reader contract is the rare place where the suppression
	// is correct rather than lazy: implementations are required to
	// pass io.EOF through unwrapped so any caller that does
	// `err == io.EOF` (pointer compare) keeps working. Wrapping
	// with %w produces a different error value that pointer
	// compare will not match, even though errors.Is would. The
	// stdlib `encoding/json` historically did pointer compare;
	// modern alternatives (`goccy/go-json` is the one this
	// package uses) may use errors.Is, but the io.Reader contract
	// holds independent of which consumer is in scope. Refactoring
	// is impossible here — the only options are
	// pass-through-and-suppress (this) or
	// wrap-and-break-anyone-doing-pointer-compare.
	return n, err //nolint:wrapcheck // io.Reader contract requires unwrapped sentinels.
}

// responseBodyLimit returns the per-peer JSON body cap. Tests can
// override the limit by assigning the unexported field directly via
// a constructor option (see WithResponseBodyLimit). Production keeps
// the default keyVizPeerResponseBodyLimit.
func (f *KeyVizFanout) responseBodyLimit() int64 {
	if f.bodyLimit > 0 {
		return f.bodyLimit
	}
	return keyVizPeerResponseBodyLimit
}

// buildKeyVizPeerURL forwards the parsed query parameters from the
// upstream request, NOT the raw query string. Forwarding parsed
// values prevents an upstream from injecting parameters we do not
// recognise (forward-compatibility quirks) and keeps the per-peer
// URL deterministic for tests.
//
// The peer string accepts two shapes:
//
//   - Full URL: http://10.0.0.2:8080  (or https when TLS lands)
//   - host:port: 10.0.0.2:8080        (interpreted as http://host:port)
//
// The host-only form is the common operator shorthand; url.Parse
// rejects it as ambiguous (10.0.0.2 looks like a scheme) so we
// detect "no scheme" by the absence of '://' and prepend http://.
func buildKeyVizPeerURL(peer string, params keyVizParams) (string, error) {
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
	base.Path = pathKeyVizMatrix
	q := base.Query()
	q.Set("series", string(params.series))
	q.Set("rows", strconv.Itoa(params.rows))
	if !params.from.IsZero() {
		q.Set("from_unix_ms", strconv.FormatInt(params.from.UnixMilli(), 10))
	}
	if !params.to.IsZero() {
		q.Set("to_unix_ms", strconv.FormatInt(params.to.UnixMilli(), 10))
	}
	base.RawQuery = q.Encode()
	return base.String(), nil
}

// mergeKeyVizMatrices combines per-node matrices into one. The merge
// is column-wise on column_unix_ms (a column missing from a node
// contributes 0 for every row); per-row keying is by BucketID. The
// rule selector follows the requested series:
//
//   - Reads / read_bytes: sum across nodes (independent local serves).
//     Never raise the conflict flag.
//   - Writes / write_bytes: §9.1 canonical merge — collapse to one
//     value per (BucketID, RaftGroupID, LeaderTerm, column) by taking
//     max within the same (group, term) (canonical Raft invariant:
//     at most one leader per term per group), then SUM across
//     distinct LeaderTerm values for the same (group, column).
//     Surface Conflicts[column]=true (and the row-level OR Conflict)
//     when ≥2 sources disagree within the SAME (group, term, column).
//     Fall back to
//     legacy max-merge (§4.2) for any cell where at least one source
//     reports a zero/unknown identity (RaftGroupID=0 or
//     LeaderTerm=0) so a legacy peer that doesn't yet emit the
//     parallel-array wire fields never over-counts. Phase 2-C+ PR-3c
//     promotes §4.2's row-level max to §9.1's per-cell (group, term)
//     algorithm — see parent design `docs/admin_ui_key_visualizer_-
//     design.md` §9.1.
func mergeKeyVizMatrices(matrices []KeyVizMatrix, series KeyVizSeries) KeyVizMatrix {
	if len(matrices) == 0 {
		return KeyVizMatrix{Series: series}
	}
	if len(matrices) == 1 {
		out := matrices[0]
		out.Fanout = nil
		return out
	}
	columns := unionColumns(matrices)
	indexByColumn := make(map[int64]int, len(columns))
	for i, ts := range columns {
		indexByColumn[ts] = i
	}
	useGroupTermDedupe := mergeUsesGroupTermDedupe(series)
	accByBucket := make(map[string]*rowMergeAcc, keyVizMergeBucketHint)
	bucketOrder := make([]string, 0, keyVizMergeBucketHint)
	for mi := range matrices {
		m := &matrices[mi]
		for ri := range m.Rows {
			mergeRowInto(&m.Rows[ri], m.ColumnUnixMs, indexByColumn, accByBucket, &bucketOrder, len(columns), useGroupTermDedupe)
		}
	}
	out := KeyVizMatrix{
		ColumnUnixMs: columns,
		Series:       series,
		Rows:         make([]KeyVizRow, 0, len(accByBucket)),
	}
	for _, bucket := range bucketOrder {
		out.Rows = append(out.Rows, resolveRowMergeAcc(accByBucket[bucket], useGroupTermDedupe))
	}
	return out
}

// rowMergeAcc is the per-bucket accumulator state populated by all
// source rows that share a BucketID. Per-cell merge state lives in
// `cells`; row-level metadata (Start, End, Aggregate, …) is captured
// from the first-seen source.
type rowMergeAcc struct {
	bucketID          string
	start, end        []byte
	aggregate         bool
	routeIDs          []uint64
	routeIDsTruncated bool
	routeCount        uint64
	cells             []cellMergeAcc
}

// cellMergeAcc tracks merge state for one (bucket, column) cell.
// For reads: only `sum` and the last-seen identity matter.
// For writes: `byTerm` maps (group, term) to the canonical (max)
// value reported by any source; `termConflict` records which keys
// saw disagreement. `hasUnknownTerm` forces the fallback max-merge
// path at resolve time so a legacy peer (or a node whose
// SetLeaderTerm publisher has not fired) never causes the per-term
// sum to over-count.
type cellMergeAcc struct {
	// Read path (sumMerge): running sum across all sources.
	sum uint64
	// Read path identity: last non-zero contributor's (group, term).
	// Best-effort "last-touched" semantics for reads (the read
	// merge has no single owner — prev+incoming is a sum, so we
	// pick whichever side contributed most recently). For writes
	// the resolved identity comes from `byTerm` largest value at
	// resolve time.
	lastGroup, lastTerm uint64

	// Write path (group-term dedupe + sum across terms):
	byTerm         map[termKey]uint64
	termConflict   map[termKey]bool
	hasUnknownTerm bool
	// Fallback max-merge state, used when hasUnknownTerm is true.
	// Tracks the maximum value seen and whether two non-zero
	// distinct values were ever observed at this cell.
	fallbackMax              uint64
	fallbackNonZeroDistinct  bool
	fallbackSeenNonZeroValue uint64
}

type termKey struct {
	group uint64
	term  uint64
}

// mergeUsesGroupTermDedupe gates the §9.1 (group, term) algorithm.
// Writes use it; reads stay on the legacy sum-across-nodes path
// because local serves are independent and never need per-leader
// dedupe.
func mergeUsesGroupTermDedupe(series KeyVizSeries) bool {
	switch series {
	case keyVizSeriesWrites, keyVizSeriesWriteBytes:
		return true
	case keyVizSeriesReads, keyVizSeriesReadBytes:
		return false
	default:
		return false
	}
}

// mergeRowInto folds one source row into the per-bucket accumulator.
// Phase 2-C+ PR-3c: per-cell state is accumulated here, but the
// resolution (sum across (group, term); fallback to max) happens
// after all sources have been seen — see resolveRowMergeAcc.
func mergeRowInto(
	row *KeyVizRow,
	srcColumns []int64,
	indexByColumn map[int64]int,
	accByBucket map[string]*rowMergeAcc,
	bucketOrder *[]string,
	mergedWidth int,
	useGroupTermDedupe bool,
) {
	acc, ok := accByBucket[row.BucketID]
	if !ok {
		acc = &rowMergeAcc{
			bucketID:          row.BucketID,
			start:             append([]byte(nil), row.Start...),
			end:               append([]byte(nil), row.End...),
			aggregate:         row.Aggregate,
			routeIDs:          append([]uint64(nil), row.RouteIDs...),
			routeIDsTruncated: row.RouteIDsTruncated,
			routeCount:        row.RouteCount,
			cells:             make([]cellMergeAcc, mergedWidth),
		}
		accByBucket[row.BucketID] = acc
		*bucketOrder = append(*bucketOrder, row.BucketID)
	}
	for j, ts := range srcColumns {
		idx, ok := indexByColumn[ts]
		if !ok || j >= len(row.Values) {
			continue
		}
		value := row.Values[j]
		var group, term uint64
		if j < len(row.RaftGroupIDs) {
			group = row.RaftGroupIDs[j]
		}
		if j < len(row.LeaderTerms) {
			term = row.LeaderTerms[j]
		}
		if useGroupTermDedupe {
			acc.cells[idx].addWrite(value, group, term)
		} else {
			acc.cells[idx].addRead(value, group, term)
		}
	}
}

// addRead is the sumMerge path: independent local serves accumulate
// across all sources. Identity is best-effort last-touched.
func (c *cellMergeAcc) addRead(value, group, term uint64) {
	c.sum += value
	if value > 0 {
		c.lastGroup = group
		c.lastTerm = term
	}
}

// addWrite is the §9.1 path: track (group, term) buckets and
// fallback-max state. Zero-valued contributions are ignored
// (legacy max-merge treats `(prev, 0)` as no-op, matching §4.2).
func (c *cellMergeAcc) addWrite(value, group, term uint64) {
	if value == 0 {
		return
	}
	c.updateFallbackState(value, group, term)
	// LeaderTerm == 0 (or group == 0) means "term not tracked" —
	// the documented sentinel from a legacy peer or a publisher
	// that has not yet fired. We CANNOT safely sum across an
	// unknown-term contribution because the unknown-term might
	// overlap with a known-term entry; fall back to max-merge for
	// the whole cell at resolve time.
	if term == 0 || group == 0 {
		c.hasUnknownTerm = true
		return
	}
	c.recordTermContribution(termKey{group: group, term: term}, value)
}

// updateFallbackState advances the fallback-max accounting that
// applies when hasUnknownTerm is set at resolve time. Maintained for
// every non-zero contribution so a cell that later flips into the
// fallback path has the correct running max + disagreement signal
// AND the identity of the contributor that supplied the max value.
// Tie-break is last-touched (matching addRead) so a cell with two
// equal contributions keeps the most recently processed identity.
// Without the identity tracking, a cell with one known-term source
// + one unknown-term source would always fall back to (0, 0) even
// when the known-term source supplied the winning max.
func (c *cellMergeAcc) updateFallbackState(value, group, term uint64) {
	if value >= c.fallbackMax {
		c.fallbackMax = value
		c.lastGroup = group
		c.lastTerm = term
	}
	if c.fallbackSeenNonZeroValue == 0 {
		c.fallbackSeenNonZeroValue = value
		return
	}
	if c.fallbackSeenNonZeroValue != value {
		c.fallbackNonZeroDistinct = true
	}
}

// recordTermContribution folds one (group, term) contribution into
// the canonical byTerm map, flipping termConflict when ≥2 sources
// disagree within the same key (a Raft invariant violation — at
// most one leader per term per group).
func (c *cellMergeAcc) recordTermContribution(key termKey, value uint64) {
	if c.byTerm == nil {
		c.byTerm = make(map[termKey]uint64, 1)
	}
	existing, exists := c.byTerm[key]
	if !exists {
		c.byTerm[key] = value
		return
	}
	if existing == value {
		return // canonical agreement, no-op
	}
	if c.termConflict == nil {
		c.termConflict = make(map[termKey]bool, 1)
	}
	c.termConflict[key] = true
	if value > existing {
		c.byTerm[key] = value
	}
}

// resolveRowMergeAcc materialises a KeyVizRow from the accumulator.
// For reads: cell.sum + last identity. For writes: either §9.1 sum
// across (group, term) or fallback max-merge when hasUnknownTerm.
// The write path stamps per-cell Conflicts[j] (Phase 2-C+ PR-3d) and
// keeps row-level Conflict as their OR so an older SPA that only reads
// `conflict` still hatches the whole row.
func resolveRowMergeAcc(acc *rowMergeAcc, useGroupTermDedupe bool) KeyVizRow {
	width := len(acc.cells)
	row := KeyVizRow{
		BucketID:          acc.bucketID,
		Start:             acc.start,
		End:               acc.end,
		Aggregate:         acc.aggregate,
		RouteIDs:          acc.routeIDs,
		RouteIDsTruncated: acc.routeIDsTruncated,
		RouteCount:        acc.routeCount,
		Values:            make([]uint64, width),
		RaftGroupIDs:      make([]uint64, width),
		LeaderTerms:       make([]uint64, width),
	}
	for j := range acc.cells {
		c := &acc.cells[j]
		if useGroupTermDedupe {
			row.Values[j], row.RaftGroupIDs[j], row.LeaderTerms[j] = c.resolveWrite()
			if c.hasConflict() {
				// Allocate Conflicts lazily so a clean row keeps it nil
				// and `json:"conflicts,omitempty"` omits it — otherwise
				// every merged write row would serialize a full
				// [false,...] array and balloon the wire payload.
				if row.Conflicts == nil {
					row.Conflicts = make([]bool, width)
				}
				row.Conflicts[j] = true
				row.Conflict = true
			}
		} else {
			row.Values[j] = c.sum
			row.RaftGroupIDs[j] = c.lastGroup
			row.LeaderTerms[j] = c.lastTerm
		}
	}
	return row
}

// resolveWrite returns the canonical cell value plus the (group, term)
// owner of the largest contribution. Falls back to max-merge when
// hasUnknownTerm is set so a legacy peer never causes summing
// across overlapping unknown/known windows.
func (c *cellMergeAcc) resolveWrite() (value, group, term uint64) {
	if c.hasUnknownTerm {
		// Fallback: max-merge across all contributions. Identity
		// comes from the contributor that supplied fallbackMax
		// (updateFallbackState stamps lastGroup/lastTerm when
		// value >= fallbackMax). Identity is (0, 0) only when
		// every contribution was unknown-term or when an
		// unknown-term source supplied the largest value.
		// Matches the legacy §4.2 max-merge behavior the sentinel
		// was designed to preserve.
		return c.fallbackMax, c.lastGroup, c.lastTerm
	}
	var sum, largestValue, largestGroup, largestTerm uint64
	for k, v := range c.byTerm {
		sum += v
		if v > largestValue {
			largestValue = v
			largestGroup = k.group
			largestTerm = k.term
		}
	}
	return sum, largestGroup, largestTerm
}

// hasConflict returns true when any (group, term) at this cell saw
// disagreement, or — under the fallback path — when ≥2 distinct
// non-zero values were reported. recordTermContribution only ever
// stores `true` in termConflict (entries are never reset), so a
// length check is equivalent to scanning the map and avoids the
// O(N) walk.
func (c *cellMergeAcc) hasConflict() bool {
	if c.hasUnknownTerm {
		return c.fallbackNonZeroDistinct
	}
	return len(c.termConflict) > 0
}

// unionColumns returns the sorted union of column timestamps across
// all matrices. Columns that appear in only some inputs still get a
// slot; the merge fills missing values with the merge-rule identity
// (0 for sum, 0 for max — both treat 0 as "no contribution").
func unionColumns(matrices []KeyVizMatrix) []int64 {
	seen := make(map[int64]struct{}, keyVizMergeBucketHint)
	for _, m := range matrices {
		for _, ts := range m.ColumnUnixMs {
			seen[ts] = struct{}{}
		}
	}
	out := make([]int64, 0, len(seen))
	for ts := range seen {
		out = append(out, ts)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

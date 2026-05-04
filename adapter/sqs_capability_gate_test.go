package adapter

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// htfifoCapabilityServer spins up a minimal /sqs_health responder
// returning HTTP 200 with the given capabilities array. Stand-in
// for a peer node during the capability-gate tests.
func htfifoCapabilityServer(t *testing.T, capabilities []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sqsHealthPath {
			http.NotFound(w, r)
			return
		}
		writeSQSHealthJSONBody(w, r, http.StatusOK, sqsHealthBody{
			Status:       "ok",
			Capabilities: capabilities,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestValidateHTFIFOCapability_ShortCircuitsOnLegacyMeta pins the
// no-op path: PartitionCount <= 1 must skip the capability poll
// entirely so legacy and single-partition CreateQueue calls do not
// pay the cluster-wide poll cost.
func TestValidateHTFIFOCapability_ShortCircuitsOnLegacyMeta(t *testing.T) {
	t.Parallel()

	// Wire a peer whose /sqs_health would FAIL the gate, then
	// verify validateHTFIFOCapability does NOT poll it (the call
	// would otherwise reject). leaderSQS is non-empty so the
	// "vacuous empty list" path isn't what's saving us.
	bad := htfifoCapabilityServer(t, nil)
	s := &SQSServer{leaderSQS: map[string]string{"raft1": strings.TrimPrefix(bad.URL, "http://")}}

	for _, pc := range []uint32{0, 1} {
		err := s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{PartitionCount: pc})
		require.NoErrorf(t, err, "PartitionCount=%d must skip the poll entirely (gate is HT-FIFO-only)", pc)
	}

	// Defensive: nil meta also short-circuits — never reach the
	// poll for an unset/zero meta. Pinned so a future refactor
	// that added a poll on the nil path would fail loudly here.
	require.NoError(t, s.validateHTFIFOCapability(context.Background(), nil))
}

// TestValidateHTFIFOCapability_AcceptsWhenAllPeersAdvertise pins
// the happy path: every peer in leaderSQS reports the htfifo
// capability via /sqs_health → the gate passes for
// PartitionCount > 1.
func TestValidateHTFIFOCapability_AcceptsWhenAllPeersAdvertise(t *testing.T) {
	t.Parallel()

	caps := []string{sqsCapabilityHTFIFO}
	good1 := htfifoCapabilityServer(t, caps)
	good2 := htfifoCapabilityServer(t, caps)
	s := &SQSServer{leaderSQS: map[string]string{
		"raft1": strings.TrimPrefix(good1.URL, "http://"),
		"raft2": strings.TrimPrefix(good2.URL, "http://"),
	}}

	require.NoError(t, s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{PartitionCount: 4}))
}

// TestValidateHTFIFOCapability_AcceptsOnEmptyPeerList pins the
// vacuous case: a single-node cluster (no peers) with the local
// htfifo capability advertised must allow PartitionCount > 1.
// htfifoCapabilityAdvertised is a build-time const = true so the
// local check passes; the empty peer list short-circuits the
// poll. This is the path the wire-level
// TestSQSServer_HTFIFO_CapabilityGate_AcceptsOnSingleNode test
// exercises end-to-end.
func TestValidateHTFIFOCapability_AcceptsOnEmptyPeerList(t *testing.T) {
	t.Parallel()
	s := &SQSServer{}
	require.NoError(t, s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{PartitionCount: 4}))
}

// TestValidateHTFIFOCapability_RejectsWhenOnePeerLacksCapability
// pins the rolling-upgrade fail-closed: one peer advertises
// htfifo, the other doesn't — the gate must reject the create
// with InvalidAttributeValue and surface the offending peer in
// the error message so the operator can fix the cluster without
// re-running the poll out-of-band.
func TestValidateHTFIFOCapability_RejectsWhenOnePeerLacksCapability(t *testing.T) {
	t.Parallel()

	good := htfifoCapabilityServer(t, []string{sqsCapabilityHTFIFO})
	old := htfifoCapabilityServer(t, []string{}) // pre-htfifo binary
	oldAddr := strings.TrimPrefix(old.URL, "http://")
	s := &SQSServer{leaderSQS: map[string]string{
		"raft1": strings.TrimPrefix(good.URL, "http://"),
		"raft2": oldAddr,
	}}

	err := s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{PartitionCount: 8})
	require.Error(t, err)

	var apiErr *sqsAPIError
	ok := errors.As(err, &apiErr)
	require.True(t, ok, "must surface as sqsAPIError so the wire layer maps to InvalidAttributeValue, got %T", err)
	require.Equal(t, http.StatusBadRequest, apiErr.status)
	require.Equal(t, sqsErrInvalidAttributeValue, apiErr.errorType)
	require.Equal(t, htfifoCapabilityRejectionPublic, apiErr.message,
		"client message must be the sanitized constant — peer addresses live in server logs (CodeRabbit major)")
	require.NotContains(t, apiErr.message, oldAddr,
		"peer host:port MUST NOT leak through the wire-level rejection")
}

// TestValidateHTFIFOCapability_RejectsWhenPeerUnreachable pins
// the network-failure fail-closed: a peer whose /sqs_health is
// unreachable (closed listener) must block the create. A
// transient network blip during a CreateQueue is exactly the
// class of partial-cluster state the gate is designed to catch —
// silently accepting the create here would let a partitioned
// queue land while a peer is offline and silently drop messages
// the moment that peer comes back as a non-htfifo binary.
func TestValidateHTFIFOCapability_RejectsWhenPeerUnreachable(t *testing.T) {
	t.Parallel()

	// Bind a port, capture its address, then close the listener
	// so dials fail immediately rather than waiting on the
	// per-peer timeout.
	srv := htfifoCapabilityServer(t, []string{sqsCapabilityHTFIFO})
	deadAddr := strings.TrimPrefix(srv.URL, "http://")
	srv.Close()

	s := &SQSServer{leaderSQS: map[string]string{"raft1": deadAddr}}

	err := s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{PartitionCount: 2})
	require.Error(t, err)
	var apiErr *sqsAPIError
	ok := errors.As(err, &apiErr)
	require.True(t, ok)
	require.Equal(t, http.StatusBadRequest, apiErr.status)
	require.Equal(t, sqsErrInvalidAttributeValue, apiErr.errorType)
	require.Equal(t, htfifoCapabilityRejectionPublic, apiErr.message,
		"client message must be the sanitized constant — transport error text lives in server logs (CodeRabbit major)")
	require.NotContains(t, apiErr.message, deadAddr,
		"peer host:port MUST NOT leak through the wire-level rejection")
}

// TestCollectSQSPeers_Deterministic pins the helper's order +
// dedup contract: leaderSQS is a map (random Go iteration order),
// but the gate's error message and the poller's per-peer index
// must be deterministic so test assertions, log lines, and
// operator triage are stable across runs.
func TestCollectSQSPeers_Deterministic(t *testing.T) {
	t.Parallel()

	s := &SQSServer{leaderSQS: map[string]string{
		"r1": "node3:9000",
		"r2": "node1:9000",
		"r3": "node2:9000",
		"r4": "node1:9000", // duplicate (two Raft nodes pointing at one SQS endpoint)
		"r5": "",           // empty string must be skipped
	}}

	got := s.collectSQSPeers()
	require.Equal(t, []string{"node1:9000", "node2:9000", "node3:9000"}, got,
		"peers must be sorted, deduped, and free of empty strings")

	// Empty leaderSQS: caller relies on len()==0 to skip the
	// poll on single-node deployments.
	require.Empty(t, (&SQSServer{}).collectSQSPeers())
}

// TestFormatHTFIFOCapabilityReportForLog_ShapesServerSideDetail
// pins the SERVER-SIDE log helper's shape — never returned to the
// client (CodeRabbit major review on PR #734: peer addresses + raw
// poller errors leak cluster topology to authenticated callers).
// Each failing peer must contribute a "(reason)" suffix so an
// operator triaging a rolling-upgrade cluster can fix the lag from
// the log lines without rerunning the poll out-of-band; peers that
// pass do not appear.
func TestFormatHTFIFOCapabilityReportForLog_ShapesServerSideDetail(t *testing.T) {
	t.Parallel()

	report := &HTFIFOCapabilityReport{
		Peers: []HTFIFOCapabilityPeerStatus{
			{Address: "ok:9000", HasHTFIFO: true},
			{Address: "old:9000", HasHTFIFO: false, Capabilities: []string{}},
			{Address: "down:9000", HasHTFIFO: false, Error: "dial tcp: refused"},
		},
	}

	detail := formatHTFIFOCapabilityReportForLog(report)
	require.NotContains(t, detail, "ok:9000", "advertising peers must NOT appear in the log detail")
	require.Contains(t, detail, "old:9000 (missing capability)")
	require.Contains(t, detail, "down:9000 (dial tcp: refused)")

	// Defensive: nil report and "all-passing-but-AllAdvertise-false" path.
	require.Contains(t, formatHTFIFOCapabilityReportForLog(nil), "no report")
	allPass := &HTFIFOCapabilityReport{Peers: []HTFIFOCapabilityPeerStatus{{Address: "x", HasHTFIFO: true}}}
	require.Contains(t, formatHTFIFOCapabilityReportForLog(allPass), "unknown peer",
		"never emit an empty detail when no peer reasons surface")
}

// TestValidateHTFIFOCapability_RejectsWhenRoutingMapMissingQueue
// pins the Codex P1 review fix on PR #734 round 2: when a partition
// resolver is installed (--sqsFifoPartitionMap is configured) but
// the requested queue is NOT in the map, CreateQueue must reject
// before the meta record commits — silently accepting it would
// land a queue whose SendMessage / ReceiveMessage calls fail
// closed at the kv.ShardRouter "no route for key" path on first
// use, surfacing as InternalFailure to the client.
func TestValidateHTFIFOCapability_RejectsWhenRoutingMapMissingQueue(t *testing.T) {
	t.Parallel()

	resolver := NewSQSPartitionResolver(map[string][]uint64{
		"other-queue.fifo": {100, 101, 102, 103},
	})
	require.NotNil(t, resolver)
	s := &SQSServer{partitionResolver: resolver}

	err := s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{
		Name:           "newqueue.fifo",
		PartitionCount: 4,
	})
	require.Error(t, err)
	var apiErr *sqsAPIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, http.StatusBadRequest, apiErr.status)
	require.Equal(t, sqsErrInvalidAttributeValue, apiErr.errorType)
	require.Equal(t, htfifoRoutingCoverageRejectionPublic, apiErr.message,
		"client message must be the sanitized routing-coverage constant — operator detail is in the slog.Warn line")
}

// TestValidateHTFIFOCapability_RejectsWhenRoutingMapPartiallyCoversQueue
// is the partial-coverage variant of the routing-map gate: the
// queue is in the map but with FEWER routes than the requested
// PartitionCount. SendMessage on the missing partitions would fail
// closed at the router; the gate must reject before the create
// commits.
func TestValidateHTFIFOCapability_RejectsWhenRoutingMapPartiallyCoversQueue(t *testing.T) {
	t.Parallel()

	resolver := NewSQSPartitionResolver(map[string][]uint64{
		"queue.fifo": {100, 101}, // only 2 routes
	})
	s := &SQSServer{partitionResolver: resolver}

	err := s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{
		Name:           "queue.fifo",
		PartitionCount: 4, // requesting 4 partitions
	})
	require.Error(t, err)
	var apiErr *sqsAPIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, sqsErrInvalidAttributeValue, apiErr.errorType)
	require.Equal(t, htfifoRoutingCoverageRejectionPublic, apiErr.message)
}

// TestValidateHTFIFOCapability_AcceptsWhenRoutingMapFullyCoversQueue
// pins the happy path: queue is in the map with at least
// PartitionCount entries → gate passes. ">=" rather than "=="
// because over-allocating routes (operator preparing for a future
// expansion) is harmless: the create only uses indices [0,
// PartitionCount).
func TestValidateHTFIFOCapability_AcceptsWhenRoutingMapFullyCoversQueue(t *testing.T) {
	t.Parallel()

	resolver := NewSQSPartitionResolver(map[string][]uint64{
		"queue.fifo": {100, 101, 102, 103, 104, 105}, // 6 routes
	})
	s := &SQSServer{partitionResolver: resolver}

	require.NoError(t, s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{
		Name:           "queue.fifo",
		PartitionCount: 4, // requesting 4, under the 6 configured
	}))
}

// TestValidateHTFIFOCapability_AcceptsWhenResolverNil pins the
// backward-compat path: a single-shard / no-flag deployment has
// no partition resolver wired (s.partitionResolver == nil), so
// the gate's coverage check is skipped and the create proceeds
// (provided the local + peer-poll checks above also pass). This
// is what TestSQSServer_HTFIFO_CapabilityGate_AcceptsOnSingleNode
// exercises end-to-end at the wire level; this unit test is the
// narrower assertion that the partitionResolver==nil branch is
// the one that bypasses the coverage check.
func TestValidateHTFIFOCapability_AcceptsWhenResolverNil(t *testing.T) {
	t.Parallel()
	s := &SQSServer{} // partitionResolver implicitly nil
	require.NoError(t, s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{
		Name:           "queue.fifo",
		PartitionCount: 4,
	}))
}

// TestRoutedPartitionCount_NilReceiver pins the typed-nil safety
// of the resolver method: a nil *SQSPartitionResolver returns 0
// (no routes) so the gate's "len(routes) < PartitionCount" check
// rejects naturally rather than panicking. Used by the
// validateHTFIFORoutingCoverage short-circuit on resolver==nil
// (the gate skips the coverage check entirely) but also documents
// the safe-by-default behaviour for any other future caller.
func TestRoutedPartitionCount_NilReceiver(t *testing.T) {
	t.Parallel()
	var r *SQSPartitionResolver
	require.Equal(t, 0, r.RoutedPartitionCount("anything"))
}

// TestRoutedPartitionCount_KnownAndUnknownQueue pins the basic
// API: the count for a configured queue equals len(routes), and
// 0 for any queue not in the map.
func TestRoutedPartitionCount_KnownAndUnknownQueue(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"a.fifo": {1, 2},
		"b.fifo": {3, 4, 5, 6, 7, 8, 9, 10},
	})
	require.Equal(t, 2, r.RoutedPartitionCount("a.fifo"))
	require.Equal(t, 8, r.RoutedPartitionCount("b.fifo"))
	require.Equal(t, 0, r.RoutedPartitionCount("c.fifo"),
		"unknown queue must return 0, not panic — gate routes that case to InvalidAttributeValue")
	require.Equal(t, 0, r.RoutedPartitionCount(""),
		"empty queue name must also return 0")
}

// TestValidateHTFIFOCapability_PublicMessageDoesNotLeakPeerDetails
// pins the CodeRabbit major review's redaction contract: the
// client-visible InvalidAttributeValue message MUST NOT include
// peer addresses or raw poller error text. The two failure-path
// tests above check that the gate REJECTS; this test specifically
// checks that the rejection message is the sanitized
// htfifoCapabilityRejectionPublic constant — no host:port, no raw
// transport error.
func TestValidateHTFIFOCapability_PublicMessageDoesNotLeakPeerDetails(t *testing.T) {
	t.Parallel()

	old := htfifoCapabilityServer(t, []string{}) // no htfifo
	oldAddr := strings.TrimPrefix(old.URL, "http://")
	s := &SQSServer{leaderSQS: map[string]string{"raft1": oldAddr}}

	err := s.validateHTFIFOCapability(context.Background(), &sqsQueueMeta{Name: "q.fifo", PartitionCount: 4})
	require.Error(t, err)
	var apiErr *sqsAPIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, htfifoCapabilityRejectionPublic, apiErr.message,
		"client message must be the sanitized constant, never the per-peer detail")
	require.NotContains(t, apiErr.message, oldAddr,
		"peer host:port MUST NOT appear in the wire-level rejection — operator detail is server-side only")
	require.Contains(t, apiErr.message, "see server logs for details",
		"public message must point operators at the server-side detail")
}

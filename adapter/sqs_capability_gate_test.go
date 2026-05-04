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
	require.Contains(t, apiErr.message, "every cluster peer to advertise the htfifo capability",
		"message must explain the gate so the operator knows what to fix")
	require.Contains(t, apiErr.message, oldAddr,
		"the offending peer must appear in the message, got %q", apiErr.message)
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
	require.Contains(t, apiErr.message, deadAddr,
		"unreachable peer must be named in the rejection message")
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

// TestBuildHTFIFOCapabilityRejection_ShapesOperatorMessage pins the
// rejection-message shape so a future refactor cannot accidentally
// truncate the per-peer detail. Each failing peer must contribute
// a "(reason)" suffix; peers that pass do not appear at all.
func TestBuildHTFIFOCapabilityRejection_ShapesOperatorMessage(t *testing.T) {
	t.Parallel()

	report := &HTFIFOCapabilityReport{
		Peers: []HTFIFOCapabilityPeerStatus{
			{Address: "ok:9000", HasHTFIFO: true},
			{Address: "old:9000", HasHTFIFO: false, Capabilities: []string{}},
			{Address: "down:9000", HasHTFIFO: false, Error: "dial tcp: refused"},
		},
	}

	msg := buildHTFIFOCapabilityRejection(report)
	require.Contains(t, msg, "every cluster peer to advertise the htfifo capability")
	require.NotContains(t, msg, "ok:9000", "advertising peers must NOT appear in the rejection")
	require.Contains(t, msg, "old:9000 (missing capability)")
	require.Contains(t, msg, "down:9000 (dial tcp: refused)")

	// Defensive: nil report and "all-passing-but-AllAdvertise-false" path.
	require.Contains(t, buildHTFIFOCapabilityRejection(nil), "no report")
	allPass := &HTFIFOCapabilityReport{Peers: []HTFIFOCapabilityPeerStatus{{Address: "x", HasHTFIFO: true}}}
	require.Contains(t, buildHTFIFOCapabilityRejection(allPass), "unknown peer",
		"never emit a truncated 'did not: ' tail when no peer details surface")
}

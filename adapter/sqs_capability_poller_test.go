package adapter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// newSQSHealthServer builds an httptest.Server that responds to
// GET /sqs_health with the given JSON body when Accept includes
// application/json. Returns the server and its address (host:port,
// suitable for the poller's bare-address path).
func newSQSHealthServer(t *testing.T, body sqsHealthBody) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != sqsHealthPath {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	return srv, addr
}

// TestPollSQSHTFIFOCapability_AllAdvertise pins the happy path:
// every peer responds with htfifo in capabilities → AllAdvertise
// is true and each peer's HasHTFIFO is true.
func TestPollSQSHTFIFOCapability_AllAdvertise(t *testing.T) {
	t.Parallel()
	_, addr1 := newSQSHealthServer(t, sqsHealthBody{Status: "ok", Capabilities: []string{sqsCapabilityHTFIFO}})
	_, addr2 := newSQSHealthServer(t, sqsHealthBody{Status: "ok", Capabilities: []string{sqsCapabilityHTFIFO}})

	report := PollSQSHTFIFOCapability(context.Background(), []string{addr1, addr2}, PollerConfig{})
	require.True(t, report.AllAdvertise,
		"all peers advertise → AllAdvertise must be true")
	require.Len(t, report.Peers, 2)
	for i, p := range report.Peers {
		require.True(t, p.HasHTFIFO, "peer %d HasHTFIFO must be true", i)
		require.Empty(t, p.Error, "peer %d Error must be empty", i)
		require.Equal(t, []string{sqsCapabilityHTFIFO}, p.Capabilities)
	}
}

// TestPollSQSHTFIFOCapability_OneMissingFailsClosed pins the
// one-bad-apple invariant: a single peer missing the capability
// drops AllAdvertise to false. The other peers' detail is still
// returned for operator triage.
func TestPollSQSHTFIFOCapability_OneMissingFailsClosed(t *testing.T) {
	t.Parallel()
	_, addrGood := newSQSHealthServer(t, sqsHealthBody{Status: "ok", Capabilities: []string{sqsCapabilityHTFIFO}})
	_, addrOld := newSQSHealthServer(t, sqsHealthBody{Status: "ok", Capabilities: []string{}})

	report := PollSQSHTFIFOCapability(context.Background(), []string{addrGood, addrOld}, PollerConfig{})
	require.False(t, report.AllAdvertise,
		"one peer without the capability must drop AllAdvertise")
	require.Len(t, report.Peers, 2)
	require.True(t, report.Peers[0].HasHTFIFO)
	require.False(t, report.Peers[1].HasHTFIFO,
		"old peer's HasHTFIFO must be false")
	require.Empty(t, report.Peers[1].Error,
		"old peer responded successfully — Error must be empty even "+
			"though HasHTFIFO is false")
	require.Equal(t, []string{}, report.Peers[1].Capabilities,
		"empty capabilities slice is the legitimate \"old binary\" signal")
}

// TestPollSQSHTFIFOCapability_HTTPErrorFailsClosed pins the
// transport-failure path: a peer that returns 500, refuses
// connections, or returns a malformed body all drop AllAdvertise
// and record the reason in Error.
func TestPollSQSHTFIFOCapability_HTTPErrorFailsClosed(t *testing.T) {
	t.Parallel()

	// Peer that returns 500.
	errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(errSrv.Close)
	addr500 := strings.TrimPrefix(errSrv.URL, "http://")

	// Peer that doesn't exist (connection refused).
	addrUnreachable := "127.0.0.1:1" // port 1 → connection refused on most systems

	// Peer that returns garbage JSON.
	garbageSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "not json {{{")
	}))
	t.Cleanup(garbageSrv.Close)
	addrGarbage := strings.TrimPrefix(garbageSrv.URL, "http://")

	report := PollSQSHTFIFOCapability(context.Background(),
		[]string{addr500, addrUnreachable, addrGarbage}, PollerConfig{})

	require.False(t, report.AllAdvertise,
		"any transport / parse failure must fail closed")
	for _, p := range report.Peers {
		require.False(t, p.HasHTFIFO)
		require.NotEmpty(t, p.Error,
			"peer %s: every failure branch must record an Error string "+
				"so operators can triage", p.Address)
	}

	require.Contains(t, report.Peers[0].Error, "HTTP 500")
	require.Contains(t, report.Peers[2].Error, "malformed JSON")
}

// TestPollSQSHTFIFOCapability_ParentContextDeadlineFailsClosed
// pins that an expiring parent ctx cancels the request — the
// poll respects whichever bound (parent ctx or per-peer cap)
// fires first.
func TestPollSQSHTFIFOCapability_ParentContextDeadlineFailsClosed(t *testing.T) {
	t.Parallel()

	hangSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Second):
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(hangSrv.Close)
	addrHang := strings.TrimPrefix(hangSrv.URL, "http://")

	// Parent ctx expires before the per-peer cap (default 3s).
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	report := PollSQSHTFIFOCapability(ctx, []string{addrHang}, PollerConfig{})
	elapsed := time.Since(start)

	require.False(t, report.AllAdvertise)
	require.Less(t, elapsed, 4*time.Second,
		"poll must respect the parent ctx deadline")
	require.NotEmpty(t, report.Peers[0].Error)
}

// TestPollSQSHTFIFOCapability_PerPeerTimeoutFailsClosed pins the
// per-peer cap independently of any parent ctx deadline. With
// context.Background() (no deadline) and PollerConfig.PerPeerTimeout
// set short, the poll MUST still abandon a hung peer at the cap —
// otherwise a missing parent deadline would let a single slow peer
// stall a CreateQueue gate indefinitely (claude nit on PR #721
// round 1).
func TestPollSQSHTFIFOCapability_PerPeerTimeoutFailsClosed(t *testing.T) {
	t.Parallel()

	hangSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Second):
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(hangSrv.Close)
	addrHang := strings.TrimPrefix(hangSrv.URL, "http://")

	// No parent deadline; per-peer cap is the only bound.
	start := time.Now()
	report := PollSQSHTFIFOCapability(context.Background(), []string{addrHang},
		PollerConfig{PerPeerTimeout: 100 * time.Millisecond})
	elapsed := time.Since(start)

	require.False(t, report.AllAdvertise)
	require.Less(t, elapsed, 2*time.Second,
		"poll must respect the per-peer cap when there is no "+
			"parent ctx deadline — a missing deadline must NOT let "+
			"a hung peer stall the CreateQueue gate")
	require.NotEmpty(t, report.Peers[0].Error)
}

// TestPollSQSHTFIFOCapability_EmptyPeersIsVacuouslyTrue pins the
// no-peers behaviour: with no peers to consult, every-of-empty is
// vacuously true. The caller (CreateQueue gate) is responsible for
// ensuring the peer list is meaningful before consulting the
// report.
func TestPollSQSHTFIFOCapability_EmptyPeersIsVacuouslyTrue(t *testing.T) {
	t.Parallel()
	report := PollSQSHTFIFOCapability(context.Background(), nil, PollerConfig{})
	require.True(t, report.AllAdvertise)
	require.Empty(t, report.Peers)
}

// TestPollSQSHTFIFOCapability_EmptyPeerAddressFailsClosed pins
// that an empty string in the peer slice is treated as a config
// error and surfaced via Error. Otherwise a "" entry would produce
// a malformed URL and a confusing transport error.
func TestPollSQSHTFIFOCapability_EmptyPeerAddressFailsClosed(t *testing.T) {
	t.Parallel()
	report := PollSQSHTFIFOCapability(context.Background(), []string{""}, PollerConfig{})
	require.False(t, report.AllAdvertise)
	require.Len(t, report.Peers, 1)
	require.Equal(t, "empty peer address", report.Peers[0].Error)
}

// TestPollSQSHTFIFOCapability_FullURLPeer pins that a caller can
// pass a fully-qualified URL (http:// or https://) instead of the
// bare host:port form. Lets operators front the endpoint with TLS
// or a proxy without the helper having to know about it.
func TestPollSQSHTFIFOCapability_FullURLPeer(t *testing.T) {
	t.Parallel()
	srv, _ := newSQSHealthServer(t, sqsHealthBody{
		Status: "ok", Capabilities: []string{sqsCapabilityHTFIFO},
	})
	report := PollSQSHTFIFOCapability(context.Background(), []string{srv.URL}, PollerConfig{})
	require.True(t, report.AllAdvertise)
	require.True(t, report.Peers[0].HasHTFIFO)
}

// TestPollSQSHTFIFOCapability_ConcurrentPolling pins that peers
// are polled in parallel — N peers each with a 200ms delay must
// finish in well under N*200ms. Without concurrent polling, a
// rolling upgrade with many nodes would gate every CreateQueue on
// a serial walk.
func TestPollSQSHTFIFOCapability_ConcurrentPolling(t *testing.T) {
	t.Parallel()
	const peerCount = 5
	const perPeerDelay = 200 * time.Millisecond

	hits := atomic.Int64{}
	mkSrv := func() string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			time.Sleep(perPeerDelay)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(sqsHealthBody{
				Status: "ok", Capabilities: []string{sqsCapabilityHTFIFO},
			})
		}))
		t.Cleanup(srv.Close)
		return strings.TrimPrefix(srv.URL, "http://")
	}
	peers := make([]string, peerCount)
	for i := range peers {
		peers[i] = mkSrv()
	}

	start := time.Now()
	report := PollSQSHTFIFOCapability(context.Background(), peers, PollerConfig{})
	elapsed := time.Since(start)

	require.True(t, report.AllAdvertise)
	require.Equal(t, int64(peerCount), hits.Load())
	// Serial poll would take peerCount * perPeerDelay = 1000ms.
	// Concurrent should be ~perPeerDelay (with some scheduler
	// slack). Allow generous bound.
	require.Less(t, elapsed, time.Duration(peerCount-1)*perPeerDelay,
		"poll elapsed %v — peers must be polled concurrently, "+
			"not serially", elapsed)
}

// TestBuildSQSHealthURL pins the URL-construction edge cases.
// The poller is otherwise opaque about its URL formation; this
// test is the hook for the bare-host-port vs full-URL contract.
func TestBuildSQSHealthURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		peer string
		want string
	}{
		{"127.0.0.1:5050", "http://127.0.0.1:5050" + sqsHealthPath},
		{"node.example:5050", "http://node.example:5050" + sqsHealthPath},
		{"http://node.example:5050", "http://node.example:5050" + sqsHealthPath},
		{"http://node.example:5050/", "http://node.example:5050" + sqsHealthPath},
		{"https://node.example", "https://node.example" + sqsHealthPath},
		// Caller passing a URL that ALREADY includes the health
		// path: documented behaviour is that the helper appends
		// the path again (claude minor on PR #721 round 1). The
		// contract is "pass a base URL or a host:port, never a
		// full request URL". This case pins the behaviour so a
		// future refactor can either keep it (and reject misuse
		// via CreateQueue input validation) or change the
		// contract intentionally.
		{
			"http://node.example:5050" + sqsHealthPath,
			"http://node.example:5050" + sqsHealthPath + sqsHealthPath,
		},
	}
	for _, tc := range cases {
		t.Run(tc.peer, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, buildSQSHealthURL(tc.peer))
		})
	}
}

// TestPollSQSHTFIFOCapability_RespectsBodyLimit pins that a
// pathologically-large /sqs_health response is bounded — the
// poller will not consume megabytes from a misconfigured peer.
// Tests the io.LimitReader cap.
func TestPollSQSHTFIFOCapability_RespectsBodyLimit(t *testing.T) {
	t.Parallel()
	// Server emits a body that exceeds the limit. The poller's
	// LimitReader will truncate it, JSON parse will fail, and the
	// peer will fail closed — pin that path.
	bigSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Valid JSON prefix but a giant string field that the
		// LimitReader will cut mid-value, leaving the body
		// unparseable.
		_, _ = fmt.Fprint(w, `{"status":"ok","capabilities":["`)
		_, _ = fmt.Fprint(w, strings.Repeat("X", 10*sqsCapabilityMaxBodyBytes))
		_, _ = fmt.Fprint(w, `"]}`)
	}))
	t.Cleanup(bigSrv.Close)
	addr := strings.TrimPrefix(bigSrv.URL, "http://")

	report := PollSQSHTFIFOCapability(context.Background(), []string{addr}, PollerConfig{})
	require.False(t, report.AllAdvertise)
	require.Contains(t, report.Peers[0].Error, "malformed JSON",
		"truncated body must surface as JSON parse error, not as "+
			"a successful read of garbage capabilities")
}

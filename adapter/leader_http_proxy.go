package adapter

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/bootjp/elastickv/kv"
)

// httpSchemePlain is the URL scheme for plain-text HTTP adapter endpoints.
const httpSchemePlain = "http"

type httpLeaderProxyFailure uint8

const (
	httpLeaderProxyNotForwarded httpLeaderProxyFailure = iota
	httpLeaderProxyForwardAttemptFailed
)

type httpLeaderErrorWriter func(w http.ResponseWriter, r *http.Request, failure httpLeaderProxyFailure)

// proxyHTTPRequestToLeader forwards r to the current Raft leader's HTTP
// adapter endpoint when this node is a follower. It returns true when the
// request was handled (either proxied or an error body was written) and
// false when the caller should serve it locally (no leader map configured,
// no coordinator, or this node is the leader).
//
// Before a forwarding attempt, the adapter can return a retryable routing
// failure. Once forwarding starts, a failed reply cannot prove noncommit.
//
// leaderMap keys are Raft addresses; values are the matching adapter HTTP
// addresses exported on that same node.
func proxyHTTPRequestToLeader(
	coordinator kv.Coordinator,
	leaderMap map[string]string,
	errWriter httpLeaderErrorWriter,
	w http.ResponseWriter,
	r *http.Request,
) bool {
	if len(leaderMap) == 0 || coordinator == nil {
		return false
	}
	if coordinator.IsLeader() {
		return false
	}
	// Follower ingress: forward to the leader so reads and writes both see a
	// single serialization point. Serving locally on a follower would expose
	// G2-item-realtime stale reads.
	leader := coordinator.RaftLeader()
	if leader == "" {
		errWriter(w, r, httpLeaderProxyNotForwarded)
		return true
	}
	targetAddr, ok := leaderMap[leader]
	if !ok || strings.TrimSpace(targetAddr) == "" {
		errWriter(w, r, httpLeaderProxyNotForwarded)
		return true
	}
	target := &url.URL{Scheme: httpSchemePlain, Host: targetAddr}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		slog.Warn("leader HTTP proxy failed", "err", err)
		errWriter(rw, req, httpLeaderProxyForwardAttemptFailed)
	}
	proxy.ServeHTTP(w, r)
	return true
}

package adapter

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/bootjp/elastickv/kv"
)

// httpLeaderErrorWriter writes an adapter-specific error envelope (JSON for
// DynamoDB/SQS, XML for S3, RESP for Redis, ...) when the HTTP leader proxy
// cannot forward a follower request. The adapter owns the shape of the body;
// this helper only decides when to call it.
type httpLeaderErrorWriter func(w http.ResponseWriter, status int, message string)

// proxyHTTPRequestToLeader forwards r to the current Raft leader's HTTP
// adapter endpoint when this node is a follower. It returns true when the
// request was handled (either proxied or an error body was written) and
// false when the caller should serve it locally (no leader map configured,
// no coordinator, or this node is the leader).
//
// Error paths:
//  1. no known Raft leader            → 503 via errWriter("no raft leader currently available")
//  2. leader id missing from leaderMap → 503 via errWriter("leader address not found")
//  3. reverse-proxy dial/copy failure  → 503 via errWriter("leader proxy error: <reason>")
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
		errWriter(w, http.StatusServiceUnavailable, "no raft leader currently available")
		return true
	}
	targetAddr, ok := leaderMap[leader]
	if !ok || strings.TrimSpace(targetAddr) == "" {
		errWriter(w, http.StatusServiceUnavailable, "leader address not found")
		return true
	}
	target := &url.URL{Scheme: "http", Host: targetAddr}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(rw http.ResponseWriter, _ *http.Request, err error) {
		errWriter(rw, http.StatusServiceUnavailable, "leader proxy error: "+err.Error())
	}
	proxy.ServeHTTP(w, r)
	return true
}

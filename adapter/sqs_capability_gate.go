package adapter

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"strings"
)

// htfifoCapabilityRejectionPublic is the sanitized client-facing
// reason returned from validateHTFIFOCapability when the cluster
// poll fails. Per CodeRabbit major review: peer addresses and raw
// poller error text MUST NOT leak to authenticated clients (the
// CreateQueue surface is part of the public AWS-shaped API), so
// the wire-level message is intentionally generic and the per-peer
// detail goes to slog.Warn for operator triage.
const htfifoCapabilityRejectionPublic = "PartitionCount > 1 requires every cluster peer to advertise the htfifo capability via /sqs_health; one or more peers did not — see server logs for details"

// validateHTFIFOCapability is the §11 PR 5b-3 gate that replaced the
// PR 2 dormancy reject. CreateQueue calls this on every request; it
// is a no-op for legacy / single-partition meta and the full
// cluster-wide capability check for partitioned FIFO meta.
//
// Two-stage check, both fail-closed:
//
//  1. Local: this binary must advertise the htfifo capability
//     (htfifoCapabilityAdvertised). If false, no amount of peer
//     polling can make the create safe — the leader handling the
//     request will write the partitioned-shape meta but its own
//     data plane does not understand the partitioned keyspace.
//
//  2. Peers: every entry in s.leaderSQS must report htfifo via
//     /sqs_health within the poller's per-peer timeout. Any
//     timeout, HTTP error, malformed body, or missing capability
//     blocks the create. This catches mid-rolling-upgrade clusters
//     where the leader is on a new binary but a follower is still
//     on the old one — the follower would silently store a
//     partitioned record under the legacy keyspace if it ever won
//     leadership, so we refuse the create until everyone is on a
//     binary that handles the new layout.
//
// The vacuous case (single-node cluster, leaderSQS empty) is
// allowed: the local check covers the only node that will ever
// host the queue. proxyToLeader has already steered the request to
// the leader, and the leadership-refusal hook (PR 4-B-3b) keeps
// non-htfifo binaries from acquiring leadership over partitioned-
// queue Raft groups, so the gate's fail-closed default holds even
// after the create succeeds.
func (s *SQSServer) validateHTFIFOCapability(ctx context.Context, requested *sqsQueueMeta) error {
	if requested == nil || requested.PartitionCount <= 1 {
		return nil
	}
	if !htfifoCapabilityAdvertised {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"PartitionCount > 1 requires the htfifo capability, which this node does not advertise")
	}
	peers := s.collectSQSPeers()
	if len(peers) == 0 {
		// Single-node deployment: the local check above is the
		// whole cluster. Vacuously true on the peer side.
		return nil
	}
	report := PollSQSHTFIFOCapability(ctx, peers, PollerConfig{})
	if report == nil || !report.AllAdvertise {
		// Log the full per-peer detail for operator triage. The
		// client-visible message stays generic (no peer addresses,
		// no raw poller error text) so the CreateQueue surface
		// does not leak cluster topology to authenticated callers
		// — CodeRabbit major review on PR #734.
		slog.Warn("sqs: htfifo capability gate rejected partitioned CreateQueue",
			"queueName", requested.Name,
			"partitionCount", requested.PartitionCount,
			"peerCount", len(peers),
			"detail", formatHTFIFOCapabilityReportForLog(report))
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			htfifoCapabilityRejectionPublic)
	}
	return nil
}

// collectSQSPeers returns every distinct, non-empty SQS-side address
// from s.leaderSQS in deterministic (sorted) order. Used by the
// CreateQueue capability gate so error messages and tests pin a
// stable peer order. The map may legitimately contain self (the
// proxy-to-leader path uses the same map to find the leader's SQS
// address by Raft addr); polling self over loopback is cheap and
// keeps the "every peer reports htfifo" invariant uniform.
func (s *SQSServer) collectSQSPeers() []string {
	if len(s.leaderSQS) == 0 {
		return nil
	}
	peers := make([]string, 0, len(s.leaderSQS))
	seen := make(map[string]struct{}, len(s.leaderSQS))
	for _, addr := range s.leaderSQS {
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		peers = append(peers, addr)
	}
	sort.Strings(peers)
	return peers
}

// formatHTFIFOCapabilityReportForLog composes the per-peer detail
// surfaced to slog.Warn when the gate rejects. Lists each failing
// peer's address and reason (per-peer Error or "missing capability")
// so an operator triaging a partial-rolling-upgrade cluster can fix
// the lag from the server logs without rerunning the poll
// out-of-band. NEVER returned to the client — that path uses the
// sanitized htfifoCapabilityRejectionPublic constant. Order matches
// report.Peers, which matches collectSQSPeers' sorted input order —
// deterministic so log lines diff cleanly across runs.
func formatHTFIFOCapabilityReportForLog(report *HTFIFOCapabilityReport) string {
	var b strings.Builder
	if report == nil {
		b.WriteString("(no report)")
		return b.String()
	}
	first := true
	for _, p := range report.Peers {
		if p.HasHTFIFO {
			continue
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		b.WriteString(p.Address)
		b.WriteString(" (")
		if p.Error != "" {
			b.WriteString(p.Error)
		} else {
			b.WriteString("missing capability")
		}
		b.WriteString(")")
	}
	if first {
		// Defensive: AllAdvertise was false but no peer surfaced a
		// reason. Should never happen, but emit a non-empty hint
		// rather than a truncated empty string.
		b.WriteString("(unknown peer)")
	}
	return b.String()
}

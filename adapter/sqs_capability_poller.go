package adapter

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// HTFIFOCapabilityReport summarises the result of polling each peer's
// /sqs_health endpoint for the htfifo capability. Used by the
// CreateQueue capability gate (Phase 3.D PR 5) and by operator
// tooling that needs to confirm a rolling upgrade has finished
// before enabling partitioned FIFO queues.
//
// AllAdvertise is the binary go/no-go signal for the gate; Peers
// carries per-node detail for log lines and operator triage.
type HTFIFOCapabilityReport struct {
	// AllAdvertise is true iff every peer in the input list
	// returned a /sqs_health body whose `capabilities` array
	// contains the htfifo capability string. False on any timeout,
	// HTTP error, malformed body, or missing-capability — the
	// gate fails closed.
	//
	// Vacuously true on an empty peer list. The caller (CreateQueue
	// gate) is responsible for ensuring the peer list reflects the
	// current cluster membership before consulting this report.
	AllAdvertise bool

	// Peers is the per-peer status, indexed in input order. Each
	// entry has either HasHTFIFO=true (peer advertised the
	// capability) or a non-empty Error explaining why the peer
	// did not pass. Capabilities is the raw list returned by the
	// peer when the body was parseable.
	Peers []HTFIFOCapabilityPeerStatus
}

// HTFIFOCapabilityPeerStatus is one peer's polling result.
type HTFIFOCapabilityPeerStatus struct {
	// Address is the peer's host:port as supplied to the poller.
	Address string

	// HasHTFIFO is true iff the peer's /sqs_health JSON body's
	// capabilities array contained the htfifo capability string.
	HasHTFIFO bool

	// Capabilities is the parsed capabilities array. Nil on any
	// failure before JSON parsing, or non-nil but missing
	// htfifo when the peer is on an older binary.
	Capabilities []string

	// Error is empty on a clean success (HTTP 200 + parseable
	// JSON, regardless of whether HasHTFIFO is true) and non-empty
	// on any failure (transport error, non-200 status, malformed
	// JSON, or context cancellation).
	Error string
}

// defaultSQSCapabilityPollTimeout caps how long the poller waits on
// any single peer when PollerConfig.PerPeerTimeout is zero. The
// §8.5 design's "fail-closed default for nodes that don't respond
// within a short timeout" turns into a concrete bound here.
// Operators wanting a longer or shorter wait can override via
// PollerConfig; the cap is enforced in addition to any
// caller-supplied context deadline so a single slow peer cannot
// stall the whole poll.
const defaultSQSCapabilityPollTimeout = 3 * time.Second

// PollerConfig tunes PollSQSHTFIFOCapability for a specific call
// site. All fields are optional — the zero value picks safe
// defaults. Tests use the explicit PerPeerTimeout to exercise the
// per-peer cap independently of any caller-supplied context
// deadline.
type PollerConfig struct {
	// HTTPClient is the client used for /sqs_health GETs. Nil
	// falls back to http.DefaultClient. Callers wanting connection
	// pooling, custom Transport, or shorter Client.Timeout pass
	// their own.
	HTTPClient *http.Client

	// PerPeerTimeout caps how long any single peer's poll runs
	// before being abandoned. Zero defaults to
	// defaultSQSCapabilityPollTimeout (3s). Tests pass a small
	// value (e.g. 100ms) so the per-peer cap path can be
	// exercised quickly without a parent context deadline.
	PerPeerTimeout time.Duration
}

// PollSQSHTFIFOCapability polls each peer's /sqs_health endpoint
// concurrently and reports whether all advertise htfifo. The
// helper is stateless — every call dials its peers fresh, so a
// transient network blip on one call does not poison subsequent
// calls.
//
// Per-peer behaviour:
//   - GET http://<peer>/sqs_health with Accept: application/json
//   - Expect HTTP 200 and a parseable JSON body matching
//     {"status":"ok","capabilities":[...]}.
//   - HasHTFIFO is the membership of htfifo in capabilities.
//   - Any failure (transport error, non-200, malformed JSON,
//     timeout, context cancellation) records the reason in Error
//     and leaves HasHTFIFO=false. The poller never returns a
//     fatal error from PollSQSHTFIFOCapability itself; the report
//     carries every per-peer outcome instead.
//
// Concurrency: peers are polled in goroutines; results land via
// an indexed channel so the slice writes are obviously race-free.
//
// Timeouts: each peer poll is bounded by
// min(ctx.Deadline(), now+cfg.PerPeerTimeout). A long ctx deadline
// does not extend the per-peer cap, and an absent ctx deadline
// still triggers fail-closed at the per-peer cap.
func PollSQSHTFIFOCapability(ctx context.Context, peers []string, cfg PollerConfig) *HTFIFOCapabilityReport {
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	perPeerTimeout := cfg.PerPeerTimeout
	if perPeerTimeout <= 0 {
		perPeerTimeout = defaultSQSCapabilityPollTimeout
	}
	report := &HTFIFOCapabilityReport{
		Peers: make([]HTFIFOCapabilityPeerStatus, len(peers)),
	}
	if len(peers) == 0 {
		// Vacuously: every-of-empty is true. Operator decides
		// whether their peer list is meaningful.
		report.AllAdvertise = true
		return report
	}

	type indexedStatus struct {
		idx    int
		status HTFIFOCapabilityPeerStatus
	}
	results := make(chan indexedStatus, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, addr string) {
			defer wg.Done()
			results <- indexedStatus{
				idx:    idx,
				status: pollOneSQSPeerForHTFIFO(ctx, client, addr, perPeerTimeout),
			}
		}(i, peer)
	}
	wg.Wait()
	close(results)

	allAdvertise := true
	for r := range results {
		report.Peers[r.idx] = r.status
		if !r.status.HasHTFIFO {
			allAdvertise = false
		}
	}
	report.AllAdvertise = allAdvertise
	return report
}

// pollOneSQSPeerForHTFIFO polls a single peer's /sqs_health and
// returns its capability status. Any error is captured in the
// returned struct's Error field — this function never returns a
// Go error itself so the caller can map peers to results in one
// pass without checking len(errors).
func pollOneSQSPeerForHTFIFO(ctx context.Context, client *http.Client, peer string, perPeerTimeout time.Duration) HTFIFOCapabilityPeerStatus {
	status := HTFIFOCapabilityPeerStatus{Address: peer}

	if peer == "" {
		status.Error = "empty peer address"
		return status
	}

	pollCtx, cancel := context.WithTimeout(ctx, perPeerTimeout)
	defer cancel()

	url := buildSQSHealthURL(peer)
	req, err := http.NewRequestWithContext(pollCtx, http.MethodGet, url, http.NoBody)
	if err != nil {
		status.Error = errors.Wrapf(err, "build request for %q", peer).Error()
		return status
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		status.Error = errors.Wrapf(err, "GET %q", url).Error()
		return status
	}
	// Close the body via a deferred closure so a non-nil close
	// error surfaces in the cluster logs rather than being
	// dropped — masking it could hide leaking connections under
	// load (gemini medium on PR #721). Body is also drained on
	// every early return below so the http.Transport can reuse
	// the underlying TCP connection (claude minor on PR #721).
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			slog.Warn("sqs capability poller: response body close failed",
				"peer", peer, "err", cerr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		// Drain the body before returning so the transport can
		// reuse the connection. Non-200 bodies under our 1 KiB
		// LimitReader are tiny, so the discard cost is negligible.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, sqsCapabilityMaxBodyBytes))
		status.Error = fmt.Sprintf("%s returned HTTP %d", url, resp.StatusCode)
		return status
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, sqsCapabilityMaxBodyBytes))
	if err != nil {
		status.Error = errors.Wrapf(err, "read body from %q", url).Error()
		return status
	}

	var parsed sqsHealthBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		status.Error = fmt.Sprintf("malformed JSON from %s: %v", url, err)
		return status
	}

	status.Capabilities = parsed.Capabilities
	for _, c := range parsed.Capabilities {
		if c == sqsCapabilityHTFIFO {
			status.HasHTFIFO = true
			break
		}
	}
	return status
}

// sqsCapabilityMaxBodyBytes caps how much of the /sqs_health
// response we read before bailing. The current body shape is a
// short JSON object; an unbounded read would let a misconfigured
// peer return megabytes. 1 KiB is far above the realistic body
// size and far below "expensive to read".
const sqsCapabilityMaxBodyBytes = 1 << 10

// buildSQSHealthURL prefixes peer with the http:// scheme when the
// caller passed a bare host:port (the common case for
// --raftSqsMap entries). Callers that need https:// can pass the
// fully-qualified URL.
func buildSQSHealthURL(peer string) string {
	if strings.HasPrefix(peer, "http://") || strings.HasPrefix(peer, "https://") {
		return strings.TrimRight(peer, "/") + sqsHealthPath
	}
	return "http://" + peer + sqsHealthPath
}

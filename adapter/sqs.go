package adapter

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// SQS target prefix for the JSON-1.0 protocol. Every supported operation is
// dispatched by the X-Amz-Target header, mirroring the DynamoDB adapter.
const sqsTargetPrefix = "AmazonSQS."

const (
	sqsCreateQueueTarget               = sqsTargetPrefix + "CreateQueue"
	sqsDeleteQueueTarget               = sqsTargetPrefix + "DeleteQueue"
	sqsListQueuesTarget                = sqsTargetPrefix + "ListQueues"
	sqsGetQueueUrlTarget               = sqsTargetPrefix + "GetQueueUrl"
	sqsGetQueueAttributesTarget        = sqsTargetPrefix + "GetQueueAttributes"
	sqsSetQueueAttributesTarget        = sqsTargetPrefix + "SetQueueAttributes"
	sqsPurgeQueueTarget                = sqsTargetPrefix + "PurgeQueue"
	sqsSendMessageTarget               = sqsTargetPrefix + "SendMessage"
	sqsSendMessageBatchTarget          = sqsTargetPrefix + "SendMessageBatch"
	sqsReceiveMessageTarget            = sqsTargetPrefix + "ReceiveMessage"
	sqsDeleteMessageTarget             = sqsTargetPrefix + "DeleteMessage"
	sqsDeleteMessageBatchTarget        = sqsTargetPrefix + "DeleteMessageBatch"
	sqsChangeMessageVisibilityTarget   = sqsTargetPrefix + "ChangeMessageVisibility"
	sqsChangeMessageVisibilityBatchTgt = sqsTargetPrefix + "ChangeMessageVisibilityBatch"
	sqsTagQueueTarget                  = sqsTargetPrefix + "TagQueue"
	sqsUntagQueueTarget                = sqsTargetPrefix + "UntagQueue"
	sqsListQueueTagsTarget             = sqsTargetPrefix + "ListQueueTags"
)

const (
	sqsHealthPath       = "/sqs_health"
	sqsLeaderHealthPath = "/sqs_leader_health"
)

// sqsCapabilityHTFIFO is the capability string a binary advertises on
// /sqs_health (when Accept: application/json) once it has the runtime
// pieces required to safely host a partitioned FIFO queue: the routing
// layer is wired through --sqsFifoPartitionMap (see main.go), and the
// leadership-refusal hook in kv refuses leadership for an SQS Raft
// group that hosts a partitioned queue when the binary itself does
// not advertise this string.
//
// CreateQueue's catalog-polling gate (Phase 3.D PR 5 lifts the
// dormancy and starts checking) reads this list off /sqs_health on
// every peer; a CreateQueue with PartitionCount > 1 is rejected
// unless every peer reports "htfifo" — fail-closed against rolling
// upgrades that have not yet finished.
const sqsCapabilityHTFIFO = "htfifo"

// htfifoCapabilityAdvertised gates whether this binary lists
// "htfifo" on /sqs_health. The §11 PR 4 contract requires BOTH
// the routing-layer wiring AND the leadership-refusal safeguard
// from §8 to be in place before this flag is true:
//
//   - Routing wiring: kv.PartitionResolver +
//     adapter.SQSPartitionResolver, merged via #715 (Phase 3.D
//     PR 4-B-2). Partition-resolver-first dispatch in ShardRouter
//     routes (queue, partition) keys to the operator-chosen Raft
//     group; coordinator helpers (groupForKey,
//     routeAndGroupForKey, groupMutations) consult the resolver
//     before falling through to the byte-range engine; OCC read
//     keys fail closed for recognised-but-unresolved partitioned
//     keys.
//   - Capability poller: PollSQSHTFIFOCapability, merged via
//     #721 (Phase 3.D PR 4-B-3a). PR 5 will use this for the
//     CreateQueue capability gate.
//   - Leadership-refusal hook:
//     raftengine.RegisterLeaderAcquiredCallback +
//     main_sqs_leadership_refusal.go (Phase 3.D PR 4-B-3b, this
//     PR). On startup AND on every leader-acquired transition,
//     the hook refuses leadership of any Raft group hosting a
//     partitioned queue when the binary lacks htfifo.
//
// Both pieces are now in the binary, so the flag flips to true.
// PR 5 lifts the PartitionCount > 1 dormancy gate AND wires the
// CreateQueue capability poll in the same commit, at which point
// a partitioned queue can land in production and every node in
// the cluster must report htfifo for the gate to allow it.
//
// Stays a const (not a var) because the flag is build-time. A
// future runtime override (env var, --no-htfifo flag for
// graceful degradation) would reroute through
// adapter.AdvertisesHTFIFO() without changing the call sites.
const htfifoCapabilityAdvertised = true

// sqsAdvertisedCapabilities returns the capability list emitted on
// /sqs_health (JSON mode). Stable iteration order is significant —
// catalog peers may diff capability lists across nodes when checking
// rollout uniformity, so the list is built deterministically. The
// returned slice is freshly allocated per call so the caller may
// mutate it without aliasing the package-level state.
func sqsAdvertisedCapabilities() []string {
	caps := make([]string, 0, 1)
	if htfifoCapabilityAdvertised {
		caps = append(caps, sqsCapabilityHTFIFO)
	}
	return caps
}

// AdvertisesHTFIFO reports whether this binary's /sqs_health
// endpoint lists the htfifo capability. Mirror of the package-
// internal htfifoCapabilityAdvertised constant, exposed for the
// SQS leadership-refusal hook in main.go that uses this signal
// to decide whether to refuse leadership of any Raft group hosting
// a partitioned FIFO queue.
//
// Stays a function (not an exported constant) so a future runtime
// override (env var, --no-htfifo flag for graceful degradation)
// can be threaded through here without changing the call site.
func AdvertisesHTFIFO() bool {
	return htfifoCapabilityAdvertised
}

const (
	sqsHealthMaxRequestBodyBytes = 1024
	sqsMaxRequestBodyBytes       = 1 << 20
	sqsContentTypeJSON           = "application/x-amz-json-1.0"
)

// AWS SQS error codes used by the JSON protocol. The canonical list is on the
// "Common Errors" page of the SQS API reference.
const (
	sqsErrInvalidAction      = "InvalidAction"
	sqsErrInternalFailure    = "InternalFailure"
	sqsErrServiceUnavailable = "ServiceUnavailable"
	sqsErrMalformedRequest   = "MalformedQueryString"
	// sqsErrThrottling is the per-queue rate-limit rejection code.
	// Returned with HTTP 400 and a Retry-After header derived from the
	// bucket's refillRate + the request's charge count (see
	// computeRetryAfter in sqs_throttle.go for the formula).
	sqsErrThrottling = "Throttling"
)

type SQSServerOption func(*SQSServer)

type SQSServer struct {
	listen         net.Listener
	store          store.MVCCStore
	coordinator    kv.Coordinator
	httpServer     *http.Server
	targetHandlers map[string]func(http.ResponseWriter, *http.Request)
	leaderSQS      map[string]string
	region         string
	staticCreds    map[string]string
	// reaperCtx / reaperCancel drive the retention sweeper goroutine.
	// Both are initialized in NewSQSServer (never reassigned) so a
	// concurrent Stop() that lands before Run() completes still reads
	// a stable cancel func — unlike a Run-time assignment, which the
	// race detector flagged because Run and Stop run on different
	// goroutines without ordering between them.
	reaperCtx    context.Context
	reaperCancel context.CancelFunc
	// throttle is the per-queue rate-limit bucket store. Always
	// non-nil; charge() short-circuits when the queue's meta has no
	// throttle config so unconfigured queues pay one nil-check per
	// request and nothing else (see sqs_throttle.go).
	throttle *bucketStore
}

// WithSQSLeaderMap configures the Raft-address-to-SQS-address mapping used to
// forward requests from followers to the current leader. Format mirrors
// WithDynamoDBLeaderMap / WithS3LeaderMap.
func WithSQSLeaderMap(m map[string]string) SQSServerOption {
	return func(s *SQSServer) {
		s.leaderSQS = make(map[string]string, len(m))
		for k, v := range m {
			s.leaderSQS[k] = v
		}
	}
}

func NewSQSServer(listen net.Listener, st store.MVCCStore, coordinate kv.Coordinator, opts ...SQSServerOption) *SQSServer {
	reaperCtx, reaperCancel := context.WithCancel(context.Background())
	s := &SQSServer{
		listen:       listen,
		store:        st,
		coordinator:  coordinate,
		reaperCtx:    reaperCtx,
		reaperCancel: reaperCancel,
		throttle:     newBucketStoreDefault(),
	}
	s.targetHandlers = map[string]func(http.ResponseWriter, *http.Request){
		sqsCreateQueueTarget:               s.createQueue,
		sqsDeleteQueueTarget:               s.deleteQueue,
		sqsListQueuesTarget:                s.listQueues,
		sqsGetQueueUrlTarget:               s.getQueueUrl,
		sqsGetQueueAttributesTarget:        s.getQueueAttributes,
		sqsSetQueueAttributesTarget:        s.setQueueAttributes,
		sqsPurgeQueueTarget:                s.purgeQueue,
		sqsSendMessageTarget:               s.sendMessage,
		sqsSendMessageBatchTarget:          s.sendMessageBatch,
		sqsReceiveMessageTarget:            s.receiveMessage,
		sqsDeleteMessageTarget:             s.deleteMessage,
		sqsDeleteMessageBatchTarget:        s.deleteMessageBatch,
		sqsChangeMessageVisibilityTarget:   s.changeMessageVisibility,
		sqsChangeMessageVisibilityBatchTgt: s.changeMessageVisibilityBatch,
		sqsTagQueueTarget:                  s.tagQueue,
		sqsUntagQueueTarget:                s.untagQueue,
		sqsListQueueTagsTarget:             s.listQueueTags,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	s.httpServer = &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func (s *SQSServer) Run() error {
	s.startReaper(s.reaperCtx)
	// Throttle bucket idle-evict runs on a background ticker so the
	// request hot path never pays the O(N) sweep cost. Cleaned up by
	// the same reaperCtx cancellation that stops the message reaper.
	go s.throttle.runSweepLoop(s.reaperCtx)
	if err := s.httpServer.Serve(s.listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.WithStack(err)
	}
	return nil
}

func (s *SQSServer) Stop() {
	if s.reaperCancel != nil {
		s.reaperCancel()
	}
	if s.httpServer != nil {
		_ = s.httpServer.Shutdown(context.Background())
	}
}

func (s *SQSServer) handle(w http.ResponseWriter, r *http.Request) {
	if s.serveHealthz(w, r) {
		return
	}
	if s.proxyToLeader(w, r) {
		return
	}

	// pickSqsProtocol decides between the JSON path (X-Amz-Target +
	// JSON body, the existing default) and the query path (form-
	// encoded body, XML response) on a per-request basis. See
	// docs/design/2026_04_26_proposed_sqs_query_protocol.md for the
	// detection rules.
	if pickSqsProtocol(r) == sqsProtocolQuery {
		s.handleQueryProtocol(w, r)
		return
	}
	// JSON / Unknown both fall through to the JSON path: the JSON-
	// style 400 is the most informative error for a client that
	// has not picked a codec yet (§3 of the design doc). The
	// dispatch table below stays the single decision point.

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeSQSError(w, http.StatusMethodNotAllowed, sqsErrMalformedRequest, "SQS JSON protocol requires POST")
		return
	}

	if authErr := s.authorizeSQSRequest(r); authErr != nil {
		writeSQSError(w, authErr.Status, authErr.Code, authErr.Message)
		return
	}

	target := r.Header.Get("X-Amz-Target")
	handler, ok := s.targetHandlers[target]
	if !ok {
		writeSQSError(w, http.StatusBadRequest, sqsErrInvalidAction, "unsupported SQS target: "+target)
		return
	}
	handler(w, r)
}

// handleQueryProtocol owns the query-protocol leg of handle(): method
// gating, SigV4 authorisation against the form body, and dispatch
// into per-verb handlers. Pulled out of handle() so the dispatcher
// stays under cyclop=10 even as more wire formats are added.
func (s *SQSServer) handleQueryProtocol(w http.ResponseWriter, r *http.Request) {
	// GET is legal for query (some legacy ListQueues callers).
	// POST is the common case. Anything else (PUT/DELETE) is
	// outside the SQS surface entirely.
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		writeSQSQueryError(w, newSQSAPIError(http.StatusMethodNotAllowed, sqsErrMalformedRequest,
			"SQS query protocol requires GET or POST"))
		return
	}
	if authErr := s.authorizeSQSRequest(r); authErr != nil {
		writeSQSQueryError(w, newSQSAPIError(authErr.Status, authErr.Code, authErr.Message))
		return
	}
	s.handleQuery(w, r)
}

func (s *SQSServer) serveHealthz(w http.ResponseWriter, r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	switch r.URL.Path {
	case sqsHealthPath:
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, sqsHealthMaxRequestBodyBytes)
		}
		serveSQSHealthz(w, r)
		return true
	case sqsLeaderHealthPath:
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, sqsHealthMaxRequestBodyBytes)
		}
		s.serveSQSLeaderHealthz(w, r)
		return true
	default:
		return false
	}
}

func serveSQSHealthz(w http.ResponseWriter, r *http.Request) {
	if !writeSQSHealthMethod(w, r) {
		return
	}
	if clientAcceptsSQSHealthJSON(r) {
		writeSQSHealthJSONBody(w, r, http.StatusOK, sqsHealthBody{
			Status:       "ok",
			Capabilities: sqsAdvertisedCapabilities(),
		})
		return
	}
	writeSQSHealthBody(w, r, http.StatusOK, "ok\n")
}

// sqsHealthBody is the JSON shape returned by /sqs_health when the
// caller passes Accept: application/json. Stable across binary
// versions — catalog peers diff this body during the CreateQueue
// gate (Phase 3.D PR 5).
type sqsHealthBody struct {
	Status       string   `json:"status"`
	Capabilities []string `json:"capabilities"`
}

// clientAcceptsSQSHealthJSON reports whether the caller signalled
// JSON in the Accept header. Treat the absence of an Accept header
// (and a bare "*/*" wildcard) as the legacy "ok\n" client to keep
// the existing health-check integrations (curl, k8s liveness probes)
// byte-identical.
//
// A substring check for "application/json" is sufficient — q-factor
// and parameter parsing would be overkill for a health endpoint, and
// any JSON-aware client passes the literal token in a comma-separated
// list. False matches against media types like "application/jsonseq"
// are accepted: a client that explicitly types out a JSON-adjacent
// media type is opting in to the JSON shape on purpose.
func clientAcceptsSQSHealthJSON(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, raw := range r.Header.Values("Accept") {
		if raw == "" || raw == "*/*" {
			continue
		}
		if strings.Contains(raw, "application/json") {
			return true
		}
	}
	return false
}

func writeSQSHealthJSONBody(w http.ResponseWriter, r *http.Request, statusCode int, body sqsHealthBody) {
	encoded, err := json.Marshal(body)
	if err != nil {
		// json.Marshal of a fixed shape with a string + []string
		// cannot realistically fail; fall back to the legacy text
		// path so a misconfigured client still gets a 200.
		writeSQSHealthBody(w, r, statusCode, "ok\n")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(encoded)
	_, _ = io.WriteString(w, "\n")
}

func (s *SQSServer) serveSQSLeaderHealthz(w http.ResponseWriter, r *http.Request) {
	if !writeSQSHealthMethod(w, r) {
		return
	}
	if isVerifiedSQSLeader(s.coordinator) {
		writeSQSHealthBody(w, r, http.StatusOK, "ok\n")
		return
	}
	writeSQSHealthBody(w, r, http.StatusServiceUnavailable, "not leader\n")
}

func isVerifiedSQSLeader(coordinator kv.Coordinator) bool {
	if coordinator == nil || !coordinator.IsLeader() {
		return false
	}
	return coordinator.VerifyLeader() == nil
}

func writeSQSHealthMethod(w http.ResponseWriter, r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		return true
	default:
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return false
	}
}

func writeSQSHealthBody(w http.ResponseWriter, r *http.Request, statusCode int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = io.WriteString(w, body)
}

// proxyToLeader forwards the HTTP request to the current SQS leader when this
// node is not the Raft leader. Returns true if the request was handled.
func (s *SQSServer) proxyToLeader(w http.ResponseWriter, r *http.Request) bool {
	return proxyHTTPRequestToLeader(s.coordinator, s.leaderSQS, sqsLeaderProxyErrorWriter, w, r)
}

func sqsLeaderProxyErrorWriter(w http.ResponseWriter, status int, message string) {
	writeSQSError(w, status, sqsErrServiceUnavailable, message)
}

// writeSQSError emits an SQS JSON-protocol error envelope. AWS returns:
//
//	{ "__type": "<code>", "message": "<text>" }
//
// with Content-Type application/x-amz-json-1.0 and the x-amzn-ErrorType header
// set to the code. SDKs key off x-amzn-ErrorType first, the body second.
func writeSQSError(w http.ResponseWriter, status int, code string, message string) {
	resp := map[string]string{"message": message}
	if code != "" {
		resp["__type"] = code
		w.Header().Set("x-amzn-ErrorType", code)
	}
	w.Header().Set("Content-Type", sqsContentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

// writeSQSThrottlingError emits the rate-limit rejection envelope: 400
// + the AWS-shaped JSON error body + a Retry-After header carrying
// the integer-second wait derived from the bucket's refill rate and
// the request's charge count. The action argument is the bucket-action
// vocabulary ("Send" | "Receive" | "*") so the operator-visible
// message names the bucket that ran out, not just the queue.
func writeSQSThrottlingError(w http.ResponseWriter, queue, action string, retryAfter time.Duration) {
	if retryAfter < time.Second {
		retryAfter = time.Second
	}
	secs := int(retryAfter / time.Second)
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	writeSQSError(w, http.StatusBadRequest, sqsErrThrottling,
		"Rate exceeded for queue '"+queue+"' action '"+action+"'")
}

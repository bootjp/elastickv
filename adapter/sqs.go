package adapter

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bootjp/elastickv/keyviz"
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
// CreateQueue's cluster-wide capability gate (validateHTFIFOCapability,
// landed in Phase 3.D PR 5b-3) reads this list off /sqs_health on
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
//     #721 (Phase 3.D PR 4-B-3a) and now consumed by
//     validateHTFIFOCapability in the CreateQueue gate (PR 5b-3).
//   - Leadership-refusal hook:
//     raftengine.RegisterLeaderAcquiredCallback +
//     main_sqs_leadership_refusal.go (Phase 3.D PR 4-B-3b, this
//     PR). On startup AND on every leader-acquired transition,
//     the hook refuses leadership of any Raft group hosting a
//     partitioned queue when the binary lacks htfifo.
//
// Both pieces are in the binary and PR 5b-3 has lifted the
// dormancy gate, so a partitioned queue can land in production —
// CreateQueue's validateHTFIFOCapability gate refuses
// PartitionCount > 1 unless every node in the cluster reports
// htfifo on /sqs_health.
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
	// receiveFanoutCounters maps queueName → *atomic.Uint32 so each
	// partitioned queue gets its own round-robin starting partition.
	// Codex P1 round 4 flagged that a server-wide counter aliases
	// across queues: when other queues' receives interleave with a
	// stride that shares a factor with PartitionCount, the queue's
	// observed counter subsequence cycles through only a subset of
	// partitions, which can starve the rest under MaxNumberOfMessages
	// pressure on the early-scanned ones. Per-queue isolation makes
	// each queue's rotation depend solely on its own receive cadence.
	//
	// sync.Map is the right shape: lookups are read-mostly (the same
	// queue keeps getting the same counter), and the keyset grows
	// only with the number of distinct queues this server has handled
	// receives for in-process — bounded by the operator-controlled
	// CreateQueue rate.
	receiveFanoutCounters sync.Map
	// partitionResolver, when non-nil, is the per-cluster resolver
	// that maps (queue, partition) keys to operator-chosen Raft
	// groups (built from --sqsFifoPartitionMap, see main.go). The
	// CreateQueue capability gate (validateHTFIFOCapability) uses
	// it to verify routing coverage on partitioned creates BEFORE
	// the meta record commits — without that check, a queue could
	// land with PartitionCount=N but only K<N routes installed,
	// and SendMessage on the missing partitions would fail closed
	// at the router with "no route for key" (Codex P1 review on
	// PR #734).
	//
	// nil on single-shard / no---sqsFifoPartitionMap deployments;
	// the gate's resolver==nil branch then skips the coverage
	// check so partitioned queues can land on a single-shard
	// cluster and route through the engine's default group.
	partitionResolver *SQSPartitionResolver
	// partitionObserver records the
	// elastickv_sqs_partition_messages_total{queue, partition,
	// action} counter for HT-FIFO operations (PR 7a). nil on
	// non-monitored test fixtures and on single-binary CLI
	// tools that build SQSServer without a monitoring registry.
	// Increment call sites use a nil-receiver-safe call so the
	// metrics path costs nothing when unwired.
	partitionObserver SQSPartitionObserver
	// throttleObserver records configured per-queue throttle
	// outcomes: rejected-request counters and remaining-token gauges.
	// nil on non-monitored fixtures; observeThrottleDecision is
	// nil-safe so the request path pays one branch when unwired.
	throttleObserver SQSThrottleObserver
}

// SQSPartitionObserver is the metrics-package interface
// (monitoring.SQSPartitionObserver) re-declared here so the
// adapter does not import monitoring at the package boundary —
// matches the existing observer pattern for DynamoDB / Redis.
type SQSPartitionObserver interface {
	ObservePartitionMessage(queue string, partition uint32, action string)
}

// SQSThrottleObserver is the metrics-package interface
// (monitoring.SQSThrottleObserver) re-declared here so the adapter
// does not import monitoring at the package boundary.
type SQSThrottleObserver interface {
	ObserveThrottleDecision(queue string, action string, tokensRemaining float64, throttled bool)
	ForgetThrottleAction(queue string, action string)
	SyncThrottleActions(queue string, enabledActions []string)
}

type sqsThrottleRejectionObserver interface {
	ObserveThrottleRejection(queue string, action string)
}

type sqsThrottleGaugeCutoffObserver interface {
	ThrottleGaugeSnapshotCutoff() uint64
}

type sqsThrottleGaugeResetObserver interface {
	ForgetThrottleActionBefore(queue string, action string, cutoff uint64)
}

// SQS metric action labels mirror the values from monitoring/sqs.go.
// Re-declared so adapter call sites do not need a monitoring import; the
// observer interface validates the value at runtime so a drift between these
// constants and the monitoring side surfaces as a dropped observation rather
// than a wedge.
const (
	SQSPartitionActionSend    = "send"
	SQSPartitionActionReceive = "receive"
	SQSPartitionActionDelete  = "delete"

	SQSThrottleActionSend    = "send"
	SQSThrottleActionReceive = "receive"
	SQSThrottleActionDefault = "default"
)

var sqsThrottleMetricActions = [...]string{
	SQSThrottleActionSend,
	SQSThrottleActionReceive,
	SQSThrottleActionDefault,
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

// WithSQSPartitionObserver installs the
// elastickv_sqs_partition_messages_total counter observer on the
// SQS server. Pass nil (the default) on non-monitored test
// fixtures; the partitioned send / receive / delete paths then
// observe via a nil interface and the metric stays at zero. The
// monitoring registry's SQSPartitionObserver() returns the
// concrete implementation in production.
func WithSQSPartitionObserver(o SQSPartitionObserver) SQSServerOption {
	return func(s *SQSServer) { s.partitionObserver = o }
}

// WithSQSThrottleObserver installs the
// elastickv_sqs_throttled_requests_total and
// elastickv_sqs_throttle_tokens_remaining observer on the SQS
// server. Pass nil (the default) on non-monitored fixtures.
func WithSQSThrottleObserver(o SQSThrottleObserver) SQSServerOption {
	return func(s *SQSServer) { s.throttleObserver = o }
}

// observePartitionMessage is a nil-receiver-safe wrapper around
// the configured observer. Pulled into a helper so the call
// sites in send / receive / delete each cost one branch instead
// of repeating the nil check.
func (s *SQSServer) observePartitionMessage(queue string, partition uint32, action string) {
	if s == nil || s.partitionObserver == nil {
		return
	}
	s.partitionObserver.ObservePartitionMessage(queue, partition, action)
}

func (s *SQSServer) observeThrottleDecision(queue string, outcome chargeOutcome, observeTokens bool) {
	if s == nil || s.throttleObserver == nil {
		return
	}
	if !outcome.bucketPresent && outcome.allowed {
		return
	}
	action := sqsThrottleMetricAction(outcome.action)
	if !observeTokens {
		if !outcome.allowed {
			if observer, ok := s.throttleObserver.(sqsThrottleRejectionObserver); ok {
				observer.ObserveThrottleRejection(queue, action)
			}
		}
		return
	}
	s.throttleObserver.ObserveThrottleDecision(
		queue,
		action,
		outcome.tokensAfter,
		!outcome.allowed,
	)
}

func (s *SQSServer) observeThrottleConfig(queue string, throttle *sqsQueueThrottle) {
	if s == nil || s.throttleObserver == nil {
		return
	}
	s.throttleObserver.SyncThrottleActions(queue, enabledThrottleMetricActions(throttle))
}

func (s *SQSServer) observeThrottleConfigChange(queue string, throttle *sqsQueueThrottle, resetActions []string, resetCutoff uint64) {
	if s == nil || s.throttleObserver == nil {
		return
	}
	s.throttleObserver.SyncThrottleActions(queue, enabledThrottleMetricActions(throttle))
	for _, action := range resetActions {
		if observer, ok := s.throttleObserver.(sqsThrottleGaugeResetObserver); ok {
			observer.ForgetThrottleActionBefore(queue, action, resetCutoff)
			continue
		}
		s.throttleObserver.ForgetThrottleAction(queue, action)
	}
}

func (s *SQSServer) observeThrottleDelete(queue string, resetCutoff uint64) {
	if s == nil || s.throttleObserver == nil {
		return
	}
	observer, ok := s.throttleObserver.(sqsThrottleGaugeResetObserver)
	if !ok {
		s.observeThrottleConfig(queue, nil)
		return
	}
	for _, action := range sqsThrottleMetricActions {
		observer.ForgetThrottleActionBefore(queue, action, resetCutoff)
	}
}

func (s *SQSServer) throttleGaugeSnapshotCutoff() uint64 {
	if s == nil || s.throttleObserver == nil {
		return 0
	}
	observer, ok := s.throttleObserver.(sqsThrottleGaugeCutoffObserver)
	if !ok {
		return 0
	}
	return observer.ThrottleGaugeSnapshotCutoff()
}

func (s *SQSServer) beginThrottleReset(queue string) uint64 {
	if s == nil || s.throttle == nil {
		return s.throttleGaugeSnapshotCutoff()
	}
	return s.throttle.beginQueueReset(queue, s.throttleGaugeSnapshotCutoff)
}

func enabledThrottleMetricActions(throttle *sqsQueueThrottle) []string {
	if throttle == nil || throttle.IsEmpty() {
		return nil
	}
	enabled := make([]string, 0, len(sqsThrottleMetricActions))
	if throttle.SendCapacity > 0 {
		enabled = append(enabled, SQSThrottleActionSend)
	}
	if throttle.RecvCapacity > 0 {
		enabled = append(enabled, SQSThrottleActionReceive)
	}
	if throttle.DefaultCapacity > 0 {
		enabled = append(enabled, SQSThrottleActionDefault)
	}
	return enabled
}

// WithSQSPartitionResolver installs the cluster's partition
// resolver on the SQS server so the CreateQueue capability gate
// (validateHTFIFOCapability) can verify routing coverage before
// admitting a partitioned create. Pass nil (the default) on
// single-shard / no---sqsFifoPartitionMap deployments — the gate
// then skips the coverage check.
//
// Callers must ensure the resolver passed here matches the one
// installed on the kv coordinator via WithPartitionResolver,
// otherwise the gate would admit a queue that the coordinator
// then fails to route. main.go builds the resolver once and
// hands the same pointer to both consumers.
func WithSQSPartitionResolver(r *SQSPartitionResolver) SQSServerOption {
	return func(s *SQSServer) { s.partitionResolver = r }
}

func NewSQSServer(listen net.Listener, st store.MVCCStore, coordinate kv.Coordinator, opts ...SQSServerOption) *SQSServer {
	reaperCtx, reaperCancel := context.WithCancel(context.Background())
	s := &SQSServer{
		listen:       listen,
		store:        st,
		coordinator:  kv.WithKeyVizLabel(coordinate, keyviz.LabelSQS),
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
	if s.listen == nil {
		// Listenless mode: the SQS adapter was constructed without a
		// public HTTP listener (--sqsAddress empty). Admin endpoints
		// in adapter/sqs_admin.go still work because they go through
		// the coordinator/store — only the SigV4 wire surface is
		// suppressed. Block until Stop() cancels reaperCtx so the
		// errgroup task lifetime matches the listening branch
		// (callers wait on Run() returning to know it's safe to tear
		// down the underlying coordinator/store).
		<-s.reaperCtx.Done()
		return nil
	}
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
		// http.Server.Shutdown returns immediately when Serve was
		// never called (listenless mode — no public listener was
		// constructed), so this is a no-op for admin-only
		// deployments. The branch is still gated on httpServer
		// being non-nil because Shutdown panics on a nil receiver.
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
	// docs/design/2026_04_26_implemented_sqs_query_protocol.md for the
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
	if isVerifiedSQSLeader(r.Context(), s.coordinator) {
		writeSQSHealthBody(w, r, http.StatusOK, "ok\n")
		return
	}
	writeSQSHealthBody(w, r, http.StatusServiceUnavailable, "not leader\n")
}

func isVerifiedSQSLeader(ctx context.Context, coordinator kv.Coordinator) bool {
	if coordinator == nil || !coordinator.IsLeader() {
		return false
	}
	return coordinator.VerifyLeader(ctx) == nil
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

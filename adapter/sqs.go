package adapter

import (
	"context"
	"io"
	"net"
	"net/http"
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
	writeSQSHealthBody(w, r, http.StatusOK, "ok\n")
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

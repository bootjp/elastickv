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
	sqsErrNotImplemented     = "NotImplemented"
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
	s := &SQSServer{
		listen:      listen,
		store:       st,
		coordinator: coordinate,
	}
	s.targetHandlers = map[string]func(http.ResponseWriter, *http.Request){
		sqsCreateQueueTarget:               s.createQueue,
		sqsDeleteQueueTarget:               s.deleteQueue,
		sqsListQueuesTarget:                s.listQueues,
		sqsGetQueueUrlTarget:               s.getQueueUrl,
		sqsGetQueueAttributesTarget:        s.getQueueAttributes,
		sqsSetQueueAttributesTarget:        s.setQueueAttributes,
		sqsPurgeQueueTarget:                s.notImplemented("PurgeQueue"),
		sqsSendMessageTarget:               s.notImplemented("SendMessage"),
		sqsSendMessageBatchTarget:          s.notImplemented("SendMessageBatch"),
		sqsReceiveMessageTarget:            s.notImplemented("ReceiveMessage"),
		sqsDeleteMessageTarget:             s.notImplemented("DeleteMessage"),
		sqsDeleteMessageBatchTarget:        s.notImplemented("DeleteMessageBatch"),
		sqsChangeMessageVisibilityTarget:   s.notImplemented("ChangeMessageVisibility"),
		sqsChangeMessageVisibilityBatchTgt: s.notImplemented("ChangeMessageVisibilityBatch"),
		sqsTagQueueTarget:                  s.notImplemented("TagQueue"),
		sqsUntagQueueTarget:                s.notImplemented("UntagQueue"),
		sqsListQueueTagsTarget:             s.notImplemented("ListQueueTags"),
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
	if err := s.httpServer.Serve(s.listen); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.WithStack(err)
	}
	return nil
}

func (s *SQSServer) Stop() {
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

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeSQSError(w, http.StatusMethodNotAllowed, sqsErrMalformedRequest, "SQS JSON protocol requires POST")
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

// notImplemented returns a handler that responds with a JSON-protocol
// NotImplemented error so clients get a clean signal while the real handlers
// are still being built out.
func (s *SQSServer) notImplemented(op string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeSQSError(w, http.StatusNotImplemented, sqsErrNotImplemented, op+" is not implemented yet")
	}
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

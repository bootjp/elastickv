package adapter

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// AWS SQS defaults, reproduced from the public API reference so clients that
// send an empty Attributes map get standard behavior.
const (
	sqsDefaultVisibilityTimeoutSeconds  = 30
	sqsDefaultRetentionSeconds          = 345600 // 4 days
	sqsDefaultDelaySeconds              = 0
	sqsDefaultReceiveMessageWaitSeconds = 0
	sqsDefaultMaximumMessageSize        = 262144 // 256 KiB

	sqsMaxVisibilityTimeoutSeconds      = 43200   // 12 hours
	sqsMaxRetentionSeconds              = 1209600 // 14 days
	sqsMinRetentionSeconds              = 60
	sqsMaxDelaySeconds                  = 900
	sqsMaxReceiveMessageWaitSeconds     = 20
	sqsMinMaximumMessageSize            = 1024
	sqsMaximumAllowedMaximumMessageSize = 262144
	sqsMaxQueueNameLength               = 80
	sqsFIFOQueueNameSuffix              = ".fifo"
	sqsListQueuesDefaultMaxResults      = 1000
	sqsListQueuesHardMaxResults         = 1000
	sqsQueueScanPageLimit               = 1024
)

// AWS error codes specific to the queue catalog.
const (
	sqsErrValidation            = "InvalidParameterValue"
	sqsErrMissingParameter      = "MissingParameter"
	sqsErrQueueNameExists       = "QueueNameExists"
	sqsErrQueueDoesNotExist     = "AWS.SimpleQueueService.NonExistentQueue"
	sqsErrInvalidAttributeName  = "InvalidAttributeName"
	sqsErrInvalidAttributeValue = "InvalidAttributeValue"
)

var sqsQueueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}(\.fifo)?$`)

// sqsQueueMeta is the Go mirror of the queue metadata record persisted at
// !sqs|queue|meta|<queue>. Serialized as JSON with a short magic prefix so
// future schema migrations can switch encoding without reading back garbage.
type sqsQueueMeta struct {
	Name                      string            `json:"name"`
	Generation                uint64            `json:"generation"`
	CreatedAtHLC              uint64            `json:"created_at_hlc,omitempty"`
	IsFIFO                    bool              `json:"is_fifo,omitempty"`
	ContentBasedDedup         bool              `json:"content_based_dedup,omitempty"`
	VisibilityTimeoutSeconds  int64             `json:"visibility_timeout_seconds"`
	MessageRetentionSeconds   int64             `json:"message_retention_seconds"`
	DelaySeconds              int64             `json:"delay_seconds"`
	ReceiveMessageWaitSeconds int64             `json:"receive_message_wait_seconds"`
	MaximumMessageSize        int64             `json:"maximum_message_size"`
	RedrivePolicy             string            `json:"redrive_policy,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
}

var storedSQSMetaPrefix = []byte{0x00, 'S', 'Q', 0x01}

func encodeSQSQueueMeta(m *sqsQueueMeta) ([]byte, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	out := make([]byte, 0, len(storedSQSMetaPrefix)+len(body))
	out = append(out, storedSQSMetaPrefix...)
	out = append(out, body...)
	return out, nil
}

func decodeSQSQueueMeta(b []byte) (*sqsQueueMeta, error) {
	if !bytes.HasPrefix(b, storedSQSMetaPrefix) {
		return nil, errors.New("unrecognized sqs meta format")
	}
	var m sqsQueueMeta
	if err := json.Unmarshal(b[len(storedSQSMetaPrefix):], &m); err != nil {
		return nil, errors.WithStack(err)
	}
	return &m, nil
}

// sqsAPIError is a typed error that captures the HTTP status and AWS error
// code so handler helpers can fail deep in the call chain and let the top
// level render a consistent envelope via writeSQSErrorFromErr.
type sqsAPIError struct {
	status    int
	errorType string
	message   string
}

func (e *sqsAPIError) Error() string {
	if e == nil {
		return ""
	}
	if e.message != "" {
		return e.message
	}
	return http.StatusText(e.status)
}

func newSQSAPIError(status int, errorType string, message string) error {
	return &sqsAPIError{status: status, errorType: errorType, message: message}
}

func writeSQSErrorFromErr(w http.ResponseWriter, err error) {
	var apiErr *sqsAPIError
	if errors.As(err, &apiErr) {
		writeSQSError(w, apiErr.status, apiErr.errorType, apiErr.message)
		return
	}
	// Internal errors can wrap Pebble file names, Raft peer ids, stack
	// frames from errors.WithStack, etc. Never echo the raw error text
	// to the client — an authenticated-but-untrusted caller could use
	// it to fingerprint the deployment. Log the detail server-side
	// (the request metrics / log pipeline already captures the full
	// chain) and return a generic 500 body.
	slog.Error("sqs adapter internal error", "err", err)
	writeSQSError(w, http.StatusInternalServerError, sqsErrInternalFailure, "internal error")
}

func writeSQSJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", sqsContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}

// ------------------------ input decoding ------------------------

type sqsCreateQueueInput struct {
	QueueName  string            `json:"QueueName"`
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"tags"`
}

type sqsDeleteQueueInput struct {
	QueueUrl string `json:"QueueUrl"`
}

type sqsListQueuesInput struct {
	QueueNamePrefix string `json:"QueueNamePrefix"`
	MaxResults      int    `json:"MaxResults"`
	NextToken       string `json:"NextToken"`
}

type sqsGetQueueUrlInput struct {
	QueueName string `json:"QueueName"`
}

type sqsGetQueueAttributesInput struct {
	QueueUrl       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

type sqsSetQueueAttributesInput struct {
	QueueUrl   string            `json:"QueueUrl"`
	Attributes map[string]string `json:"Attributes"`
}

func decodeSQSJSONInput(r *http.Request, v any) error {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, sqsMaxRequestBodyBytes))
	if err != nil {
		return newSQSAPIError(http.StatusBadRequest, sqsErrMalformedRequest, err.Error())
	}
	if len(bytes.TrimSpace(body)) == 0 {
		// Empty body is legal for some actions; leave v at its zero value.
		return nil
	}
	if err := json.Unmarshal(body, v); err != nil {
		return newSQSAPIError(http.StatusBadRequest, sqsErrMalformedRequest, err.Error())
	}
	return nil
}

// ------------------------ URL helpers ------------------------

// queueURL builds the AWS-compatible URL echoed back to clients. We follow
// the endpoint the client addressed so SDKs that re-sign subsequent requests
// against the same URL keep working behind reverse proxies.
func (s *SQSServer) queueURL(r *http.Request, queueName string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if host == "" && s.listen != nil {
		host = s.listen.Addr().String()
	}
	return scheme + "://" + host + "/" + queueName
}

// scanContinuationSentinel is appended to a key to produce the exclusive
// upper bound for the next scan page — i.e. the smallest key strictly
// greater than k.
const scanContinuationSentinel = 0x00

func nextScanCursorAfter(key []byte) []byte {
	return append(bytes.Clone(key), scanContinuationSentinel)
}

func queueNameFromURL(queueUrl string) (string, error) {
	if strings.TrimSpace(queueUrl) == "" {
		return "", newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter, "missing QueueUrl")
	}
	parsed, err := url.Parse(queueUrl)
	if err != nil {
		return "", newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "invalid QueueUrl: "+err.Error())
	}
	name := strings.TrimPrefix(parsed.Path, "/")
	if name == "" {
		return "", newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "QueueUrl path is empty")
	}
	// Strip an AWS-style account-id prefix so http://host/12345/MyQueue works.
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	return name, nil
}

func validateQueueName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter, "missing QueueName")
	}
	if len(name) > sqsMaxQueueNameLength {
		return newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "QueueName too long")
	}
	if !sqsQueueNamePattern.MatchString(name) {
		return newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "QueueName contains invalid characters")
	}
	return nil
}

// ------------------------ attribute parsing ------------------------

func parseAttributesIntoMeta(name string, attrs map[string]string) (*sqsQueueMeta, error) {
	// AWS SQS requires FifoQueue=true to be explicitly set to create a
	// FIFO queue; the .fifo suffix alone is not enough. IsFIFO therefore
	// defaults to false and is only flipped on when the attribute is
	// present and true. A .fifo-suffixed name without the attribute is
	// an error (InvalidAttributeValue), because silently creating a
	// Standard queue with a .fifo suffix would later break real FIFO
	// clients that address the queue.
	meta := &sqsQueueMeta{
		Name:                      name,
		IsFIFO:                    false,
		VisibilityTimeoutSeconds:  sqsDefaultVisibilityTimeoutSeconds,
		MessageRetentionSeconds:   sqsDefaultRetentionSeconds,
		DelaySeconds:              sqsDefaultDelaySeconds,
		ReceiveMessageWaitSeconds: sqsDefaultReceiveMessageWaitSeconds,
		MaximumMessageSize:        sqsDefaultMaximumMessageSize,
	}
	if err := applyAttributes(meta, attrs); err != nil {
		return nil, err
	}
	if err := resolveFifoQueueFlag(meta, name, attrs); err != nil {
		return nil, err
	}
	if meta.ContentBasedDedup && !meta.IsFIFO {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "ContentBasedDeduplication is only valid on FIFO queues")
	}
	return meta, nil
}

// resolveFifoQueueFlag reconciles the FifoQueue attribute with the
// queue name's .fifo suffix. AWS requires both to match, and the
// attribute must be explicitly true for a FIFO queue — the name suffix
// alone is not enough.
func resolveFifoQueueFlag(meta *sqsQueueMeta, name string, attrs map[string]string) error {
	nameHasFIFOSuffix := strings.HasSuffix(name, sqsFIFOQueueNameSuffix)
	v, ok := attrs["FifoQueue"]
	if !ok {
		if nameHasFIFOSuffix {
			return newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "FIFO queue name requires FifoQueue=true attribute")
		}
		return nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "FifoQueue must be a boolean")
	}
	if b && !nameHasFIFOSuffix {
		return newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "FIFO queue name must end in .fifo")
	}
	if !b && nameHasFIFOSuffix {
		return newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "Queue name ends in .fifo but FifoQueue=false")
	}
	meta.IsFIFO = b
	return nil
}

// attributeApplier writes one attribute into meta. Keeping one applier per
// attribute keeps applyAttributes trivial to dispatch through.
type attributeApplier func(meta *sqsQueueMeta, value string) error

var sqsAttributeAppliers = map[string]attributeApplier{
	// FifoQueue is applied after applyAttributes returns (see
	// parseAttributesIntoMeta) because it needs cross-field validation
	// against the queue name.
	"FifoQueue": func(_ *sqsQueueMeta, _ string) error { return nil },
	"VisibilityTimeout": func(m *sqsQueueMeta, v string) error {
		n, err := parseIntAttr("VisibilityTimeout", v, 0, sqsMaxVisibilityTimeoutSeconds)
		if err != nil {
			return err
		}
		m.VisibilityTimeoutSeconds = n
		return nil
	},
	"MessageRetentionPeriod": func(m *sqsQueueMeta, v string) error {
		n, err := parseIntAttr("MessageRetentionPeriod", v, sqsMinRetentionSeconds, sqsMaxRetentionSeconds)
		if err != nil {
			return err
		}
		m.MessageRetentionSeconds = n
		return nil
	},
	"DelaySeconds": func(m *sqsQueueMeta, v string) error {
		n, err := parseIntAttr("DelaySeconds", v, 0, sqsMaxDelaySeconds)
		if err != nil {
			return err
		}
		m.DelaySeconds = n
		return nil
	},
	"ReceiveMessageWaitTimeSeconds": func(m *sqsQueueMeta, v string) error {
		n, err := parseIntAttr("ReceiveMessageWaitTimeSeconds", v, 0, sqsMaxReceiveMessageWaitSeconds)
		if err != nil {
			return err
		}
		m.ReceiveMessageWaitSeconds = n
		return nil
	},
	"MaximumMessageSize": func(m *sqsQueueMeta, v string) error {
		n, err := parseIntAttr("MaximumMessageSize", v, sqsMinMaximumMessageSize, sqsMaximumAllowedMaximumMessageSize)
		if err != nil {
			return err
		}
		m.MaximumMessageSize = n
		return nil
	},
	"ContentBasedDeduplication": func(m *sqsQueueMeta, v string) error {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "ContentBasedDeduplication must be a boolean")
		}
		m.ContentBasedDedup = b
		return nil
	},
	"RedrivePolicy": func(m *sqsQueueMeta, v string) error {
		m.RedrivePolicy = v
		return nil
	},
}

// applyAttributes writes every entry in attrs into meta, returning a typed
// SQS error on the first unknown key or out-of-range value.
func applyAttributes(meta *sqsQueueMeta, attrs map[string]string) error {
	for k, v := range attrs {
		apply, ok := sqsAttributeAppliers[k]
		if !ok {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeName, "unsupported attribute: "+k)
		}
		if err := apply(meta, v); err != nil {
			return err
		}
	}
	return nil
}

func parseIntAttr(name, value string, minVal, maxVal int64) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, name+" must be an integer")
	}
	if n < minVal || n > maxVal {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, name+" is out of range")
	}
	return n, nil
}

// attributesEqual is used by CreateQueue for idempotency: calling the same
// CreateQueue twice with identical attributes is a no-op; differing values
// must fail with QueueNameExists.
func attributesEqual(a, b *sqsQueueMeta) bool {
	if a == nil || b == nil {
		return false
	}
	return a.IsFIFO == b.IsFIFO &&
		a.ContentBasedDedup == b.ContentBasedDedup &&
		a.VisibilityTimeoutSeconds == b.VisibilityTimeoutSeconds &&
		a.MessageRetentionSeconds == b.MessageRetentionSeconds &&
		a.DelaySeconds == b.DelaySeconds &&
		a.ReceiveMessageWaitSeconds == b.ReceiveMessageWaitSeconds &&
		a.MaximumMessageSize == b.MaximumMessageSize &&
		a.RedrivePolicy == b.RedrivePolicy
}

// ------------------------ storage primitives ------------------------

func (s *SQSServer) nextTxnReadTS(ctx context.Context) uint64 {
	maxTS := uint64(0)
	if p, ok := s.store.(globalLastCommitTSProvider); ok {
		maxTS = p.GlobalLastCommitTS(ctx)
	} else if s.store != nil {
		maxTS = s.store.LastCommitTS()
	}
	if s.coordinator != nil {
		if clock := s.coordinator.Clock(); clock != nil && maxTS > 0 {
			clock.Observe(maxTS)
		}
	}
	if maxTS == 0 {
		return 1
	}
	return maxTS
}

func (s *SQSServer) loadQueueMetaAt(ctx context.Context, queueName string, ts uint64) (*sqsQueueMeta, bool, error) {
	b, err := s.store.GetAt(ctx, sqsQueueMetaKey(queueName), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	meta, err := decodeSQSQueueMeta(b)
	if err != nil {
		return nil, false, err
	}
	return meta, true, nil
}

func (s *SQSServer) loadQueueGenerationAt(ctx context.Context, queueName string, ts uint64) (uint64, error) {
	b, err := s.store.GetAt(ctx, sqsQueueGenKey(queueName), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return n, nil
}

// ------------------------ handlers ------------------------

func (s *SQSServer) createQueue(w http.ResponseWriter, r *http.Request) {
	var in sqsCreateQueueInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := validateQueueName(in.QueueName); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	requested, err := parseAttributesIntoMeta(in.QueueName, in.Attributes)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	requested.Tags = in.Tags

	if err := s.createQueueWithRetry(r.Context(), requested); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]string{"QueueUrl": s.queueURL(r, in.QueueName)})
}

func (s *SQSServer) createQueueWithRetry(ctx context.Context, requested *sqsQueueMeta) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		done, err := s.tryCreateQueueOnce(ctx, requested)
		if err == nil && done {
			return nil
		}
		if err != nil && !isRetryableTransactWriteError(err) {
			return err
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "create queue retry attempts exhausted")
}

// tryCreateQueueOnce runs one read/check/dispatch pass. The first bool reports
// whether the caller should stop retrying: true means the queue now exists
// with the requested attributes, false means the dispatch hit a retryable
// conflict and should be retried after backoff.
func (s *SQSServer) tryCreateQueueOnce(ctx context.Context, requested *sqsQueueMeta) (bool, error) {
	readTS := s.nextTxnReadTS(ctx)
	existing, exists, err := s.loadQueueMetaAt(ctx, requested.Name, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if exists {
		if attributesEqual(existing, requested) {
			return true, nil
		}
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrQueueNameExists, "queue already exists with different attributes")
	}
	lastGen, err := s.loadQueueGenerationAt(ctx, requested.Name, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	requested.Generation = lastGen + 1
	if clock := s.coordinator.Clock(); clock != nil {
		requested.CreatedAtHLC = clock.Current()
	}
	metaBytes, err := encodeSQSQueueMeta(requested)
	if err != nil {
		return false, errors.WithStack(err)
	}
	metaKey := sqsQueueMetaKey(requested.Name)
	genKey := sqsQueueGenKey(requested.Name)
	// StartTS pins OCC to the snapshot we took the existence + generation
	// read at, and ReadKeys cover both the meta and generation records so
	// a concurrent CreateQueue that committed between our read and our
	// dispatch causes ErrWriteConflict and the retry loop re-reads.
	// Without this, two races could both decide "queue missing" and both
	// write their own generation, leaving the later write on top of a
	// record the loser never observed.
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{metaKey, genKey},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: metaKey, Value: metaBytes},
			{Op: kv.Put, Key: genKey, Value: []byte(strconv.FormatUint(requested.Generation, 10))},
		},
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

func (s *SQSServer) deleteQueue(w http.ResponseWriter, r *http.Request) {
	var in sqsDeleteQueueInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	name, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := s.deleteQueueWithRetry(r.Context(), name); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	// SQS DeleteQueue returns 200 with an empty body.
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) deleteQueueWithRetry(ctx context.Context, queueName string) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := s.nextTxnReadTS(ctx)
		_, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if !exists {
			return newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
		}

		// Bump the generation counter so any stragglers under the old
		// generation are unreachable by routing. Actual message cleanup
		// lands in a follow-up PR along with the message keyspace.
		lastGen, err := s.loadQueueGenerationAt(ctx, queueName, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		metaKey := sqsQueueMetaKey(queueName)
		genKey := sqsQueueGenKey(queueName)
		// StartTS + ReadKeys fence against a concurrent CreateQueue /
		// SetQueueAttributes landing between our load and dispatch.
		req := &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  readTS,
			ReadKeys: [][]byte{metaKey, genKey},
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Del, Key: metaKey},
				{Op: kv.Put, Key: genKey, Value: []byte(strconv.FormatUint(lastGen+1, 10))},
			},
		}
		if _, err := s.coordinator.Dispatch(ctx, req); err == nil {
			return nil
		} else if !isRetryableTransactWriteError(err) {
			return errors.WithStack(err)
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "delete queue retry attempts exhausted")
}

func (s *SQSServer) listQueues(w http.ResponseWriter, r *http.Request) {
	var in sqsListQueuesInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	maxResults := clampListQueuesMaxResults(in.MaxResults)

	names, err := s.scanQueueNames(r.Context())
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	sort.Strings(names)
	filtered := filterByPrefix(names, in.QueueNamePrefix)
	start := resolveListQueuesStart(filtered, in.NextToken)

	end := start + maxResults
	truncated := end < len(filtered)
	if !truncated {
		end = len(filtered)
	}
	page := filtered[start:end]

	urls := make([]string, 0, len(page))
	for _, n := range page {
		urls = append(urls, s.queueURL(r, n))
	}
	resp := map[string]any{"QueueUrls": urls}
	if truncated && len(page) > 0 {
		resp["NextToken"] = encodeSQSSegment(page[len(page)-1])
	}
	writeSQSJSON(w, resp)
}

func clampListQueuesMaxResults(requested int) int {
	if requested <= 0 {
		return sqsListQueuesDefaultMaxResults
	}
	if requested > sqsListQueuesHardMaxResults {
		return sqsListQueuesHardMaxResults
	}
	return requested
}

func filterByPrefix(names []string, prefix string) []string {
	if prefix == "" {
		return names
	}
	out := names[:0]
	for _, n := range names {
		if strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}
	return out
}

// resolveListQueuesStart decodes the NextToken boundary and returns the index
// of the first name strictly greater than it. A malformed token behaves as
// "start from the beginning" to match AWS's lenient behavior.
func resolveListQueuesStart(names []string, token string) int {
	if token == "" {
		return 0
	}
	boundary, err := decodeSQSSegment(token)
	if err != nil {
		return 0
	}
	for i, n := range names {
		if n > boundary {
			return i
		}
	}
	return len(names)
}

func (s *SQSServer) scanQueueNames(ctx context.Context) ([]string, error) {
	prefix := []byte(SqsQueueMetaPrefix)
	end := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	readTS := s.nextTxnReadTS(ctx)
	var names []string
	for {
		kvs, err := s.store.ScanAt(ctx, start, end, sqsQueueScanPageLimit, readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(kvs) == 0 {
			break
		}
		for _, kvp := range kvs {
			if !bytes.HasPrefix(kvp.Key, prefix) {
				return names, nil
			}
			name, ok := queueNameFromMetaKey(kvp.Key)
			if !ok {
				continue
			}
			names = append(names, name)
		}
		if len(kvs) < sqsQueueScanPageLimit {
			break
		}
		start = nextScanCursorAfter(kvs[len(kvs)-1].Key)
		if end != nil && bytes.Compare(start, end) > 0 {
			break
		}
	}
	return names, nil
}

func (s *SQSServer) getQueueUrl(w http.ResponseWriter, r *http.Request) {
	var in sqsGetQueueUrlInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := validateQueueName(in.QueueName); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	_, exists, err := s.loadQueueMetaAt(r.Context(), in.QueueName, s.nextTxnReadTS(r.Context()))
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if !exists {
		writeSQSError(w, http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
		return
	}
	writeSQSJSON(w, map[string]string{"QueueUrl": s.queueURL(r, in.QueueName)})
}

func (s *SQSServer) getQueueAttributes(w http.ResponseWriter, r *http.Request) {
	var in sqsGetQueueAttributesInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	name, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	meta, exists, err := s.loadQueueMetaAt(r.Context(), name, s.nextTxnReadTS(r.Context()))
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if !exists {
		writeSQSError(w, http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
		return
	}
	selection := selectedAttributeNames(in.AttributeNames)
	attrs := queueMetaToAttributes(meta, selection)
	writeSQSJSON(w, map[string]any{"Attributes": attrs})
}

// selectedAttributeNames returns a set of attribute names to include in the
// response. An empty selection, or any entry equal to "All", expands to
// every supported attribute.
func selectedAttributeNames(req []string) map[string]bool {
	selection := map[string]bool{}
	if len(req) == 0 {
		return nil
	}
	for _, n := range req {
		if n == "All" {
			return nil
		}
		selection[n] = true
	}
	return selection
}

func queueMetaToAttributes(meta *sqsQueueMeta, selection map[string]bool) map[string]string {
	all := map[string]string{
		"VisibilityTimeout":             strconv.FormatInt(meta.VisibilityTimeoutSeconds, 10),
		"MessageRetentionPeriod":        strconv.FormatInt(meta.MessageRetentionSeconds, 10),
		"DelaySeconds":                  strconv.FormatInt(meta.DelaySeconds, 10),
		"ReceiveMessageWaitTimeSeconds": strconv.FormatInt(meta.ReceiveMessageWaitSeconds, 10),
		"MaximumMessageSize":            strconv.FormatInt(meta.MaximumMessageSize, 10),
		"FifoQueue":                     strconv.FormatBool(meta.IsFIFO),
		"ContentBasedDeduplication":     strconv.FormatBool(meta.ContentBasedDedup),
	}
	if meta.RedrivePolicy != "" {
		all["RedrivePolicy"] = meta.RedrivePolicy
	}
	if selection == nil {
		return all
	}
	out := make(map[string]string, len(selection))
	for k := range selection {
		if v, ok := all[k]; ok {
			out[k] = v
		}
	}
	return out
}

func (s *SQSServer) setQueueAttributes(w http.ResponseWriter, r *http.Request) {
	var in sqsSetQueueAttributesInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	name, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := s.setQueueAttributesWithRetry(r.Context(), name, in.Attributes); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) setQueueAttributesWithRetry(ctx context.Context, queueName string, attrs map[string]string) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		done, err := s.trySetQueueAttributesOnce(ctx, queueName, attrs)
		if err == nil && done {
			return nil
		}
		if err != nil && !isRetryableTransactWriteError(err) {
			return err
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "set queue attributes retry attempts exhausted")
}

// trySetQueueAttributesOnce is one read-validate-commit pass. The first
// return reports whether the caller should stop retrying (the attrs
// are now committed); an error means either a non-retryable failure
// (propagate) or a retryable write conflict (retry after backoff).
func (s *SQSServer) trySetQueueAttributesOnce(ctx context.Context, queueName string, attrs map[string]string) (bool, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !exists {
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	if err := applyAttributes(meta, attrs); err != nil {
		return false, err
	}
	// ContentBasedDeduplication is FIFO-only; a Standard queue
	// silently accepting it would advertise unsupported behavior to
	// clients. Same rule enforced on CreateQueue.
	if meta.ContentBasedDedup && !meta.IsFIFO {
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "ContentBasedDeduplication is only valid on FIFO queues")
	}
	metaBytes, err := encodeSQSQueueMeta(meta)
	if err != nil {
		return false, errors.WithStack(err)
	}
	metaKey := sqsQueueMetaKey(queueName)
	// StartTS + ReadKeys prevent two concurrent SetQueueAttributes from
	// both reading the same old meta and the later dispatch clobbering
	// the earlier commit's changes.
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{metaKey},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: metaKey, Value: metaBytes},
		},
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

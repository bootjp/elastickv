package adapter

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math"
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
	// sqsPurgeRateLimitMillis is AWS's "one PurgeQueue per 60 seconds per
	// queue" limit. PurgeInProgress is returned to callers that try
	// again before the cooldown ends.
	sqsPurgeRateLimitMillis = 60_000
)

// AWS error codes specific to PurgeQueue.
const (
	sqsErrPurgeInProgress = "AWS.SimpleQueueService.PurgeQueueInProgress"
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
	// LastPurgedAtMillis is the wall-clock time of the last successful
	// PurgeQueue. AWS rate-limits PurgeQueue to once per 60 seconds per
	// queue; tracking it on the meta record means the limit survives
	// leader failover (in-memory cooldowns would let the new leader
	// accept a second purge a few seconds later).
	LastPurgedAtMillis int64 `json:"last_purged_at_millis,omitempty"`
	// CreatedAtMillis / LastModifiedAtMillis are wall-clock timestamps
	// surfaced by GetQueueAttributes (AWS reports them in second
	// granularity). HLC is unsuitable for this — it is a logical
	// counter, not a wall clock — so we record the local Now() at
	// commit time and trust HLC monotonicity to keep ordering sane.
	CreatedAtMillis      int64 `json:"created_at_millis,omitempty"`
	LastModifiedAtMillis int64 `json:"last_modified_at_millis,omitempty"`
	// Throttle is the per-queue rate-limit configuration. nil disables
	// throttling (default). Set via SetQueueAttributes with the AWS-style
	// names ThrottleSendCapacity / ThrottleSendRefillPerSecond / etc.
	// Persisted on the meta so a leader failover loads the configuration
	// along with the rest of the queue.
	Throttle *sqsQueueThrottle `json:"throttle,omitempty"`
	// PartitionCount is the number of FIFO partitions for this queue
	// (Phase 3.D HT-FIFO, see docs/design/2026_04_26_proposed_sqs_split_queue_fifo.md).
	// Zero or 1 means the legacy single-partition layout — no schema
	// change. Greater than 1 enables HT-FIFO. Set at CreateQueue time
	// and immutable thereafter (SetQueueAttributes rejects any change).
	// Power-of-two values only (validator rejects others). PR 2 of the
	// rollout introduces this field but a temporary CreateQueue gate
	// rejects PartitionCount > 1 until PR 5 lifts the gate atomically
	// with the data-plane fanout — so the schema exists but no
	// partitioned data can land before the data plane is wired.
	PartitionCount uint32 `json:"partition_count,omitempty"`
	// FifoThroughputLimit mirrors the AWS attribute. "perMessageGroupId"
	// (default for HT-FIFO) keeps the §3.3 hash-by-MessageGroupId
	// routing; "perQueue" activates the partition-0 short-circuit so
	// every group ID routes to one partition (effectively N=1).
	// Set at CreateQueue time and immutable thereafter — flipping it
	// live would re-route in-flight messages and silently violate
	// within-group FIFO ordering (see §3.2 of the design).
	FifoThroughputLimit string `json:"fifo_throughput_limit,omitempty"`
	// DeduplicationScope mirrors the AWS attribute. "messageGroup"
	// (default for HT-FIFO) means the dedup window is per
	// (queue, partition, MessageGroupId, dedupId); "queue" is the
	// legacy single-window behaviour. Set at CreateQueue time and
	// immutable thereafter — changing live can resurrect or suppress
	// messages depending on the direction of the change. The
	// validator additionally rejects {PartitionCount > 1,
	// DeduplicationScope = "queue"} at CreateQueue time because the
	// dedup key cannot be globally unique across partitions without
	// a cross-partition OCC transaction.
	DeduplicationScope string `json:"deduplication_scope,omitempty"`
}

// sqsQueueThrottle is the per-queue token-bucket configuration. Three
// independent buckets per queue: Send (SendMessage[Batch]), Recv
// (ReceiveMessage / DeleteMessage[Batch] / ChangeMessageVisibility[Batch],
// charged on the consumer side), Default (catch-all for any future
// non-Send/Recv verb that gets wired into the throttle path).
//
// Field-name vocabulary uses short forms (Send*, Recv*, Default*) for the
// JSON contract and AWS-style attribute names; the in-memory bucketKey
// uses the canonical action vocabulary ("Send" | "Receive" | "*").
// throttleConfigToBucketAction and bucketActionForCharge bridge the two.
type sqsQueueThrottle struct {
	SendCapacity           float64 `json:"send_capacity,omitempty"`
	SendRefillPerSecond    float64 `json:"send_refill_per_second,omitempty"`
	RecvCapacity           float64 `json:"recv_capacity,omitempty"`
	RecvRefillPerSecond    float64 `json:"recv_refill_per_second,omitempty"`
	DefaultCapacity        float64 `json:"default_capacity,omitempty"`
	DefaultRefillPerSecond float64 `json:"default_refill_per_second,omitempty"`
}

// IsEmpty reports whether the configuration is the no-op (all six
// fields zero), in which case throttling is disabled for the queue.
func (t *sqsQueueThrottle) IsEmpty() bool {
	if t == nil {
		return true
	}
	return t.SendCapacity == 0 && t.SendRefillPerSecond == 0 &&
		t.RecvCapacity == 0 && t.RecvRefillPerSecond == 0 &&
		t.DefaultCapacity == 0 && t.DefaultRefillPerSecond == 0
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
	// HT-FIFO validation runs after resolveFifoQueueFlag so the
	// IsFIFO-only checks see the post-resolution flag. The temporary
	// dormancy gate (§11 PR 2) runs separately in createQueue so
	// SetQueueAttributes paths share the schema validator without
	// re-rejecting on the gate.
	if err := validatePartitionConfig(meta); err != nil {
		return nil, err
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
	// PartitionCount enables HT-FIFO when > 1 (Phase 3.D, see
	// docs/design/2026_04_26_proposed_sqs_split_queue_fifo.md). Set
	// at CreateQueue time; SetQueueAttributes attempts to change it
	// reject via the immutability check in trySetQueueAttributesOnce.
	// PR 2 of the rollout introduces the field but the temporary
	// dormancy gate in tryCreateQueueOnce rejects PartitionCount > 1
	// until PR 5 lifts the gate atomically with the data plane.
	"PartitionCount": func(m *sqsQueueMeta, v string) error {
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 32)
		if err != nil {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"PartitionCount must be a non-negative integer")
		}
		m.PartitionCount = uint32(n) //nolint:gosec // bounded by ParseUint(_, _, 32) above.
		return nil
	},
	"FifoThroughputLimit": func(m *sqsQueueMeta, v string) error {
		v = strings.TrimSpace(v)
		switch v {
		case "", htfifoThroughputPerMessageGroupID, htfifoThroughputPerQueue:
			m.FifoThroughputLimit = v
			return nil
		}
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"FifoThroughputLimit must be 'perMessageGroupId' or 'perQueue'")
	},
	"DeduplicationScope": func(m *sqsQueueMeta, v string) error {
		v = strings.TrimSpace(v)
		switch v {
		case "", htfifoDedupeScopeMessageGroup, htfifoDedupeScopeQueue:
			m.DeduplicationScope = v
			return nil
		}
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"DeduplicationScope must be 'messageGroup' or 'queue'")
	},
	// Throttle* are non-AWS extensions for per-queue rate limiting,
	// see docs/design/2026_04_26_proposed_sqs_per_queue_throttling.md.
	// Each accepts a non-negative float64; the cross-attribute
	// validation that enforces both-zero-or-both-positive on each
	// (capacity, refill) pair, capacity ≥ refill, hard ceiling, and
	// the capacity ≥ 10 floor for batch-charging buckets runs in
	// validateThrottleConfig after every Throttle* applier has fired.
	"ThrottleSendCapacity":           applyThrottleField(throttleSetSendCapacity),
	"ThrottleSendRefillPerSecond":    applyThrottleField(throttleSetSendRefill),
	"ThrottleRecvCapacity":           applyThrottleField(throttleSetRecvCapacity),
	"ThrottleRecvRefillPerSecond":    applyThrottleField(throttleSetRecvRefill),
	"ThrottleDefaultCapacity":        applyThrottleField(throttleSetDefaultCapacity),
	"ThrottleDefaultRefillPerSecond": applyThrottleField(throttleSetDefaultRefill),
	"RedrivePolicy": func(m *sqsQueueMeta, v string) error {
		// Validate the policy at attribute-apply time so a malformed
		// RedrivePolicy never makes it onto the queue meta record. The
		// receive path re-parses on every check rather than caching
		// the struct on meta, because DLQ existence has to be
		// re-validated at the readTS anyway.
		policy, err := parseRedrivePolicy(v)
		if err != nil {
			return err
		}
		// AWS rejects self-referential DLQ targets. Without this gate
		// a redrive transaction would delete the source record and
		// rewrite it to the same queue with a fresh receipt token,
		// looping the poison message forever.
		if policy.DLQName == m.Name {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"RedrivePolicy.deadLetterTargetArn must not point at the source queue")
		}
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
	// Throttle* validation has to run after every applier so the
	// pair-wise rules (both-zero-or-both-positive, capacity ≥ refill,
	// capacity ≥ 10 for batch buckets) see the post-update meta as a
	// whole. Running per-applier would reject a valid two-attribute
	// update (e.g. SendCapacity + SendRefillPerSecond) on the first
	// applier because the second value is not yet present.
	if err := validateThrottleConfig(meta); err != nil {
		return err
	}
	// HT-FIFO partition validation runs in parseAttributesIntoMeta /
	// trySetQueueAttributesOnce, AFTER resolveFifoQueueFlag, so the
	// IsFIFO-only checks see the post-resolution flag. Running here
	// would reject a valid CreateQueue with FifoQueue=true +
	// FifoThroughputLimit=perMessageGroupId because IsFIFO is still
	// false at this point in the flow.
	return nil
}

// applyThrottleField wraps a setter that writes one Throttle* field
// into meta.Throttle, allocating the struct lazily on first use. The
// per-field setter does the float parse + non-negative + hard-ceiling
// check; cross-field rules run later in validateThrottleConfig.
func applyThrottleField(set func(*sqsQueueThrottle, float64)) attributeApplier {
	return func(m *sqsQueueMeta, v string) error {
		f, err := parseThrottleFloat(v)
		if err != nil {
			return err
		}
		if m.Throttle == nil {
			m.Throttle = &sqsQueueThrottle{}
		}
		set(m.Throttle, f)
		return nil
	}
}

// parseThrottleFloat parses the wire string into a non-negative float
// bounded by the hard ceiling. Any malformed or out-of-range value
// turns into InvalidAttributeValue with a self-describing message so
// the operator sees the cause without grepping the server log.
func parseThrottleFloat(value string) (float64, error) {
	v := strings.TrimSpace(value)
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"throttle attribute must be a non-negative number")
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"throttle attribute must be finite and non-negative")
	}
	if f > throttleHardCeilingPerSecond {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"throttle attribute exceeds hard ceiling 100000")
	}
	return f, nil
}

// Per-field setters keep applyThrottleField a one-liner per attribute
// and let validateThrottleConfig stay outside the applier dispatch
// table. Defined as functions (not closures) so a future caller from
// outside applyAttributes — e.g. a programmatic admin surface — can
// reuse them without recreating the closure boilerplate.
func throttleSetSendCapacity(t *sqsQueueThrottle, f float64)    { t.SendCapacity = f }
func throttleSetSendRefill(t *sqsQueueThrottle, f float64)      { t.SendRefillPerSecond = f }
func throttleSetRecvCapacity(t *sqsQueueThrottle, f float64)    { t.RecvCapacity = f }
func throttleSetRecvRefill(t *sqsQueueThrottle, f float64)      { t.RecvRefillPerSecond = f }
func throttleSetDefaultCapacity(t *sqsQueueThrottle, f float64) { t.DefaultCapacity = f }
func throttleSetDefaultRefill(t *sqsQueueThrottle, f float64)   { t.DefaultRefillPerSecond = f }

// validateThrottleConfig enforces the §3.2 cross-attribute rules on
// the post-applier meta. The single-field constraints (non-negative,
// hard ceiling) are already enforced inside parseThrottleFloat;
// what's left is pair-wise:
//
//   - Each (capacity, refill) pair must be both zero (action disabled)
//     or both positive. A capacity-without-refill bucket would never
//     refill; a refill-without-capacity bucket has no burst headroom.
//   - capacity ≥ refill, otherwise the bucket can never burst above
//     steady state (the bucket can only ever hold one second's worth).
//   - For action buckets that cover a batch verb (Send, Recv) the
//     capacity must be ≥ throttleMinBatchCapacity (== 10). A capacity
//     below the largest single charge is permanently unserviceable
//     for full batches.
//
// If meta.Throttle is empty (the IsEmpty short-circuit) the function
// also drops the empty struct so a round-trip GetQueueAttributes
// reports the queue as untrothttled rather than zero-valued. Mirrors
// how nil throttle on the meta means "not configured".
func validateThrottleConfig(meta *sqsQueueMeta) error {
	if meta.Throttle == nil {
		return nil
	}
	t := meta.Throttle
	if err := validateThrottlePair("ThrottleSend", t.SendCapacity, t.SendRefillPerSecond, true); err != nil {
		return err
	}
	if err := validateThrottlePair("ThrottleRecv", t.RecvCapacity, t.RecvRefillPerSecond, true); err != nil {
		return err
	}
	// Default* gets the same batch-capacity floor as Send*/Recv*
	// because resolveActionConfig in sqs_throttle.go falls Send and
	// Receive traffic through to Default whenever the corresponding
	// Send*/Recv* pair is unset. Without the floor, a config like
	// `ThrottleDefaultCapacity=5, ThrottleDefaultRefillPerSecond=1`
	// would be accepted but make every full SendMessageBatch /
	// DeleteMessageBatch (charge=10) permanently unserviceable —
	// the bucket can never accumulate the 10 tokens. Codex P1 on
	// PR #679 round 5 caught the gap; the design doc note in §3.2
	// claiming Default* is exempt was wrong about the fall-through.
	if err := validateThrottlePair("ThrottleDefault", t.DefaultCapacity, t.DefaultRefillPerSecond, true); err != nil {
		return err
	}
	if t.IsEmpty() {
		// All-zero post-apply means the operator wrote a "disable"
		// command; canonicalise to nil so downstream code hits the
		// nil-throttle short-circuit rather than the IsEmpty branch.
		meta.Throttle = nil
	}
	return nil
}

// addThrottleAttributes renders the non-zero Throttle* pairs into out.
// Per §3.2 the wire-side vocabulary stays Send*/Recv*/Default*; the
// canonical bucket-action vocabulary is internal to the bucket store.
func addThrottleAttributes(out map[string]string, t *sqsQueueThrottle) {
	if t.IsEmpty() {
		return
	}
	if t.SendCapacity > 0 {
		out["ThrottleSendCapacity"] = strconv.FormatFloat(t.SendCapacity, 'g', -1, 64)
		out["ThrottleSendRefillPerSecond"] = strconv.FormatFloat(t.SendRefillPerSecond, 'g', -1, 64)
	}
	if t.RecvCapacity > 0 {
		out["ThrottleRecvCapacity"] = strconv.FormatFloat(t.RecvCapacity, 'g', -1, 64)
		out["ThrottleRecvRefillPerSecond"] = strconv.FormatFloat(t.RecvRefillPerSecond, 'g', -1, 64)
	}
	if t.DefaultCapacity > 0 {
		out["ThrottleDefaultCapacity"] = strconv.FormatFloat(t.DefaultCapacity, 'g', -1, 64)
		out["ThrottleDefaultRefillPerSecond"] = strconv.FormatFloat(t.DefaultRefillPerSecond, 'g', -1, 64)
	}
}

// validateThrottlePair runs the per-(action, capacity, refill) checks.
// requireBatchCapacity gates the capacity ≥ 10 rule so the catch-all
// Default* bucket (no batch verbs in scope today) does not get the
// extra constraint.
func validateThrottlePair(prefix string, capacity, refill float64, requireBatchCapacity bool) error {
	if capacity == 0 && refill == 0 {
		return nil
	}
	if capacity == 0 || refill == 0 {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			prefix+"Capacity and "+prefix+"RefillPerSecond must both be zero (disabled) or both positive")
	}
	if capacity < refill {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			prefix+"Capacity must be ≥ "+prefix+"RefillPerSecond (capacity is the burst cap; below refill the bucket cannot accumulate)")
	}
	if requireBatchCapacity && capacity < throttleMinBatchCapacity {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			prefix+"Capacity must be ≥ 10 — batch verbs (SendMessageBatch / DeleteMessageBatch) charge up to 10 tokens per call; a smaller capacity makes every full batch permanently unserviceable")
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
	return baseAttributesEqual(a, b) &&
		throttleConfigEqual(a.Throttle, b.Throttle) &&
		htfifoAttributesEqual(a, b)
}

// baseAttributesEqual compares the pre-Phase-3.C/3.D attribute set.
// Split from attributesEqual so adding fields per phase does not
// push the function over the cyclop ceiling.
func baseAttributesEqual(a, b *sqsQueueMeta) bool {
	return a.IsFIFO == b.IsFIFO &&
		a.ContentBasedDedup == b.ContentBasedDedup &&
		a.VisibilityTimeoutSeconds == b.VisibilityTimeoutSeconds &&
		a.MessageRetentionSeconds == b.MessageRetentionSeconds &&
		a.DelaySeconds == b.DelaySeconds &&
		a.ReceiveMessageWaitSeconds == b.ReceiveMessageWaitSeconds &&
		a.MaximumMessageSize == b.MaximumMessageSize &&
		a.RedrivePolicy == b.RedrivePolicy
}

// throttleConfigEqual compares two Throttle configs for the
// CreateQueue idempotency check. Without including the throttle
// fields in attributesEqual, a re-create with different limits would
// be treated as idempotent and silently keep the old limits.
func throttleConfigEqual(a, b *sqsQueueThrottle) bool {
	aEmpty := a.IsEmpty()
	bEmpty := b.IsEmpty()
	if aEmpty && bEmpty {
		return true
	}
	if aEmpty != bEmpty {
		return false
	}
	return a.SendCapacity == b.SendCapacity &&
		a.SendRefillPerSecond == b.SendRefillPerSecond &&
		a.RecvCapacity == b.RecvCapacity &&
		a.RecvRefillPerSecond == b.RecvRefillPerSecond &&
		a.DefaultCapacity == b.DefaultCapacity &&
		a.DefaultRefillPerSecond == b.DefaultRefillPerSecond
}

// htfifoAttributesEqual compares the Phase 3.D HT-FIFO fields.
func htfifoAttributesEqual(a, b *sqsQueueMeta) bool {
	return a.PartitionCount == b.PartitionCount &&
		a.FifoThroughputLimit == b.FifoThroughputLimit &&
		a.DeduplicationScope == b.DeduplicationScope
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
	// Temporary dormancy gate (Phase 3.D §11 PR 2). PartitionCount > 1
	// must reject until PR 5 wires the data plane atomically with the
	// gate-lift. Without this, accepting a partitioned-queue create
	// would let SendMessage write under the legacy single-partition
	// prefix; the PR 5 reader would never find those messages and the
	// reaper would not enumerate them — silent message loss.
	if err := validatePartitionDormancyGate(requested); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if len(in.Tags) > sqsMaxTagsPerQueue {
		// AWS caps tags per queue at 50. CreateQueue must reject
		// over-cap tag bundles up front; a silent slice-and-store
		// would let queues land with more tags than TagQueue would
		// ever accept on the same queue.
		writeSQSError(w, http.StatusBadRequest, sqsErrInvalidAttributeValue, "queue tag count exceeds 50")
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
	now := time.Now().UnixMilli()
	requested.CreatedAtMillis = now
	requested.LastModifiedAtMillis = now
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
	// Drop any throttle bucket that survived a delete-then-create race
	// (Codex P2 on PR #679 round 5). DeleteQueue invalidates after its
	// commit, but a sendMessage holding pre-delete meta can recreate
	// a bucket between that invalidate and this CreateQueue commit;
	// invalidating again here on a genuine create (not the idempotent
	// return path above, which exits before this point) guarantees
	// the new queue starts with a fresh full-capacity bucket
	// regardless of in-flight traffic to the prior incarnation.
	s.throttle.invalidateQueue(requested.Name)
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
	// Drop in-memory throttle buckets belonging to this queue so a
	// same-name CreateQueue immediately after this delete starts with
	// a fresh full-capacity bucket, not the stale balance from the
	// previous incarnation. Without this step the old throttle would
	// keep enforcing for up to the idle-evict window (default 1 h),
	// surprising operators who use DeleteQueue+CreateQueue to reset
	// queue state.
	s.throttle.invalidateQueue(name)
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
		// generation are unreachable by routing. The tombstone gives the
		// reaper a way to find leftover data / vis / byage / dedup /
		// group records once meta is gone — without it, scanQueueNames
		// would never see the deleted queue again and its message
		// keyspace would leak forever.
		lastGen, err := s.loadQueueGenerationAt(ctx, queueName, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		metaKey := sqsQueueMetaKey(queueName)
		genKey := sqsQueueGenKey(queueName)
		tombstoneKey := sqsQueueTombstoneKey(queueName, lastGen)
		// StartTS + ReadKeys fence against a concurrent CreateQueue /
		// SetQueueAttributes landing between our load and dispatch.
		req := &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  readTS,
			ReadKeys: [][]byte{metaKey, genKey},
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Del, Key: metaKey},
				{Op: kv.Put, Key: genKey, Value: []byte(strconv.FormatUint(lastGen+1, 10))},
				{Op: kv.Put, Key: tombstoneKey, Value: []byte{1}},
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
	readTS := s.nextTxnReadTS(r.Context())
	meta, exists, err := s.loadQueueMetaAt(r.Context(), name, readTS)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if !exists {
		writeSQSError(w, http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
		return
	}
	selection := selectedAttributeNames(in.AttributeNames)
	// Counter computation is the only path that touches per-message
	// state, so skip the scan when the caller did not ask for any of
	// the Approximate* attributes. AWS itself documents these as
	// approximate; a snapshot read is correct enough.
	var counters *sqsApproxCounters
	if selectionWantsApproxCounters(selection) {
		c, scanErr := s.scanApproxCounters(r.Context(), name, meta.Generation, readTS)
		if scanErr != nil {
			writeSQSErrorFromErr(w, scanErr)
			return
		}
		counters = &c
	}
	attrs := queueMetaToAttributes(meta, selection, counters, s.queueArn(name))
	writeSQSJSON(w, map[string]any{"Attributes": attrs})
}

// queueArn synthesises the AWS-shaped ARN clients expect to find on
// GetQueueAttributes. Account id is fixed at "000000000000" — IAM is
// out of scope, so emitting a sentinel placeholder gives SDKs a
// well-formed string without inventing identity.
func (s *SQSServer) queueArn(queueName string) string {
	region := s.effectiveRegion()
	return "arn:aws:sqs:" + region + ":000000000000:" + queueName
}

// sqsAttributeSelection is a tri-state result from selectedAttributeNames:
// expandAll = AWS "All" (or any entry equals "All"); a non-nil map lists
// the specific attribute names the caller asked for; and an empty
// selection (no AttributeNames supplied at all) maps to "return no
// attributes" per AWS semantics — which is the opposite of treating
// omission as "All", and matters because real clients that omit
// AttributeNames specifically do NOT want the server to echo every
// piece of queue metadata back.
type sqsAttributeSelection struct {
	expandAll bool
	names     map[string]bool
}

// selectedAttributeNames parses the AttributeNames array according to
// AWS GetQueueAttributes semantics:
//   - missing / empty array: return NO attributes (empty result).
//   - any element equal to "All": return every supported attribute.
//   - otherwise: return only the listed names.
func selectedAttributeNames(req []string) sqsAttributeSelection {
	if len(req) == 0 {
		return sqsAttributeSelection{}
	}
	selection := map[string]bool{}
	for _, n := range req {
		if n == "All" {
			return sqsAttributeSelection{expandAll: true}
		}
		selection[n] = true
	}
	return sqsAttributeSelection{names: selection}
}

// sqsApproxCounters bundles the three AWS-published "Approximate"
// counters; nil means the caller did not request them and so the
// per-message scan was skipped.
type sqsApproxCounters struct {
	Visible    int64
	NotVisible int64
	Delayed    int64
}

// approxCounterAttributeNames is every attribute that requires a
// per-message scan. queueMetaToAttributes only invokes the scan when
// the selection overlaps this set.
var approxCounterAttributeNames = map[string]bool{
	"ApproximateNumberOfMessages":           true,
	"ApproximateNumberOfMessagesNotVisible": true,
	"ApproximateNumberOfMessagesDelayed":    true,
}

func selectionWantsApproxCounters(selection sqsAttributeSelection) bool {
	if selection.expandAll {
		return true
	}
	for k := range selection.names {
		if approxCounterAttributeNames[k] {
			return true
		}
	}
	return false
}

func queueMetaToAttributes(meta *sqsQueueMeta, selection sqsAttributeSelection, counters *sqsApproxCounters, queueArn string) map[string]string {
	// No AttributeNames supplied and no "All" → AWS returns nothing.
	// The handler still emits "Attributes" as an empty map so the
	// response shape is stable.
	if !selection.expandAll && len(selection.names) == 0 {
		return map[string]string{}
	}
	all := map[string]string{
		"VisibilityTimeout":             strconv.FormatInt(meta.VisibilityTimeoutSeconds, 10),
		"MessageRetentionPeriod":        strconv.FormatInt(meta.MessageRetentionSeconds, 10),
		"DelaySeconds":                  strconv.FormatInt(meta.DelaySeconds, 10),
		"ReceiveMessageWaitTimeSeconds": strconv.FormatInt(meta.ReceiveMessageWaitSeconds, 10),
		"MaximumMessageSize":            strconv.FormatInt(meta.MaximumMessageSize, 10),
		"FifoQueue":                     strconv.FormatBool(meta.IsFIFO),
		"ContentBasedDeduplication":     strconv.FormatBool(meta.ContentBasedDedup),
		"QueueArn":                      queueArn,
	}
	if created := meta.CreatedAtMillis; created > 0 {
		// AWS reports timestamps in unix seconds (string-encoded).
		all["CreatedTimestamp"] = strconv.FormatInt(created/sqsMillisPerSecond, 10)
	}
	if mod := meta.LastModifiedAtMillis; mod > 0 {
		all["LastModifiedTimestamp"] = strconv.FormatInt(mod/sqsMillisPerSecond, 10)
	}
	if counters != nil {
		all["ApproximateNumberOfMessages"] = strconv.FormatInt(counters.Visible, 10)
		all["ApproximateNumberOfMessagesNotVisible"] = strconv.FormatInt(counters.NotVisible, 10)
		all["ApproximateNumberOfMessagesDelayed"] = strconv.FormatInt(counters.Delayed, 10)
	}
	if meta.RedrivePolicy != "" {
		all["RedrivePolicy"] = meta.RedrivePolicy
	}
	// Throttle* are non-AWS extensions. Surfacing them in
	// GetQueueAttributes lets operators read back what they set; SDKs
	// that strictly validate the attribute set will ignore unknown
	// keys. Extracted into a helper so queueMetaToAttributes stays
	// under the cyclop ceiling.
	addThrottleAttributes(all, meta.Throttle)
	// HT-FIFO attributes (Phase 3.D). Same omission rule as Throttle*:
	// only present when configured. Extracted into a helper so this
	// function stays under the cyclop ceiling.
	addHTFIFOAttributes(all, meta)
	if selection.expandAll {
		return all
	}
	out := make(map[string]string, len(selection.names))
	for k := range selection.names {
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
	// AWS marks Attributes as a required parameter on SetQueueAttributes.
	// Without this check, a client that forgets (or mis-serializes) the
	// field gets a 200 success and no change — a silent failure that hides
	// automation bugs. Reject omission with MissingParameter.
	if len(in.Attributes) == 0 {
		writeSQSError(w, http.StatusBadRequest, sqsErrMissingParameter, "Attributes is required")
		return
	}
	if err := s.setQueueAttributesWithRetry(r.Context(), name, in.Attributes); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	// Drop the in-memory bucket entries belonging to this queue *after*
	// the Raft commit so the next request rebuilds from the freshly
	// committed throttle config. Gated on whether the request actually
	// touched a Throttle* attribute — an unconditional invalidate
	// would reset the bucket on every unrelated SetQueueAttributes
	// (e.g. VisibilityTimeout-only update), giving any caller a way to
	// silently restore a noisy tenant's burst capacity by writing a
	// no-op SetQueueAttributes (Codex P1 on PR #679). The bucket
	// reconciliation in loadOrInit also catches a stale bucket if a
	// throttle change slips past this gate (e.g. via a future admin
	// path), so the gating here is purely a hot-path optimisation.
	if throttleAttributesPresent(in.Attributes) {
		s.throttle.invalidateQueue(name)
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

// applyAndValidateSetAttributes runs the apply + cross-validator
// chain for a SetQueueAttributes request. Extracted from
// trySetQueueAttributesOnce so that function stays under the cyclop
// ceiling once HT-FIFO immutability + Throttle validators were
// added. Returns nil on success; on rejection returns the typed
// sqsAPIError the caller forwards to writeSQSErrorFromErr.
//
// preApply snapshot allocation is gated on htfifoAttributesPresent
// so the common "mutable-only update" path stays alloc-free per the
// Gemini medium feedback on PR #681.
func applyAndValidateSetAttributes(meta *sqsQueueMeta, attrs map[string]string) error {
	var preApply *sqsQueueMeta
	if htfifoAttributesPresent(attrs) {
		preApply = snapshotImmutableHTFIFO(meta)
	}
	if err := applyAttributes(meta, attrs); err != nil {
		return err
	}
	if preApply != nil {
		if err := validatePartitionImmutability(preApply, meta); err != nil {
			return err
		}
	}
	// ContentBasedDeduplication is FIFO-only; a Standard queue
	// silently accepting it would advertise unsupported behavior to
	// clients. Same rule enforced on CreateQueue.
	if meta.ContentBasedDedup && !meta.IsFIFO {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "ContentBasedDeduplication is only valid on FIFO queues")
	}
	// HT-FIFO schema validator runs after applyAttributes so the
	// FIFO-only checks see the post-apply state. IsFIFO comes from
	// the loaded meta record (immutable from CreateQueue) so the
	// validator sees the same flag CreateQueue set.
	return validatePartitionConfig(meta)
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
	if err := applyAndValidateSetAttributes(meta, attrs); err != nil {
		return false, err
	}
	meta.LastModifiedAtMillis = time.Now().UnixMilli()
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

// sqsApproxCounterScanLimit caps a single GetQueueAttributes scan. AWS
// reports the counters as "approximate"; once the queue is past this
// many records the per-bucket totals are best-effort. Picking 50_000
// keeps the scan latency under ~100 ms on a warm Pebble cache.
const sqsApproxCounterScanLimit = 50_000

// scanApproxCounters walks every data record under (queue, generation)
// and buckets it into visible / not-visible / delayed by the same
// definitions GetQueueAttributes documents. The scan runs at the same
// snapshot timestamp the meta read used so the returned numbers are a
// coherent snapshot; concurrent sends or receives that commit after
// readTS just show up on the next call.
func (s *SQSServer) scanApproxCounters(ctx context.Context, queueName string, gen uint64, readTS uint64) (sqsApproxCounters, error) {
	prefix := []byte(SqsMsgDataPrefix)
	prefix = append(prefix, []byte(encodeSQSSegment(queueName))...)
	prefix = appendU64(prefix, gen)
	end := prefixScanEnd(prefix)

	now := time.Now().UnixMilli()
	var counters sqsApproxCounters
	start := bytes.Clone(prefix)
	for {
		page, err := s.store.ScanAt(ctx, start, end, sqsQueueScanPageLimit, readTS)
		if err != nil {
			return counters, errors.WithStack(err)
		}
		if len(page) == 0 {
			return counters, nil
		}
		bucketApproxCounterPage(page, now, &counters)
		if exhausted(counters, page, end, &start) {
			return counters, nil
		}
	}
}

// bucketApproxCounterPage walks one ScanAt page and bumps the right
// counter for each record. Pulled out of scanApproxCounters so the
// outer loop stays under the cyclomatic budget.
func bucketApproxCounterPage(page []*store.KVPair, now int64, counters *sqsApproxCounters) {
	for _, kvp := range page {
		rec, err := decodeSQSMessageRecord(kvp.Value)
		if err != nil {
			// Malformed record means a programmer bug, not a
			// counter bug — drop it from the totals rather than
			// failing the whole call.
			continue
		}
		if rec.VisibleAtMillis <= now {
			counters.Visible++
			continue
		}
		if rec.ReceiveCount == 0 {
			counters.Delayed++
		} else {
			counters.NotVisible++
		}
	}
}

// exhausted reports whether the scan has reached its budget or the
// end of the prefix range. Mutates `start` so the caller can resume.
func exhausted(counters sqsApproxCounters, page []*store.KVPair, end []byte, start *[]byte) bool {
	total := counters.Visible + counters.NotVisible + counters.Delayed
	if total >= sqsApproxCounterScanLimit {
		return true
	}
	if len(page) < sqsQueueScanPageLimit {
		return true
	}
	*start = nextScanCursorAfter(page[len(page)-1].Key)
	if end != nil && bytes.Compare(*start, end) >= 0 {
		return true
	}
	return false
}

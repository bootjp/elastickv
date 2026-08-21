package backup

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

// Scope identifies one user-visible adapter namespace in a live backup.
type Scope struct {
	Adapter string
	Name    string
}

const (
	adapterDynamoDB = "dynamodb"
	adapterS3       = "s3"
	adapterRedis    = "redis"
	adapterSQS      = "sqs"
)

func (s Scope) ID() string {
	if s.Adapter == adapterRedis {
		return adapterRedis + "/" + s.Name
	}
	return s.Adapter + "/" + s.Name
}

// ErrScopeKeyMalformed marks a recognized adapter key whose scope cannot be
// recovered. Live backups fail closed on these keys instead of silently
// producing an incomplete expected-key baseline.
var ErrScopeKeyMalformed = errors.New("backup: malformed scoped key")

// ScopeForKey maps an internal user-data key to its logical backup scope.
// Internal control-plane keys and derivable indexes return (_, false, nil).
func ScopeForKey(key []byte) (Scope, bool, error) {
	switch {
	case hasAnyBackupPrefix(key, DDBTableMetaPrefix, DDBItemPrefix, DDBGSIPrefix):
		return scopeForDDBKey(key)
	case hasAnyBackupPrefix(key,
		S3BucketMetaPrefix, S3ObjectManifestPrefix,
		S3UploadMetaPrefix, S3UploadPartPrefix, S3BlobPrefix, S3GCUploadPrefix, S3RoutePrefix,
	):
		return scopeForS3Key(key)
	case hasAnyBackupPrefix(key,
		SQSQueueMetaPrefix, SQSQueueGenPrefix, SQSMsgDataPrefix, SQSQueueSeqPrefix,
		SQSQueueTombstonePrefix, SQSMsgVisPrefix, SQSMsgByAgePrefix, SQSMsgDedupPrefix, SQSMsgGroupPrefix,
	):
		return scopeForSQSKey(key)
	case isRedisBackupKey(key):
		return Scope{Adapter: adapterRedis, Name: "db_0"}, true, nil
	default:
		return Scope{}, false, nil
	}
}

// AdapterForKey reports the backup adapter family for keys that belong to a
// known adapter namespace, including malformed scoped keys. Callers can use it
// to skip disabled adapters before running stricter scope parsing.
func AdapterForKey(key []byte) (string, bool) {
	switch {
	case hasAnyBackupPrefix(key, DDBTableMetaPrefix, DDBTableGenPrefix, DDBItemPrefix, DDBGSIPrefix):
		return adapterDynamoDB, true
	case hasAnyBackupPrefix(key,
		S3BucketMetaPrefix, S3BucketGenPrefix, S3ObjectManifestPrefix,
		S3UploadMetaPrefix, S3UploadPartPrefix, S3BlobPrefix, S3GCUploadPrefix, S3RoutePrefix,
	):
		return adapterS3, true
	case hasAnyBackupPrefix(key,
		SQSQueueMetaPrefix, SQSQueueGenPrefix, SQSMsgDataPrefix, SQSQueueSeqPrefix,
		SQSQueueTombstonePrefix, SQSMsgVisPrefix, SQSMsgByAgePrefix, SQSMsgDedupPrefix, SQSMsgGroupPrefix,
	):
		return adapterSQS, true
	case isRedisBackupKey(key):
		return adapterRedis, true
	default:
		return "", false
	}
}

// AdapterEnabled reports whether name is enabled in set.
func AdapterEnabled(set AdapterSet, name string) bool {
	switch name {
	case adapterDynamoDB:
		return set.DynamoDB
	case adapterS3:
		return set.S3
	case adapterRedis:
		return set.Redis
	case adapterSQS:
		return set.SQS
	default:
		return false
	}
}

func scopeForDDBKey(key []byte) (Scope, bool, error) {
	switch {
	case bytes.HasPrefix(key, []byte(DDBTableMetaPrefix)):
		return ddbScopeFromDirectSegment(key, DDBTableMetaPrefix)
	case bytes.HasPrefix(key, []byte(DDBTableGenPrefix)):
		return Scope{}, false, nil
	case bytes.HasPrefix(key, []byte(DDBItemPrefix)):
		encoded, _, err := parseDDBItemKey(key)
		if err != nil {
			return Scope{}, false, err
		}
		return decodedScope("dynamodb", encoded)
	case bytes.HasPrefix(key, []byte(DDBGSIPrefix)):
		return Scope{}, false, nil
	default:
		return Scope{}, false, nil
	}
}

func scopeForS3Key(key []byte) (Scope, bool, error) {
	switch {
	case bytes.HasPrefix(key, []byte(S3BucketMetaPrefix)):
		bucket, ok := s3keys.ParseBucketMetaKey(key)
		return parsedS3Scope(bucket, ok, key)
	case bytes.HasPrefix(key, []byte(S3BucketGenPrefix)):
		return Scope{}, false, nil
	case bytes.HasPrefix(key, []byte(S3ObjectManifestPrefix)):
		bucket, _, _, ok := s3keys.ParseObjectManifestKey(key)
		return parsedS3Scope(bucket, ok, key)
	case bytes.HasPrefix(key, []byte(S3UploadMetaPrefix)), bytes.HasPrefix(key, []byte(S3UploadPartPrefix)):
		return Scope{}, false, nil
	case bytes.HasPrefix(key, []byte(S3BlobPrefix)):
		bucket, _, _, _, _, _, _, ok := s3keys.ParseBlobKey(key)
		return parsedS3Scope(bucket, ok, key)
	case bytes.HasPrefix(key, []byte(S3GCUploadPrefix)), bytes.HasPrefix(key, []byte(S3RoutePrefix)):
		return Scope{}, false, nil
	default:
		return Scope{}, false, nil
	}
}

func scopeForSQSKey(key []byte) (Scope, bool, error) {
	switch {
	case bytes.HasPrefix(key, []byte(SQSQueueMetaPrefix)):
		return sqsScopeFromDirectSegment(key, SQSQueueMetaPrefix)
	case bytes.HasPrefix(key, []byte(SQSQueueGenPrefix)):
		return sqsScopeFromDirectSegment(key, SQSQueueGenPrefix)
	case bytes.HasPrefix(key, []byte(SQSMsgDataPrefix)):
		encoded, _, _, err := parseSQSMessageDataKey(key)
		if err != nil {
			return Scope{}, false, err
		}
		return decodedScope("sqs", encoded)
	default:
		return Scope{}, false, nil
	}
}

func hasAnyBackupPrefix(key []byte, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if bytes.HasPrefix(key, []byte(prefix)) {
			return true
		}
	}
	return false
}

func ddbScopeFromDirectSegment(key []byte, prefix string) (Scope, bool, error) {
	encoded := string(key[len(prefix):])
	if encoded == "" || strings.ContainsRune(encoded, '|') {
		return Scope{}, false, errors.Wrapf(ErrScopeKeyMalformed, "dynamodb key %q", key)
	}
	return decodedScope("dynamodb", encoded)
}

func sqsScopeFromDirectSegment(key []byte, prefix string) (Scope, bool, error) {
	encoded := string(key[len(prefix):])
	if encoded == "" {
		return Scope{}, false, errors.Wrapf(ErrScopeKeyMalformed, "sqs key %q", key)
	}
	return decodedScope("sqs", encoded)
}

func decodedScope(adapter, encoded string) (Scope, bool, error) {
	name, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(name) == 0 {
		return Scope{}, false, errors.Wrapf(ErrScopeKeyMalformed, "%s segment %q", adapter, encoded)
	}
	return Scope{Adapter: adapter, Name: string(name)}, true, nil
}

func parsedS3Scope(name string, ok bool, key []byte) (Scope, bool, error) {
	if !ok || name == "" {
		return Scope{}, false, errors.Wrapf(ErrScopeKeyMalformed, "s3 key %q", key)
	}
	return Scope{Adapter: "s3", Name: name}, true, nil
}

func isRedisBackupKey(key []byte) bool {
	if hasAnyBackupPrefix(key,
		RedisHashMetaDeltaPrefix,
		ListMetaDeltaPrefix,
		ListClaimPrefix,
		RedisSetMetaDeltaPrefix,
		RedisZSetMetaDeltaPrefix,
		RedisZSetScorePrefix,
	) {
		return false
	}
	prefixes := [...]string{
		RedisHashMetaPrefix, RedisHashFieldPrefix,
		ListMetaPrefix, ListItemPrefix,
		RedisSetMetaPrefix, RedisSetMemberPrefix,
		RedisZSetMetaPrefix, RedisZSetMemberPrefix, RedisZSetLegacyBlobPrefix,
		RedisStreamMetaPrefix, RedisStreamEntryPrefix,
		RedisStringPrefix, RedisHLLPrefix, RedisTTLPrefix,
	}
	for _, prefix := range prefixes {
		if bytes.HasPrefix(key, []byte(prefix)) {
			return true
		}
	}
	return false
}

// SortedScopes returns a deterministic adapter/name ordering.
func SortedScopes(scopes map[Scope]uint64) []Scope {
	out := make([]Scope, 0, len(scopes))
	for scope := range scopes {
		out = append(out, scope)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Adapter != out[j].Adapter {
			return out[i].Adapter < out[j].Adapter
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// LiveDecoder feeds key/value pairs read at one pinned timestamp through the
// Phase 0 adapter encoders, producing the same logical directory format.
type LiveDecoder struct {
	d         *dispatcher
	finalized bool
}

func NewLiveDecoder(opts DecodeOptions) (*LiveDecoder, error) {
	d, err := newDispatcher(opts)
	if err != nil {
		return nil, err
	}
	return &LiveDecoder{d: d}, nil
}

func (d *LiveDecoder) Add(key, value []byte) error {
	if d == nil || d.d == nil || d.finalized {
		return errors.Wrap(ErrDecodeOptionsInvalid, "live decoder is unavailable or finalized")
	}
	d.d.counters.Total++
	return d.d.route(key, value)
}

func (d *LiveDecoder) Finalize() (DecodeCounters, error) {
	if d == nil || d.d == nil || d.finalized {
		return DecodeCounters{}, errors.Wrap(ErrDecodeOptionsInvalid, "live decoder is unavailable or finalized")
	}
	d.finalized = true
	if err := d.d.finalize(); err != nil {
		return DecodeCounters{}, err
	}
	return d.d.counters, nil
}

// FinalizedScopeCounts replaces streamed counts for adapters whose encoders
// make relational keep/drop decisions during Finalize. The encoder counts are
// authoritative because orphan and stale records are only known after the
// complete adapter state has been assembled.
func (d *LiveDecoder) FinalizedScopeCounts(streamed map[Scope]uint64) (map[Scope]uint64, error) {
	if d == nil || d.d == nil || !d.finalized {
		return nil, errors.Wrap(ErrDecodeOptionsInvalid, "live decoder is not finalized")
	}
	return finalizedScopeCounts(d.d, streamed), nil
}

// LiveScopeCounter classifies live backup key/value pairs using the same
// adapter state as the decoder, but in count-only mode. Server-side baseline
// scans use it so BeginBackup.expected_keys uses the same retained-record
// denominator that the live producer validates after decoder finalization.
type LiveScopeCounter struct {
	d        *dispatcher
	streamed map[Scope]uint64
}

func NewLiveScopeCounter(adapters AdapterSet) (*LiveScopeCounter, error) {
	d, err := newDispatcher(DecodeOptions{Adapters: adapters, countOnly: true})
	if err != nil {
		return nil, err
	}
	return &LiveScopeCounter{d: d, streamed: make(map[Scope]uint64)}, nil
}

func (c *LiveScopeCounter) Add(key, value []byte) error {
	needsValue, err := c.AddKey(key)
	if err != nil {
		return err
	}
	if !needsValue {
		return nil
	}
	return c.AddValue(key, value)
}

// AddKey records a key observed by a count-only baseline scan. It returns true
// when the caller must also provide the value through AddValue for metadata
// needed by retained-count finalization.
func (c *LiveScopeCounter) AddKey(key []byte) (bool, error) {
	if c == nil || c.d == nil {
		return false, errors.Wrap(ErrDecodeOptionsInvalid, "live scope counter is unavailable")
	}
	scope, scoped, err := ScopeForKey(key)
	if err != nil {
		return false, err
	}
	if !scoped {
		return false, nil
	}
	c.streamed[scope]++
	if liveScopeCounterNeedsValue(key) {
		return true, nil
	}
	return false, c.d.route(key, nil)
}

func (c *LiveScopeCounter) AddValue(key, value []byte) error {
	if c == nil || c.d == nil {
		return errors.Wrap(ErrDecodeOptionsInvalid, "live scope counter is unavailable")
	}
	return c.d.route(key, value)
}

func (c *LiveScopeCounter) RetainedCounts() (map[Scope]uint64, error) {
	if c == nil || c.d == nil {
		return nil, errors.Wrap(ErrDecodeOptionsInvalid, "live scope counter is unavailable")
	}
	return finalizedScopeCounts(c.d, c.streamed), nil
}

func liveScopeCounterNeedsValue(key []byte) bool {
	switch {
	case hasAnyBackupPrefix(key, DDBTableMetaPrefix):
		return true
	case hasAnyBackupPrefix(key, S3BucketMetaPrefix, S3ObjectManifestPrefix):
		return true
	case hasAnyBackupPrefix(key, SQSQueueMetaPrefix, SQSQueueGenPrefix):
		return true
	default:
		adapter, ok := AdapterForKey(key)
		return ok && adapter == adapterRedis
	}
}

func finalizedScopeCounts(d *dispatcher, streamed map[Scope]uint64) map[Scope]uint64 {
	out := make(map[Scope]uint64, len(streamed))
	for scope, count := range streamed {
		if count > 0 && !adapterFinalizesScopeCounts(scope.Adapter) {
			out[scope] = count
		}
	}
	d.addFinalizedScopeCounts(out)
	return out
}

func adapterFinalizesScopeCounts(adapter string) bool {
	switch adapter {
	case adapterDynamoDB, adapterS3, adapterRedis, adapterSQS:
		return true
	default:
		return false
	}
}

func (d *dispatcher) addFinalizedScopeCounts(out map[Scope]uint64) {
	if d.ddb != nil {
		for name, count := range d.ddb.RetainedRecordCounts() {
			addPositiveScopeCount(out, Scope{Adapter: adapterDynamoDB, Name: name}, count)
		}
	}
	if d.s3 != nil {
		for name, count := range d.s3.RetainedRecordCounts() {
			addPositiveScopeCount(out, Scope{Adapter: adapterS3, Name: name}, count)
		}
	}
	if d.redis != nil {
		addPositiveScopeCount(out, Scope{Adapter: adapterRedis, Name: "db_0"}, d.redis.RetainedRecordCount())
	}
	if d.sqs != nil {
		for name, count := range d.sqs.RetainedRecordCounts() {
			addPositiveScopeCount(out, Scope{Adapter: adapterSQS, Name: name}, count)
		}
	}
}

func addPositiveScopeCount(out map[Scope]uint64, scope Scope, count uint64) {
	if count == 0 {
		delete(out, scope)
		return
	}
	out[scope] = count
}

func (s Scope) String() string {
	return fmt.Sprintf("%s/%s", s.Adapter, s.Name)
}

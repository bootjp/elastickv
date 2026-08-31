package kv

import (
	"bytes"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
)

const redisInternalRoutePrefix = "!redis|"

var redisInternalRoutePrefixBytes = []byte(redisInternalRoutePrefix)

const wideColumnEncodedKeyLengthSize = 4

const (
	dynamoRoutePrefix = "!ddb|route|table|"

	// DynamoTableMetaPrefix prefixes DynamoDB table metadata keys.
	DynamoTableMetaPrefix = "!ddb|meta|table|"
	// DynamoTableGenerationPrefix prefixes DynamoDB table generation keys.
	DynamoTableGenerationPrefix = "!ddb|meta|gen|"
	// DynamoItemPrefix prefixes DynamoDB item storage keys.
	DynamoItemPrefix = "!ddb|item|"
	// DynamoGSIPrefix prefixes DynamoDB GSI storage keys.
	DynamoGSIPrefix = "!ddb|gsi|"
)

const (
	// SqsRoutePrefix is the logical route prefix all SQS internal keys
	// normalize to. The adapter stores queue metadata and per-queue
	// message keys under several !sqs|... families; routing must map
	// every one of them to a single stable prefix so the distribution
	// engine colocates a queue's storage on one Raft group.
	sqsRoutePrefix = "!sqs|route|"

	// SqsInternalPrefix is the shared prefix of every SQS-owned key
	// family (!sqs|queue|meta|, !sqs|msg|vis|, etc.). Used by
	// sqsRouteKey to dispatch the routing decision.
	sqsInternalPrefix = "!sqs|"

	sqsQueueMetaPrefix      = "!sqs|queue|meta|"
	sqsQueueGenPrefix       = "!sqs|queue|gen|"
	sqsQueueSeqPrefix       = "!sqs|queue|seq|"
	sqsQueueTombstonePrefix = "!sqs|queue|tombstone|"
	sqsMsgDataPrefix        = "!sqs|msg|data|"
	sqsMsgVisPrefix         = "!sqs|msg|vis|"
	sqsMsgDedupPrefix       = "!sqs|msg|dedup|"
	sqsMsgGroupPrefix       = "!sqs|msg|group|"
	sqsMsgByAgePrefix       = "!sqs|msg|byage|"
)

var (
	dynamoRoutePrefixBytes           = []byte(dynamoRoutePrefix)
	dynamoTableMetaPrefixBytes       = []byte(DynamoTableMetaPrefix)
	dynamoTableGenerationPrefixBytes = []byte(DynamoTableGenerationPrefix)
	dynamoItemPrefixBytes            = []byte(DynamoItemPrefix)
	dynamoGSIPrefixBytes             = []byte(DynamoGSIPrefix)
	sqsInternalPrefixBytes           = []byte(sqsInternalPrefix)
	sqsGlobalRouteKey                = []byte(sqsRoutePrefix + "global")
	sqsConcreteInternalPrefixBytes   = [][]byte{
		[]byte(sqsQueueMetaPrefix),
		[]byte(sqsQueueGenPrefix),
		[]byte(sqsQueueSeqPrefix),
		[]byte(sqsQueueTombstonePrefix),
		[]byte(sqsMsgDataPrefix),
		[]byte(sqsMsgVisPrefix),
		[]byte(sqsMsgDedupPrefix),
		[]byte(sqsMsgGroupPrefix),
		[]byte(sqsMsgByAgePrefix),
	}
	// Families whose physical keys can still appear in a scan result and need a
	// point-read canonicalization.
	redisWideColumnScanPrefixes = [][]byte{
		[]byte(store.HashMetaDeltaPrefix),
		[]byte(store.HashMetaPrefix),
		[]byte(store.HashFieldPrefix),
		[]byte(store.SetMetaDeltaPrefix),
		[]byte(store.SetMetaPrefix),
		[]byte(store.SetMemberPrefix),
		[]byte(store.ZSetMetaDeltaPrefix),
		[]byte(store.ZSetMetaPrefix),
		[]byte(store.ZSetMemberPrefix),
		[]byte(store.ZSetScorePrefix),
	}
	redisStreamScanPrefixes = [][]byte{
		[]byte(store.StreamMetaPrefix),
		[]byte(store.StreamEntryPrefix),
	}
	// Every family here encodes its keys as prefix + 4-byte user-key length +
	// user key + suffix, so one scan-range projection covers all of them.
	// Streams belong in this list because normalizeRouteKey places stream writes
	// on the logical user-key route: leaving their scans on the raw prefix range
	// would send XRANGE/XREAD/XTRIM to a different group than the XADD that
	// produced the entries whenever a split separates the two.
	redisEncodedUserKeyScanPrefixes = append(
		append([][]byte{}, redisWideColumnScanPrefixes...),
		redisStreamScanPrefixes...,
	)
	dynamoTablePrefixWriteFamilies = [][]byte{
		[]byte(DynamoItemPrefix),
		[]byte(DynamoGSIPrefix),
	}
	redisListAuxiliaryScanPrefixes = [][]byte{
		[]byte(store.ListMetaDeltaPrefix),
		[]byte(store.ListClaimPrefix),
	}
)

// RouteKey normalizes internal keys (e.g., list metadata/items) to the logical
// user key used for shard routing.
func RouteKey(key []byte) []byte {
	return routeKey(key)
}

// RouteOwnershipKey normalizes a stored key to the catalog key used when
// answering route ownership queries.
func RouteOwnershipKey(key []byte) []byte {
	return routeOwnershipKey(key)
}

func routeKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	if embedded, ok := txnRouteKey(key); ok {
		return normalizeRouteKey(embedded)
	}
	return normalizeRouteKey(key)
}

func routeOwnershipKey(key []byte) []byte {
	if bucket, ok := s3keys.ParseBucketMetaKey(key); ok {
		return s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	}
	if bucket, ok := s3keys.ParseBucketGenerationKey(key); ok {
		return s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	}
	return routeKey(key)
}

func routeFilterKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	if embedded, ok := txnRouteKey(key); ok {
		return normalizeRouteFilterKey(embedded)
	}
	return normalizeRouteFilterKey(key)
}

func normalizeRouteKey(key []byte) []byte {
	if user := listRouteKey(key); user != nil {
		return user
	}
	if user := redisStreamRouteKey(key); user != nil {
		return user
	}
	if user := redisRouteKey(key); user != nil {
		return user
	}
	if user := redisWideColumnRouteKey(key); user != nil {
		return user
	}
	if table := dynamoRouteKey(key); table != nil {
		return table
	}
	if route := sqsRouteKey(key); route != nil {
		return route
	}
	if user := s3keys.ExtractRouteKey(key); user != nil {
		return user
	}
	if user := fskeys.ExtractRouteKey(key); user != nil {
		return user
	}
	return key
}

func listRouteKey(key []byte) []byte {
	if user := store.ExtractListUserKeyFromDelta(key); user != nil {
		return user
	}
	if user := store.ExtractListUserKeyFromDeltaScanPrefix(key); user != nil {
		return user
	}
	if user := store.ExtractListUserKeyFromClaim(key); user != nil {
		return user
	}
	if user := store.ExtractListUserKeyFromClaimScanPrefix(key); user != nil {
		return user
	}
	if user := store.ExtractListUserKey(key); user != nil {
		return user
	}
	return nil
}

func normalizeRouteFilterKey(key []byte) []byte {
	if user := redisListAuxiliaryRouteKey(key); user != nil {
		return user
	}
	if user := redisStreamRouteKey(key); user != nil {
		return user
	}
	return normalizeRouteKey(key)
}

// legacyPointRouteKey returns the raw physical key as a second route candidate
// for families whose logical placement changed, so a point read consults where
// an older writer put the value as well as where a new one would.
//
// This is the routing counterpart of the legacy candidate the scan path already
// adds through redisWideColumnLegacyScanRouteRange. Streams need it for the same
// reason wide-column families do: normalizeRouteKey now places stream writes on
// the logical user-key route, so metadata written before that normalization sits
// on the physical !stream|meta| route, which a split can separate from the user
// key. A point read that consults only the logical route reports an existing
// stream as missing -- and because the caller then never scans, the legacy
// candidate the scan path carries is never reached.
//
// It is deliberately not redisWideColumnLegacyPointRouteKey: that one answers a
// different question -- whether a scan row is a physical form that needs a
// canonicalizing point read -- and streams have no such physical form.
func legacyPointRouteKey(key []byte) []byte {
	if embedded, ok := txnRouteKey(key); ok {
		key = embedded
	}
	if redisWideColumnRouteKey(key) == nil && redisStreamRouteKey(key) == nil {
		return nil
	}
	return key
}

func redisWideColumnLegacyPointRouteKey(key []byte) []byte {
	if embedded, ok := txnRouteKey(key); ok {
		key = embedded
	}
	if redisWideColumnRouteKey(key) == nil {
		return nil
	}
	return key
}

func redisWideColumnRouteKey(key []byte) []byte {
	if user := redisHashRouteKey(key); user != nil {
		return user
	}
	if user := redisSetRouteKey(key); user != nil {
		return user
	}
	return redisZSetRouteKey(key)
}

func redisWideColumnScanRouteParts(key []byte) (prefix []byte, userKey []byte, userPrefix []byte, owned bool, parsed bool) {
	for _, prefix := range redisEncodedUserKeyScanPrefixes {
		if !bytes.HasPrefix(key, prefix) {
			continue
		}
		user := wideColumnScanUserKey(key, prefix)
		if user == nil {
			return prefix, nil, nil, true, false
		}
		prefixLen := len(prefix) + wideColumnEncodedKeyLengthSize + len(user)
		return prefix, user, key[:prefixLen], true, true
	}
	return nil, nil, nil, false, false
}

func redisWideColumnLegacyScanRouteRange(start []byte, end []byte) ([]byte, []byte, bool) {
	_, _, _, owned, parsed := redisWideColumnScanRouteParts(start)
	if !owned || !parsed {
		return nil, nil, false
	}
	return start, end, true
}

// redisWideColumnCanonicalizableScan reports whether a scan range can return
// legacy physical wide-column keys that the caller must replace with a
// canonical point read. Streams share the encoded key layout for routing but
// have no legacy physical form, so they are excluded: routing them through the
// value-reading canonicalization path would cost one value read per entry and
// change nothing.
func redisWideColumnCanonicalizableScan(start []byte) bool {
	for _, prefix := range redisWideColumnScanPrefixes {
		if bytes.HasPrefix(start, prefix) {
			return true
		}
	}
	return false
}

func redisWideColumnScanRouteRange(start []byte, end []byte) (routeStart []byte, routeEnd []byte, exact bool, ok bool) {
	prefix, userKey, userPrefix, owned, parsed := redisWideColumnScanRouteParts(start)
	if !owned {
		return nil, nil, false, false
	}
	if !parsed {
		return nil, nil, false, true
	}
	if exactEnd := prefixScanEnd(userPrefix); end != nil && bytes.Compare(end, exactEnd) <= 0 {
		return userKey, nil, true, true
	}
	if bytes.Equal(start, userPrefix) && end != nil && bytes.Compare(end, prefixScanEnd(prefix)) <= 0 {
		return userKey, prefixScanEnd(userKey), false, true
	}
	// Physical wide-column cursors include a field/member suffix. Their raw
	// ordering cannot be projected to a logical user-key lower bound, so the
	// remaining namespace must fan out to every logical route.
	return nil, nil, false, true
}

func listAuxiliaryScanRouteRange(start []byte, end []byte) (routeStart []byte, exact bool, ok bool) {
	for _, prefix := range redisListAuxiliaryScanPrefixes {
		if !bytes.HasPrefix(start, prefix) {
			continue
		}
		user := wideColumnScanUserKey(start, prefix)
		if user == nil {
			return nil, false, true
		}
		userPrefixLen := len(prefix) + wideColumnEncodedKeyLengthSize + len(user)
		userPrefix := start[:userPrefixLen]
		if exactEnd := prefixScanEnd(userPrefix); end != nil && bytes.Compare(end, exactEnd) <= 0 {
			return user, true, true
		}
		return nil, false, true
	}
	return nil, false, false
}

func wideColumnScanUserKey(key []byte, prefix []byte) []byte {
	if !bytes.HasPrefix(key, prefix) {
		return nil
	}
	rest := key[len(prefix):]
	if len(rest) < wideColumnEncodedKeyLengthSize {
		return nil
	}
	keyLen := binary.BigEndian.Uint32(rest[:wideColumnEncodedKeyLengthSize])
	rest = rest[wideColumnEncodedKeyLengthSize:]
	if uint64(keyLen) > uint64(len(rest)) {
		return nil
	}
	return rest[:keyLen]
}

func redisHashRouteKey(key []byte) []byte {
	switch {
	case store.IsHashMetaDeltaKey(key):
		return store.ExtractHashUserKeyFromDelta(key)
	case store.IsHashMetaKey(key):
		return store.ExtractHashUserKeyFromMeta(key)
	case store.IsHashFieldKey(key):
		return store.ExtractHashUserKeyFromField(key)
	default:
		return nil
	}
}

func redisSetRouteKey(key []byte) []byte {
	switch {
	case store.IsSetMetaDeltaKey(key):
		return store.ExtractSetUserKeyFromDelta(key)
	case store.IsSetMetaKey(key):
		return store.ExtractSetUserKeyFromMeta(key)
	case store.IsSetMemberKey(key):
		return store.ExtractSetUserKeyFromMember(key)
	default:
		return nil
	}
}

func redisZSetRouteKey(key []byte) []byte {
	switch {
	case store.IsZSetMetaDeltaKey(key):
		return store.ExtractZSetUserKeyFromDelta(key)
	case store.IsZSetMetaKey(key):
		return store.ExtractZSetUserKeyFromMeta(key)
	case store.IsZSetMemberKey(key):
		return store.ExtractZSetUserKeyFromMember(key)
	case store.IsZSetScoreKey(key):
		return store.ExtractZSetUserKeyFromScore(key)
	default:
		return nil
	}
}

func redisListAuxiliaryRouteKey(key []byte) []byte {
	switch {
	case store.IsListMetaDeltaKey(key):
		return store.ExtractListUserKeyFromDelta(key)
	case store.IsListClaimKey(key):
		return store.ExtractListUserKeyFromClaim(key)
	default:
		return nil
	}
}

func redisStreamRouteKey(key []byte) []byte {
	switch {
	case store.IsStreamMetaKey(key):
		return store.ExtractStreamUserKeyFromMeta(key)
	case store.IsStreamEntryKey(key):
		return store.ExtractStreamUserKeyFromEntry(key)
	default:
		return nil
	}
}

func redisRouteKey(key []byte) []byte {
	if !bytes.HasPrefix(key, redisInternalRoutePrefixBytes) {
		return nil
	}
	rest := key[len(redisInternalRoutePrefix):]
	sep := bytes.IndexByte(rest, '|')
	if sep <= 0 {
		return nil
	}
	return rest[sep+1:]
}

func dynamoRouteKey(key []byte) []byte {
	switch {
	case bytes.HasPrefix(key, dynamoTableMetaPrefixBytes):
		return dynamoRouteTableKey(key[len(dynamoTableMetaPrefixBytes):])
	case bytes.HasPrefix(key, dynamoTableGenerationPrefixBytes):
		return dynamoRouteTableKey(key[len(dynamoTableGenerationPrefixBytes):])
	case bytes.HasPrefix(key, dynamoItemPrefixBytes):
		return dynamoRouteFromTablePrefixedKey(key[len(dynamoItemPrefixBytes):])
	case bytes.HasPrefix(key, dynamoGSIPrefixBytes):
		return dynamoRouteFromTablePrefixedKey(key[len(dynamoGSIPrefixBytes):])
	default:
		return nil
	}
}

func dynamoRouteFromTablePrefixedKey(rest []byte) []byte {
	sep := bytes.IndexByte(rest, '|')
	if sep <= 0 {
		return nil
	}
	return dynamoRouteTableKey(rest[:sep])
}

func dynamoRouteTableKey(tableSegment []byte) []byte {
	if len(tableSegment) == 0 {
		return nil
	}
	out := make([]byte, 0, len(dynamoRoutePrefixBytes)+len(tableSegment))
	out = append(out, dynamoRoutePrefixBytes...)
	out = append(out, tableSegment...)
	return out
}

// sqsRouteKey maps concrete persisted !sqs|... storage prefixes to a stable
// route key. Adapter-looking raw user keys such as !sqs|foo intentionally stay
// on their raw route and migrate through the user-key bracket.
func sqsRouteKey(key []byte) []byte {
	if !bytes.HasPrefix(key, sqsInternalPrefixBytes) {
		return nil
	}
	if !hasSQSConcreteInternalPrefix(key) {
		return nil
	}
	return sqsGlobalRouteKey
}

func hasSQSConcreteInternalPrefix(key []byte) bool {
	for _, prefix := range sqsConcreteInternalPrefixBytes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

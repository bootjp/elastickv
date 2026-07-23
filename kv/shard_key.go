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
)

var (
	dynamoRoutePrefixBytes           = []byte(dynamoRoutePrefix)
	dynamoTableMetaPrefixBytes       = []byte(DynamoTableMetaPrefix)
	dynamoTableGenerationPrefixBytes = []byte(DynamoTableGenerationPrefix)
	dynamoItemPrefixBytes            = []byte(DynamoItemPrefix)
	dynamoGSIPrefixBytes             = []byte(DynamoGSIPrefix)
	sqsRoutePrefixBytes              = []byte(sqsRoutePrefix)
	sqsInternalPrefixBytes           = []byte(sqsInternalPrefix)
	redisWideColumnScanPrefixes      = [][]byte{
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

func routeKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	if embedded, ok := txnRouteKey(key); ok {
		return normalizeRouteKey(embedded)
	}
	return normalizeRouteKey(key)
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
	if user := store.ExtractListUserKey(key); user != nil {
		return user
	}
	return key
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
	for _, prefix := range redisWideColumnScanPrefixes {
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

func redisWideColumnExactScanLegacyRouteKey(start []byte, end []byte) []byte {
	_, _, userPrefix, owned, parsed := redisWideColumnScanRouteParts(start)
	if !owned || !parsed {
		return nil
	}
	if exactEnd := prefixScanEnd(userPrefix); end != nil && bytes.Compare(end, exactEnd) <= 0 {
		return start
	}
	return nil
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
	if sep < 0 || sep+1 >= len(rest) {
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

// sqsRouteKey maps any !sqs|... internal key to a stable route key so
// multi-shard deployments that partition by user-key range still land
// every SQS mutation on a configured group. Milestone 1 collapses all
// SQS keys to a single !sqs|route|global route — this keeps the
// catalog and every queue's message keyspace on the same group, which
// is the minimum needed for FIFO group-lock semantics (landing later)
// to work. When per-queue sharding is implemented it will live here.
func sqsRouteKey(key []byte) []byte {
	if !bytes.HasPrefix(key, sqsInternalPrefixBytes) {
		return nil
	}
	out := make([]byte, 0, len(sqsRoutePrefixBytes)+len("global"))
	out = append(out, sqsRoutePrefixBytes...)
	out = append(out, []byte("global")...)
	return out
}

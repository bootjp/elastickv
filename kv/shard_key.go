package kv

import (
	"bytes"
	"encoding/binary"

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
	sqsPartitionMarker      = "p|"
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
	routeKeyExtractors = []func([]byte) []byte{
		redisRouteKey,
		dynamoRouteKey,
		sqsRouteKey,
		s3keys.ExtractRouteKey,
		listRouteKey,
		hashRouteKey,
		setRouteKey,
		zsetRouteKey,
		streamRouteKey,
		store.ExtractListUserKey,
	}
)

// routeKey normalizes internal keys (e.g., list metadata/items) to the logical
// user key used for shard routing.
func routeKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	if embedded, ok := txnRouteKey(key); ok {
		return normalizeRouteKey(embedded)
	}
	return normalizeRouteKey(key)
}

func normalizeRouteKey(key []byte) []byte {
	for _, extract := range routeKeyExtractors {
		if user := extract(key); user != nil {
			return user
		}
	}
	return key
}

func redisWideColumnScanRouteKey(key []byte) []byte {
	for _, prefix := range [][]byte{
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
	} {
		if user := wideColumnScanUserKey(key, prefix); user != nil {
			return user
		}
	}
	return nil
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
	if uint64(keyLen) > uint64(len(rest)-wideColumnEncodedKeyLengthSize) { //nolint:gosec // non-negative slice length fits uint64.
		return nil
	}
	return rest[wideColumnEncodedKeyLengthSize : uint32(wideColumnEncodedKeyLengthSize)+keyLen]
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

func listRouteKey(key []byte) []byte {
	if userKey := store.ExtractListUserKeyFromDelta(key); userKey != nil {
		return userKey
	}
	if userKey := store.ExtractListUserKeyFromClaim(key); userKey != nil {
		return userKey
	}
	return nil
}

func hashRouteKey(key []byte) []byte {
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

func setRouteKey(key []byte) []byte {
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

func zsetRouteKey(key []byte) []byte {
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

func streamRouteKey(key []byte) []byte {
	switch {
	case store.IsStreamMetaKey(key):
		return store.ExtractStreamUserKeyFromMeta(key)
	case store.IsStreamEntryKey(key):
		return store.ExtractStreamUserKeyFromEntry(key)
	default:
		return nil
	}
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

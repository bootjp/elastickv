package kv

import (
	"bytes"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
)

const redisInternalRoutePrefix = "!redis|"

var redisInternalRoutePrefixBytes = []byte(redisInternalRoutePrefix)

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
	if user := redisRouteKey(key); user != nil {
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
	if user := store.ExtractListUserKey(key); user != nil {
		return user
	}
	return key
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

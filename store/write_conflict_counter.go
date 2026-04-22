package store

import (
	"bytes"
	"sync"
)

// Write-conflict classification labels. These are emitted as the
// "key_prefix" Prometheus label on elastickv_store_write_conflict_total
// so operators can tell WHICH namespace is producing OCC conflicts.
// The label set is deliberately bounded and stable — adding a new
// bucket is a conscious schema change, not a cardinality surprise.
//
// Buckets are aligned with the internal key prefixes declared across
// the codebase:
//
//	!txn|lock|, !txn|int|, !txn|cmt|, !txn|rb|  (kv/txn_keys.go)
//	!redis|str|, !redis|ttl|, !redis|hll|       (adapter/redis_compat_types.go)
//	!hs|, !st|, !zs|, !lst|                     (store/*_helpers.go)
//	!ddb|                                       (kv/shard_key.go)
const (
	WriteConflictKindWrite = "write"
	WriteConflictKindRead  = "read"

	writeConflictClassTxnLock     = "txn_lock"
	writeConflictClassTxnIntent   = "txn_intent"
	writeConflictClassTxnCommit   = "txn_commit"
	writeConflictClassTxnRollback = "txn_rollback"
	writeConflictClassTxnMeta     = "txn_meta"
	writeConflictClassTxnOther    = "txn_other"
	writeConflictClassRedisString = "redis_string"
	writeConflictClassRedisTTL    = "redis_ttl"
	writeConflictClassRedisHLL    = "redis_hll"
	writeConflictClassRedisOther  = "redis_other"
	writeConflictClassHash        = "hash"
	writeConflictClassSet         = "set"
	writeConflictClassZSet        = "zset"
	writeConflictClassList        = "list"
	writeConflictClassStream      = "stream"
	writeConflictClassDynamoDB    = "dynamodb"
	writeConflictClassOther       = "other"
)

var (
	// Txn-internal prefixes (from kv/txn_keys.go; duplicated here to
	// avoid the store→kv import cycle).
	wcPrefixTxnLock     = []byte("!txn|lock|")
	wcPrefixTxnIntent   = []byte("!txn|int|")
	wcPrefixTxnCommit   = []byte("!txn|cmt|")
	wcPrefixTxnRollback = []byte("!txn|rb|")
	wcPrefixTxnMeta     = []byte("!txn|meta|")
	wcPrefixTxn         = []byte("!txn|")

	// Redis-adapter prefixes (from adapter/redis_compat_types.go).
	wcPrefixRedisString = []byte("!redis|str|")
	wcPrefixRedisTTL    = []byte("!redis|ttl|")
	wcPrefixRedisHLL    = []byte("!redis|hll|")
	wcPrefixRedisStream = []byte("!redis|stream|")
	wcPrefixRedisHash   = []byte("!redis|hash|")
	wcPrefixRedisSet    = []byte("!redis|set|")
	wcPrefixRedisZSet   = []byte("!redis|zset|")
	wcPrefixRedis       = []byte("!redis|")

	// Composite-type helper prefixes (store/*_helpers.go).
	wcPrefixHash = []byte("!hs|")
	wcPrefixSet  = []byte("!st|")
	wcPrefixZSet = []byte("!zs|")
	wcPrefixList = []byte("!lst|")

	// DynamoDB-adapter prefix (kv/shard_key.go).
	wcPrefixDynamoDB = []byte("!ddb|")
)

// writeConflictPrefixMatcher pairs a literal byte prefix with the
// bucket label it maps to. Evaluated in order against the candidate
// key; the first match wins, so more-specific prefixes (e.g.
// "!txn|lock|") must be listed before the namespace umbrella
// ("!txn|") that would otherwise shadow them.
type writeConflictPrefixMatcher struct {
	prefix []byte
	label  string
}

var writeConflictPrefixTable = []writeConflictPrefixMatcher{
	// Txn-internal namespaces first (most specific), then the
	// umbrella "!txn|" fallback bucket for future additions.
	{wcPrefixTxnLock, writeConflictClassTxnLock},
	{wcPrefixTxnIntent, writeConflictClassTxnIntent},
	{wcPrefixTxnCommit, writeConflictClassTxnCommit},
	{wcPrefixTxnRollback, writeConflictClassTxnRollback},
	{wcPrefixTxnMeta, writeConflictClassTxnMeta},
	{wcPrefixTxn, writeConflictClassTxnOther},
	// Redis-adapter namespaces, specific-first.
	{wcPrefixRedisString, writeConflictClassRedisString},
	{wcPrefixRedisTTL, writeConflictClassRedisTTL},
	{wcPrefixRedisHLL, writeConflictClassRedisHLL},
	{wcPrefixRedisStream, writeConflictClassStream},
	{wcPrefixRedisHash, writeConflictClassHash},
	{wcPrefixRedisSet, writeConflictClassSet},
	{wcPrefixRedisZSet, writeConflictClassZSet},
	{wcPrefixRedis, writeConflictClassRedisOther},
	// Composite-type helpers (server-internal naming).
	{wcPrefixHash, writeConflictClassHash},
	{wcPrefixSet, writeConflictClassSet},
	{wcPrefixZSet, writeConflictClassZSet},
	{wcPrefixList, writeConflictClassList},
	// DynamoDB-adapter namespace.
	{wcPrefixDynamoDB, writeConflictClassDynamoDB},
}

// classifyWriteConflictKey returns a bounded, stable label identifying
// the conflicting key's namespace. Unknown shapes fall back to "other"
// so the label set cannot grow unbounded with user-supplied keys.
func classifyWriteConflictKey(key []byte) string {
	for _, m := range writeConflictPrefixTable {
		if bytes.HasPrefix(key, m.prefix) {
			return m.label
		}
	}
	return writeConflictClassOther
}

// writeConflictCounter is a small, lock-protected set of per-bucket
// counters. Conflicts are rare — at most one increment per failing
// ApplyMutations call — so a mutex is cheaper than a shard map. The
// (kind, key_prefix) tuple is flattened into a single string key with
// "|" as separator; this keeps the snapshot map dense and avoids a
// nested map allocation per tick.
type writeConflictCounter struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func newWriteConflictCounter() *writeConflictCounter {
	return &writeConflictCounter{counts: map[string]uint64{}}
}

// record increments the counter for (kind, keyClass) where kind is one
// of WriteConflictKindWrite / WriteConflictKindRead and keyClass is a
// classifyWriteConflictKey result.
func (c *writeConflictCounter) record(kind string, keyClass string) {
	if c == nil {
		return
	}
	label := kind + "|" + keyClass
	c.mu.Lock()
	c.counts[label]++
	c.mu.Unlock()
}

// snapshot returns a copy of the counter map suitable for handing to
// the monitoring collector without callers holding the counter lock.
func (c *writeConflictCounter) snapshot() map[string]uint64 {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]uint64, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

// total returns the sum across all (kind, keyClass) buckets. Useful
// for an aggregate accessor and for tests that just want to assert
// "some conflict was recorded".
func (c *writeConflictCounter) total() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var sum uint64
	for _, v := range c.counts {
		sum += v
	}
	return sum
}

// EncodeWriteConflictLabel joins a (kind, keyClass) tuple into the
// flat map key used by WriteConflictCountsByPrefix snapshots. Exposed
// so the monitoring collector can build the same string without
// re-implementing the convention.
func EncodeWriteConflictLabel(kind, keyClass string) string {
	return kind + "|" + keyClass
}

// DecodeWriteConflictLabel splits a flat label back into (kind,
// keyClass). Returns ok=false for malformed input so collector code
// can skip rather than emit bogus series.
func DecodeWriteConflictLabel(label string) (kind, keyClass string, ok bool) {
	for i := 0; i < len(label); i++ {
		if label[i] == '|' {
			return label[:i], label[i+1:], true
		}
	}
	return "", "", false
}

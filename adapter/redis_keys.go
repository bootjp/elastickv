package adapter

import (
	"bytes"
	"context"
	"maps"
	"math"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

const keysCommandArgs = 2

func (r *RedisServer) keys(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < keysCommandArgs {
		conn.WriteError("ERR wrong number of arguments for 'keys' command")
		return
	}
	pattern := cmd.Args[1]

	if r.coordinator.IsLeader() {
		// Per-call ctx with redisDispatchTimeout instead of the
		// long-lived handlerContext: a stalled VerifyLeader on KEYS
		// must not pin the command handler indefinitely. The same
		// bound the rest of the dispatch path (sadd, set, …) uses;
		// see Codex P1 review on PR #749.
		ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
		defer cancel()
		if err := r.coordinator.VerifyLeader(ctx); err != nil {
			writeRedisError(conn, err)
			return
		}
		keys, err := r.visibleKeys(pattern)
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulk(k)
		}
		return
	}

	keys, err := r.proxyKeys(pattern)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
}

func (r *RedisServer) localKeys(pattern []byte) ([][]byte, error) {
	if !bytes.Contains(pattern, []byte("*")) {
		return r.localKeysExact(pattern)
	}
	return r.localKeysPattern(pattern)
}

func (r *RedisServer) localKeysExact(pattern []byte) ([][]byte, error) {
	typ, err := r.keyTypeAt(context.Background(), pattern, r.readTS())
	if err != nil {
		return nil, err
	}
	if typ != redisTypeNone {
		return [][]byte{bytes.Clone(pattern)}, nil
	}
	return [][]byte{}, nil
}

// mergeInternalNamespaces scans all internal key namespaces (list, hash, set,
// zset, and other internal prefixes) for keys that match pattern and merges
// them into the caller's keyset via mergeScannedKeys. Called only when the
// pattern is bounded (start != nil) because unbounded scans already cover the
// full keyspace.
func (r *RedisServer) mergeInternalNamespaces(start []byte, pattern []byte, mergeScannedKeys func([]byte, []byte) error) error {
	metaStart, metaEnd := listPatternScanBounds(store.ListMetaPrefix, pattern)
	if err := mergeScannedKeys(metaStart, metaEnd); err != nil {
		return err
	}
	itemStart, itemEnd := listPatternScanBounds(store.ListItemPrefix, pattern)
	if err := mergeScannedKeys(itemStart, itemEnd); err != nil {
		return err
	}
	for _, prefix := range redisInternalPrefixes {
		// !stream|meta| keys are length-prefixed (see store.StreamMetaKey):
		// a pattern-bound scan over the raw prefix would mask out every
		// migrated stream because the user-key bytes do not start at
		// prefix[len(prefix):]. Delegate to the wide-column scan below,
		// which uses streamMetaScanStart(start) to place the user-key
		// lower bound past the length field.
		if prefix == store.StreamMetaPrefix {
			continue
		}
		internalStart, internalEnd := listPatternScanBounds(prefix, pattern)
		if err := mergeScannedKeys(internalStart, internalEnd); err != nil {
			return err
		}
	}
	// Wide-column hash/set/zset keys embed the user-key as
	// <prefix><4-byte-len><userKey><field|member>, so the binary length
	// prefix makes straightforward bounds-based scanning non-trivial.
	// Use the user-key prefix as the lower bound and scan to the end of each
	// namespace; collectUserKeys filters false positives by pattern.
	hashFieldStart := store.HashFieldScanPrefix(start)
	hashFieldEnd := prefixScanEnd([]byte(store.HashFieldPrefix))
	if err := mergeScannedKeys(hashFieldStart, hashFieldEnd); err != nil {
		return err
	}
	setMemberStart := store.SetMemberScanPrefix(start)
	setMemberEnd := prefixScanEnd([]byte(store.SetMemberPrefix))
	if err := mergeScannedKeys(setMemberStart, setMemberEnd); err != nil {
		return err
	}
	zsetMemberStart := store.ZSetMemberScanPrefix(start)
	zsetMemberEnd := prefixScanEnd([]byte(store.ZSetMemberPrefix))
	if err := mergeScannedKeys(zsetMemberStart, zsetMemberEnd); err != nil {
		return err
	}
	// Post-migration streams live under !stream|meta|<len><userKey>.
	// The meta record is enough to expose the logical key via KEYS;
	// entry rows are filtered out by redisVisibleUserKey / collectUserKeys
	// so the result stays one-line-per-stream regardless of entry count.
	streamMetaStart := streamMetaScanStart(start)
	streamMetaEnd := prefixScanEnd([]byte(store.StreamMetaPrefix))
	return mergeScannedKeys(streamMetaStart, streamMetaEnd)
}

// streamMetaScanStart returns the lower bound for scanning stream meta
// keys that begin with the given user-key prefix. The store helper
// already returns StreamMetaPrefix + len(userKey) + userKey, so callers
// only need to supply the bounded pattern prefix.
func streamMetaScanStart(userPrefix []byte) []byte {
	if len(userPrefix) == 0 {
		return []byte(store.StreamMetaPrefix)
	}
	return store.StreamMetaKey(userPrefix)
}

func (r *RedisServer) localKeysPattern(pattern []byte) ([][]byte, error) {
	start, end := patternScanBounds(pattern)
	keyset := map[string][]byte{}
	readTS := r.readTS()

	mergeScannedKeys := func(scanStart, scanEnd []byte) error {
		keys, err := r.store.ScanAt(context.Background(), scanStart, scanEnd, math.MaxInt, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		maps.Copy(keyset, r.collectUserKeys(keys, pattern))
		return nil
	}

	if err := mergeScannedKeys(start, end); err != nil {
		return nil, err
	}

	// When the pattern is bounded (start != nil), user-key scans do not
	// naturally include internal data namespaces, so scan those separately
	// and map them back to logical user keys.  For unbounded patterns
	// (e.g. "*"), the full-keyspace scan already covers everything.
	if start != nil {
		if err := r.mergeInternalNamespaces(start, pattern, mergeScannedKeys); err != nil {
			return nil, err
		}
	}

	out := make([][]byte, 0, len(keyset))
	for _, v := range keyset {
		out = append(out, v)
	}
	return out, nil
}

func patternScanBounds(pattern []byte) ([]byte, []byte) {
	if bytes.Equal(pattern, []byte("*")) {
		return nil, nil
	}

	i := bytes.IndexByte(pattern, '*')
	if i <= 0 {
		return nil, nil
	}

	start := bytes.Clone(pattern[:i])
	return start, prefixScanEnd(start)
}

func listPatternScanBounds(prefix string, pattern []byte) ([]byte, []byte) {
	userStart, userEnd := patternScanBounds(pattern)
	prefixBytes := []byte(prefix)

	if userStart == nil && userEnd == nil {
		return prefixBytes, prefixScanEnd(prefixBytes)
	}

	start := append(bytes.Clone(prefixBytes), userStart...)
	if userEnd == nil {
		return start, prefixScanEnd(prefixBytes)
	}
	end := append(bytes.Clone(prefixBytes), userEnd...)
	return start, end
}

func matchesAsteriskPattern(pattern, key []byte) bool {
	parts := bytes.Split(pattern, []byte("*"))
	if len(parts) == 1 {
		return bytes.Equal(pattern, key)
	}

	pos := 0
	if len(parts[0]) > 0 {
		if !bytes.HasPrefix(key, parts[0]) {
			return false
		}
		pos = len(parts[0])
	}

	for i := 1; i < len(parts)-1; i++ {
		part := parts[i]
		if len(part) == 0 {
			continue
		}
		idx := bytes.Index(key[pos:], part)
		if idx < 0 {
			return false
		}
		pos += idx + len(part)
	}

	last := parts[len(parts)-1]
	if len(last) > 0 && !bytes.HasSuffix(key, last) {
		return false
	}

	return true
}

func (r *RedisServer) collectUserKeys(kvs []*store.KVPair, pattern []byte) map[string][]byte {
	keyset := map[string][]byte{}
	for _, kvPair := range kvs {
		userKey := redisVisibleUserKey(kvPair.Key)
		if userKey == nil || !matchesAsteriskPattern(pattern, userKey) {
			continue
		}
		keyset[string(userKey)] = userKey
	}
	return keyset
}

// zsetWideColumnVisibleUserKey handles the ZSet-specific part of wide-column key mapping.
// Returns (nil, true) for internal-only keys and (userKey, true) for visible keys.
func zsetWideColumnVisibleUserKey(key []byte) (userKey []byte, isWide bool) {
	if store.IsZSetMetaDeltaKey(key) || store.IsZSetMetaKey(key) {
		return nil, true
	}
	if store.IsZSetMemberKey(key) {
		return store.ExtractZSetUserKeyFromMember(key), true
	}
	if store.IsZSetScoreKey(key) {
		return store.ExtractZSetUserKeyFromScore(key), true
	}
	return nil, false
}

// wideColumnVisibleUserKey maps a wide-column internal key to its visible user
// key, or returns (nil, true) for internal-only keys (meta/delta), and
// (nil, false) if the key is not a wide-column key at all.
func wideColumnVisibleUserKey(key []byte) (userKey []byte, isWide bool) {
	// Check delta prefixes before meta prefixes (delta starts with meta prefix).
	if store.IsHashMetaDeltaKey(key) || store.IsHashMetaKey(key) {
		return nil, true
	}
	if store.IsHashFieldKey(key) {
		return store.ExtractHashUserKeyFromField(key), true
	}
	if store.IsSetMetaDeltaKey(key) || store.IsSetMetaKey(key) {
		return nil, true
	}
	if store.IsSetMemberKey(key) {
		return store.ExtractSetUserKeyFromMember(key), true
	}
	if userKey, ok := streamWideColumnVisibleUserKey(key); ok {
		return userKey, true
	}
	return zsetWideColumnVisibleUserKey(key)
}

// streamWideColumnVisibleUserKey maps a wide-column stream key to its
// visible user key. Meta keys expose the stream exactly once; entry keys
// are internal-only so KEYS / SCAN don't leak one result per entry.
func streamWideColumnVisibleUserKey(key []byte) ([]byte, bool) {
	if store.IsStreamMetaKey(key) {
		return store.ExtractStreamUserKeyFromMeta(key), true
	}
	if store.IsStreamEntryKey(key) {
		return nil, true
	}
	return nil, false
}

func redisVisibleUserKey(key []byte) []byte {
	if bytes.HasPrefix(key, redisTxnKeyPrefix) || isRedisTTLKey(key) {
		return nil
	}
	if redisTxnWideFenceUserKey(key) != nil {
		return nil
	}
	// List item keys are visible; meta, delta, and claim keys are internal-only.
	if store.IsListItemKey(key) {
		return store.ExtractListUserKey(key)
	}
	if store.IsListMetaKey(key) || store.IsListMetaDeltaKey(key) || store.IsListClaimKey(key) {
		return nil
	}
	if userKey, isWide := wideColumnVisibleUserKey(key); isWide {
		return userKey
	}
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		return userKey
	}
	return key
}

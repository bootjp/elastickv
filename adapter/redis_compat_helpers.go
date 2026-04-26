package adapter

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// maxWideColumnItems is the maximum number of fields/members a single
// wide-column collection (Hash, Set, ZSet, or List) may contain.
// Operations that would materialize more than this many items are rejected
// to prevent unbounded memory growth (OOM).
const maxWideColumnItems = 100_000

// maxWideScanLimit is passed to ScanAt when loading an entire collection.
// It is set to maxWideColumnItems+1 so that receiving exactly limit results
// indicates the collection is over the cap and the caller can return an error
// instead of silently truncating.
const maxWideScanLimit = maxWideColumnItems + 1

// ErrCollectionTooLarge is returned when a collection exceeds maxWideColumnItems.
var ErrCollectionTooLarge = errors.New("collection too large")

const wrongTypeMessage = "WRONGTYPE Operation against a key holding the wrong kind of value"

// setKind and hllKind are the internal kind discriminators used for Set and
// HyperLogLog operations. They distinguish code paths within functions that
// handle both types (e.g. loadSetAt, mutateExactSet).
const (
	setKind = "set"
	hllKind = "hll"
)

// ErrDeltaScanTruncated is returned when the delta scan result is truncated,
// indicating that synchronous compaction is required before the operation can proceed.
var ErrDeltaScanTruncated = errors.New("delta scan truncated: compaction required")

func wrongTypeError() error {
	return errors.New(wrongTypeMessage)
}

// normalizeStartTS converts a "fresh read" sentinel (^uint64(0)) to 0.
// The coordinator's Dispatch requires startTS=0 to mean "no conflict check",
// while internally we use ^uint64(0) to indicate an uninitialized read timestamp.
func normalizeStartTS(ts uint64) uint64 {
	if ts == ^uint64(0) {
		return 0
	}
	return ts
}

// detectWideColumnType checks for the presence of wide-column hash, set, or zset keys
// and returns the corresponding redis type, or redisTypeNone if none is found.
// It checks field/member keys first, then falls back to metadata and delta keys to
// correctly detect collections whose fields were all deleted (metadata key exists but
// no member keys) or newly created collections that only have delta keys.
func (r *RedisServer) detectWideColumnType(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	if typ, err := r.detectWideColumnTypeSkipZSet(ctx, key, readTS); err != nil || typ != redisTypeNone {
		return typ, err
	}
	if found, err := r.wideColumnTypeExists(ctx, key, readTS, store.ZSetMemberScanPrefix, store.ZSetMetaKey, store.ZSetMetaDeltaScanPrefix); err != nil {
		return redisTypeNone, err
	} else if found {
		return redisTypeZSet, nil
	}
	return redisTypeNone, nil
}

// detectWideColumnTypeSkipZSet runs the wide-column hash / set probes
// only. Callers that have already eliminated ZSet (e.g.
// rawZSetPhysTypeAt's fallback after the member-prefix and meta/delta
// scans came back empty) use this to avoid re-issuing the three
// ZSet-side probes detectWideColumnType would otherwise repeat.
func (r *RedisServer) detectWideColumnTypeSkipZSet(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	if found, err := r.wideColumnTypeExists(ctx, key, readTS, store.HashFieldScanPrefix, store.HashMetaKey, store.HashMetaDeltaScanPrefix); err != nil {
		return redisTypeNone, err
	} else if found {
		return redisTypeHash, nil
	}
	if found, err := r.wideColumnTypeExists(ctx, key, readTS, store.SetMemberScanPrefix, store.SetMetaKey, store.SetMetaDeltaScanPrefix); err != nil {
		return redisTypeNone, err
	} else if found {
		return redisTypeSet, nil
	}
	return redisTypeNone, nil
}

// wideColumnTypeExists checks whether a wide-column collection of the given type exists,
// trying: member/field prefix → metadata key → delta key prefix (in order).
func (r *RedisServer) wideColumnTypeExists(
	ctx context.Context,
	key []byte,
	readTS uint64,
	memberPrefix func([]byte) []byte,
	metaKeyFn func([]byte) []byte,
	deltaPrefix func([]byte) []byte,
) (bool, error) {
	if found, err := r.prefixExistsAt(ctx, memberPrefix(key), readTS); err != nil || found {
		return found, err
	}
	if exists, err := r.store.ExistsAt(ctx, metaKeyFn(key), readTS); err != nil {
		return false, errors.WithStack(err)
	} else if exists {
		return true, nil
	}
	return r.prefixExistsAt(ctx, deltaPrefix(key), readTS)
}

// prefixExistsAt reports whether any key under prefix exists at readTS.
func (r *RedisServer) prefixExistsAt(ctx context.Context, prefix []byte, readTS uint64) (bool, error) {
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, 1, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return len(kvs) > 0, nil
}

// zsetStorageHint bundles the storage-probe results needed by zsetState so that
// the member-prefix scan is performed at most once.
type zsetStorageHint struct {
	physType    redisValueType // type ignoring TTL expiry
	logType     redisValueType // type after TTL check (redisTypeNone if expired)
	memberFound bool           // true when the member-prefix scan returned ≥1 key
}

// zsetStorageHintAt probes storage for ZSet data at readTS.
// It performs the ZSetMemberScanPrefix scan only once, so callers (zsetState)
// do not need a second ScanAt to determine wide-column vs legacy-blob format.
// For non-ZSet keys the full rawKeyTypeAt path is used for correctness.
func (r *RedisServer) zsetStorageHintAt(ctx context.Context, key []byte, readTS uint64) (zsetStorageHint, error) {
	physType, memberFound, err := r.rawZSetPhysTypeAt(ctx, key, readTS)
	if err != nil {
		return zsetStorageHint{}, err
	}
	h := zsetStorageHint{physType: physType, logType: physType, memberFound: memberFound}
	if physType != redisTypeNone {
		// Known-physType TTL probe: for collection types the embedded
		// TTL only lives under the collection-side key, so we can skip
		// the `!redis|str|` probe (nonStringOnly=true). For any string
		// types we reach via rawZSetPhysTypeAt's fallback (mixed
		// corruption), we still need the string-side check too.
		expired, err := r.hasExpired(ctx, key, readTS, isNonStringCollectionType(physType))
		if err != nil {
			return zsetStorageHint{}, err
		}
		if expired {
			h.logType = redisTypeNone
		}
	}
	return h, nil
}

// rawZSetPhysTypeAt detects whether a ZSet exists physically at readTS (ignoring
// TTL) and whether the detection was via the member-prefix scan (memberFound).
// For non-ZSet keys it falls back to rawKeyTypeAt.
func (r *RedisServer) rawZSetPhysTypeAt(ctx context.Context, key []byte, readTS uint64) (redisValueType, bool, error) {
	// Single scan: probe member prefix (common path).
	memberFound, err := r.prefixExistsAt(ctx, store.ZSetMemberScanPrefix(key), readTS)
	if err != nil {
		return redisTypeNone, false, err
	}
	if memberFound {
		return redisTypeZSet, true, nil
	}
	// No member rows — check meta/delta for a memberless wide-column ZSet.
	zsetOnly, err := r.zsetMetaOrDeltaExistsAt(ctx, key, readTS)
	if err != nil {
		return redisTypeNone, false, err
	}
	if zsetOnly {
		return redisTypeZSet, false, nil
	}
	// Not a wide-column ZSet — probe other types without re-scanning
	// the three ZSet-side prefixes we already ruled out above.
	physType, err := r.rawKeyTypeAtSkipZSet(ctx, key, readTS)
	return physType, false, err
}

// rawKeyTypeAtSkipZSet is rawKeyTypeAt minus the ZSet wide-column
// probes. Used by rawZSetPhysTypeAt when the caller has already
// confirmed no ZSet member / meta / delta rows exist, so the three
// ZSet probes inside detectWideColumnType would be pure redundant I/O.
func (r *RedisServer) rawKeyTypeAtSkipZSet(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	if typ, found, err := r.probeStringTypes(ctx, key, readTS); err != nil || found {
		return typ, err
	}
	if typ, found, err := r.probeListType(ctx, key, readTS); err != nil || found {
		return typ, err
	}
	if typ, err := r.detectWideColumnTypeSkipZSet(ctx, key, readTS); err != nil || typ != redisTypeNone {
		return typ, err
	}
	return r.probeLegacyCollectionTypes(ctx, key, readTS)
}

// zsetMetaOrDeltaExistsAt reports whether a ZSet meta key or delta prefix exists.
func (r *RedisServer) zsetMetaOrDeltaExistsAt(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	if exists, err := r.store.ExistsAt(ctx, store.ZSetMetaKey(key), readTS); err != nil {
		return false, errors.WithStack(err)
	} else if exists {
		return true, nil
	}
	return r.prefixExistsAt(ctx, store.ZSetMetaDeltaScanPrefix(key), readTS)
}

// rawKeyTypeAt classifies the Redis encoding under which key is
// currently stored. Probes run string-first because real workloads are
// dominated by string keys: a live new-format string resolves in 1
// pebble seek here, versus the ~17 seeks the prior collection-first
// ordering required before falling through to the string block.
//
// Tiebreaker invariant when the same user key carries BOTH a string
// and a collection entry (legal only during data-corruption recovery):
// string wins. replaceWithStringTxn still uses the returned type as a
// cleanup hint, so on corrupt input any lingering collection keys are
// evicted by TTL or an explicit DEL rather than piggy-backing on SET.
// In non-corrupt data at most one encoding exists per user key, so
// the ordering is indistinguishable.
func (r *RedisServer) rawKeyTypeAt(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	if typ, found, err := r.probeStringTypes(ctx, key, readTS); err != nil || found {
		return typ, err
	}
	if typ, found, err := r.probeListType(ctx, key, readTS); err != nil || found {
		return typ, err
	}
	if typ, err := r.detectWideColumnType(ctx, key, readTS); err != nil || typ != redisTypeNone {
		return typ, err
	}
	return r.probeLegacyCollectionTypes(ctx, key, readTS)
}

// probeStringTypes runs the three cheap point lookups against the
// string-family prefixes. Returns (String, true, nil) on first hit.
func (r *RedisServer) probeStringTypes(ctx context.Context, key []byte, readTS uint64) (redisValueType, bool, error) {
	candidates := [...][]byte{
		redisStrKey(key), // new-format prefixed string
		redisHLLKey(key), // HyperLogLog (reported as string)
		key,              // legacy bare key (pre-migration)
	}
	for _, k := range candidates {
		exists, err := r.store.ExistsAt(ctx, k, readTS)
		if err != nil {
			return redisTypeNone, false, errors.WithStack(err)
		}
		if exists {
			return redisTypeString, true, nil
		}
	}
	return redisTypeNone, false, nil
}

// probeListType detects lists via the base meta key or any delta key.
// Delta scan is bounded to 1 result.
func (r *RedisServer) probeListType(ctx context.Context, key []byte, readTS uint64) (redisValueType, bool, error) {
	metaExists, err := r.store.ExistsAt(ctx, store.ListMetaKey(key), readTS)
	if err != nil {
		return redisTypeNone, false, errors.WithStack(err)
	}
	if metaExists {
		return redisTypeList, true, nil
	}
	deltaPrefix := store.ListMetaDeltaScanPrefix(key)
	deltaEnd := store.PrefixScanEnd(deltaPrefix)
	deltaKVs, err := r.store.ScanAt(ctx, deltaPrefix, deltaEnd, 1, readTS)
	if err != nil {
		return redisTypeNone, false, errors.WithStack(err)
	}
	if len(deltaKVs) > 0 {
		return redisTypeList, true, nil
	}
	return redisTypeNone, false, nil
}

// probeLegacyCollectionTypes checks for single-blob hash/set/zset/stream
// encodings left by pre-wide-column code paths. For streams, both the new
// entry-per-key meta and the legacy single-blob key are probed here so
// type-detection is unaffected by the migration state.
func (r *RedisServer) probeLegacyCollectionTypes(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	checks := []struct {
		typ redisValueType
		key []byte
	}{
		{typ: redisTypeHash, key: redisHashKey(key)},
		{typ: redisTypeSet, key: redisSetKey(key)},
		{typ: redisTypeZSet, key: redisZSetKey(key)},
		{typ: redisTypeStream, key: store.StreamMetaKey(key)},
		{typ: redisTypeStream, key: redisStreamKey(key)},
	}
	for _, check := range checks {
		exists, err := r.store.ExistsAt(ctx, check.key, readTS)
		if err != nil {
			return redisTypeNone, errors.WithStack(err)
		}
		if exists {
			return check.typ, nil
		}
	}
	return redisTypeNone, nil
}

func (r *RedisServer) keyTypeAt(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	typ, err := r.rawKeyTypeAt(ctx, key, readTS)
	if err != nil {
		return typ, err
	}
	return r.applyTTLFilter(ctx, key, readTS, typ)
}

// keyTypeAtExpect is a fast-path replacement for keyTypeAt callers that
// know the type they expect to find. The slow path probes ~19 Pebble
// seeks across every collection family before returning. The fast path:
//
//  1. Probe only the prefixes for `expected` (typically 2-3 seeks).
//  2. On hit, run the same string-priority guard the wide-column
//     fast-path callers use (hashFieldFastLookup, zsetMemberFastScore,
//     setMemberFastExists, hashFieldFastExists). When a redisStrKey
//     row also exists at the same user key, fall back to the slow
//     path so the rawKeyTypeAt "string wins" tiebreaker fires and
//     the caller gets WRONGTYPE / nil instead of the
//     collection-specific answer. The guard is the narrow form (see
//     hasHigherPriorityStringEncoding's doc comment): only redisStrKey
//     is checked, the rarer HLL / legacy-bare-key dual-encoding cases
//     remain a known residual risk shared with the other fast-path
//     callers.
//  3. On miss, fall back to the full keyTypeAt slow path so that
//     wrongType collisions (the key exists under a different type)
//     still surface as the correct redisValueType.
//
// Steady-state production: most XADD/XREAD/HSET/etc. calls are on a key
// of the expected type, so step 1 hits and the slow-path 19 seeks shrink
// to 2-3 (plus the priority-guard ExistsAt). The slow path stays in
// place for first-write and wrongType cases, which keep their existing
// semantics — wrongTypeError detection is preserved by the
// fall-through.
func (r *RedisServer) keyTypeAtExpect(ctx context.Context, key []byte, readTS uint64, expected redisValueType) (redisValueType, error) {
	if expected == redisTypeNone {
		return r.keyTypeAt(ctx, key, readTS)
	}
	found, err := r.probeExpectedType(ctx, key, readTS, expected)
	if err != nil {
		return redisTypeNone, err
	}
	if !found {
		return r.keyTypeAt(ctx, key, readTS)
	}
	if expected != redisTypeString {
		higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS)
		if hErr != nil {
			return redisTypeNone, hErr
		}
		if higher {
			return r.keyTypeAt(ctx, key, readTS)
		}
	}
	return r.applyTTLFilter(ctx, key, readTS, expected)
}

// keyTypeAtExpectFast is the signal-driven-wake variant of
// keyTypeAtExpect. On a fast-probe miss it returns redisTypeNone
// directly (no rawKeyTypeAt slow-path fallback, no
// hasHigherPriorityStringEncoding guard). Callers MUST have an
// invariant that the only mutation since the last full check could
// have produced a row visible to probeExpectedType — typically a
// blocking-command wait loop after a Signal-driven wake, where the
// only writes that fire keyWaiterRegistry.Signal are
// expected-type-creating writes (ZADD/ZINCRBY for zsets,
// XADD-and-friends for streams). A wrongType-introducing write
// (HSET, SET, etc.) does NOT signal, so a non-zset key that
// appeared between iterations is invisible to this fast path; the
// blocking command's fallback-timer wake (which uses the slow
// keyTypeAtExpect) is the safety net that detects it within
// ~redisBlockWaitFallback (100ms).
//
// Compared to keyTypeAtExpect on the empty-key case
// (probeExpectedType -> false -> rawKeyTypeAt slow path = ~19
// seeks), the fast variant returns after the 3-seek probe. For a
// BZPOPMIN waiting on an empty zset and being woken by Signal at
// the ZADD rate, this turns each wake from "19 seeks just to
// confirm still-empty" into "0 seeks because the probe found the
// new ZADD's row" or "3 seeks to confirm the ZADD raced and the
// queue is empty again".
func (r *RedisServer) keyTypeAtExpectFast(ctx context.Context, key []byte, readTS uint64, expected redisValueType) (redisValueType, error) {
	if expected == redisTypeNone {
		return redisTypeNone, nil
	}
	found, err := r.probeExpectedType(ctx, key, readTS, expected)
	if err != nil {
		return redisTypeNone, err
	}
	if !found {
		return redisTypeNone, nil
	}
	return r.applyTTLFilter(ctx, key, readTS, expected)
}

// probeExpectedType issues only the prefix probes for the given type.
// It is intentionally conservative: returning false here means "no row
// of the expected type was visible at readTS", not "the key does not
// exist". Callers that need strict "does any value type exist for this
// key" semantics must take the keyTypeAt slow path; keyTypeAtExpect
// composes both.
func (r *RedisServer) probeExpectedType(ctx context.Context, key []byte, readTS uint64, expected redisValueType) (bool, error) {
	switch expected {
	case redisTypeString:
		_, found, err := r.probeStringTypes(ctx, key, readTS)
		return found, err
	case redisTypeList:
		_, found, err := r.probeListType(ctx, key, readTS)
		return found, err
	case redisTypeHash:
		return r.wideColumnTypeExists(ctx, key, readTS, store.HashFieldScanPrefix, store.HashMetaKey, store.HashMetaDeltaScanPrefix)
	case redisTypeSet:
		return r.wideColumnTypeExists(ctx, key, readTS, store.SetMemberScanPrefix, store.SetMetaKey, store.SetMetaDeltaScanPrefix)
	case redisTypeZSet:
		return r.wideColumnTypeExists(ctx, key, readTS, store.ZSetMemberScanPrefix, store.ZSetMetaKey, store.ZSetMetaDeltaScanPrefix)
	case redisTypeStream:
		return r.probeStreamExists(ctx, key, readTS)
	case redisTypeNone:
		// Caller already short-circuited.
		return false, nil
	}
	return false, nil
}

// probeStreamExists checks whether a stream is present at readTS in
// either the new entry-per-key meta layout or the legacy single-blob
// encoding. Two ExistsAt seeks worst-case; one when the new layout is
// present (the common case post-#620 migration).
func (r *RedisServer) probeStreamExists(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	if exists, err := r.store.ExistsAt(ctx, store.StreamMetaKey(key), readTS); err != nil {
		return false, errors.WithStack(err)
	} else if exists {
		return true, nil
	}
	exists, err := r.store.ExistsAt(ctx, redisStreamKey(key), readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return exists, nil
}

// applyTTLFilter takes a raw (TTL-unaware) type and returns the
// TTL-filtered equivalent. Callers that need BOTH the raw and filtered
// types (SET NX/XX/GET against a possibly-expired key) can reuse a
// single rawKeyTypeAt result and skip the duplicate ~17-seek probe
// that keyTypeAt would otherwise issue.
//
// For non-string raw types we skip the embedded-TTL probe that
// hasExpired does by default: the embedded TTL only lives under
// !redis|str|<key>, so probing it for a hash/set/zset/stream/list is
// a guaranteed-miss GetAt. Passing nonStringOnly=true jumps straight
// to the !redis|ttl| secondary index, saving one pebble seek per
// non-string SET / type check.
func (r *RedisServer) applyTTLFilter(ctx context.Context, key []byte, readTS uint64, rawTyp redisValueType) (redisValueType, error) {
	if rawTyp == redisTypeNone {
		return rawTyp, nil
	}
	expired, err := r.hasExpired(ctx, key, readTS, rawTyp != redisTypeString)
	if err != nil {
		return redisTypeNone, err
	}
	if expired {
		return redisTypeNone, nil
	}
	return rawTyp, nil
}

func (r *RedisServer) keyType(ctx context.Context, key []byte) (redisValueType, error) {
	return r.keyTypeAt(ctx, key, r.readTS())
}

func (r *RedisServer) logicalExistsAt(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		return false, err
	}
	return typ != redisTypeNone, nil
}

// loadHashFieldsAt scans all wide-column !hs|fld| keys and returns them as a
// redisHashValue map.
func (r *RedisServer) loadHashFieldsAt(ctx context.Context, key []byte, readTS uint64) (redisHashValue, error) {
	prefix := store.HashFieldScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, maxWideScanLimit, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(kvs) > maxWideColumnItems {
		return nil, errors.Wrapf(ErrCollectionTooLarge, "hash %q exceeds %d fields", key, maxWideColumnItems)
	}
	result := make(redisHashValue, len(kvs))
	for _, kv := range kvs {
		field := store.ExtractHashFieldName(kv.Key, key)
		if field != nil {
			result[string(field)] = string(kv.Value)
		}
	}
	return result, nil
}

func (r *RedisServer) loadHashAt(ctx context.Context, key []byte, readTS uint64) (redisHashValue, error) {
	// Wide-column path: scan !hs|fld| prefix first.
	prefix := store.HashFieldScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, 1, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(kvs) > 0 {
		return r.loadHashFieldsAt(ctx, key, readTS)
	}
	// Legacy blob fallback.
	raw, err := r.store.GetAt(ctx, redisHashKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return redisHashValue{}, nil
		}
		return nil, errors.WithStack(err)
	}
	val, err := unmarshalHashValue(raw)
	return val, err
}

// loadSetMembersAt scans all wide-column !st|mem| keys and returns them as a
// redisSetValue.  Only used for kind=="set" (HLL stays as a legacy blob).
func (r *RedisServer) loadSetMembersAt(ctx context.Context, key []byte, readTS uint64) (redisSetValue, error) {
	prefix := store.SetMemberScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, maxWideScanLimit, readTS)
	if err != nil {
		return redisSetValue{}, errors.WithStack(err)
	}
	if len(kvs) > maxWideColumnItems {
		return redisSetValue{}, errors.Wrapf(ErrCollectionTooLarge, "set %q exceeds %d members", key, maxWideColumnItems)
	}
	members := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		member := store.ExtractSetMemberName(kv.Key, key)
		if member != nil {
			members = append(members, string(member))
		}
	}
	sort.Strings(members)
	return redisSetValue{Members: members}, nil
}

func (r *RedisServer) loadSetAt(ctx context.Context, kind string, key []byte, readTS uint64) (redisSetValue, error) {
	if kind == "set" {
		// Wide-column path: check !st|mem| prefix first.
		prefix := store.SetMemberScanPrefix(key)
		end := store.PrefixScanEnd(prefix)
		kvs, err := r.store.ScanAt(ctx, prefix, end, 1, readTS)
		if err != nil {
			return redisSetValue{}, errors.WithStack(err)
		}
		if len(kvs) > 0 {
			return r.loadSetMembersAt(ctx, key, readTS)
		}
	}
	// Legacy blob fallback (also the only path for HLL).
	storageKey := redisExactSetStorageKey(kind, key)
	raw, err := r.store.GetAt(ctx, storageKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return redisSetValue{}, nil
		}
		return redisSetValue{}, errors.WithStack(err)
	}
	val, err := unmarshalSetValue(raw)
	return val, err
}

// loadZSetMembersAt scans all wide-column !zs|mem| keys and returns them as a redisZSetValue
// sorted by (score, member), matching the ordering produced by the legacy blob path.
func (r *RedisServer) loadZSetMembersAt(ctx context.Context, key []byte, readTS uint64) (redisZSetValue, error) {
	prefix := store.ZSetMemberScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, maxWideScanLimit, readTS)
	if err != nil {
		return redisZSetValue{}, errors.WithStack(err)
	}
	if len(kvs) > maxWideColumnItems {
		return redisZSetValue{}, errors.Wrapf(ErrCollectionTooLarge, "zset %q exceeds %d members", key, maxWideColumnItems)
	}
	entries := make([]redisZSetEntry, 0, len(kvs))
	for _, kv := range kvs {
		member := store.ExtractZSetMemberName(kv.Key, key)
		if member == nil {
			continue
		}
		score, scoreErr := store.UnmarshalZSetScore(kv.Value)
		if scoreErr != nil {
			return redisZSetValue{}, errors.WithStack(scoreErr)
		}
		entries = append(entries, redisZSetEntry{Member: string(member), Score: score})
	}
	sortZSetEntries(entries)
	return redisZSetValue{Entries: entries}, nil
}

func (r *RedisServer) loadZSetAt(ctx context.Context, key []byte, readTS uint64) (redisZSetValue, bool, error) {
	// Wide-column path: check !zs|mem| prefix first.
	prefix := store.ZSetMemberScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, 1, readTS)
	if err != nil {
		return redisZSetValue{}, false, errors.WithStack(err)
	}
	if len(kvs) > 0 {
		val, loadErr := r.loadZSetMembersAt(ctx, key, readTS)
		return val, true, loadErr
	}
	// Legacy blob fallback.
	raw, err := r.store.GetAt(ctx, redisZSetKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return redisZSetValue{}, false, nil
		}
		return redisZSetValue{}, false, errors.WithStack(err)
	}
	val, err := unmarshalZSetValue(raw)
	return val, true, err
}

// loadStreamAt reads the entire stream as a redisStreamValue from the
// entry-per-key layout. Per the PR #620 operator directive, any legacy
// single-blob data is explicitly discarded: if the new meta key is absent
// we return an empty stream, even when a legacy blob still exists on disk.
// The legacy blob is actively deleted by the next write (see
// streamWriteBase) and by any DEL via deleteStreamWideColumnElems.
func (r *RedisServer) loadStreamAt(ctx context.Context, key []byte, readTS uint64) (redisStreamValue, error) {
	meta, metaFound, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil {
		return redisStreamValue{}, err
	}
	if !metaFound {
		return redisStreamValue{}, nil
	}
	entries, err := r.scanStreamEntriesAt(ctx, key, readTS, meta.Length)
	if err != nil {
		return redisStreamValue{}, err
	}
	return redisStreamValue{Entries: entries}, nil
}

// loadStreamMetaAt returns the current StreamMeta for key, or (_, false, nil)
// when the meta key does not exist.
func (r *RedisServer) loadStreamMetaAt(ctx context.Context, key []byte, readTS uint64) (store.StreamMeta, bool, error) {
	raw, err := r.store.GetAt(ctx, store.StreamMetaKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.StreamMeta{}, false, nil
		}
		return store.StreamMeta{}, false, errors.WithStack(err)
	}
	meta, err := store.UnmarshalStreamMeta(raw)
	if err != nil {
		return store.StreamMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

// scanStreamEntriesAt returns all entries for key in ascending ID order.
// This path exists to reconstruct the full stream for callers — the Lua
// stream bridge (streamState) and the legacy compatibility surface — that
// previously loaded the entire stream as a single blob.
//
// User-bounded scans (XREAD/XRANGE/XREVRANGE) use
// scanStreamEntriesAfter / rangeStreamNewLayout. For the
// materialise-everything path, expectedLen <= 0 represents an empty or
// uninitialized stream (meta.Length == 0) and intentionally yields an
// empty slice — this is the correct state for a newly-created or empty
// stream; callers need not distinguish it from a missing stream.
// When expectedLen > 0 we cap the scan at meta.Length plus slack,
// matching existing store ScanAt semantics for non-positive limits.
func (r *RedisServer) scanStreamEntriesAt(ctx context.Context, key []byte, readTS uint64, expectedLen int64) ([]redisStreamEntry, error) {
	prefix := store.StreamEntryScanPrefix(key)
	end := store.PrefixScanEnd(prefix)
	limit := scanStreamEntriesLimit(expectedLen)
	if limit == 0 && expectedLen <= 0 {
		return []redisStreamEntry{}, nil
	}
	kvs, err := r.store.ScanAt(ctx, prefix, end, limit, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	entries := make([]redisStreamEntry, 0, len(kvs))
	for _, pair := range kvs {
		entry, err := unmarshalStreamEntry(pair.Value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// scanStreamEntriesLimit derives the ScanAt limit for scanStreamEntriesAt.
// Arithmetic is performed in int64 and the result is clamped to math.MaxInt
// before narrowing; this keeps the helper correct on 32-bit targets and
// when expectedLen is corrupted into a value that would otherwise overflow
// int on addition with the slack. A negative or zero expectedLen falls
// through to the ScanAt "limit==0 means no limit" convention.
func scanStreamEntriesLimit(expectedLen int64) int {
	const concurrentWriteSlack = int64(64)
	if expectedLen <= 0 {
		return 0
	}
	want := expectedLen + concurrentWriteSlack
	// Overflow guard: expectedLen is a corrupted meta away from anything;
	// if the sum wraps, fall back to "no limit" (ScanAt stores its own
	// hard caps downstream) rather than pass a negative value.
	if want < expectedLen {
		return 0
	}
	if want > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(want)
}

func (r *RedisServer) dispatchElems(ctx context.Context, isTxn bool, startTS uint64, elems []*kv.Elem[kv.OP]) error {
	if len(elems) == 0 {
		return nil
	}
	// Guard against the MaxUint64 sentinel returned by snapshotTS when no
	// writes have been committed yet.  The coordinator cannot create a
	// commitTS larger than MaxUint64, so let it assign its own startTS.
	if startTS == ^uint64(0) {
		startTS = 0
	}
	_, err := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:   isTxn,
		StartTS: startTS,
		Elems:   elems,
	})
	return errors.WithStack(err)
}

// readRedisStringAt reads a Redis string value, trying the prefixed key first
// and falling back to the bare key for legacy data written before the
// !redis|str| prefix migration. Returns the decoded user value and the
// embedded (or legacy-index) TTL.
//
// To minimise read amplification the function bypasses readValueAt/ttlAt so it
// does not re-enter the hasExpiredTTLAt → ttlAt lookup chain: the new-format
// path does a single GetAt on the prefixed key (value + TTL in one read);
// the legacy path does at most two extra reads (TTL index, then bare key).
// Expiration is checked locally from the TTL we just decoded.
func (r *RedisServer) readRedisStringAt(key []byte, readTS uint64) ([]byte, *time.Time, error) {
	return r.readRedisStringWith(key, readTS, r.leaderAwareGetAt)
}

// readRedisStringAtSnapshot reads a string without re-verifying leadership on
// every sub-call. The caller must have already called coordinator.VerifyLeader()
// once before invoking this (e.g. at Lua script startTS acquisition time).
func (r *RedisServer) readRedisStringAtSnapshot(key []byte, readTS uint64) ([]byte, *time.Time, error) {
	return r.readRedisStringWith(key, readTS, r.snapshotGetAt)
}

func (r *RedisServer) readRedisStringWith(key []byte, readTS uint64, get rawGetFn) ([]byte, *time.Time, error) {
	raw, err := get(redisStrKey(key), readTS)
	if err == nil {
		return r.decodePrefixedStringWith(key, raw, readTS, get)
	}
	if !errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil, err
	}
	return r.readBareLegacyStringWith(key, readTS, get)
}

// decodePrefixedString handles the !redis|str|<key> payload: new-format values
// carry their TTL inline, while legacy-format payloads that still sit under
// the prefixed key during rolling upgrade must consult the secondary index.
func (r *RedisServer) decodePrefixedStringWith(key, raw []byte, readTS uint64, get rawGetFn) ([]byte, *time.Time, error) {
	userValue, ttl, err := decodeRedisStr(raw)
	if err != nil {
		return nil, nil, err
	}
	if !isNewRedisStrFormat(raw) {
		legacyTTL, ttlErr := r.readLegacyTTLWith(key, readTS, get)
		if ttlErr != nil {
			return nil, nil, ttlErr
		}
		ttl = legacyTTL
	}
	if ttl != nil && !ttl.After(time.Now()) {
		return nil, nil, errors.WithStack(store.ErrKeyNotFound)
	}
	return userValue, ttl, nil
}

// readBareLegacyStringWith handles pre-migration data still under the bare user
// key: TTL in the secondary index, value at the bare key itself.
func (r *RedisServer) readBareLegacyStringWith(key []byte, readTS uint64, get rawGetFn) ([]byte, *time.Time, error) {
	legacyTTL, err := r.readLegacyTTLWith(key, readTS, get)
	if err != nil {
		return nil, nil, err
	}
	if legacyTTL != nil && !legacyTTL.After(time.Now()) {
		return nil, nil, errors.WithStack(store.ErrKeyNotFound)
	}
	legacy, err := get(key, readTS)
	if err != nil {
		return nil, nil, err
	}
	return legacy, legacyTTL, nil
}

// readLegacyTTLWith fetches the pre-migration !redis|ttl| entry, returning nil
// when no index is present.
func (r *RedisServer) readLegacyTTLWith(key []byte, readTS uint64, get rawGetFn) (*time.Time, error) {
	raw, err := get(redisTTLKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	ttl, err := decodeRedisTTL(raw)
	if err != nil {
		return nil, err
	}
	return &ttl, nil
}

// rawGetFn is the signature shared by leaderAwareGetAt and snapshotGetAt so
// that the string-read helpers can be parameterised without duplication.
type rawGetFn func(key []byte, readTS uint64) ([]byte, error)

// leaderAwareGetAt is a GetAt that honors the per-key leader routing readValueAt
// uses, but without calling back into hasExpiredTTLAt. Callers are responsible
// for handling expiration themselves using the TTL they just read.
func (r *RedisServer) leaderAwareGetAt(key []byte, readTS uint64) ([]byte, error) {
	return r.doGetAt(key, readTS, true)
}

// snapshotGetAt reads at readTS without re-verifying leadership on every call.
// The caller must have already called coordinator.VerifyLeader() once (e.g. at
// Lua script startTS acquisition time) before using this method.
func (r *RedisServer) snapshotGetAt(key []byte, readTS uint64) ([]byte, error) {
	return r.doGetAt(key, readTS, false)
}

func (r *RedisServer) doGetAt(key []byte, readTS uint64, verify bool) ([]byte, error) {
	// Leadership is partitioned by the logical user key, so strip the internal
	// prefix before asking the coordinator.
	routingKey := key
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		routingKey = userKey
	}
	if r.coordinator.IsLeaderForKey(routingKey) {
		if verify {
			if err := r.coordinator.VerifyLeaderForKey(routingKey); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		v, err := r.store.GetAt(context.Background(), key, readTS)
		return v, errors.WithStack(err)
	}
	return r.tryLeaderGetAt(key, readTS)
}

func (r *RedisServer) saveString(ctx context.Context, key []byte, value []byte, ttl *time.Time) error {
	encoded := encodeRedisStr(value, ttl)
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisStrKey(key), Value: encoded},
	}
	// Write !redis|ttl| as a secondary scan index for background expiration (if TTL set).
	// Otherwise clear any pre-existing index so a persistent string is not later expired.
	if ttl != nil {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(*ttl)})
	} else {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(key)})
	}
	return r.dispatchElems(ctx, false, 0, elems)
}

// deleteListElems returns delete operations for all list keys: item keys, the base
// meta key, all delta keys, and all claim keys.
// It does not call resolveListMeta so that DEL succeeds even when a list has
// more than MaxDeltaScanLimit uncompacted deltas (resolveListMeta would return
// ErrDeltaScanTruncated in that case). The constituent key scans return empty
// slices when no list exists, so the tombstone written for the meta key is the
// only overhead when called on a non-existent key.
func (r *RedisServer) deleteListElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	elems, err := r.scanListItemDelElems(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	// Always delete the base meta key (no-op tombstone if it doesn't exist).
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(key)})
	// Delete all delta keys (paginated).
	deltaElems, err := r.scanAllDeltaElems(ctx, store.ListMetaDeltaScanPrefix(key), readTS)
	if err != nil {
		return nil, err
	}
	elems = append(elems, deltaElems...)
	// Delete all claim keys (paginated).
	claimElems, err := r.scanAllDeltaElems(ctx, store.ListClaimScanPrefix(key), readTS)
	if err != nil {
		return nil, err
	}
	return append(elems, claimElems...), nil
}

// scanListItemDelElems returns Del elems for every item key that belongs to
// the given list userKey. It uses a paginated prefix scan instead of
// enumerating positions in [Head, Tail) so that sparse lists with large
// sequence gaps do not cause O(range) iterations.
func (r *RedisServer) scanListItemDelElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	const cursorAdv = byte(0x00)
	itemPrefix := append(append([]byte(nil), []byte(store.ListItemPrefix)...), key...)
	itemEnd := store.PrefixScanEnd(itemPrefix)
	var elems []*kv.Elem[kv.OP]
	cursor := itemPrefix
	for {
		itemKVs, scanErr := r.store.ScanAt(ctx, cursor, itemEnd, store.MaxDeltaScanLimit, readTS)
		if scanErr != nil {
			return nil, errors.WithStack(scanErr)
		}
		for _, pair := range itemKVs {
			// Guard against prefix collision with lexicographically adjacent userKeys.
			if _, ok := store.ExtractListItemSeq(pair.Key, key); !ok {
				continue
			}
			if len(elems)+1 > maxWideColumnItems {
				return nil, errors.Wrapf(ErrCollectionTooLarge, "list %q exceeds %d items", key, maxWideColumnItems)
			}
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
		}
		if len(itemKVs) < store.MaxDeltaScanLimit {
			break
		}
		cursor = append(bytes.Clone(itemKVs[len(itemKVs)-1].Key), cursorAdv)
	}
	return elems, nil
}

// scanAllDeltaElems scans all delta keys under deltaPrefix and returns Del
// elems for each. It paginates internally so callers are not limited to
// MaxDeltaScanLimit entries. Total results are capped at maxWideColumnItems
// to prevent unbounded memory growth if the compactor falls behind.
func (r *RedisServer) scanAllDeltaElems(ctx context.Context, deltaPrefix []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	const cursorAdv = byte(0x00) // appended to advance past the last scanned key
	var elems []*kv.Elem[kv.OP]
	deltaEnd := store.PrefixScanEnd(deltaPrefix)
	deltaCursor := deltaPrefix
	for {
		deltaKVs, scanErr := r.store.ScanAt(ctx, deltaCursor, deltaEnd, store.MaxDeltaScanLimit, readTS)
		if scanErr != nil {
			return nil, errors.WithStack(scanErr)
		}
		// Check before appending so len(elems) never exceeds maxWideColumnItems
		// by more than one scan page.
		if len(elems)+len(deltaKVs) > maxWideColumnItems {
			return nil, errors.Wrapf(ErrCollectionTooLarge, "delta key count exceeds %d", maxWideColumnItems)
		}
		for _, pair := range deltaKVs {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
		}
		if len(deltaKVs) < store.MaxDeltaScanLimit {
			break
		}
		deltaCursor = append(bytes.Clone(deltaKVs[len(deltaKVs)-1].Key), cursorAdv)
	}
	return elems, nil
}

// deleteWideColumnElems returns delete operations for all wide-column field/member keys,
// the base meta key, and all delta keys for a collection identified by the given scan prefix,
// meta key, and delta prefix.
func (r *RedisServer) deleteWideColumnElems(ctx context.Context, readTS uint64, fieldPrefix, metaKey, deltaPrefix []byte) ([]*kv.Elem[kv.OP], error) {
	const cursorAdv = byte(0x00)
	fieldEnd := store.PrefixScanEnd(fieldPrefix)
	var elems []*kv.Elem[kv.OP]
	cursor := fieldPrefix
	for {
		fieldKVs, err := r.store.ScanAt(ctx, cursor, fieldEnd, store.MaxDeltaScanLimit, readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// Check before appending so len(elems) never exceeds maxWideColumnItems
		// by more than one scan page.
		if len(elems)+len(fieldKVs) > maxWideColumnItems {
			return nil, errors.Wrapf(ErrCollectionTooLarge, "field key count exceeds %d", maxWideColumnItems)
		}
		for _, pair := range fieldKVs {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
		}
		if len(fieldKVs) < store.MaxDeltaScanLimit {
			break
		}
		cursor = append(bytes.Clone(fieldKVs[len(fieldKVs)-1].Key), cursorAdv)
	}
	// Always delete the metadata key and all delta keys regardless of whether
	// field keys were found. A collection may have a metadata key (or uncompacted
	// delta keys) with no field keys if all fields were individually deleted but
	// compaction has not yet run. Omitting this would leak the metadata key.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: metaKey})
	deltaElems, err := r.scanAllDeltaElems(ctx, deltaPrefix, readTS)
	if err != nil {
		return nil, err
	}
	return append(elems, deltaElems...), nil
}

func (r *RedisServer) deleteLogicalKeyElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], bool, error) {
	existed, err := r.logicalExistsAt(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}

	elems := []*kv.Elem[kv.OP]{}

	for _, internalKey := range [][]byte{
		redisStrKey(key),
		key, // legacy bare string key
		redisHashKey(key),
		redisSetKey(key),
		redisHLLKey(key),
		redisZSetKey(key),
		redisStreamKey(key),
		redisTTLKey(key),
	} {
		ok, err := r.store.ExistsAt(ctx, internalKey, readTS)
		if err != nil {
			return nil, false, errors.WithStack(err)
		}
		if ok {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: internalKey})
		}
	}

	listElems, err := r.deleteListElems(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	elems = append(elems, listElems...)

	// Wide-column hash cleanup: delete all !hs|fld| keys, meta, and delta keys.
	hashElems, err := r.deleteWideColumnElems(ctx, readTS,
		store.HashFieldScanPrefix(key), store.HashMetaKey(key), store.HashMetaDeltaScanPrefix(key))
	if err != nil {
		return nil, false, err
	}
	elems = append(elems, hashElems...)

	// Wide-column set cleanup: delete all !st|mem| keys, meta, and delta keys.
	setElems, err := r.deleteWideColumnElems(ctx, readTS,
		store.SetMemberScanPrefix(key), store.SetMetaKey(key), store.SetMetaDeltaScanPrefix(key))
	if err != nil {
		return nil, false, err
	}
	elems = append(elems, setElems...)

	// Wide-column zset cleanup: delete all !zs|mem|, !zs|scr|, meta, and delta keys.
	zsetElems, err := r.deleteZSetWideColumnElems(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	elems = append(elems, zsetElems...)

	// Wide-column stream cleanup: delete the meta key and every entry key.
	streamElems, err := r.deleteStreamWideColumnElems(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	elems = append(elems, streamElems...)

	return elems, existed, nil
}

// deleteStreamWideColumnElems returns delete operations for all stream
// wide-column keys: the meta key (if it exists) and every entry under the
// entry scan prefix. Total results are capped at maxWideColumnItems to
// prevent unbounded memory growth; DEL/EXPIRE 0/MULTI-EXEC DEL on a stream
// that exceeds the cap returns ErrCollectionTooLarge, consistent with other
// wide-column types (Hash, Set, ZSet). Streams are also capped at
// maxWideColumnItems via xaddEnforceMaxWideColumn in XADD, so a stream that
// migrated from a legacy blob larger than the cap will require a XTRIM before
// it can be deleted.
func (r *RedisServer) deleteStreamWideColumnElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	var elems []*kv.Elem[kv.OP]
	// Delete any legacy single-blob remnant in the same commit so DEL
	// leaves no stale data on disk even when the stream was never
	// migrated. ExistsAt is cheap; the Del is a no-op on the storage
	// side when the key is already absent.
	legacyCleanup, err := r.legacyStreamCleanupElems(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	elems = append(elems, legacyCleanup...)
	metaKey := store.StreamMetaKey(key)
	if exists, err := r.store.ExistsAt(ctx, metaKey, readTS); err != nil {
		return nil, errors.WithStack(err)
	} else if exists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: metaKey})
	}
	entryElems, err := r.scanAllDeltaElems(ctx, store.StreamEntryScanPrefix(key), readTS)
	if err != nil {
		return nil, err
	}
	return append(elems, entryElems...), nil
}

// deleteZSetWideColumnElems returns delete operations for all ZSet wide-column keys:
// member keys (!zs|mem|), score index keys (!zs|scr|), the meta key, and all delta keys.
func (r *RedisServer) deleteZSetWideColumnElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	memberElems, err := r.deleteWideColumnElems(ctx, readTS,
		store.ZSetMemberScanPrefix(key), store.ZSetMetaKey(key), store.ZSetMetaDeltaScanPrefix(key))
	if err != nil {
		return nil, err
	}
	// deleteWideColumnElems covers member + meta + delta. Also scan score index keys (paginated).
	scoreElems, err := r.scanAllDeltaElems(ctx, store.ZSetScoreScanPrefix(key), readTS)
	if err != nil {
		return nil, err
	}
	return append(memberElems, scoreElems...), nil
}

func (r *RedisServer) listValuesAt(ctx context.Context, key []byte, readTS uint64) ([]string, error) {
	meta, exists, err := r.resolveListMeta(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if !exists || meta.Len == 0 {
		return []string{}, nil
	}
	return r.fetchListRange(ctx, key, meta, 0, meta.Len-1, readTS)
}

func (r *RedisServer) rewriteListTxn(ctx context.Context, key []byte, readTS uint64, values []string) error {
	elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
	if err != nil {
		return err
	}

	if len(values) == 0 {
		return r.dispatchElems(ctx, true, readTS, elems)
	}

	rawValues := make([][]byte, 0, len(values))
	for _, value := range values {
		rawValues = append(rawValues, []byte(value))
	}
	commitTS := r.coordinator.Clock().Next()
	ops, _, err := r.buildRPushOps(store.ListMeta{}, key, rawValues, commitTS, 0)
	if err != nil {
		return err
	}
	elems = append(elems, ops...)
	if readTS == ^uint64(0) {
		readTS = 0
	}
	_, err = r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		CommitTS: commitTS,
		Elems:    elems,
	})
	return errors.WithStack(err)
}

func (r *RedisServer) visibleKeys(pattern []byte) ([][]byte, error) {
	keys, err := r.localKeys(pattern)
	if err != nil {
		return nil, err
	}
	visible := make([][]byte, 0, len(keys))
	readTS := r.readTS()
	for _, key := range keys {
		ok, err := r.logicalExistsAt(context.Background(), key, readTS)
		if err != nil {
			return nil, err
		}
		if ok {
			visible = append(visible, key)
		}
	}
	sort.Slice(visible, func(i, j int) bool {
		return string(visible[i]) < string(visible[j])
	})
	return visible, nil
}

func normalizeRankRange(start, end, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if length == 0 || end < start || start >= length {
		return 0, -1
	}
	return start, end
}

func normalizeIndex(index, length int) int {
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return -1
	}
	return index
}

func minRedisInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// aggregateLenDeltas scans delta keys under prefix and sums the LenDelta values
// via unmarshalDelta. Returns (sum, hasDeltas, error).
// ErrDeltaScanTruncated is returned when the scan hits MaxDeltaScanLimit.
func (r *RedisServer) aggregateLenDeltas(ctx context.Context, prefix []byte, readTS uint64, unmarshalDelta func([]byte) (int64, error)) (int64, bool, error) {
	end := store.PrefixScanEnd(prefix)
	// Scan one extra key beyond the limit so we can distinguish "exactly
	// MaxDeltaScanLimit results" (no truncation) from "more than MaxDeltaScanLimit
	// results" (truncated). Without the +1, a collection with exactly
	// MaxDeltaScanLimit deltas would incorrectly trigger ErrDeltaScanTruncated.
	deltas, err := r.store.ScanAt(ctx, prefix, end, store.MaxDeltaScanLimit+1, readTS)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	if len(deltas) > store.MaxDeltaScanLimit {
		return 0, false, ErrDeltaScanTruncated
	}
	var sum int64
	for _, d := range deltas {
		delta, err := unmarshalDelta(d.Value)
		if err != nil {
			return 0, false, errors.WithStack(err)
		}
		sum += delta
	}
	return sum, len(deltas) > 0, nil
}

// resolveListMeta aggregates the base list metadata with all uncompacted Delta keys
// visible at readTS. Returns ErrDeltaScanTruncated if > MaxDeltaScanLimit deltas exist.
func (r *RedisServer) resolveListMeta(ctx context.Context, key []byte, readTS uint64) (store.ListMeta, bool, error) {
	// 1. Read base metadata.
	baseMeta, exists, err := r.loadListMetaAt(ctx, key, readTS)
	if err != nil {
		return store.ListMeta{}, false, err
	}

	// 2. Scan and aggregate delta keys.
	// The closure also captures baseMeta to accumulate the list-specific HeadDelta.
	prefix := store.ListMetaDeltaScanPrefix(key)
	lenSum, hasDeltas, err := r.aggregateLenDeltas(ctx, prefix, readTS, func(b []byte) (int64, error) {
		d, unmarshalErr := store.UnmarshalListMetaDelta(b)
		baseMeta.Head += d.HeadDelta
		return d.LenDelta, errors.WithStack(unmarshalErr)
	})
	if err != nil {
		if errors.Is(err, ErrDeltaScanTruncated) {
			r.triggerUrgentCompaction("list", key)
		}
		return store.ListMeta{}, false, err
	}
	baseMeta.Len += lenSum

	if baseMeta.Len < 0 {
		slog.Warn("resolveListMeta: clamping negative Len to 0", "key", string(key), "len", baseMeta.Len)
		baseMeta.Len = 0
	}
	baseMeta.Tail = baseMeta.Head + baseMeta.Len
	return baseMeta, exists || hasDeltas, nil
}

// resolveCollectionLen reads the base meta key, then aggregates all delta keys
// into the final length. It is used by resolveHashMeta, resolveSetMeta, and
// resolveZSetMeta which all follow the same pattern.
func (r *RedisServer) resolveCollectionLen(
	ctx context.Context,
	key []byte,
	readTS uint64,
	metaKey []byte,
	deltaPrefix []byte,
	unmarshalBase func([]byte) (int64, error),
	unmarshalDelta func([]byte) (int64, error),
	clampMsg string,
) (int64, bool, error) {
	raw, err := r.store.GetAt(ctx, metaKey, readTS)
	var baseLen int64
	exists := true
	if err != nil {
		if !errors.Is(err, store.ErrKeyNotFound) {
			return 0, false, errors.WithStack(err)
		}
		exists = false
	} else {
		baseLen, err = unmarshalBase(raw)
		if err != nil {
			return 0, false, errors.WithStack(err)
		}
	}

	deltaSum, hasDeltas, err := r.aggregateLenDeltas(ctx, deltaPrefix, readTS, unmarshalDelta)
	if err != nil {
		return 0, false, err
	}

	length := baseLen + deltaSum
	if length < 0 {
		slog.Warn(clampMsg, "key", string(key), "len", length)
		length = 0
	}
	return length, exists || hasDeltas, nil
}

// resolveHashMeta aggregates the base hash metadata with all uncompacted Delta keys.
func (r *RedisServer) resolveHashMeta(ctx context.Context, key []byte, readTS uint64) (int64, bool, error) {
	n, exists, err := r.resolveCollectionLen(
		ctx, key, readTS,
		store.HashMetaKey(key),
		store.HashMetaDeltaScanPrefix(key),
		func(b []byte) (int64, error) {
			m, err := store.UnmarshalHashMeta(b)
			return m.Len, errors.WithStack(err)
		},
		func(b []byte) (int64, error) {
			d, err := store.UnmarshalHashMetaDelta(b)
			return d.LenDelta, errors.WithStack(err)
		},
		"resolveHashMeta: clamping negative Len to 0",
	)
	if errors.Is(err, ErrDeltaScanTruncated) {
		r.triggerUrgentCompaction("hash", key)
	}
	return n, exists, err
}

// resolveSetMeta aggregates the base set metadata with all uncompacted Delta keys.
func (r *RedisServer) resolveSetMeta(ctx context.Context, key []byte, readTS uint64) (int64, bool, error) {
	n, exists, err := r.resolveCollectionLen(
		ctx, key, readTS,
		store.SetMetaKey(key),
		store.SetMetaDeltaScanPrefix(key),
		func(b []byte) (int64, error) {
			m, err := store.UnmarshalSetMeta(b)
			return m.Len, errors.WithStack(err)
		},
		func(b []byte) (int64, error) {
			d, err := store.UnmarshalSetMetaDelta(b)
			return d.LenDelta, errors.WithStack(err)
		},
		"resolveSetMeta: clamping negative Len to 0",
	)
	if errors.Is(err, ErrDeltaScanTruncated) {
		r.triggerUrgentCompaction("set", key)
	}
	return n, exists, err
}

// resolveZSetMeta aggregates the base sorted set metadata with all uncompacted Delta keys.
func (r *RedisServer) resolveZSetMeta(ctx context.Context, key []byte, readTS uint64) (int64, bool, error) {
	n, exists, err := r.resolveCollectionLen(
		ctx, key, readTS,
		store.ZSetMetaKey(key),
		store.ZSetMetaDeltaScanPrefix(key),
		func(b []byte) (int64, error) {
			m, err := store.UnmarshalZSetMeta(b)
			return m.Len, errors.WithStack(err)
		},
		func(b []byte) (int64, error) {
			d, err := store.UnmarshalZSetMetaDelta(b)
			return d.LenDelta, errors.WithStack(err)
		},
		"resolveZSetMeta: clamping negative Len to 0",
	)
	if errors.Is(err, ErrDeltaScanTruncated) {
		r.triggerUrgentCompaction("zset", key)
	}
	return n, exists, err
}

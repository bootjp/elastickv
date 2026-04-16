package adapter

import (
	"bytes"
	"context"
	"log/slog"
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
	if found, err := r.wideColumnTypeExists(ctx, key, readTS, store.ZSetMemberScanPrefix, store.ZSetMetaKey, store.ZSetMetaDeltaScanPrefix); err != nil {
		return redisTypeNone, err
	} else if found {
		return redisTypeZSet, nil
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

func (r *RedisServer) rawKeyTypeAt(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	// Check list base metadata key first.
	listMetaExists, err := r.store.ExistsAt(ctx, store.ListMetaKey(key), readTS)
	if err != nil {
		return redisTypeNone, errors.WithStack(err)
	}
	if listMetaExists {
		return redisTypeList, nil
	}
	// Fallback: detect a delta-only list (base meta not yet written or
	// already compacted away but deltas still present).
	deltaPrefix := store.ListMetaDeltaScanPrefix(key)
	deltaEnd := store.PrefixScanEnd(deltaPrefix)
	deltaKVs, err := r.store.ScanAt(ctx, deltaPrefix, deltaEnd, 1, readTS)
	if err != nil {
		return redisTypeNone, errors.WithStack(err)
	}
	if len(deltaKVs) > 0 {
		return redisTypeList, nil
	}

	// Check wide-column hash and set types.
	if typ, wideErr := r.detectWideColumnType(ctx, key, readTS); wideErr != nil || typ != redisTypeNone {
		return typ, wideErr
	}

	checks := []struct {
		typ redisValueType
		key []byte
	}{
		{typ: redisTypeHash, key: redisHashKey(key)},
		{typ: redisTypeSet, key: redisSetKey(key)},
		{typ: redisTypeZSet, key: redisZSetKey(key)},
		{typ: redisTypeStream, key: redisStreamKey(key)},
		// HyperLogLog is a Redis string subtype. Treat it as "string" for TYPE.
		{typ: redisTypeString, key: redisHLLKey(key)},
		{typ: redisTypeString, key: redisStrKey(key)},
		// Fallback: check bare key for legacy data written before the
		// !redis|str| prefix migration.
		{typ: redisTypeString, key: key},
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
	if err != nil || typ == redisTypeNone {
		return typ, err
	}
	expired, err := r.hasExpiredTTLAt(ctx, key, readTS)
	if err != nil {
		return redisTypeNone, err
	}
	if expired {
		return redisTypeNone, nil
	}
	return typ, nil
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

func (r *RedisServer) loadStreamAt(ctx context.Context, key []byte, readTS uint64) (redisStreamValue, error) {
	raw, err := r.store.GetAt(ctx, redisStreamKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return redisStreamValue{}, nil
		}
		return redisStreamValue{}, errors.WithStack(err)
	}
	val, err := unmarshalStreamValue(raw)
	return val, err
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
// !redis|str| prefix migration.
func (r *RedisServer) readRedisStringAt(key []byte, readTS uint64) ([]byte, error) {
	v, err := r.readValueAt(redisStrKey(key), readTS)
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, store.ErrKeyNotFound) {
		return nil, err
	}
	return r.readValueAt(key, readTS)
}

func (r *RedisServer) saveString(ctx context.Context, key []byte, value []byte, ttl *time.Time) error {
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisStrKey(key), Value: bytes.Clone(value)},
	}
	if ttl == nil {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(key)})
	} else {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(*ttl)})
	}
	return r.dispatchElems(ctx, false, 0, elems)
}

// deleteListElems returns delete operations for all list keys: item keys, the base
// meta key, all delta keys, and all claim keys.
func (r *RedisServer) deleteListElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	_, listExists, err := r.resolveListMeta(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if !listExists {
		return nil, nil
	}
	elems, err := r.scanListItemDelElems(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(key)})
	// Delete all delta keys (paginated).
	deltaElems, err := r.scanAllDeltaElems(ctx, store.ListMetaDeltaScanPrefix(key), readTS)
	if err != nil {
		return nil, err
	}
	elems = append(elems, deltaElems...)
	// Delete all claim keys (paginated).
	claimPrefix := store.ListClaimScanPrefix(key)
	claimEnd := store.PrefixScanEnd(claimPrefix)
	claimKVs, claimScanErr := r.store.ScanAt(ctx, claimPrefix, claimEnd, maxWideScanLimit, readTS)
	if claimScanErr != nil {
		return nil, errors.WithStack(claimScanErr)
	}
	if len(claimKVs) > maxWideColumnItems {
		return nil, errors.Wrapf(ErrCollectionTooLarge, "list %q has more than %d claim keys", key, maxWideColumnItems)
	}
	for _, pair := range claimKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
	}
	return elems, nil
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
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
		}
		if len(elems) > maxWideColumnItems {
			return nil, errors.Wrapf(ErrCollectionTooLarge, "list %q exceeds %d items", key, maxWideColumnItems)
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
		for _, pair := range deltaKVs {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
		}
		if len(elems) > maxWideColumnItems {
			return nil, errors.Wrapf(ErrCollectionTooLarge, "delta key count exceeds %d", maxWideColumnItems)
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
		for _, pair := range fieldKVs {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
		}
		if len(elems) > maxWideColumnItems {
			return nil, errors.Wrapf(ErrCollectionTooLarge, "field key count exceeds %d", maxWideColumnItems)
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

	return elems, existed, nil
}

// deleteZSetWideColumnElems returns delete operations for all ZSet wide-column keys:
// member keys (!zs|mem|), score index keys (!zs|scr|), the meta key, and all delta keys.
func (r *RedisServer) deleteZSetWideColumnElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	memberElems, err := r.deleteWideColumnElems(ctx, readTS,
		store.ZSetMemberScanPrefix(key), store.ZSetMetaKey(key), store.ZSetMetaDeltaScanPrefix(key))
	if err != nil {
		return nil, err
	}
	// deleteWideColumnElems covers member + meta + delta. Also scan score index keys.
	scorePrefix := store.ZSetScoreScanPrefix(key)
	scoreEnd := store.PrefixScanEnd(scorePrefix)
	scoreKVs, scanErr := r.store.ScanAt(ctx, scorePrefix, scoreEnd, maxWideScanLimit, readTS)
	if scanErr != nil {
		return nil, errors.WithStack(scanErr)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(memberElems)+len(scoreKVs))
	elems = append(elems, memberElems...)
	for _, pair := range scoreKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
	}
	return elems, nil
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
	deltas, err := r.store.ScanAt(ctx, prefix, end, store.MaxDeltaScanLimit, readTS)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	if len(deltas) == store.MaxDeltaScanLimit {
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
	return r.resolveCollectionLen(
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
}

// resolveSetMeta aggregates the base set metadata with all uncompacted Delta keys.
func (r *RedisServer) resolveSetMeta(ctx context.Context, key []byte, readTS uint64) (int64, bool, error) {
	return r.resolveCollectionLen(
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
}

// resolveZSetMeta aggregates the base sorted set metadata with all uncompacted Delta keys.
func (r *RedisServer) resolveZSetMeta(ctx context.Context, key []byte, readTS uint64) (int64, bool, error) {
	return r.resolveCollectionLen(
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
}

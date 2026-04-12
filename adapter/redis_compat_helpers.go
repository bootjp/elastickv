package adapter

import (
	"bytes"
	"context"
	"math"
	"sort"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const wrongTypeMessage = "WRONGTYPE Operation against a key holding the wrong kind of value"

func wrongTypeError() error {
	return errors.New(wrongTypeMessage)
}

func (r *RedisServer) rawKeyTypeAt(ctx context.Context, key []byte, readTS uint64) (redisValueType, error) {
	checks := []struct {
		typ redisValueType
		key []byte
	}{
		{typ: redisTypeList, key: store.ListMetaKey(key)},
		{typ: redisTypeHash, key: redisHashKey(key)},
		{typ: redisTypeSet, key: redisSetKey(key)},
		{typ: redisTypeZSet, key: store.ZSetMetaKey(key)},
		{typ: redisTypeZSet, key: redisZSetKey(key)}, // legacy blob fallback
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

func (r *RedisServer) loadHashAt(ctx context.Context, key []byte, readTS uint64) (redisHashValue, error) {
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

func (r *RedisServer) loadSetAt(ctx context.Context, kind string, key []byte, readTS uint64) (redisSetValue, error) {
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

func (r *RedisServer) loadZSetAt(ctx context.Context, key []byte, readTS uint64) (redisZSetValue, bool, error) {
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

// loadZSetMetaAt loads the ZSetMeta from the wide-column meta key.
// Returns (meta, true, nil) if found, (zero, false, nil) if not found.
func (r *RedisServer) loadZSetMetaAt(ctx context.Context, key []byte, readTS uint64) (store.ZSetMeta, bool, error) {
	raw, err := r.store.GetAt(ctx, store.ZSetMetaKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.ZSetMeta{}, false, nil
		}
		return store.ZSetMeta{}, false, errors.WithStack(err)
	}
	meta, err := store.UnmarshalZSetMeta(raw)
	if err != nil {
		return store.ZSetMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

// scanZSetScoreEntries scans the score index with a limit and returns entries
// in score order. The score index keys are naturally sorted by score then
// member, so no in-memory sort is needed.
func (r *RedisServer) scanZSetScoreEntries(ctx context.Context, key []byte, limit int, readTS uint64) ([]redisZSetEntry, error) {
	prefix := store.ZSetScoreScanPrefix(key)
	end := store.PrefixEnd(prefix)
	kvs, err := r.store.ScanAt(ctx, prefix, end, limit, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	entries := make([]redisZSetEntry, 0, len(kvs))
	ukLen := len(key)
	for _, kvp := range kvs {
		score, member := store.ExtractZSetScoreAndMember(kvp.Key, ukLen)
		if member == nil {
			continue
		}
		entries = append(entries, redisZSetEntry{Member: string(member), Score: score})
	}
	return entries, nil
}

// scanZSetAllMembers scans the score index for a ZSet and returns all entries
// in score order. The limit is derived from meta.Len to bound memory usage.
func (r *RedisServer) scanZSetAllMembers(ctx context.Context, key []byte, memberCount int64, readTS uint64) ([]redisZSetEntry, error) {
	return r.scanZSetScoreEntries(ctx, key, int(memberCount), readTS)
}

// zsetLoadResult holds the result of loading a ZSet's members.
type zsetLoadResult struct {
	members    map[string]float64
	exists     bool
	fromLegacy bool // true if data was loaded from the legacy blob format
}

// loadZSetMembersMap loads all members into a map for in-memory operations.
// It first checks the wide-column format, then falls back to legacy blob.
func (r *RedisServer) loadZSetMembersMap(ctx context.Context, key []byte, readTS uint64) (zsetLoadResult, error) {
	// Check wide-column meta first.
	meta, metaExists, err := r.loadZSetMetaAt(ctx, key, readTS)
	if err != nil {
		return zsetLoadResult{}, err
	}
	if metaExists {
		entries, err := r.scanZSetAllMembers(ctx, key, meta.Len, readTS)
		if err != nil {
			return zsetLoadResult{}, err
		}
		return zsetLoadResult{members: zsetEntriesToMap(entries), exists: true}, nil
	}
	// Fall back to legacy blob format.
	value, exists, err := r.loadZSetAt(ctx, key, readTS)
	if err != nil {
		return zsetLoadResult{}, err
	}
	if !exists {
		return zsetLoadResult{members: map[string]float64{}}, nil
	}
	return zsetLoadResult{members: zsetEntriesToMap(value.Entries), exists: true, fromLegacy: true}, nil
}

// deleteZSetWideColumnElems deletes all wide-column ZSet keys. Prefix
// deletions are dispatched as standalone operations (required by FSM), while
// the meta key delete is returned for the caller to batch with other elems.
// deleteZSetWideColumnElems generates delete operations for all wide-column
// ZSet keys (meta + members + score index). Individual keys are scanned and
// deleted to preserve atomicity with the caller's transaction — kv.DelPrefix
// cannot be mixed with other mutations in a single Raft request.
func (r *RedisServer) deleteZSetWideColumnElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	_, metaExists, err := r.loadZSetMetaAt(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if !metaExists {
		return nil, nil
	}

	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: store.ZSetMetaKey(key)},
	}

	// Scan the full prefix range to ensure all keys are deleted, including any
	// orphaned keys from partial writes or meta.Len inconsistencies.
	scanLimit := math.MaxInt

	// Scan member keys and score-index keys for individual deletion.
	for _, prefix := range [][]byte{
		store.ZSetMemberScanPrefix(key),
		store.ZSetScoreScanPrefix(key),
	} {
		kvs, err := r.store.ScanAt(ctx, prefix, store.PrefixEnd(prefix), scanLimit, readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		for _, kvp := range kvs {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(kvp.Key)})
		}
	}

	return elems, nil
}

// buildZSetRemoveEntryElems builds KV operations to remove specific entries
// from a wide-column ZSet and update (or delete) the meta key.
func buildZSetRemoveEntryElems(key []byte, entries []redisZSetEntry, currentLen int64) ([]*kv.Elem[kv.OP], error) {
	newLen := currentLen - int64(len(entries))
	elems := make([]*kv.Elem[kv.OP], 0, 1+len(entries)*2)
	for _, e := range entries {
		memberBytes := []byte(e.Member)
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMemberKey(key, memberBytes)},
			&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, e.Score, memberBytes)},
		)
	}
	if newLen <= 0 {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMetaKey(key)})
	} else {
		metaBytes, err := store.MarshalZSetMeta(store.ZSetMeta{Len: newLen})
		if err != nil {
			return nil, errors.WithStack(err)
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetMetaKey(key), Value: metaBytes})
	}
	return elems, nil
}

// buildZSetWriteElems builds the KV operations for writing a full ZSet
// from a members map. Generates meta + member + score-index keys.
func buildZSetWriteElems(key []byte, members map[string]float64) ([]*kv.Elem[kv.OP], error) {
	metaBytes, err := store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(members))})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	elems := make([]*kv.Elem[kv.OP], 0, 1+len(members)*2)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetMetaKey(key), Value: metaBytes})
	for member, score := range members {
		memberBytes := []byte(member)
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetMemberKey(key, memberBytes), Value: store.MarshalZSetScore(score)},
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, score, memberBytes), Value: nil},
		)
	}
	return elems, nil
}

// buildZSetDiffElems builds KV operations for an incremental update from
// origMembers to newMembers. Only the changed/added/removed members are
// written, plus a meta update.
func buildZSetDiffElems(key []byte, origMembers, newMembers map[string]float64) ([]*kv.Elem[kv.OP], error) {
	elems := make([]*kv.Elem[kv.OP], 0)

	// Remove members that are gone or have changed score.
	for member, oldScore := range origMembers {
		newScore, exists := newMembers[member]
		if !exists {
			memberBytes := []byte(member)
			elems = append(elems,
				&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMemberKey(key, memberBytes)},
				&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, memberBytes)},
			)
		} else if oldScore != newScore {
			// Score changed: delete old score index.
			elems = append(elems,
				&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))},
			)
		}
	}

	// Add or update members.
	for member, newScore := range newMembers {
		oldScore, existed := origMembers[member]
		if existed && oldScore == newScore {
			continue // unchanged
		}
		memberBytes := []byte(member)
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetMemberKey(key, memberBytes), Value: store.MarshalZSetScore(newScore)},
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, newScore, memberBytes), Value: nil},
		)
	}

	// Update meta with new length, or delete if empty.
	if len(newMembers) == 0 {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMetaKey(key)})
	} else if len(origMembers) != len(newMembers) {
		metaBytes, err := store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(newMembers))})
		if err != nil {
			return nil, errors.WithStack(err)
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetMetaKey(key), Value: metaBytes})
	}

	return elems, nil
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

func (r *RedisServer) deleteLogicalKeyElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], bool, error) {
	existed, err := r.logicalExistsAt(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}

	elems := []*kv.Elem[kv.OP]{}

	for _, k := range [][]byte{
		redisStrKey(key),
		key, // legacy bare string key
		redisHashKey(key),
		redisSetKey(key),
		redisHLLKey(key),
		redisZSetKey(key), // legacy blob
		redisStreamKey(key),
		redisTTLKey(key),
	} {
		ok, err := r.store.ExistsAt(ctx, k, readTS)
		if err != nil {
			return nil, false, errors.WithStack(err)
		}
		if ok {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: k})
		}
	}

	meta, listExists, err := r.loadListMetaAt(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	if listExists {
		for seq := meta.Head; seq < meta.Tail; seq++ {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listItemKey(key, seq)})
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(key)})
	}

	// Wide-column ZSet: delete meta + all member keys + all score-index keys.
	zsetElems, err := r.deleteZSetWideColumnElems(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	elems = append(elems, zsetElems...)

	return elems, existed, nil
}

func (r *RedisServer) listValuesAt(ctx context.Context, key []byte, readTS uint64) ([]string, error) {
	meta, exists, err := r.loadListMetaAt(ctx, key, readTS)
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
	ops, _, err := r.buildRPushOps(store.ListMeta{}, key, rawValues)
	if err != nil {
		return err
	}
	elems = append(elems, ops...)
	return r.dispatchElems(ctx, true, readTS, elems)
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

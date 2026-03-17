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
		{typ: redisTypeZSet, key: redisZSetKey(key)},
		{typ: redisTypeStream, key: redisStreamKey(key)},
		// HyperLogLog is a Redis string subtype. Treat it as "string" for TYPE.
		{typ: redisTypeString, key: redisHLLKey(key)},
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

func (r *RedisServer) saveString(ctx context.Context, key []byte, value []byte, ttl *time.Time) error {
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: key, Value: bytes.Clone(value)},
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

	stringExists, err := r.store.ExistsAt(ctx, key, readTS)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if stringExists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	}

	for _, internalKey := range [][]byte{
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

// flushAllKeyElems scans the entire store and generates delete operations for
// all Redis-visible keys in a single pass, avoiding the per-key existence
// checks that visibleKeys + deleteLogicalKeyElems would perform.
func (r *RedisServer) flushAllKeyElems(ctx context.Context, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	kvs, err := r.store.ScanAt(ctx, nil, nil, math.MaxInt, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(kvs))
	for _, kvPair := range kvs {
		// Skip transaction-internal keys.
		if bytes.HasPrefix(kvPair.Key, redisTxnKeyPrefix) {
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(kvPair.Key)})
	}
	return elems, nil
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

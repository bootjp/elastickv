package adapter

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	redisSimpleMetaLegacySizeBytes = 8
	redisSimpleMetaInlineSizeBytes = 16
	redisWideMetaLegacySizeBytes   = 24
	redisWideMetaInlineSizeBytes   = 32
)

func redisExpireAtMillis(expireAt time.Time) uint64 {
	ms := expireAt.UnixMilli()
	if ms < 0 {
		return 0
	}
	return uint64(ms) // #nosec G115 -- negative values are clamped above.
}

func redisTimeFromMillis(ms uint64) *time.Time {
	if ms == 0 {
		return nil
	}
	clamped := min(ms, uint64(math.MaxInt64))
	t := time.UnixMilli(int64(clamped)) // #nosec G115 -- clamped to MaxInt64.
	return &t
}

func (r *RedisServer) collectionTTLAt(ctx context.Context, userKey []byte, readTS uint64) (*time.Time, bool, error) {
	if ttl, found, err := r.listInlineTTLAt(ctx, userKey, readTS); err != nil || found {
		return ttl, found, err
	}
	if ttl, found, err := r.simpleInlineTTLAt(ctx, store.HashMetaKey(userKey), readTS, hashMetaExpireAt); err != nil || found {
		return ttl, found, err
	}
	if ttl, found, err := r.simpleInlineTTLAt(ctx, store.SetMetaKey(userKey), readTS, setMetaExpireAt); err != nil || found {
		return ttl, found, err
	}
	if ttl, found, err := r.simpleInlineTTLAt(ctx, store.ZSetMetaKey(userKey), readTS, zsetMetaExpireAt); err != nil || found {
		return ttl, found, err
	}
	return r.streamInlineTTLAt(ctx, userKey, readTS)
}

func (r *RedisServer) listInlineTTLAt(ctx context.Context, userKey []byte, readTS uint64) (*time.Time, bool, error) {
	raw, err := r.store.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	if len(raw) != redisWideMetaInlineSizeBytes {
		return nil, false, nil
	}
	meta, err := store.UnmarshalListMeta(raw)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	ttl := redisTimeFromMillis(meta.ExpireAt)
	return ttl, true, nil
}

func isStreamInlineMeta(raw []byte) bool {
	return len(raw) == redisWideMetaInlineSizeBytes || len(raw) == store.StreamMetaTrimBinarySize
}

func (r *RedisServer) simpleInlineTTLAt(
	ctx context.Context,
	metaKey []byte,
	readTS uint64,
	expireAtFromMeta func([]byte) (uint64, error),
) (*time.Time, bool, error) {
	raw, err := r.store.GetAt(ctx, metaKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	if len(raw) != redisSimpleMetaInlineSizeBytes {
		return nil, false, nil
	}
	expireAt, err := expireAtFromMeta(raw)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	ttl := redisTimeFromMillis(expireAt)
	return ttl, true, nil
}

func hashMetaExpireAt(raw []byte) (uint64, error) {
	meta, err := store.UnmarshalHashMeta(raw)
	return meta.ExpireAt, errors.WithStack(err)
}

func setMetaExpireAt(raw []byte) (uint64, error) {
	meta, err := store.UnmarshalSetMeta(raw)
	return meta.ExpireAt, errors.WithStack(err)
}

func zsetMetaExpireAt(raw []byte) (uint64, error) {
	meta, err := store.UnmarshalZSetMeta(raw)
	return meta.ExpireAt, errors.WithStack(err)
}

func (r *RedisServer) streamInlineTTLAt(ctx context.Context, userKey []byte, readTS uint64) (*time.Time, bool, error) {
	raw, err := r.store.GetAt(ctx, store.StreamMetaKey(userKey), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	if !isStreamInlineMeta(raw) {
		return nil, false, nil
	}
	meta, err := store.UnmarshalStreamMeta(raw)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	ttl := redisTimeFromMillis(meta.ExpireAt)
	return ttl, true, nil
}

func (r *RedisServer) dispatchCollectionExpire(
	ctx context.Context,
	key []byte,
	readTS uint64,
	typ redisValueType,
	expireAt time.Time,
) (bool, error) {
	ttlMs := redisExpireAtMillis(expireAt)
	elems, ok, err := r.collectionExpireElems(ctx, key, readTS, typ, ttlMs)
	if err != nil || !ok {
		return ok, err
	}
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(expireAt)})
	return true, r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) collectionExpireElems(
	ctx context.Context,
	key []byte,
	readTS uint64,
	typ redisValueType,
	expireAtMs uint64,
) ([]*kv.Elem[kv.OP], bool, error) {
	elems, ok, err := r.collectionMetaExpireElems(ctx, key, readTS, typ, expireAtMs)
	if err != nil || ok {
		return elems, ok, err
	}
	return r.legacyCollectionExpireFallback(ctx, key, readTS, typ)
}

func (r *RedisServer) expiredCollectionCleanupForRecreate(
	ctx context.Context,
	key []byte,
	readTS uint64,
	typ redisValueType,
	expected redisValueType,
) ([]*kv.Elem[kv.OP], bool, error) {
	if typ != redisTypeNone {
		return nil, false, nil
	}
	expired, err := r.hasExpired(ctx, key, readTS, true)
	if err != nil {
		return nil, false, err
	}
	if expired {
		staleDeltaOnly, err := r.expiredTTLIndexPrecedesDeltaOnlyCollection(ctx, key, expected, readTS)
		if err != nil {
			return nil, false, err
		}
		if staleDeltaOnly {
			elems, err := r.staleDeltaOnlyCollectionCleanupElems(ctx, key, readTS, expected)
			return elems, false, err
		}
		return r.deleteExpiredLogicalKeyForRecreate(ctx, key, readTS)
	}
	hllExpired, err := r.hllExpiredAt(ctx, key, readTS)
	if err != nil || !hllExpired {
		return nil, false, err
	}
	return r.deleteExpiredLogicalKeyForRecreate(ctx, key, readTS)
}

func (r *RedisServer) staleDeltaOnlyCollectionCleanupElems(ctx context.Context, key []byte, readTS uint64, expected redisValueType) ([]*kv.Elem[kv.OP], error) {
	elems := []*kv.Elem[kv.OP]{{Op: kv.Del, Key: redisTTLKey(key)}}
	if legacyKey, ok := legacyCollectionBlobKey(key, expected); ok {
		exists, err := r.store.ExistsAt(ctx, legacyKey, readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if exists {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: legacyKey})
		}
	}
	if expected != redisTypeSet {
		return elems, nil
	}
	hllExpired, err := r.hllExpiredAt(ctx, key, readTS)
	if err != nil || !hllExpired {
		return elems, err
	}
	return append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHLLKey(key)}), nil
}

func cleanupDeletesLegacyCollection(elems []*kv.Elem[kv.OP], key []byte, typ redisValueType) bool {
	legacyKey, ok := legacyCollectionBlobKey(key, typ)
	if !ok {
		return false
	}
	return opElemsDeleteKey(elems, legacyKey)
}

func opElemsDeleteKey(elems []*kv.Elem[kv.OP], want []byte) bool {
	for _, elem := range elems {
		if elem != nil && elem.Op == kv.Del && bytes.Equal(elem.Key, want) {
			return true
		}
	}
	return false
}

func (r *RedisServer) expiredTTLIndexPrecedesDeltaOnlyCollection(
	ctx context.Context,
	userKey []byte,
	typ redisValueType,
	readTS uint64,
) (bool, error) {
	return expiredTTLIndexPrecedesDeltaOnlyCollection(ctx, r.store, r, userKey, typ, readTS)
}

func (r *RedisServer) deleteExpiredLogicalKeyForRecreate(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], bool, error) {
	elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	return append(elems, redisTxnWideCollectionFenceElems(key)...), true, nil
}

func (r *RedisServer) hllExpiredAt(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	ttl, found, err := r.hllTTLAt(ctx, key, readTS)
	if err != nil || !found || ttl == nil {
		return false, err
	}
	return !ttl.After(time.Now()), nil
}

func (r *RedisServer) collectionMetaExpireElems(
	ctx context.Context,
	key []byte,
	readTS uint64,
	typ redisValueType,
	expireAtMs uint64,
) ([]*kv.Elem[kv.OP], bool, error) {
	switch typ {
	case redisTypeNone, redisTypeString:
		return nil, false, nil
	case redisTypeList:
		return r.listMetaExpireElems(ctx, key, readTS, expireAtMs)
	case redisTypeHash:
		return r.simpleMetaExpireElems(ctx, key, readTS, expireAtMs, "hash", store.HashMetaKey(key), store.HashMetaDeltaScanPrefix(key),
			func(raw []byte) (int64, uint64, error) {
				m, err := store.UnmarshalHashMeta(raw)
				return m.Len, m.ExpireAt, errors.WithStack(err)
			},
			func(n int64, ttl uint64) []byte { return store.MarshalHashMeta(store.HashMeta{Len: n, ExpireAt: ttl}) },
			func(raw []byte) (int64, error) {
				d, err := store.UnmarshalHashMetaDelta(raw)
				return d.LenDelta, errors.WithStack(err)
			},
		)
	case redisTypeSet:
		return r.simpleMetaExpireElems(ctx, key, readTS, expireAtMs, "set", store.SetMetaKey(key), store.SetMetaDeltaScanPrefix(key),
			func(raw []byte) (int64, uint64, error) {
				m, err := store.UnmarshalSetMeta(raw)
				return m.Len, m.ExpireAt, errors.WithStack(err)
			},
			func(n int64, ttl uint64) []byte { return store.MarshalSetMeta(store.SetMeta{Len: n, ExpireAt: ttl}) },
			func(raw []byte) (int64, error) {
				d, err := store.UnmarshalSetMetaDelta(raw)
				return d.LenDelta, errors.WithStack(err)
			},
		)
	case redisTypeZSet:
		return r.simpleMetaExpireElems(ctx, key, readTS, expireAtMs, "zset", store.ZSetMetaKey(key), store.ZSetMetaDeltaScanPrefix(key),
			func(raw []byte) (int64, uint64, error) {
				m, err := store.UnmarshalZSetMeta(raw)
				return m.Len, m.ExpireAt, errors.WithStack(err)
			},
			func(n int64, ttl uint64) []byte { return store.MarshalZSetMeta(store.ZSetMeta{Len: n, ExpireAt: ttl}) },
			func(raw []byte) (int64, error) {
				d, err := store.UnmarshalZSetMetaDelta(raw)
				return d.LenDelta, errors.WithStack(err)
			},
		)
	case redisTypeStream:
		return r.streamMetaExpireElems(ctx, key, readTS, expireAtMs)
	}
	return nil, false, nil
}

func (r *RedisServer) legacyCollectionExpireFallback(ctx context.Context, key []byte, readTS uint64, typ redisValueType) ([]*kv.Elem[kv.OP], bool, error) {
	legacyKey, ok := legacyCollectionBlobKey(key, typ)
	if !ok {
		return nil, false, nil
	}
	exists, err := r.store.ExistsAt(ctx, legacyKey, readTS)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return nil, exists, nil
}

func legacyCollectionBlobKey(key []byte, typ redisValueType) ([]byte, bool) {
	switch typ {
	case redisTypeHash:
		return redisHashKey(key), true
	case redisTypeSet:
		return redisSetKey(key), true
	case redisTypeZSet:
		return redisZSetKey(key), true
	case redisTypeStream:
		return redisStreamKey(key), true
	case redisTypeNone, redisTypeString, redisTypeList:
		return nil, false
	}
	return nil, false
}

func (r *RedisServer) listMetaExpireElems(ctx context.Context, key []byte, readTS uint64, expireAtMs uint64) ([]*kv.Elem[kv.OP], bool, error) {
	meta, exists, err := r.loadListMetaAt(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	prefix := store.ListMetaDeltaScanPrefix(key)
	deltas, err := r.scanDeltaKVs(ctx, prefix, readTS)
	if err != nil {
		return r.listMetaExpireScanErr(key, meta, exists, expireAtMs, err)
	}
	if !exists && len(deltas) == 0 {
		return nil, false, nil
	}
	meta, err = listMetaWithDeltas(meta, deltas)
	if err != nil {
		return nil, false, err
	}
	if meta.Len < 0 {
		meta.Len = 0
	}
	meta.Tail = meta.Head + meta.Len
	if meta.Len == 0 {
		return nil, false, nil
	}
	meta.ExpireAt = expireAtMs
	metaBytes, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: store.ListMetaKey(key), Value: metaBytes}}
	return appendDeltaDeletes(elems, deltas), true, nil
}

func (r *RedisServer) listMetaExpireScanErr(
	key []byte,
	meta store.ListMeta,
	exists bool,
	expireAtMs uint64,
	err error,
) ([]*kv.Elem[kv.OP], bool, error) {
	if !errors.Is(err, ErrDeltaScanTruncated) {
		return nil, false, err
	}
	r.triggerUrgentCompaction("list", key)
	if exists {
		return listMetaTTLUpdateElem(key, meta, expireAtMs)
	}
	return nil, false, ErrDeltaScanTruncated
}

func listMetaWithDeltas(meta store.ListMeta, deltas []*store.KVPair) (store.ListMeta, error) {
	for _, d := range deltas {
		md, err := store.UnmarshalListMetaDelta(d.Value)
		if err != nil {
			return store.ListMeta{}, errors.WithStack(err)
		}
		meta.Head += md.HeadDelta
		meta.Len += md.LenDelta
	}
	return meta, nil
}

func listMetaTTLUpdateElem(key []byte, meta store.ListMeta, expireAtMs uint64) ([]*kv.Elem[kv.OP], bool, error) {
	meta.ExpireAt = expireAtMs
	metaBytes, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: store.ListMetaKey(key), Value: metaBytes}}, true, nil
}

func (r *RedisServer) simpleMetaExpireElems(
	ctx context.Context,
	key []byte,
	readTS uint64,
	expireAtMs uint64,
	typeName string,
	metaKey []byte,
	deltaPrefix []byte,
	unmarshalBase func([]byte) (int64, uint64, error),
	marshalBase func(int64, uint64) []byte,
	unmarshalDelta func([]byte) (int64, error),
) ([]*kv.Elem[kv.OP], bool, error) {
	var baseLen int64
	raw, err := r.store.GetAt(ctx, metaKey, readTS)
	exists := true
	if err != nil {
		if !errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, errors.WithStack(err)
		}
		exists = false
	} else {
		var unmarshalErr error
		baseLen, _, unmarshalErr = unmarshalBase(raw)
		if unmarshalErr != nil {
			return nil, false, unmarshalErr
		}
	}
	deltas, err := r.scanDeltaKVs(ctx, deltaPrefix, readTS)
	if err != nil {
		return r.simpleMetaExpireScanErr(key, typeName, metaKey, baseLen, exists, expireAtMs, marshalBase, err)
	}
	if !exists && len(deltas) == 0 {
		return nil, false, nil
	}
	newLen, err := simpleMetaLenWithDeltas(baseLen, deltas, unmarshalDelta)
	if err != nil {
		return nil, false, err
	}
	if newLen <= 0 {
		if len(deltas) > 0 {
			return nil, false, nil
		}
		elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: metaKey, Value: marshalBase(0, expireAtMs)}}
		return appendDeltaDeletes(elems, deltas), true, nil
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: metaKey, Value: marshalBase(newLen, expireAtMs)}}
	return appendDeltaDeletes(elems, deltas), true, nil
}

func (r *RedisServer) simpleMetaExpireScanErr(
	key []byte,
	typeName string,
	metaKey []byte,
	baseLen int64,
	exists bool,
	expireAtMs uint64,
	marshalBase func(int64, uint64) []byte,
	err error,
) ([]*kv.Elem[kv.OP], bool, error) {
	if !errors.Is(err, ErrDeltaScanTruncated) {
		return nil, false, err
	}
	r.triggerUrgentCompaction(typeName, key)
	if exists {
		return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: metaKey, Value: marshalBase(baseLen, expireAtMs)}}, true, nil
	}
	return nil, false, ErrDeltaScanTruncated
}

func simpleMetaLenWithDeltas(
	baseLen int64,
	deltas []*store.KVPair,
	unmarshalDelta func([]byte) (int64, error),
) (int64, error) {
	newLen := baseLen
	for _, d := range deltas {
		v, err := unmarshalDelta(d.Value)
		if err != nil {
			return 0, err
		}
		newLen += v
	}
	return newLen, nil
}

func (r *RedisServer) streamMetaExpireElems(ctx context.Context, key []byte, readTS uint64, expireAtMs uint64) ([]*kv.Elem[kv.OP], bool, error) {
	meta, found, err := r.loadStreamMetaAt(ctx, key, readTS)
	if err != nil || !found {
		return nil, found, err
	}
	meta.ExpireAt = expireAtMs
	metaBytes, err := store.MarshalStreamMeta(meta)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: store.StreamMetaKey(key), Value: metaBytes}}, true, nil
}

func (r *RedisServer) scanDeltaKVs(ctx context.Context, prefix []byte, readTS uint64) ([]*store.KVPair, error) {
	end := store.PrefixScanEnd(prefix)
	deltas, err := r.store.ScanAt(ctx, prefix, end, store.MaxDeltaScanLimit+1, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(deltas) > store.MaxDeltaScanLimit {
		return nil, ErrDeltaScanTruncated
	}
	return deltas, nil
}

type redisDeltaKVScanner interface {
	scanDeltaKVs(context.Context, []byte, uint64) ([]*store.KVPair, error)
}

func appendDeltaDeletes(elems []*kv.Elem[kv.OP], deltas []*store.KVPair) []*kv.Elem[kv.OP] {
	for _, d := range deltas {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(d.Key)})
	}
	return elems
}

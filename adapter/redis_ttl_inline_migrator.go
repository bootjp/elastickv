package adapter

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	ttlInlineMigrationTickScanLimit = 512
	ttlInlineMigrationBatchKeyLimit = 32
)

type ttlInlineMigrationHandler struct {
	typeName       string
	prefix         []byte
	extractUserKey func([]byte) []byte
	buildElems     func(context.Context, *store.KVPair, uint64) ([]*kv.Elem[kv.OP], error)
}

func (c *DeltaCompactor) migrateTTLInlineOnce(ctx context.Context, readTS uint64) error {
	var combined error
	for _, h := range c.ttlInlineMigrationHandlers() {
		if err := c.migrateTTLInlineHandler(ctx, h, readTS); err != nil {
			combined = errors.CombineErrors(combined, err)
		}
	}
	return errors.WithStack(combined)
}

func (c *DeltaCompactor) ttlInlineMigrationHandlers() []ttlInlineMigrationHandler {
	return []ttlInlineMigrationHandler{
		{
			typeName:       "ttl-inline-string",
			prefix:         []byte(redisStrPrefix),
			extractUserKey: trimKnownPrefix([]byte(redisStrPrefix)),
			buildElems:     c.migrateStringTTLInlineElems,
		},
		{
			typeName:       "ttl-inline-hll",
			prefix:         []byte(redisHLLPrefix),
			extractUserKey: trimKnownPrefix([]byte(redisHLLPrefix)),
			buildElems:     c.migrateHLLTTLInlineElems,
		},
		{
			typeName:       "ttl-inline-indexed-collection",
			prefix:         []byte(redisTTLPrefix),
			extractUserKey: trimKnownPrefix([]byte(redisTTLPrefix)),
			buildElems:     c.migrateTTLIndexedCollectionElems,
		},
		{
			typeName:       "ttl-inline-list",
			prefix:         []byte(store.ListMetaPrefix),
			extractUserKey: extractListMetaMigrationUserKey,
			buildElems:     c.migrateListTTLInlineElems,
		},
		c.simpleTTLInlineMigrationHandler(
			"ttl-inline-hash",
			[]byte(store.HashMetaPrefix),
			extractHashMetaMigrationUserKey,
			func(raw []byte) (int64, uint64, error) {
				m, err := store.UnmarshalHashMeta(raw)
				return m.Len, m.ExpireAt, errors.WithStack(err)
			},
			func(n int64, ttl uint64) []byte { return store.MarshalHashMeta(store.HashMeta{Len: n, ExpireAt: ttl}) },
		),
		c.simpleTTLInlineMigrationHandler(
			"ttl-inline-set",
			[]byte(store.SetMetaPrefix),
			extractSetMetaMigrationUserKey,
			func(raw []byte) (int64, uint64, error) {
				m, err := store.UnmarshalSetMeta(raw)
				return m.Len, m.ExpireAt, errors.WithStack(err)
			},
			func(n int64, ttl uint64) []byte { return store.MarshalSetMeta(store.SetMeta{Len: n, ExpireAt: ttl}) },
		),
		c.simpleTTLInlineMigrationHandler(
			"ttl-inline-zset",
			[]byte(store.ZSetMetaPrefix),
			extractZSetMetaMigrationUserKey,
			func(raw []byte) (int64, uint64, error) {
				m, err := store.UnmarshalZSetMeta(raw)
				return m.Len, m.ExpireAt, errors.WithStack(err)
			},
			func(n int64, ttl uint64) []byte { return store.MarshalZSetMeta(store.ZSetMeta{Len: n, ExpireAt: ttl}) },
		),
		{
			typeName:       "ttl-inline-stream",
			prefix:         []byte(store.StreamMetaPrefix),
			extractUserKey: store.ExtractStreamUserKeyFromMeta,
			buildElems:     c.migrateStreamTTLInlineElems,
		},
	}
}

func trimKnownPrefix(prefix []byte) func([]byte) []byte {
	return func(key []byte) []byte {
		return bytes.TrimPrefix(key, prefix)
	}
}

func extractListMetaMigrationUserKey(key []byte) []byte {
	return store.ExtractListUserKey(key)
}

func extractHashMetaMigrationUserKey(key []byte) []byte {
	if store.IsHashMetaDeltaKey(key) {
		return nil
	}
	return store.ExtractHashUserKeyFromMeta(key)
}

func extractSetMetaMigrationUserKey(key []byte) []byte {
	if store.IsSetMetaDeltaKey(key) {
		return nil
	}
	return store.ExtractSetUserKeyFromMeta(key)
}

func extractZSetMetaMigrationUserKey(key []byte) []byte {
	if store.IsZSetMetaDeltaKey(key) {
		return nil
	}
	return store.ExtractZSetUserKeyFromMeta(key)
}

func (c *DeltaCompactor) simpleTTLInlineMigrationHandler(
	typeName string,
	prefix []byte,
	extractUserKey func([]byte) []byte,
	unmarshal func([]byte) (int64, uint64, error),
	marshal func(int64, uint64) []byte,
) ttlInlineMigrationHandler {
	return ttlInlineMigrationHandler{
		typeName:       typeName,
		prefix:         prefix,
		extractUserKey: extractUserKey,
		buildElems: func(ctx context.Context, pair *store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
			userKey := extractUserKey(pair.Key)
			if userKey == nil {
				return nil, nil
			}
			n, ttlMs, err := unmarshal(pair.Value)
			if err != nil {
				return nil, err
			}
			if ttlMs == 0 && isLegacySimpleMeta(pair.Value) {
				ttlMs, err = legacyTTLMillisAt(ctx, c.st, userKey, readTS)
				if err != nil {
					return nil, err
				}
			}
			desired := marshal(n, ttlMs)
			elems := putIfChanged(pair.Key, pair.Value, desired)
			return appendTTLIndexSyncElem(ctx, c.st, elems, userKey, ttlMs, readTS)
		},
	}
}

func (c *DeltaCompactor) migrateTTLInlineHandler(ctx context.Context, h ttlInlineMigrationHandler, readTS uint64) error {
	kvs, truncated, err := c.scanTTLInlineMigrationRaw(ctx, h, readTS)
	if err != nil {
		return err
	}
	var (
		elems      []*kv.Elem[kv.OP]
		batchKeys  int
		flushBatch = func() error {
			if err := c.dispatchCompaction(ctx, readTS, elems); err != nil {
				c.logger.WarnContext(ctx, "ttl inline migrator: batch dispatch failed",
					"type", h.typeName, "error", err)
				return err
			}
			elems = nil
			batchKeys = 0
			return nil
		}
	)
	for _, pair := range kvs {
		userKey := h.extractUserKey(pair.Key)
		if userKey == nil || !c.coord.IsLeaderForKey(userKey) {
			continue
		}
		built, buildErr := h.buildElems(ctx, pair, readTS)
		if buildErr != nil {
			c.logger.WarnContext(ctx, "ttl inline migrator: failed to build elems",
				"type", h.typeName, "key", string(userKey), "error", buildErr)
			continue
		}
		elems = append(elems, built...)
		batchKeys++
		if batchKeys >= ttlInlineMigrationBatchKeyLimit {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}
	if err := flushBatch(); err != nil {
		return err
	}
	c.advanceCursor(h.typeName, lastKVKey(kvs), truncated)
	return nil
}

func (c *DeltaCompactor) scanTTLInlineMigrationRaw(ctx context.Context, h ttlInlineMigrationHandler, readTS uint64) ([]*store.KVPair, bool, error) {
	c.cursorMu.Lock()
	start := h.prefix
	if cur := c.cursors[h.typeName]; len(cur) > 0 {
		start = append(bytes.Clone(cur), cursorNextByte)
	}
	c.cursorMu.Unlock()

	end := store.PrefixScanEnd(h.prefix)
	kvs, err := c.st.ScanAt(ctx, start, end, ttlInlineMigrationTickScanLimit, readTS)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return kvs, len(kvs) == ttlInlineMigrationTickScanLimit, nil
}

func (c *DeltaCompactor) migrateStringTTLInlineElems(ctx context.Context, pair *store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	userKey := bytes.TrimPrefix(pair.Key, []byte(redisStrPrefix))
	var (
		desired []byte
		ttlMs   uint64
		err     error
	)
	if isNewRedisStrFormat(pair.Value) {
		_, ttl, decErr := decodeRedisStr(pair.Value)
		if decErr != nil {
			return nil, decErr
		}
		ttlMs = ttlMillis(ttl)
		desired = pair.Value
	} else {
		ttlMs, err = legacyTTLMillisAt(ctx, c.st, userKey, readTS)
		if err != nil {
			return nil, err
		}
		desired = encodeRedisStr(pair.Value, redisTimeFromMillis(ttlMs))
	}
	elems := putIfChanged(pair.Key, pair.Value, desired)
	return appendTTLIndexSyncElem(ctx, c.st, elems, userKey, ttlMs, readTS)
}

func (c *DeltaCompactor) migrateHLLTTLInlineElems(ctx context.Context, pair *store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	userKey := bytes.TrimPrefix(pair.Key, []byte(redisHLLPrefix))
	value, ttl, embedded, err := decodeRedisHLL(pair.Value)
	if err != nil {
		return nil, err
	}
	ttlMs := ttlMillis(ttl)
	desired := pair.Value
	if !embedded {
		ttlMs, err = legacyTTLMillisAt(ctx, c.st, userKey, readTS)
		if err != nil {
			return nil, err
		}
		desired, err = encodeRedisHLL(value, redisTimeFromMillis(ttlMs))
		if err != nil {
			return nil, err
		}
	}
	elems := putIfChanged(pair.Key, pair.Value, desired)
	return appendTTLIndexSyncElem(ctx, c.st, elems, userKey, ttlMs, readTS)
}

func (c *DeltaCompactor) migrateTTLIndexedCollectionElems(ctx context.Context, pair *store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	userKey := bytes.TrimPrefix(pair.Key, []byte(redisTTLPrefix))
	ttl, err := decodeRedisTTL(pair.Value)
	if err != nil {
		return nil, err
	}
	ttlMs := redisExpireAtMillis(ttl)
	if ttlMs == 0 {
		return nil, nil
	}

	server := &RedisServer{store: c.st, compactor: c}
	typ, err := server.rawKeyTypeAt(ctx, userKey, readTS)
	if err != nil {
		return nil, err
	}
	switch typ {
	case redisTypeNone, redisTypeString:
		return nil, nil
	case redisTypeList, redisTypeHash, redisTypeSet, redisTypeZSet, redisTypeStream:
	}
	baseExists, err := c.collectionBaseMetaExistsAt(ctx, userKey, typ, readTS)
	if err != nil || baseExists {
		return nil, err
	}
	elems, ok, err := server.collectionMetaExpireElems(ctx, userKey, readTS, typ, ttlMs)
	if err != nil || ok {
		return elems, err
	}
	return c.legacyCollectionTTLInlineElems(ctx, userKey, typ, ttlMs, readTS)
}

func (c *DeltaCompactor) collectionBaseMetaExistsAt(ctx context.Context, userKey []byte, typ redisValueType, readTS uint64) (bool, error) {
	var metaKey []byte
	switch typ {
	case redisTypeNone, redisTypeString:
		return false, nil
	case redisTypeList:
		metaKey = store.ListMetaKey(userKey)
	case redisTypeHash:
		metaKey = store.HashMetaKey(userKey)
	case redisTypeSet:
		metaKey = store.SetMetaKey(userKey)
	case redisTypeZSet:
		metaKey = store.ZSetMetaKey(userKey)
	case redisTypeStream:
		metaKey = store.StreamMetaKey(userKey)
	}
	exists, err := c.st.ExistsAt(ctx, metaKey, readTS)
	return exists, errors.WithStack(err)
}

func (c *DeltaCompactor) legacyCollectionTTLInlineElems(
	ctx context.Context,
	userKey []byte,
	typ redisValueType,
	ttlMs uint64,
	readTS uint64,
) ([]*kv.Elem[kv.OP], error) {
	switch typ {
	case redisTypeNone, redisTypeString, redisTypeList:
		return nil, nil
	case redisTypeStream:
		return c.legacyStreamTTLInlineElems(ctx, userKey, ttlMs, readTS)
	case redisTypeHash:
		return c.legacyHashTTLInlineElems(ctx, userKey, ttlMs, readTS)
	case redisTypeSet:
		return c.legacySetTTLInlineElems(ctx, userKey, ttlMs, readTS)
	case redisTypeZSet:
		return c.legacyZSetTTLInlineElems(ctx, userKey, ttlMs, readTS)
	}
	return nil, nil
}

func (c *DeltaCompactor) legacyHashTTLInlineElems(ctx context.Context, userKey []byte, ttlMs uint64, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := c.st.GetAt(ctx, redisHashKey(userKey), readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	value, err := unmarshalHashValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value)+setWideColOverhead)
	for field, val := range value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(userKey, []byte(field)),
			Value: []byte(val),
		})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(userKey)},
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashMetaKey(userKey),
			Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(value)), ExpireAt: ttlMs}),
		},
	)
	return elems, nil
}

func (c *DeltaCompactor) legacySetTTLInlineElems(ctx context.Context, userKey []byte, ttlMs uint64, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := c.st.GetAt(ctx, redisSetKey(userKey), readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	value, err := unmarshalSetValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Members)+setWideColOverhead)
	for _, member := range value.Members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey(userKey, []byte(member)),
			Value: []byte{},
		})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Del, Key: redisSetKey(userKey)},
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMetaKey(userKey),
			Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(value.Members)), ExpireAt: ttlMs}),
		},
	)
	return elems, nil
}

func (c *DeltaCompactor) legacyZSetTTLInlineElems(ctx context.Context, userKey []byte, ttlMs uint64, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := c.st.GetAt(ctx, redisZSetKey(userKey), readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	value, err := unmarshalZSetValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Entries)*zsetOpsPerEntry+setWideColOverhead)
	for _, entry := range value.Entries {
		elems = append(elems,
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMemberKey(userKey, []byte(entry.Member)),
				Value: store.MarshalZSetScore(entry.Score),
			},
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetScoreKey(userKey, entry.Score, []byte(entry.Member)),
				Value: []byte{},
			},
		)
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey(userKey)},
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaKey(userKey),
			Value: store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(value.Entries)), ExpireAt: ttlMs}),
		},
	)
	return elems, nil
}

func (c *DeltaCompactor) legacyStreamTTLInlineElems(ctx context.Context, userKey []byte, ttlMs uint64, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := c.st.GetAt(ctx, redisStreamKey(userKey), readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	value, err := unmarshalStreamValue(raw)
	if err != nil {
		return nil, err
	}
	if len(value.Entries) > maxWideColumnItems {
		return nil, errors.Wrapf(ErrCollectionTooLarge, "legacy stream entry count exceeds %d", maxWideColumnItems)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Entries)+setWideColOverhead)
	var last redisStreamID
	for _, entry := range value.Entries {
		parsed, ok := tryParseRedisStreamID(entry.ID)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("invalid legacy stream ID %q", entry.ID))
		}
		if compareStreamIDs(parsed.ms, parsed.seq, last.ms, last.seq) > 0 {
			last = parsed
		}
		entryValue, err := marshalStreamEntry(entry)
		if err != nil {
			return nil, err
		}
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.StreamEntryKey(userKey, parsed.ms, parsed.seq),
			Value: entryValue,
		})
	}
	metaBytes, err := store.MarshalStreamMeta(store.StreamMeta{
		Length:   int64(len(value.Entries)),
		LastMs:   last.ms,
		LastSeq:  last.seq,
		ExpireAt: ttlMs,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Del, Key: redisStreamKey(userKey)},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.StreamMetaKey(userKey), Value: metaBytes},
	)
	return elems, nil
}

func (c *DeltaCompactor) migrateListTTLInlineElems(ctx context.Context, pair *store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	if isListMetaMigrationDelta(pair) {
		return nil, nil
	}
	userKey := extractListMetaMigrationUserKey(pair.Key)
	if userKey == nil {
		return nil, nil
	}
	meta, err := store.UnmarshalListMeta(pair.Value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if meta.ExpireAt == 0 && isLegacyWideMeta(pair.Value) {
		legacyTTL, err := legacyTTLMillisAt(ctx, c.st, userKey, readTS)
		if err != nil {
			return nil, err
		}
		meta.ExpireAt = legacyTTL
	}
	desired, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	elems := putIfChanged(pair.Key, pair.Value, desired)
	return appendTTLIndexSyncElem(ctx, c.st, elems, userKey, meta.ExpireAt, readTS)
}

func isListMetaMigrationDelta(pair *store.KVPair) bool {
	if pair == nil || !store.IsListMetaDeltaKey(pair.Key) {
		return false
	}
	return len(pair.Value) != redisWideMetaLegacySizeBytes && len(pair.Value) != redisWideMetaInlineSizeBytes
}

func (c *DeltaCompactor) migrateStreamTTLInlineElems(ctx context.Context, pair *store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	userKey := store.ExtractStreamUserKeyFromMeta(pair.Key)
	if userKey == nil {
		return nil, nil
	}
	meta, err := store.UnmarshalStreamMeta(pair.Value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if meta.ExpireAt == 0 && isLegacyWideMeta(pair.Value) {
		legacyTTL, err := legacyTTLMillisAt(ctx, c.st, userKey, readTS)
		if err != nil {
			return nil, err
		}
		meta.ExpireAt = legacyTTL
	}
	desired, err := store.MarshalStreamMeta(meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	elems := putIfChanged(pair.Key, pair.Value, desired)
	return appendTTLIndexSyncElem(ctx, c.st, elems, userKey, meta.ExpireAt, readTS)
}

func isLegacySimpleMeta(raw []byte) bool {
	return len(raw) == redisSimpleMetaLegacySizeBytes
}

func isLegacyWideMeta(raw []byte) bool {
	return len(raw) == redisWideMetaLegacySizeBytes
}

func legacyTTLMillisAt(ctx context.Context, st store.MVCCStore, userKey []byte, readTS uint64) (uint64, error) {
	raw, err := st.GetAt(ctx, redisTTLKey(userKey), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	ttl, err := decodeRedisTTL(raw)
	if err != nil {
		return 0, err
	}
	return redisExpireAtMillis(ttl), nil
}

func appendTTLIndexSyncElem(
	ctx context.Context,
	st store.MVCCStore,
	elems []*kv.Elem[kv.OP],
	userKey []byte,
	ttlMs uint64,
	readTS uint64,
) ([]*kv.Elem[kv.OP], error) {
	elem, err := ttlIndexSyncElem(ctx, st, userKey, ttlMs, readTS)
	if err != nil || elem == nil {
		return elems, err
	}
	return append(elems, elem), nil
}

func ttlIndexSyncElem(ctx context.Context, st store.MVCCStore, userKey []byte, ttlMs uint64, readTS uint64) (*kv.Elem[kv.OP], error) {
	raw, err := st.GetAt(ctx, redisTTLKey(userKey), readTS)
	if ttlMs == 0 {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(userKey)}, nil
	}
	if err == nil {
		current, decErr := decodeRedisTTL(raw)
		if decErr != nil {
			return nil, decErr
		}
		if redisExpireAtMillis(current) == ttlMs {
			return nil, nil
		}
	} else if !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
	}
	ttl := redisTimeFromMillis(ttlMs)
	return &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(userKey), Value: encodeRedisTTL(*ttl)}, nil
}

func putIfChanged(key, current, desired []byte) []*kv.Elem[kv.OP] {
	if bytes.Equal(current, desired) {
		return nil
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: bytes.Clone(key), Value: bytes.Clone(desired)}}
}

func ttlMillis(ttl *time.Time) uint64 {
	if ttl == nil {
		return 0
	}
	return redisExpireAtMillis(*ttl)
}

func lastKVKey(kvs []*store.KVPair) []byte {
	if len(kvs) == 0 {
		return nil
	}
	return kvs[len(kvs)-1].Key
}

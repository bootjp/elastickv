package adapter

import (
	"bytes"
	"context"
	"strconv"
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

// ttlInlineGroupScanner is *kv.ShardStore in the server. It uses
// ScanGroupOwnedAt because ScanGroupAt resolves a bare anchor prefix to the
// first route only and refuses every other group. ScanGroupOwnedAt reads one
// page per call and returns where it stopped, so the cursor advances even when
// the page held no row the group serves.
type ttlInlineGroupScanner interface {
	ScanGroupOwnedAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, []byte, error)
}

type ttlInlineMigrationScanPlan struct {
	cursorName string
	groupID    uint64
	scanner    ttlInlineGroupScanner
}

// ttlInlineMigrationTickPlan is one handler's scan plan within a tick.
type ttlInlineMigrationTickPlan struct {
	handler ttlInlineMigrationHandler
	plan    ttlInlineMigrationScanPlan
}

// migrateTTLInlineOnce runs every handler's scan plans under the tick's
// budget. The plans share that budget, so each tick starts at the plan after
// the last one the previous tick ran, as compactBackgroundHandlers rotates its
// handlers; starting from the first plan every tick starved the later handlers
// and groups whenever the earlier plans used up the budget.
func (c *DeltaCompactor) migrateTTLInlineOnce(ctx context.Context, readTS uint64) error {
	plans := c.ttlInlineMigrationTickPlans()
	c.pruneTTLInlineMigrationPlanStates(plans)
	start := c.rotationStart(&c.ttlInlinePlanCursor, len(plans))
	var combined error
	for offset := range plans {
		if ctx.Err() != nil {
			return errors.WithStack(errors.CombineErrors(combined, ctx.Err()))
		}
		idx := (start + offset) % len(plans)
		err := c.migrateTTLInlineScanPlan(ctx, plans[idx].handler, plans[idx].plan, readTS)
		c.advanceRotation(&c.ttlInlinePlanCursor, idx, len(plans))
		if err != nil {
			combined = errors.CombineErrors(combined, err)
		}
	}
	return errors.WithStack(combined)
}

func (c *DeltaCompactor) ttlInlineMigrationTickPlans() []ttlInlineMigrationTickPlan {
	var plans []ttlInlineMigrationTickPlan
	for _, h := range c.ttlInlineMigrationHandlers() {
		for _, plan := range c.ttlInlineMigrationScanPlans(h) {
			plans = append(plans, ttlInlineMigrationTickPlan{handler: h, plan: plan})
		}
	}
	return plans
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

func (c *DeltaCompactor) migrateTTLInlineScanPlan(
	ctx context.Context,
	h ttlInlineMigrationHandler,
	plan ttlInlineMigrationScanPlan,
	readTS uint64,
) error {
	page, err := c.scanTTLInlineMigrationRaw(ctx, h, plan, readTS)
	if err != nil {
		return err
	}
	tally, err := c.migrateTTLInlinePage(ctx, h, page.kvs, readTS)
	if err != nil {
		// The next tick reads this page again, so only the keys its earlier
		// batches already rewrote count toward the pass.
		c.noteTTLInlineMigrationPageFailed(plan.cursorName, page.fromStart, tally.keysMigrated)
		return err
	}
	if pass, prevClean, done := c.finishTTLInlineMigrationPage(plan.cursorName, page, tally); done {
		c.reportTTLInlineMigrationPass(ctx, h, plan, pass, prevClean)
	}
	return nil
}

// migrateTTLInlinePage rewrites the page's legacy anchors in batches of at most
// ttlInlineMigrationBatchKeyLimit keys and tallies what it did with each
// anchor. On a dispatch error the tally's keysMigrated still counts the keys
// of the batches dispatched before it.
func (c *DeltaCompactor) migrateTTLInlinePage(
	ctx context.Context,
	h ttlInlineMigrationHandler,
	kvs []*store.KVPair,
	readTS uint64,
) (ttlInlineMigrationTally, error) {
	var (
		tally      ttlInlineMigrationTally
		elems      []*kv.Elem[kv.OP]
		batchKeys  int
		rewrites   int
		flushBatch = func() error {
			if err := c.dispatchCompaction(ctx, readTS, elems); err != nil {
				c.logger.WarnContext(ctx, "ttl inline migrator: batch dispatch failed",
					"type", h.typeName, "error", err)
				return err
			}
			tally.keysMigrated += rewrites
			elems = nil
			batchKeys = 0
			rewrites = 0
			return nil
		}
	)
	for _, pair := range kvs {
		userKey := h.extractUserKey(pair.Key)
		if userKey == nil {
			continue
		}
		tally.keysScanned++
		if !c.coord.IsLeaderForKey(redisUserRouteKey(userKey)) {
			tally.keysSkipped++
			continue
		}
		built, buildErr := h.buildElems(ctx, pair, readTS)
		if buildErr != nil {
			tally.keysFailed++
			c.logger.WarnContext(ctx, "ttl inline migrator: failed to build elems",
				"type", h.typeName, "key", string(userKey), "error", buildErr)
			continue
		}
		if len(built) > 0 {
			rewrites++
		}
		elems = append(elems, built...)
		batchKeys++
		if batchKeys >= ttlInlineMigrationBatchKeyLimit {
			if err := flushBatch(); err != nil {
				return tally, err
			}
		}
	}
	// flushBatch adds the last batch's keys to tally, so call it before
	// returning tally.
	err := flushBatch()
	return tally, err
}

// ttlInlineMigrationScanPlans scans only the groups this node leads when the
// store can scan one group and the coordinator knows which groups those are. A
// known empty list means this node leads no group and plans nothing. When the
// groups are unknown the migrator falls back to one ScanAt over the whole
// prefix, filtered per key by IsLeaderForKey, rather than migrating nothing.
func (c *DeltaCompactor) ttlInlineMigrationScanPlans(h ttlInlineMigrationHandler) []ttlInlineMigrationScanPlan {
	scanner, hasGroupScanner := c.st.(ttlInlineGroupScanner)
	if hasGroupScanner {
		if groupIDs, known := kv.LocalLeaderGroupIDsThrough(c.coord); known {
			plans := make([]ttlInlineMigrationScanPlan, 0, len(groupIDs))
			for _, groupID := range groupIDs {
				plans = append(plans, ttlInlineMigrationScanPlan{
					cursorName: ttlInlineMigrationGroupCursorName(h.typeName, groupID),
					groupID:    groupID,
					scanner:    scanner,
				})
			}
			return plans
		}
		c.warnTTLInlineLocalLeaderGroupsUnknown()
	}
	return []ttlInlineMigrationScanPlan{{cursorName: h.typeName}}
}

// warnTTLInlineLocalLeaderGroupsUnknown logs once when the store can scan one
// group but the coordinator cannot name the groups this node leads. In the
// server that means a coordinator decorator dropped LocalLeaderGroupIDs, and
// every node is back to whole-prefix migration scans.
func (c *DeltaCompactor) warnTTLInlineLocalLeaderGroupsUnknown() {
	c.ttlInlineGroupsUnknownOnce.Do(func() {
		c.logger.Warn("ttl inline migrator: coordinator does not report local leader groups; falling back to whole-prefix scans")
	})
}

func ttlInlineMigrationGroupCursorName(typeName string, groupID uint64) string {
	return typeName + ":group:" + strconv.FormatUint(groupID, 10)
}

// ttlInlineMigrationPage is one page a scan plan read in a tick.
type ttlInlineMigrationPage struct {
	kvs []*store.KVPair
	// cursorKey is the key the plan's cursor moves to.
	cursorKey []byte
	// truncated reports that the scan stopped before the end of the prefix.
	truncated bool
	// fromStart reports that the page starts at the beginning of the prefix,
	// so it is the first page of a pass.
	fromStart bool
}

func (c *DeltaCompactor) scanTTLInlineMigrationRaw(
	ctx context.Context,
	h ttlInlineMigrationHandler,
	plan ttlInlineMigrationScanPlan,
	readTS uint64,
) (ttlInlineMigrationPage, error) {
	c.cursorMu.Lock()
	start := h.prefix
	cur := c.cursors[plan.cursorName]
	if len(cur) > 0 {
		start = append(bytes.Clone(cur), cursorNextByte)
	}
	c.cursorMu.Unlock()

	page := ttlInlineMigrationPage{fromStart: len(cur) == 0}
	end := store.PrefixScanEnd(h.prefix)
	if plan.scanner != nil {
		kvs, resume, err := plan.scanner.ScanGroupOwnedAt(ctx, plan.groupID, start, end, ttlInlineMigrationTickScanLimit, readTS)
		if err != nil {
			return ttlInlineMigrationPage{}, errors.WithStack(err)
		}
		page.kvs, page.cursorKey, page.truncated = kvs, resume, resume != nil
		return page, nil
	}
	kvs, err := c.st.ScanAt(ctx, start, end, ttlInlineMigrationTickScanLimit, readTS)
	if err != nil {
		return ttlInlineMigrationPage{}, errors.WithStack(err)
	}
	page.kvs, page.cursorKey, page.truncated = kvs, lastKVKey(kvs), len(kvs) == ttlInlineMigrationTickScanLimit
	return page, nil
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
	if !ttlIndexedCollectionMigrationType(typ) {
		return nil, nil
	}
	if redisTTLMillisExpired(ttlMs) {
		return c.expiredTTLIndexedCollectionElems(ctx, server, userKey, typ, readTS)
	}
	baseExists, err := collectionBaseMetaExistsAt(ctx, c.st, userKey, typ, readTS)
	if err != nil || baseExists {
		return nil, err
	}
	elems, ok, err := server.collectionMetaExpireElems(ctx, userKey, readTS, typ, ttlMs)
	if err != nil || ok {
		return elems, err
	}
	return c.legacyCollectionTTLInlineElems(ctx, userKey, typ, ttlMs, readTS)
}

func (c *DeltaCompactor) expiredTTLIndexedCollectionElems(
	ctx context.Context,
	server *RedisServer,
	userKey []byte,
	typ redisValueType,
	readTS uint64,
) ([]*kv.Elem[kv.OP], error) {
	inlineTTL, inlineFound, err := server.collectionTTLAt(ctx, userKey, readTS)
	if err != nil {
		return nil, err
	}
	if inlineFound {
		inlineMs := ttlMillis(inlineTTL)
		if inlineMs == 0 || !redisTTLMillisExpired(inlineMs) {
			return appendTTLIndexSyncElem(ctx, c.st, nil, userKey, inlineMs, readTS)
		}
	}
	staleDeltaOnly, err := c.expiredTTLIndexPrecedesDeltaOnlyCollection(ctx, server, userKey, typ, readTS)
	if err != nil {
		return nil, err
	}
	if staleDeltaOnly {
		return appendTTLIndexSyncElem(ctx, c.st, nil, userKey, 0, readTS)
	}
	elems, _, err := server.deleteLogicalKeyElems(ctx, userKey, readTS)
	return elems, err
}

func (c *DeltaCompactor) expiredTTLIndexPrecedesDeltaOnlyCollection(
	ctx context.Context,
	server *RedisServer,
	userKey []byte,
	typ redisValueType,
	readTS uint64,
) (bool, error) {
	return expiredTTLIndexPrecedesDeltaOnlyCollection(ctx, c.st, server, userKey, typ, readTS)
}

func expiredTTLIndexPrecedesDeltaOnlyCollection(
	ctx context.Context,
	st store.MVCCStore,
	scanner redisDeltaKVScanner,
	userKey []byte,
	typ redisValueType,
	readTS uint64,
) (bool, error) {
	baseExists, err := collectionBaseMetaExistsAt(ctx, st, userKey, typ, readTS)
	if err != nil || baseExists {
		return false, err
	}
	prefixes, ok := collectionMetaDeltaScanPrefixes(userKey, typ)
	if !ok {
		return false, nil
	}
	_, ttlCommitTS, found, err := legacyTTLMillisWithCommitTSAt(ctx, st, userKey, readTS)
	if err != nil || !found {
		return false, err
	}
	return legacyTTLPrecedesCollectionMetaDeltas(ctx, scanner, userKey, prefixes, ttlCommitTS, readTS)
}

func legacyTTLPrecedesCollectionMetaDeltas(
	ctx context.Context,
	scanner redisDeltaKVScanner,
	userKey []byte,
	prefixes [][]byte,
	ttlCommitTS uint64,
	readTS uint64,
) (bool, error) {
	found := false
	for _, prefix := range prefixes {
		deltas, err := scanner.scanDeltaKVs(ctx, prefix, readTS)
		if err != nil {
			if errors.Is(err, ErrDeltaScanTruncated) {
				return false, nil
			}
			return false, err
		}
		if isLegacyListMetaDeltaPrefix(prefix) {
			deltas = filterLegacyListMetaDeltas(deltas, userKey)
		}
		minDeltaTS, ok := minMetaDeltaCommitTS(deltas, prefix)
		if !ok {
			continue
		}
		found = true
		if ttlCommitTS >= minDeltaTS {
			return false, nil
		}
	}
	return found, nil
}

func filterLegacyListMetaDeltas(deltas []*store.KVPair, userKey []byte) []*store.KVPair {
	filtered := deltas[:0]
	for _, pair := range deltas {
		if legacyListDeltaPairForUserKey(pair, userKey) {
			filtered = append(filtered, pair)
		}
	}
	return filtered
}

func collectionMetaDeltaScanPrefixes(userKey []byte, typ redisValueType) ([][]byte, bool) {
	switch typ {
	case redisTypeList:
		return store.ListMetaDeltaScanPrefixes(userKey), true
	case redisTypeHash:
		return [][]byte{store.HashMetaDeltaScanPrefix(userKey)}, true
	case redisTypeSet:
		return [][]byte{store.SetMetaDeltaScanPrefix(userKey)}, true
	case redisTypeZSet:
		return [][]byte{store.ZSetMetaDeltaScanPrefix(userKey)}, true
	case redisTypeNone, redisTypeString, redisTypeStream:
		return nil, false
	}
	return nil, false
}

func ttlIndexedCollectionMigrationType(typ redisValueType) bool {
	switch typ {
	case redisTypeList, redisTypeHash, redisTypeSet, redisTypeZSet, redisTypeStream:
		return true
	case redisTypeNone, redisTypeString:
		return false
	}
	return false
}

func collectionBaseMetaExistsAt(ctx context.Context, st store.MVCCStore, userKey []byte, typ redisValueType, readTS uint64) (bool, error) {
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
	exists, err := st.ExistsAt(ctx, metaKey, readTS)
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
	_, err := c.st.GetAt(ctx, redisStreamKey(userKey), readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	metaBytes, err := store.MarshalStreamMeta(store.StreamMeta{
		ExpireAt: ttlMs,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: redisStreamKey(userKey)},
		{Op: kv.Put, Key: store.StreamMetaKey(userKey), Value: metaBytes},
	}, nil
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
	if pair == nil || !store.IsListMetaDeltaValue(pair.Value) {
		return false
	}
	return store.IsListMetaDeltaKey(pair.Key) || store.ExtractLegacyListUserKeyFromDelta(pair.Key) != nil
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

func legacyTTLMillisForMigrationAt(ctx context.Context, st store.MVCCStore, userKey []byte, readTS uint64) (uint64, bool, error) {
	ttlMs, err := legacyTTLMillisAt(ctx, st, userKey, readTS)
	if err != nil {
		return 0, false, err
	}
	return ttlMs, redisTTLMillisExpired(ttlMs), nil
}

func legacyExpiredCollectionCleanupElems(userKey []byte, legacyKey []byte) []*kv.Elem[kv.OP] {
	return []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: legacyKey},
		{Op: kv.Del, Key: redisTTLKey(userKey)},
	}
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

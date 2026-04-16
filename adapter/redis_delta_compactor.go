package adapter

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	defaultDeltaCompactorMaxDeltaCount = 64
	defaultDeltaCompactorScanInterval  = 30 * time.Second
	defaultDeltaCompactorTimeout       = 5 * time.Second

	// deltaCompactorTickScanLimit is the maximum number of delta keys scanned
	// per collection type per compaction tick. It bounds per-tick I/O while
	// still catching hot keys.
	deltaCompactorTickScanLimit = 4096

	// cursorNextByte is appended to the last-scanned key to produce an
	// exclusive lower bound that skips past it on the next tick.
	cursorNextByte = byte(0x00)
)

// DeltaCompactor folds accumulated delta keys into their corresponding base
// metadata keys for all wide-column collection types (List, Hash, Set, ZSet).
//
// It runs as a background goroutine on the Raft leader. Non-leaders skip each
// tick silently. Compaction is performed as an OCC transaction so concurrent
// writers never conflict with the compactor.
type DeltaCompactor struct {
	st       store.MVCCStore
	coord    kv.Coordinator
	logger   *slog.Logger
	maxCount int
	interval time.Duration
	timeout  time.Duration
	// cursors tracks the exclusive lower bound for the next scan of each
	// collection type (keyed by collectionDeltaHandler.typeName). This allows
	// each tick to resume where the previous one stopped, ensuring that keys
	// later in the lexicographic range are eventually reached even when the
	// prefix contains more than deltaCompactorTickScanLimit delta keys.
	// A nil or missing cursor means "start from the beginning of the prefix".
	// cursorMu protects cursors against concurrent access (e.g. SyncOnce
	// called from a test while Run is active).
	cursorMu sync.Mutex
	cursors  map[string][]byte
}

// DeltaCompactorOption configures a DeltaCompactor.
type DeltaCompactorOption func(*DeltaCompactor)

// WithDeltaCompactorMaxDeltaCount sets the soft threshold at which a key's
// deltas are folded into its base metadata. Default: 64.
func WithDeltaCompactorMaxDeltaCount(n int) DeltaCompactorOption {
	return func(c *DeltaCompactor) {
		if n > 0 {
			c.maxCount = n
		}
	}
}

// WithDeltaCompactorScanInterval sets the period between compaction passes.
// Default: 30s.
func WithDeltaCompactorScanInterval(d time.Duration) DeltaCompactorOption {
	return func(c *DeltaCompactor) {
		if d > 0 {
			c.interval = d
		}
	}
}

// WithDeltaCompactorTimeout sets the per-tick timeout. Default: 5s.
func WithDeltaCompactorTimeout(d time.Duration) DeltaCompactorOption {
	return func(c *DeltaCompactor) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithDeltaCompactorLogger sets the logger.
func WithDeltaCompactorLogger(l *slog.Logger) DeltaCompactorOption {
	return func(c *DeltaCompactor) {
		if l != nil {
			c.logger = l
		}
	}
}

// NewDeltaCompactor creates a DeltaCompactor that operates on st using coord.
func NewDeltaCompactor(st store.MVCCStore, coord kv.Coordinator, opts ...DeltaCompactorOption) *DeltaCompactor {
	c := &DeltaCompactor{
		st:       st,
		coord:    coord,
		logger:   slog.Default(),
		maxCount: defaultDeltaCompactorMaxDeltaCount,
		interval: defaultDeltaCompactorScanInterval,
		timeout:  defaultDeltaCompactorTimeout,
		cursors:  make(map[string][]byte),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// Run starts the background compaction loop and blocks until ctx is cancelled.
func (c *DeltaCompactor) Run(ctx context.Context) error {
	if err := c.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.WarnContext(ctx, "delta compactor initial pass failed", "error", err)
	}
	timer := time.NewTimer(c.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := c.SyncOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.WarnContext(ctx, "delta compactor pass failed", "error", err)
			}
			timer.Reset(c.interval)
		}
	}
}

// SyncOnce runs one compaction pass. Non-leaders return immediately.
// Each collection-type handler runs in its own goroutine so that a slow
// handler (e.g. one with many list deltas) does not delay Hash/Set/ZSet
// compaction. All goroutines share the same per-tick timeout context.
func (c *DeltaCompactor) SyncOnce(ctx context.Context) error {
	if c.coord == nil || !c.coord.IsLeader() {
		return nil
	}
	readTS := snapshotTS(c.coord.Clock(), c.st)
	tickCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	handlers := c.allHandlers()
	errs := make([]error, len(handlers))
	var wg sync.WaitGroup
	for i, h := range handlers {
		wg.Add(1)
		go func(idx int, handler collectionDeltaHandler) {
			defer wg.Done()
			errs[idx] = c.compactHandler(tickCtx, handler, readTS)
		}(i, h)
	}
	wg.Wait()

	var combined error
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			combined = errors.CombineErrors(combined, err)
		}
	}
	return errors.WithStack(combined)
}

// collectionDeltaHandler holds the type-specific functions for one collection type.
type collectionDeltaHandler struct {
	typeName       string
	prefix         []byte
	extractUserKey func(key []byte) []byte
	buildElems     func(ctx context.Context, userKey []byte, deltaKVs []*store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error)
}

// allHandlers returns the compaction handlers for all wide-column collection types.
func (c *DeltaCompactor) allHandlers() []collectionDeltaHandler {
	return []collectionDeltaHandler{
		c.listHandler(),
		c.hashHandler(),
		c.setHandler(),
		c.zsetHandler(),
	}
}

// scanDeltaRaw scans the delta prefix for h starting from the stored cursor
// position and returns the scanned KVPairs plus a truncated flag.
// truncated is true when the scan returned exactly deltaCompactorTickScanLimit
// results, indicating there may be more keys beyond the current window.
// The caller is responsible for advancing the cursor via advanceCursor.
func (c *DeltaCompactor) scanDeltaRaw(ctx context.Context, h collectionDeltaHandler, readTS uint64) ([]*store.KVPair, bool, error) {
	// Build the effective start key: one byte past the cursor so we do not
	// re-scan the key we stopped at last tick.
	c.cursorMu.Lock()
	start := h.prefix
	if cur := c.cursors[h.typeName]; len(cur) > 0 {
		start = append(bytes.Clone(cur), cursorNextByte)
	}
	c.cursorMu.Unlock()

	end := store.PrefixScanEnd(h.prefix)
	kvs, err := c.st.ScanAt(ctx, start, end, deltaCompactorTickScanLimit, readTS)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return kvs, len(kvs) == deltaCompactorTickScanLimit, nil
}

// advanceCursor updates the persistent cursor for h after a compaction tick.
// lastScannedKey is the last key returned by the scan this tick (nil if the
// scan returned no results). truncated is true when the scan returned exactly
// deltaCompactorTickScanLimit results, indicating there may be more keys.
//
// Cursor policy: always advance to the last scanned key when the scan was
// truncated so the compactor makes steady forward progress through the
// keyspace. Near-threshold keys that were not compacted this tick will be
// revisited on the next full cycle. When the scan was not truncated, reset
// the cursor so the next tick starts from the beginning.
func (c *DeltaCompactor) advanceCursor(typeName string, lastScannedKey []byte, truncated bool) {
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	if truncated && len(lastScannedKey) > 0 {
		c.cursors[typeName] = bytes.Clone(lastScannedKey)
	} else {
		delete(c.cursors, typeName)
	}
}

// compactHandler scans the delta prefix for h, groups entries by userKey,
// and compacts any key that has accumulated enough deltas.
// All compactable keys are batched into a single OCC transaction to reduce
// Raft round-trips.
func (c *DeltaCompactor) compactHandler(ctx context.Context, h collectionDeltaHandler, readTS uint64) error {
	kvs, truncated, err := c.scanDeltaRaw(ctx, h, readTS)
	if err != nil {
		return err
	}

	byKey, ukOrder := c.groupByUserKey(kvs, h.extractUserKey)
	lastScannedKey := c.splitGuardCursor(byKey, ukOrder, kvs, truncated)

	allElems := c.buildBatchElems(ctx, h, byKey, readTS)

	if err := c.dispatchCompaction(ctx, readTS, allElems); err != nil {
		c.logger.WarnContext(ctx, "delta compactor: batch dispatch failed",
			"type", h.typeName, "error", err)
		return err
	}

	c.advanceCursor(h.typeName, lastScannedKey, truncated)
	return nil
}

// groupByUserKey groups KVPairs by their user key, returning both the map and
// the unique user keys in lexicographic (scan) order.
func (c *DeltaCompactor) groupByUserKey(kvs []*store.KVPair, extractUserKey func([]byte) []byte) (map[string][]*store.KVPair, []string) {
	byKey := make(map[string][]*store.KVPair)
	var ukOrder []string
	for _, pair := range kvs {
		uk := extractUserKey(pair.Key)
		if uk == nil {
			continue
		}
		k := string(uk)
		if _, seen := byKey[k]; !seen {
			ukOrder = append(ukOrder, k)
		}
		byKey[k] = append(byKey[k], pair)
	}
	return byKey, ukOrder
}

// splitGuardCursor implements the scan window split guard and returns the
// effective last-scanned key for cursor advancement.
//
// When the scan is truncated the last user key in the window may have deltas
// that straddle the window boundary. If its visible count is below the
// compaction threshold those deltas would be skipped this tick, and advancing
// the cursor past them prevents them from merging with the deltas in the next
// window — leaving them stranded until the next full cycle.
//
// When that condition is detected, the last user key is removed from byKey and
// the cursor is backtracked to the last delta key of the penultimate user key
// (or nil to reset to the prefix start) so the next tick re-scans the full
// combined delta set.
func (c *DeltaCompactor) splitGuardCursor(byKey map[string][]*store.KVPair, ukOrder []string, kvs []*store.KVPair, truncated bool) []byte {
	var lastScannedKey []byte
	if len(kvs) > 0 {
		lastScannedKey = kvs[len(kvs)-1].Key
	}
	if !truncated || len(ukOrder) == 0 {
		return lastScannedKey
	}
	lastUK := ukOrder[len(ukOrder)-1]
	if len(byKey[lastUK]) >= c.maxCount {
		return lastScannedKey
	}
	// Backtrack: exclude last user key and reset cursor to penultimate key.
	delete(byKey, lastUK)
	ukOrder = ukOrder[:len(ukOrder)-1]
	if len(ukOrder) == 0 {
		return nil // reset to prefix start
	}
	prev := ukOrder[len(ukOrder)-1]
	prevPairs := byKey[prev]
	return prevPairs[len(prevPairs)-1].Key
}

// buildBatchElems builds OCC elems for all user keys in byKey that meet the
// compaction threshold, logging and skipping keys whose build fails.
func (c *DeltaCompactor) buildBatchElems(ctx context.Context, h collectionDeltaHandler, byKey map[string][]*store.KVPair, readTS uint64) []*kv.Elem[kv.OP] {
	var allElems []*kv.Elem[kv.OP]
	for ukStr, deltaKVs := range byKey {
		if len(deltaKVs) < c.maxCount {
			continue
		}
		userKey := []byte(ukStr)
		elems, buildErr := h.buildElems(ctx, userKey, deltaKVs, readTS)
		if buildErr != nil {
			c.logger.WarnContext(ctx, "delta compactor: failed to build elems",
				"type", h.typeName, "key", ukStr, "error", buildErr)
			continue
		}
		allElems = append(allElems, elems...)
		c.logger.InfoContext(ctx, "delta compactor: queued for compaction",
			"type", h.typeName, "key", ukStr, "delta_count", len(deltaKVs))
	}
	return allElems
}

// dispatchCompaction commits elems as an OCC transaction.
func (c *DeltaCompactor) dispatchCompaction(ctx context.Context, readTS uint64, elems []*kv.Elem[kv.OP]) error {
	if len(elems) == 0 {
		return nil
	}
	commitTS := c.coord.Clock().Next()
	_, err := c.coord.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return errors.WithStack(err)
}

// loadListBaseMeta reads the base list metadata key. Returns a zero ListMeta
// if the key does not exist (all state may be in uncompacted delta keys).
func (c *DeltaCompactor) loadListBaseMeta(ctx context.Context, userKey []byte, readTS uint64) (store.ListMeta, error) {
	raw, err := c.st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.ListMeta{}, nil
		}
		return store.ListMeta{}, errors.WithStack(err)
	}
	meta, err := store.UnmarshalListMeta(raw)
	return meta, errors.WithStack(err)
}

// sumListMetaDeltas aggregates HeadDelta and LenDelta across all delta KVPairs.
func sumListMetaDeltas(deltaKVs []*store.KVPair) (headDelta, lenDelta int64, err error) {
	for _, d := range deltaKVs {
		md, unmarshalErr := store.UnmarshalListMetaDelta(d.Value)
		if unmarshalErr != nil {
			return 0, 0, errors.WithStack(unmarshalErr)
		}
		headDelta += md.HeadDelta
		lenDelta += md.LenDelta
	}
	return headDelta, lenDelta, nil
}

// --- List handler ---

func (c *DeltaCompactor) listHandler() collectionDeltaHandler {
	return collectionDeltaHandler{
		typeName:       "list",
		prefix:         []byte(store.ListMetaDeltaPrefix),
		extractUserKey: store.ExtractListUserKeyFromDelta,
		buildElems:     c.buildListCompactElems,
	}
}

func (c *DeltaCompactor) buildListCompactElems(ctx context.Context, userKey []byte, deltaKVs []*store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	// Read base metadata (may not exist if all state is in deltas).
	baseMeta, err := c.loadListBaseMeta(ctx, userKey, readTS)
	if err != nil {
		return nil, err
	}

	headDelta, lenDelta, err := sumListMetaDeltas(deltaKVs)
	if err != nil {
		return nil, err
	}

	newMeta := store.ListMeta{
		Head: baseMeta.Head + headDelta,
		Len:  baseMeta.Len + lenDelta,
	}
	if newMeta.Len < 0 {
		c.logger.WarnContext(ctx, "delta compactor: clamping negative list length to 0",
			"key", string(userKey), "computed_len", newMeta.Len)
		newMeta.Len = 0
	}
	newMeta.Tail = newMeta.Head + newMeta.Len

	metaElem, err := listMetaElemForLen(store.ListMetaKey(userKey), newMeta)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, 1+len(deltaKVs))
	elems = append(elems, metaElem)
	for _, d := range deltaKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(d.Key)})
	}

	// GC stale claim keys produced by LPOP/RPOP. Claim keys outside the
	// current [Head, Tail) window will never again be needed for OCC
	// conflict detection and can be deleted safely.
	claimElems, err := c.collectStaleClaimElems(ctx, userKey, newMeta, readTS)
	if err != nil {
		c.logger.WarnContext(ctx, "delta compactor: claim key GC scan failed; skipping",
			"key", string(userKey), "error", err)
	} else {
		elems = append(elems, claimElems...)
	}
	return elems, nil
}

// collectStaleClaimElems returns Del elems for claim keys that lie outside the
// current [newMeta.Head, newMeta.Tail) window. These are claim keys for
// positions that have already been popped (< Head) or never existed (>= Tail).
// At most deltaCompactorTickScanLimit keys are collected per call to bound I/O.
func (c *DeltaCompactor) collectStaleClaimElems(ctx context.Context, userKey []byte, newMeta store.ListMeta, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	claimPrefix := store.ListClaimScanPrefix(userKey)
	claimPrefixEnd := store.PrefixScanEnd(claimPrefix)
	var elems []*kv.Elem[kv.OP]

	// Stale LPOP claims: claim keys for seq < newMeta.Head.
	lpopEnd := store.ListClaimKey(userKey, newMeta.Head)
	lpopKVs, err := c.st.ScanAt(ctx, claimPrefix, lpopEnd, deltaCompactorTickScanLimit, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, pair := range lpopKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(pair.Key)})
	}

	// Stale RPOP claims: claim keys for seq >= newMeta.Tail.
	rpopStart := store.ListClaimKey(userKey, newMeta.Tail)
	rpopKVs, err := c.st.ScanAt(ctx, rpopStart, claimPrefixEnd, deltaCompactorTickScanLimit, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, pair := range rpopKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(pair.Key)})
	}

	return elems, nil
}

// listMetaElemForLen returns a Put or Del elem for the list metadata key.
// When Len==0 a Del is returned to match Redis semantics (empty list = non-existent).
func listMetaElemForLen(metaKey []byte, meta store.ListMeta) (*kv.Elem[kv.OP], error) {
	if meta.Len == 0 {
		return &kv.Elem[kv.OP]{Op: kv.Del, Key: metaKey}, nil
	}
	metaBytes, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &kv.Elem[kv.OP]{Op: kv.Put, Key: metaKey, Value: metaBytes}, nil
}

// simpleLenHandlerParams holds all type-specific configuration for a
// Hash/Set/ZSet compaction handler. All three types carry only a LenDelta,
// so a single factory (makeSimpleLenHandler) produces all three handlers.
type simpleLenHandlerParams struct {
	typeName       string
	deltaPrefix    string
	extractUserKey func(key []byte) []byte
	metaKeyFn      func(userKey []byte) []byte
	unmarshalBase  func([]byte) (int64, error)
	unmarshalDelta func([]byte) (int64, error)
	marshalBase    func(int64) []byte
}

func (c *DeltaCompactor) makeSimpleLenHandler(p simpleLenHandlerParams) collectionDeltaHandler {
	return collectionDeltaHandler{
		typeName:       p.typeName,
		prefix:         []byte(p.deltaPrefix),
		extractUserKey: p.extractUserKey,
		buildElems: func(ctx context.Context, userKey []byte, deltaKVs []*store.KVPair, readTS uint64) ([]*kv.Elem[kv.OP], error) {
			return c.buildSimpleCompactElems(ctx, userKey, deltaKVs, readTS,
				p.metaKeyFn(userKey),
				p.unmarshalBase,
				p.unmarshalDelta,
				p.marshalBase,
			)
		},
	}
}

func (c *DeltaCompactor) hashHandler() collectionDeltaHandler {
	return c.makeSimpleLenHandler(simpleLenHandlerParams{
		typeName:       "hash",
		deltaPrefix:    store.HashMetaDeltaPrefix,
		extractUserKey: store.ExtractHashUserKeyFromDelta,
		metaKeyFn:      store.HashMetaKey,
		unmarshalBase: func(b []byte) (int64, error) {
			m, err := store.UnmarshalHashMeta(b)
			return m.Len, errors.WithStack(err)
		},
		unmarshalDelta: func(b []byte) (int64, error) {
			d, err := store.UnmarshalHashMetaDelta(b)
			return d.LenDelta, errors.WithStack(err)
		},
		marshalBase: func(n int64) []byte { return store.MarshalHashMeta(store.HashMeta{Len: n}) },
	})
}

func (c *DeltaCompactor) setHandler() collectionDeltaHandler {
	return c.makeSimpleLenHandler(simpleLenHandlerParams{
		typeName:       "set",
		deltaPrefix:    store.SetMetaDeltaPrefix,
		extractUserKey: store.ExtractSetUserKeyFromDelta,
		metaKeyFn:      store.SetMetaKey,
		unmarshalBase: func(b []byte) (int64, error) {
			m, err := store.UnmarshalSetMeta(b)
			return m.Len, errors.WithStack(err)
		},
		unmarshalDelta: func(b []byte) (int64, error) {
			d, err := store.UnmarshalSetMetaDelta(b)
			return d.LenDelta, errors.WithStack(err)
		},
		marshalBase: func(n int64) []byte { return store.MarshalSetMeta(store.SetMeta{Len: n}) },
	})
}

func (c *DeltaCompactor) zsetHandler() collectionDeltaHandler {
	return c.makeSimpleLenHandler(simpleLenHandlerParams{
		typeName:       "zset",
		deltaPrefix:    store.ZSetMetaDeltaPrefix,
		extractUserKey: store.ExtractZSetUserKeyFromDelta,
		metaKeyFn:      store.ZSetMetaKey,
		unmarshalBase: func(b []byte) (int64, error) {
			m, err := store.UnmarshalZSetMeta(b)
			return m.Len, errors.WithStack(err)
		},
		unmarshalDelta: func(b []byte) (int64, error) {
			d, err := store.UnmarshalZSetMetaDelta(b)
			return d.LenDelta, errors.WithStack(err)
		},
		marshalBase: func(n int64) []byte { return store.MarshalZSetMeta(store.ZSetMeta{Len: n}) },
	})
}

// buildSimpleCompactElems folds deltaKVs into a single base-meta update.
// It is used by Hash, Set, and ZSet compaction, which all carry only a LenDelta.
func (c *DeltaCompactor) buildSimpleCompactElems(
	ctx context.Context,
	_ []byte,
	deltaKVs []*store.KVPair,
	readTS uint64,
	metaKey []byte,
	unmarshalBase func([]byte) (int64, error),
	unmarshalDelta func([]byte) (int64, error),
	marshalBase func(int64) []byte,
) ([]*kv.Elem[kv.OP], error) {
	raw, err := c.st.GetAt(ctx, metaKey, readTS)
	var baseLen int64
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
	}
	if err == nil {
		baseLen, err = unmarshalBase(raw)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	var deltaSum int64
	for _, d := range deltaKVs {
		v, unmarshalErr := unmarshalDelta(d.Value)
		if unmarshalErr != nil {
			return nil, errors.WithStack(unmarshalErr)
		}
		deltaSum += v
	}

	newLen := baseLen + deltaSum
	if newLen < 0 {
		newLen = 0
	}

	metaElem := simpleMetaElemForLen(metaKey, newLen, marshalBase)
	elems := make([]*kv.Elem[kv.OP], 0, 1+len(deltaKVs))
	elems = append(elems, metaElem)
	for _, d := range deltaKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(d.Key)})
	}
	return elems, nil
}

// simpleMetaElemForLen returns a Put or Del elem for a Hash/Set/ZSet metadata key.
// When newLen==0 a Del is returned to match Redis semantics (empty collection = non-existent).
func simpleMetaElemForLen(metaKey []byte, newLen int64, marshalBase func(int64) []byte) *kv.Elem[kv.OP] {
	if newLen == 0 {
		return &kv.Elem[kv.OP]{Op: kv.Del, Key: metaKey}
	}
	return &kv.Elem[kv.OP]{Op: kv.Put, Key: metaKey, Value: marshalBase(newLen)}
}

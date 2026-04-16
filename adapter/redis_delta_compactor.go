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
// lastCompactedKey is the lexicographically greatest delta key that was
// included in a successful compaction this tick (nil if nothing was compacted).
// truncated indicates whether the scan returned a full page (there may be more
// keys beyond the current window).
//
// Cursor policy:
//   - Compaction happened: advance to lastCompactedKey so the next tick
//     re-evaluates any non-compacted keys that followed it.
//   - No compaction, scan was truncated: do not advance; re-scan the same
//     window next tick so near-threshold keys are caught quickly.
//   - No compaction, scan was not truncated: reset; next tick starts fresh.
func (c *DeltaCompactor) advanceCursor(typeName string, lastCompactedKey []byte, truncated bool) {
	c.cursorMu.Lock()
	defer c.cursorMu.Unlock()
	switch {
	case lastCompactedKey != nil:
		c.cursors[typeName] = bytes.Clone(lastCompactedKey)
	case truncated:
		// Nothing compacted but there are more keys — keep current cursor
		// position so near-threshold keys are re-scanned next tick.
	default:
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

	// Group deltas per userKey.
	byKey := make(map[string][]*store.KVPair)
	for _, pair := range kvs {
		uk := h.extractUserKey(pair.Key)
		if uk == nil {
			continue
		}
		k := string(uk)
		byKey[k] = append(byKey[k], pair)
	}

	// Collect elems for all compactable keys into one batch, tracking the
	// lexicographically greatest compacted delta key for cursor placement.
	var allElems []*kv.Elem[kv.OP]
	var lastCompactedKey []byte
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
		// Track the last delta key of this user key for cursor placement.
		if last := deltaKVs[len(deltaKVs)-1].Key; bytes.Compare(last, lastCompactedKey) > 0 {
			lastCompactedKey = last
		}
		c.logger.InfoContext(ctx, "delta compactor: queued for compaction",
			"type", h.typeName, "key", ukStr, "delta_count", len(deltaKVs))
	}

	if err := c.dispatchCompaction(ctx, readTS, allElems); err != nil {
		c.logger.WarnContext(ctx, "delta compactor: batch dispatch failed",
			"type", h.typeName, "error", err)
		// On failure, do not advance cursor; the same keys will be retried
		// on the next tick.
		return nil
	}

	c.advanceCursor(h.typeName, lastCompactedKey, truncated)
	return nil
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
	raw, err := c.st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	var baseMeta store.ListMeta
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
	}
	if err == nil {
		baseMeta, err = store.UnmarshalListMeta(raw)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	// Aggregate all provided deltas.
	var headDelta, lenDelta int64
	for _, d := range deltaKVs {
		md, unmarshalErr := store.UnmarshalListMetaDelta(d.Value)
		if unmarshalErr != nil {
			return nil, errors.WithStack(unmarshalErr)
		}
		headDelta += md.HeadDelta
		lenDelta += md.LenDelta
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

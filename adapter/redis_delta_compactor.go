package adapter

import (
	"bytes"
	"context"
	"log/slog"
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
	cursors map[string][]byte
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
func (c *DeltaCompactor) SyncOnce(ctx context.Context) error {
	if c.coord == nil || !c.coord.IsLeader() {
		return nil
	}
	readTS := snapshotTS(c.coord.Clock(), c.st)
	tickCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var combined error
	for _, h := range c.allHandlers() {
		if err := c.compactHandler(tickCtx, h, readTS); err != nil && !errors.Is(err, context.Canceled) {
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

// scanDeltaPrefix scans the delta prefix for h starting from the stored
// cursor position, updates the cursor for the next tick, and returns the
// scanned KVPairs.
func (c *DeltaCompactor) scanDeltaPrefix(ctx context.Context, h collectionDeltaHandler, readTS uint64) ([]*store.KVPair, error) {
	// Build the effective start key: one byte past the cursor so we do not
	// re-scan the key we stopped at last tick.
	start := h.prefix
	if cur := c.cursors[h.typeName]; len(cur) > 0 {
		start = append(bytes.Clone(cur), cursorNextByte)
	}

	end := store.PrefixScanEnd(h.prefix)
	kvs, err := c.st.ScanAt(ctx, start, end, deltaCompactorTickScanLimit, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Advance or reset the cursor for the next tick.
	if len(kvs) == deltaCompactorTickScanLimit {
		c.cursors[h.typeName] = bytes.Clone(kvs[len(kvs)-1].Key)
	} else {
		delete(c.cursors, h.typeName)
	}
	return kvs, nil
}

// compactHandler scans the delta prefix for h, groups entries by userKey,
// and compacts any key that has accumulated enough deltas.
func (c *DeltaCompactor) compactHandler(ctx context.Context, h collectionDeltaHandler, readTS uint64) error {
	kvs, err := c.scanDeltaPrefix(ctx, h, readTS)
	if err != nil {
		return err
	}

	// Count deltas per userKey and collect their KVPairs.
	type entry struct {
		deltaKVs []*store.KVPair
	}
	byKey := make(map[string]*entry)
	for _, kv := range kvs {
		uk := h.extractUserKey(kv.Key)
		if uk == nil {
			continue
		}
		k := string(uk)
		if byKey[k] == nil {
			byKey[k] = &entry{}
		}
		byKey[k].deltaKVs = append(byKey[k].deltaKVs, kv)
	}

	for ukStr, e := range byKey {
		if len(e.deltaKVs) < c.maxCount {
			continue
		}
		userKey := []byte(ukStr)
		elems, buildErr := h.buildElems(ctx, userKey, e.deltaKVs, readTS)
		if buildErr != nil {
			c.logger.WarnContext(ctx, "delta compactor: failed to build elems",
				"type", h.typeName, "key", ukStr, "error", buildErr)
			continue
		}
		if err := c.dispatchCompaction(ctx, readTS, elems); err != nil {
			c.logger.WarnContext(ctx, "delta compactor: dispatch failed",
				"type", h.typeName, "key", ukStr, "error", err)
		} else {
			c.logger.InfoContext(ctx, "delta compactor: compacted",
				"type", h.typeName, "key", ukStr, "delta_count", len(e.deltaKVs))
		}
	}
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
		newMeta.Len = 0
	}
	metaBytes, err := store.MarshalListMeta(newMeta)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	elems := make([]*kv.Elem[kv.OP], 0, 1+len(deltaKVs))
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ListMetaKey(userKey),
		Value: metaBytes,
	})
	for _, d := range deltaKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(d.Key)})
	}
	return elems, nil
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

	elems := make([]*kv.Elem[kv.OP], 0, 1+len(deltaKVs))
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   metaKey,
		Value: marshalBase(newLen),
	})
	for _, d := range deltaKVs {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: bytes.Clone(d.Key)})
	}
	return elems, nil
}

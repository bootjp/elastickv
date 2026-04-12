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
	defaultListCompactorInterval       = 30 * time.Second
	defaultListCompactorMaxDeltaCount  = 64
	defaultListCompactorMaxKeysPerTick = 256
	defaultListCompactorTimeout        = 5 * time.Second
)

// ListDeltaCompactor periodically scans for accumulated list Delta keys
// and folds them into the base metadata. This prevents unbounded delta
// accumulation and keeps resolveListMeta fast.
type ListDeltaCompactor struct {
	store       store.MVCCStore
	coordinator kv.Coordinator
	logger      *slog.Logger

	interval       time.Duration
	maxDeltaCount  int
	maxKeysPerTick int
	timeout        time.Duration

	// cursor tracks the position for incremental scanning.
	cursor []byte
}

// ListDeltaCompactorOption configures a ListDeltaCompactor.
type ListDeltaCompactorOption func(*ListDeltaCompactor)

func WithListCompactorInterval(d time.Duration) ListDeltaCompactorOption {
	return func(c *ListDeltaCompactor) {
		if d > 0 {
			c.interval = d
		}
	}
}

func WithListCompactorMaxDeltaCount(n int) ListDeltaCompactorOption {
	return func(c *ListDeltaCompactor) {
		if n > 0 {
			c.maxDeltaCount = n
		}
	}
}

func WithListCompactorLogger(l *slog.Logger) ListDeltaCompactorOption {
	return func(c *ListDeltaCompactor) {
		if l != nil {
			c.logger = l
		}
	}
}

// NewListDeltaCompactor creates a new compactor for list delta keys.
func NewListDeltaCompactor(st store.MVCCStore, coord kv.Coordinator, opts ...ListDeltaCompactorOption) *ListDeltaCompactor {
	c := &ListDeltaCompactor{
		store:          st,
		coordinator:    coord,
		logger:         slog.Default(),
		interval:       defaultListCompactorInterval,
		maxDeltaCount:  defaultListCompactorMaxDeltaCount,
		maxKeysPerTick: defaultListCompactorMaxKeysPerTick,
		timeout:        defaultListCompactorTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// Run starts the compactor loop. It blocks until ctx is cancelled.
func (c *ListDeltaCompactor) Run(ctx context.Context) error {
	timer := time.NewTimer(c.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := c.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Warn("list delta compactor tick failed", "error", err)
			}
			timer.Reset(c.interval)
		}
	}
}

// Tick performs one incremental scan pass over the delta key space,
// compacting any list that exceeds maxDeltaCount.
func (c *ListDeltaCompactor) Tick(ctx context.Context) error {
	if !c.coordinator.IsLeader() {
		return nil
	}

	start := c.scanStart()
	end := store.PrefixScanEnd([]byte(store.ListMetaDeltaPrefix))
	if end == nil {
		return nil
	}

	readTS := c.store.LastCommitTS()
	if readTS == 0 {
		return nil
	}

	entries, err := c.store.ScanAt(ctx, start, end, c.maxKeysPerTick, readTS)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(entries) == 0 {
		c.cursor = nil // wrap around
		return nil
	}

	// Advance cursor past the last scanned key.
	lastKey := entries[len(entries)-1].Key
	c.cursor = incrementKey(lastKey)

	// Group delta keys by user key.
	groups := groupDeltasByUserKey(entries)

	for userKey, deltaKeys := range groups {
		if len(deltaKeys) < c.maxDeltaCount {
			continue
		}
		if err := c.compactList(ctx, []byte(userKey), readTS); err != nil {
			c.logger.Warn("list delta compaction failed",
				"user_key", userKey,
				"error", err,
			)
		}
	}
	return nil
}

// compactList folds all deltas for a single list into its base metadata.
func (c *ListDeltaCompactor) compactList(ctx context.Context, userKey []byte, readTS uint64) error {
	// Read base metadata.
	baseMeta, _, err := loadListMetaFromStore(ctx, c.store, userKey, readTS)
	if err != nil {
		return err
	}

	// Scan all deltas for this key.
	prefix := store.ListMetaDeltaScanPrefix(userKey)
	deltas, err := c.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), maxDeltaScanLimit, readTS)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(deltas) == 0 {
		return nil
	}

	// Aggregate deltas into base metadata.
	for _, d := range deltas {
		delta, derr := store.UnmarshalListMetaDelta(d.Value)
		if derr != nil {
			return errors.WithStack(derr)
		}
		baseMeta.Head += delta.HeadDelta
		baseMeta.Len += delta.LenDelta
	}
	baseMeta.Tail = baseMeta.Head + baseMeta.Len

	// Build compaction transaction: write merged meta + delete deltas.
	metaBytes, err := store.MarshalListMeta(baseMeta)
	if err != nil {
		return errors.WithStack(err)
	}

	elems := make([]*kv.Elem[kv.OP], 0, len(deltas)+1)
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ListMetaKey(userKey),
		Value: metaBytes,
	})
	for _, d := range deltas {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:  kv.Del,
			Key: bytes.Clone(d.Key),
		})
	}

	compactCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	_, err = c.coordinator.Dispatch(compactCtx, &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: readTS,
		Elems:   elems,
	})
	if err != nil {
		return errors.Wrap(err, "compact list delta dispatch")
	}

	c.logger.Info("compacted list deltas",
		"user_key", string(userKey),
		"deltas_folded", len(deltas),
		"merged_len", baseMeta.Len,
	)
	return nil
}

func (c *ListDeltaCompactor) scanStart() []byte {
	if len(c.cursor) > 0 {
		return c.cursor
	}
	return []byte(store.ListMetaDeltaPrefix)
}

// loadListMetaFromStore reads the base list metadata directly from the store.
func loadListMetaFromStore(ctx context.Context, st store.MVCCStore, userKey []byte, readTS uint64) (store.ListMeta, bool, error) {
	val, err := st.GetAt(ctx, store.ListMetaKey(userKey), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.ListMeta{}, false, nil
		}
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	meta, err := store.UnmarshalListMeta(val)
	if err != nil {
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

// groupDeltasByUserKey groups delta scan entries by their extracted user key.
func groupDeltasByUserKey(entries []*store.KVPair) map[string][][]byte {
	groups := make(map[string][][]byte)
	for _, e := range entries {
		uk := store.ExtractListUserKeyFromDelta(e.Key)
		if uk == nil {
			continue
		}
		key := string(uk)
		groups[key] = append(groups[key], e.Key)
	}
	return groups
}

// incrementKey returns a key that is lexicographically just past k.
func incrementKey(k []byte) []byte {
	out := bytes.Clone(k)
	out = append(out, 0)
	return out
}

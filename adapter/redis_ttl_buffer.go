package adapter

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/kv"
)

const (
	defaultTTLFlushInterval  = 100 * time.Millisecond
	ttlShutdownFlushAttempts = 3
	ttlShutdownFlushBackoff  = 100 * time.Millisecond
	// Flush at most this many TTL updates per dispatch to avoid oversized raft ops.
	ttlFlushBatchMax = 4096
	// Rate-limit buffer-drop logs to the first drop and then each Nth drop.
	ttlBufferDropLogEvery = 1024
	// ttlBufferMaxSize is the maximum number of entries retained in the buffer.
	// Entries beyond this limit are dropped with a warning; the Raft store still
	// holds the last flushed value so the impact is limited to a missed update.
	ttlBufferMaxSize = 1_000_000
)

// ttlBufferEntry holds a pending TTL update for a single user key.
// expireAt == nil represents a TTL deletion (PERSIST semantics).
type ttlBufferEntry struct {
	expireAt *time.Time
	seq      uint64 // monotonically increasing; later writes win on the same key
}

// TTLBuffer is a thread-safe in-memory buffer for pending TTL writes.
// Entries are flushed to Raft in batches by a background goroutine, which
// eliminates TTL write conflicts from concurrent Lua script executions.
type TTLBuffer struct {
	mu      sync.RWMutex
	entries map[string]ttlBufferEntry
	counter atomic.Uint64
	dropped atomic.Uint64
	maxSize int
}

func newTTLBuffer() *TTLBuffer {
	return newTTLBufferWithMaxSize(ttlBufferMaxSize)
}

// newTTLBufferWithMaxSize exists for tests that need a smaller buffer limit.
// Non-positive maxSize falls back to ttlBufferMaxSize.
func newTTLBufferWithMaxSize(maxSize int) *TTLBuffer {
	if maxSize <= 0 {
		maxSize = ttlBufferMaxSize
	}
	return &TTLBuffer{
		entries: make(map[string]ttlBufferEntry),
		maxSize: maxSize,
	}
}

// Set writes a TTL entry for key into the buffer.
// expireAt == nil marks the TTL for deletion (PERSIST).
// Concurrent calls for the same key are resolved by keeping the latest seq.
// Set is a no-op on a nil *TTLBuffer.
// If the buffer already holds b.maxSize distinct keys and key is new,
// the entry is dropped with a warning; the Raft store retains the last flushed
// value so the impact is limited to a missed in-flight update.
func (b *TTLBuffer) Set(key []byte, expireAt *time.Time) {
	if b == nil {
		return
	}
	var (
		size   int
		keyLen int
	)
	s := b.counter.Add(1)
	b.mu.Lock()
	k := string(key)
	if _, exists := b.entries[k]; !exists && len(b.entries) >= b.maxSize {
		size = len(b.entries)
		keyLen = len(k)
		b.mu.Unlock()
		goto logDrop
	}
	b.entries[k] = ttlBufferEntry{expireAt: expireAt, seq: s}
	b.mu.Unlock()
	return

logDrop:
	// Log outside the mutex and rate-limit to avoid lock contention/log spam under overload.
	dropCount := b.dropped.Add(1)
	if dropCount == 1 || dropCount%ttlBufferDropLogEvery == 0 {
		slog.Warn("ttl buffer full, dropping entry", "size", size, "key_len", keyLen, "dropped", dropCount)
	}
}

// Get returns the buffered TTL for key.
// found=false means no buffered entry; callers should fall back to the Raft store.
// A nil expireAt with found=true means TTL was explicitly deleted (PERSIST).
// Get is safe to call on a nil *TTLBuffer and always returns (nil, false).
func (b *TTLBuffer) Get(key []byte) (expireAt *time.Time, found bool) {
	if b == nil {
		return nil, false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	e, ok := b.entries[string(key)]
	if !ok {
		return nil, false
	}
	return e.expireAt, true
}

// Drain atomically snapshots all buffer entries and resets the buffer.
// The returned map is owned by the caller. The buffer may accept new writes
// concurrently after this call returns.
func (b *TTLBuffer) Drain() map[string]ttlBufferEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) == 0 {
		return nil
	}
	snapshot := b.entries
	b.entries = make(map[string]ttlBufferEntry)
	return snapshot
}

// DrainN atomically snapshots up to limit entries and removes them from
// the buffer. If limit <= 0, all entries are drained.
func (b *TTLBuffer) DrainN(limit int) map[string]ttlBufferEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) == 0 {
		return nil
	}
	if limit <= 0 || limit >= len(b.entries) {
		snapshot := b.entries
		b.entries = make(map[string]ttlBufferEntry)
		return snapshot
	}
	snapshot := make(map[string]ttlBufferEntry, limit)
	count := 0
	for key, entry := range b.entries {
		snapshot[key] = entry
		delete(b.entries, key)
		count++
		if count >= limit {
			break
		}
	}
	return snapshot
}

// MergeBack re-inserts entries from a failed flush attempt.
// For each key, the entry is only restored if the buffer does not already hold
// a newer write (higher seq) for that key. Keys that are new to the current
// buffer are skipped once the buffer is full (same policy as Set), and dropped
// entries are counted/logged to make recovery-path loss observable.
func (b *TTLBuffer) MergeBack(entries map[string]ttlBufferEntry) {
	if len(entries) == 0 {
		return
	}
	b.mu.Lock()
	var dropped uint64
	var size int
	for key, entry := range entries {
		if e, ok := b.entries[key]; ok {
			if e.seq > entry.seq {
				continue // a newer write supersedes the failed entry
			}
		} else if len(b.entries) >= b.maxSize {
			dropped++
			continue // buffer full; drop the restored entry rather than growing unbounded
		}
		b.entries[key] = entry
	}
	size = len(b.entries)
	b.mu.Unlock()

	if dropped == 0 {
		return
	}
	dropCount := b.dropped.Add(dropped)
	prevDropCount := dropCount - dropped
	if prevDropCount == 0 || prevDropCount/ttlBufferDropLogEvery != dropCount/ttlBufferDropLogEvery {
		slog.Warn(
			"ttl buffer merge-back dropped entries",
			"size", size,
			"dropped", dropped,
			"total_dropped", dropCount,
		)
	}
}

// Len returns the number of buffered entries.
func (b *TTLBuffer) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.entries)
}

// buildTTLFlushElems converts a drained snapshot into Raft kv elements.
// Keys are sorted for deterministic Raft log ordering.
func buildTTLFlushElems(entries map[string]ttlBufferEntry) []*kv.Elem[kv.OP] {
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	elems := make([]*kv.Elem[kv.OP], 0, len(entries))
	for _, key := range keys {
		entry := entries[key]
		if entry.expireAt == nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey([]byte(key))})
		} else {
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   redisTTLKey([]byte(key)),
				Value: encodeRedisTTL(*entry.expireAt),
			})
		}
	}
	return elems
}

// runTTLFlusher periodically flushes the TTL buffer to Raft until ctx is cancelled.
// On cancellation it performs bounded final-flush retries to minimise TTL loss
// during shutdown and logs if entries remain unflushed.
func (r *RedisServer) runTTLFlusher(ctx context.Context) {
	ticker := time.NewTicker(r.ttlFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.flushTTLBuffer(ctx, false)
		case <-ctx.Done():
			for attempt := 1; attempt <= ttlShutdownFlushAttempts; attempt++ {
				if r.flushTTLBuffer(context.Background(), true) {
					return
				}
				if attempt < ttlShutdownFlushAttempts {
					time.Sleep(ttlShutdownFlushBackoff)
				}
			}
			if remaining := r.ttlBuffer.Len(); remaining > 0 {
				slog.Warn(
					"ttl buffer shutdown flush incomplete",
					"remaining_entries", remaining,
					"attempts", ttlShutdownFlushAttempts,
				)
			}
			return
		}
	}
}

// flushTTLBuffer drains the buffer and dispatches the entries to Raft as a
// non-transactional (last-writer-wins) operation group.
// On failure the entries are merged back into the buffer for retry on the next tick.
func (r *RedisServer) flushTTLBuffer(ctx context.Context, shutdown bool) bool {
	entries := r.ttlBuffer.DrainN(ttlFlushBatchMax)
	if len(entries) == 0 {
		return true
	}
	elems := buildTTLFlushElems(entries)
	flushCtx, cancel := context.WithTimeout(ctx, redisDispatchTimeout)
	defer cancel()
	_, err := r.coordinator.Dispatch(flushCtx, &kv.OperationGroup[kv.OP]{
		IsTxn: false, // no conflict detection; last writer wins
		Elems: elems,
	})
	if err != nil {
		// MergeBack on any error, including context.Canceled.
		// If a ticker tick races with shutdown (ctx already canceled), MergeBack
		// ensures those entries are not lost: the final shutdown flush uses
		// context.Background() and will retry them successfully.
		r.ttlBuffer.MergeBack(entries)
		if shutdown {
			slog.Warn("ttl buffer shutdown flush attempt failed", "err", err, "entries", len(entries))
		} else {
			slog.Warn("ttl buffer flush failed, will retry on next tick", "err", err, "entries", len(entries))
		}
		return false
	}
	return r.ttlBuffer.Len() == 0
}

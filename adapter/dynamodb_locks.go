package adapter

import (
	"context"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func (d *DynamoDBServer) lockItemUpdate(lockKey string) func() {
	idx := stripeIndex(lockKey, itemUpdateLockStripeCount)
	d.itemUpdateLocks[idx].Lock()
	return d.itemUpdateLocks[idx].Unlock
}

func (d *DynamoDBServer) lockTableOperations(tableNames []string) func() {
	if len(tableNames) == 0 {
		return func() {}
	}
	idxs := make([]int, 0, len(tableNames))
	seen := map[int]struct{}{}
	for _, tableName := range tableNames {
		idx := stripeIndex(tableName, tableLockStripeCount)
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	for _, idx := range idxs {
		d.tableLocks[idx].Lock()
	}
	return func() {
		for i := len(idxs) - 1; i >= 0; i-- {
			d.tableLocks[idxs[i]].Unlock()
		}
	}
}

func stripeIndex(key string, stripeCount uint32) int {
	if stripeCount == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % stripeCount)
}

func dynamoItemUpdateLockKey(tableName string, key map[string]attributeValue) (string, error) {
	parts := make([]string, 0, len(key))
	for name := range key {
		parts = append(parts, name)
	}
	sort.Strings(parts)
	var b strings.Builder
	b.WriteString(tableName)
	b.WriteByte('|')
	for _, name := range parts {
		val, err := attributeValueAsKey(key[name])
		if err != nil {
			return "", errors.WithStack(err)
		}
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(val)
		b.WriteByte('|')
	}
	return b.String(), nil
}

// globalLastCommitTSProvider is the interface satisfied by stores (e.g.
// LeaderRoutedStore) that can proxy the leader's LastCommitTS.  Defined as a
// local interface so DynamoDBServer avoids a hard dependency on the concrete
// kv type.
type globalLastCommitTSProvider interface {
	GlobalLastCommitTS(ctx context.Context) uint64
}

func (d *DynamoDBServer) nextTxnReadTS() uint64 {
	// On a follower the local store.LastCommitTS() may lag behind the leader.
	// Use GlobalLastCommitTS so ConsistentRead snapshots and transaction
	// start timestamps are aligned with the leader's committed watermark,
	// preventing stale pre-reads that cause false Jepsen anomalies and
	// unnecessary WriteConflict retries on every follower request.
	maxTS := uint64(0)
	if p, ok := d.store.(globalLastCommitTSProvider); ok {
		maxTS = p.GlobalLastCommitTS(context.Background())
	} else if d.store != nil {
		maxTS = d.store.LastCommitTS()
	}

	// Advance the HLC so subsequent commitTS calls produce values > maxTS,
	// but return maxTS directly as the snapshot — NOT clock.Next().
	//
	// clock.Next() can be ahead of store.LastCommitTS() because concurrent
	// dispatchTxn calls advance the HLC before their Raft entry is applied.
	// If readTS = clock.Next() = T and a concurrent write obtained
	// commitTS = T-1 (still in the Raft pipeline), the version at T-1 is
	// not yet in Pebble.  Reads would see stale data and the FSM conflict
	// check (latestTS > startTS: T-1 > T → false) would silently pass,
	// allowing corrupted writes.  Returning maxTS closes this gap: every
	// version at ≤ maxTS is guaranteed visible, and any concurrent write at
	// > maxTS triggers a WriteConflict and a retry.
	clock := d.coordinator.Clock()
	if clock != nil && maxTS > 0 {
		clock.Observe(maxTS)
	}
	if maxTS == 0 {
		return 1
	}
	return maxTS
}

func (d *DynamoDBServer) beginTxnReadTimestamp(ctx context.Context, label string) (kv.ReadTimestamp, error) {
	readTimestamp, err := kv.BeginReadTimestampThrough(ctx, d.coordinator, d.nextTxnReadTS(), label)
	return readTimestamp, errors.WithStack(err)
}

func (d *DynamoDBServer) pinReadTS(ts uint64) *kv.ActiveTimestampToken {
	if d == nil || d.readTracker == nil {
		return &kv.ActiveTimestampToken{}
	}
	return d.readTracker.Pin(ts)
}

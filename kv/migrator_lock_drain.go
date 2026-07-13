package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// TxnLockDrainEntry describes one prepared transaction lock that still belongs
// to a migration route. The lock key is cloned so callers can safely retain it
// across drain ticks.
type TxnLockDrainEntry struct {
	LockKey      []byte
	UserKey      []byte
	StartTS      uint64
	TTLExpireAt  uint64
	PrimaryKey   []byte
	IsPrimaryKey bool
}

// PendingTxnLocksInRoute scans the txn-lock namespace and filters each lock by
// routeKey(userKey). It intentionally does not bracket the scan by
// txnLockKey(routeStart)/txnLockKey(routeEnd): txn locks are sorted by raw user
// key while migration routes are defined in route-key space.
func PendingTxnLocksInRoute(ctx context.Context, st store.MVCCStore, routeStart, routeEnd []byte, ts uint64, limit int) ([]TxnLockDrainEntry, error) {
	if st == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = maxTxnLockScanResults
	}
	start := txnLockKey(nil)
	end := prefixScanEnd(start)
	filter := RouteKeyFilter(routeStart, routeEnd)
	cursor := start
	out := make([]TxnLockDrainEntry, 0, min(limit, lockPageLimit))

	for {
		lockKVs, nextCursor, done, err := scanTxnLockPageAt(ctx, st, cursor, end, ts)
		if err != nil {
			return nil, err
		}
		for _, kvp := range lockKVs {
			entry, ok, err := txnLockDrainEntry(kvp, filter)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			out = append(out, entry)
			if len(out) >= limit {
				return out, nil
			}
		}
		if done {
			return out, nil
		}
		cursor = nextCursor
	}
}

func txnLockDrainEntry(kvp *store.KVPair, filter func([]byte) bool) (TxnLockDrainEntry, bool, error) {
	if kvp == nil || !bytes.HasPrefix(kvp.Key, txnLockPrefixBytes) {
		return TxnLockDrainEntry{}, false, nil
	}
	userKey := kvp.Key[len(txnLockPrefixBytes):]
	if !filter(userKey) {
		return TxnLockDrainEntry{}, false, nil
	}
	lock, err := decodeTxnLock(kvp.Value)
	if err != nil {
		return TxnLockDrainEntry{}, false, errors.Wrap(err, "decode txn lock during migration drain")
	}
	return TxnLockDrainEntry{
		LockKey:      bytes.Clone(kvp.Key),
		UserKey:      bytes.Clone(userKey),
		StartTS:      lock.StartTS,
		TTLExpireAt:  lock.TTLExpireAt,
		PrimaryKey:   bytes.Clone(lock.PrimaryKey),
		IsPrimaryKey: lock.IsPrimaryKey,
	}, true, nil
}

package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (s *S3Server) readTS() uint64 {
	return snapshotTS(s.clock(), s.store)
}

func (s *SQSServer) nextTxnReadTS(ctx context.Context) uint64 {
	maxTS := uint64(0)
	if p, ok := s.store.(globalLastCommitTSProvider); ok {
		maxTS = p.GlobalLastCommitTS(ctx)
	} else if s.store != nil {
		maxTS = s.store.LastCommitTS()
	}
	if s.coordinator != nil {
		if clock := s.coordinator.Clock(); clock != nil && maxTS > 0 {
			clock.Observe(maxTS)
		}
	}
	if maxTS == 0 {
		return 1
	}
	return maxTS
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

func (r *RedisServer) readTS() uint64 {
	return snapshotTS(r.coordinator.Clock(), r.store)
}

func (r *RedisServer) txnStartTS() uint64 {
	// store.LastCommitTS() is the authoritative safe-snapshot watermark: it is
	// updated atomically only AFTER the corresponding Pebble batch commit, so
	// every version with commitTS ≤ store.LastCommitTS() is guaranteed visible
	// in the store when we read.
	//
	// We must NOT return clock.Next() here.  clock.Next() can be AHEAD of
	// store.LastCommitTS() because concurrent dispatchTxn calls advance the HLC
	// before their Raft entry is applied.  If startTS = clock.Next() = T, a
	// concurrent transaction that already called clock.Next() to obtain
	// commitTS = T-1 and is still in the Raft pipeline will satisfy
	//   latestTS(key) = T-1  ≤  T = startTS
	// causing the FSM conflict check (latestTS > startTS) to silently pass even
	// though we read stale data.  This allows two concurrent RPUSHes to pick the
	// same sequence number, with the second overwriting the first — a lost write.
	//
	// Using store.LastCommitTS() directly closes this gap: any concurrent commit
	// at > maxTS triggers a WriteConflict and a retry via retryRedisWrite.
	//
	// The Observe call still advances the HLC so that dispatchTxn's clock.Next()
	// produces a commitTS strictly greater than maxTS (leader-election safety).
	//
	// When maxTS is 0 (empty store) we return 1 so the coordinator treats this
	// as a valid startTS and does not override it with clock.Next() — which
	// could be ahead of unapplied Raft entries and reintroduce the anomaly.
	var maxTS uint64
	if r.store != nil {
		maxTS = r.store.LastCommitTS()
	}
	if r.coordinator != nil && r.coordinator.Clock() != nil && maxTS > 0 {
		r.coordinator.Clock().Observe(maxTS)
	}
	if maxTS == 0 {
		return 1
	}
	return maxTS
}

// txnStartTS returns the read/start timestamp for an S3 transaction.
// When the caller passes the sentinel ^uint64(0) ("server, please
// allocate"), the timestamp is drawn through the coordinator timestamp source.
// TSO-enabled deployments use the allocator path; the legacy fallback still
// applies the HLC-4 (iii) physical-ceiling fence because the returned value is
// persistence-grade (it pins MVCC reads and is the startTS half of a 2PC
// commit). Returning ErrCeilingExpired here lets the SigV4 handler surface the
// failure as an S3 5xx instead of silently issuing a stale-leader timestamp.
func (s *S3Server) txnStartTS(ctx context.Context, readTS uint64) (uint64, error) {
	if readTS != ^uint64(0) {
		return readTS, nil
	}
	ts, err := kv.NextTimestampThrough(ctx, s.coordinator, "s3: allocate txn startTS")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return ts, nil
}

func snapshotTS(clk *kv.HLC, st store.MVCCStore) uint64 { return eventualSnapshotTS(clk, st) }

// redisFencedSelectTS normalizes the empty-store watermark before it reaches
// the Phase-D boundary. snapshotTS answers ^uint64(0) for a store with no
// committed record -- "the latest of everything" for a direct MVCC read -- but
// BeginReadTimestampThrough rejects both 0 and ^uint64(0) as invalid read
// timestamps once Phase D is required, so reading a nonexistent key on a fresh
// cluster would return an error instead of an empty result, and Phase D could
// never activate from that path. 1 is the floor txnStartTS already uses for
// the same case: below every real commit, so the read still sees nothing.
func redisFencedSelectTS(selectTS func() uint64) uint64 {
	if selectTS == nil {
		return 1
	}
	ts := selectTS()
	if ts == 0 || ts == ^uint64(0) {
		return 1
	}
	return ts
}

// Existing version-inspection fixtures use an explicit historical horizon.
const consistentReadLatestTS = ^uint64(0) - 1

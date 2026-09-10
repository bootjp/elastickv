package s3keys

import "github.com/cockroachdb/errors"

// Reference-count mutation planning for the §3.5 blob GC.
//
// This is the decision layer the chunkref transaction calls: given the
// reference deltas a txn is about to apply and the reference-count
// records as of its read timestamp, it produces the exact set of
// additional mutations that txn must carry. It is deliberately pure —
// no store, no clock — so the atomic-pair semantics can be tested
// exhaustively without standing up a Raft group.
//
// The §3.5 invariant it enforces: the (chunkref change, RC update)
// pair is the linearisation point for "this blob is now / no longer
// reachable", and the GC queue must reflect *currently* RC==0 rather
// than *ever was* zero. That second clause is what forces a
// re-referencing txn to delete the existing queue entry rather than
// leaving it for the sweeper to re-validate.

// maxMutationsPerDelta is the most keys one SHA's delta can produce:
// its reference-count record, plus at most one GC-queue insert or
// delete. Named so the pre-size reads as the bound it is.
const maxMutationsPerDelta = 2

// ErrChunkRefRCUnderflow reports a decrement that would drive a
// reference count below zero.
//
// It fails the txn rather than clamping. A count that underflows means
// the caller's view of which chunkrefs exist disagrees with the stored
// record, and clamping to zero would queue a blob for deletion on the
// strength of that disagreement — turning a bookkeeping bug into data
// loss.
var ErrChunkRefRCUnderflow = errors.New("s3keys: chunkref reference count would underflow")

// ChunkRefDelta is one SHA's reference-count change within a txn.
//
// Added and Removed are counted separately rather than pre-netted so a
// txn that both adds and removes references to the same content — a
// part rewritten to identical bytes — is expressed honestly and nets
// to zero here instead of at the call site.
type ChunkRefDelta struct {
	ContentSHA256 [chunkBlobSHA256Bytes]byte
	Added         uint64
	Removed       uint64
}

// ChunkRefRCMutation is one key the txn must write or delete.
//
// Value is nil for a delete. Callers apply these alongside their own
// chunkref mutations in the SAME txn; applying them separately would
// break the linearisation point the design depends on.
type ChunkRefRCMutation struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// PlanChunkRefRCMutations computes the reference-count and GC-queue
// mutations a txn must carry for the given deltas.
//
// current maps a SHA to its reference-count record as of the txn's
// read timestamp; a SHA absent from the map is treated as count zero
// with no queue entry, which is the correct reading of a missing key.
//
// commitTS is the txn's commit timestamp, used as the eligibility
// timestamp for any SHA this txn drives to zero. It must be an HLC
// commit timestamp — see ChunkBlobGCQueueKey.
func PlanChunkRefRCMutations(
	deltas []ChunkRefDelta,
	current map[[chunkBlobSHA256Bytes]byte]ChunkRefRC,
	commitTS uint64,
) ([]ChunkRefRCMutation, error) {
	if commitTS == 0 {
		return nil, errors.Wrap(ErrInvalidChunkRefPlan, "commit timestamp is required")
	}
	out := make([]ChunkRefRCMutation, 0, len(deltas)*maxMutationsPerDelta)
	for _, delta := range deltas {
		planned, err := planOneChunkRefDelta(delta, current[delta.ContentSHA256], commitTS)
		if err != nil {
			return nil, err
		}
		out = append(out, planned...)
	}
	return out, nil
}

// ErrInvalidChunkRefPlan reports a malformed planning request.
var ErrInvalidChunkRefPlan = errors.New("s3keys: invalid chunkref reference-count plan")

func planOneChunkRefDelta(
	delta ChunkRefDelta, existing ChunkRefRC, commitTS uint64,
) ([]ChunkRefRCMutation, error) {
	if delta.Added == 0 && delta.Removed == 0 {
		// A net-zero delta still must not touch the queue: the blob's
		// reachability did not change, so neither should the record.
		return nil, nil
	}
	if delta.Removed > existing.Count+delta.Added {
		return nil, errors.Wrapf(ErrChunkRefRCUnderflow,
			"sha=%x count=%d added=%d removed=%d",
			delta.ContentSHA256[:4], existing.Count, delta.Added, delta.Removed)
	}
	next := ChunkRefRC{Count: existing.Count + delta.Added - delta.Removed}

	mutations := make([]ChunkRefRCMutation, 0, maxMutationsPerDelta)
	switch {
	case next.Count == 0:
		// Newly unreachable. Record WHEN so the sweeper's grace window
		// has a time signal, and queue it.
		//
		// An already-queued record keeps its original timestamp: the
		// blob has been continuously unreachable, and restamping it
		// would silently restart a grace period that was already
		// running.
		if existing.Queued() {
			next.QueuedAtTS = existing.QueuedAtTS
		} else {
			next.QueuedAtTS = commitTS
			mutations = append(mutations, ChunkRefRCMutation{
				Key:   ChunkBlobGCQueueKey(commitTS, delta.ContentSHA256),
				Value: []byte{},
			})
		}
	case existing.Queued():
		// Reachable again before the sweeper ran. The queue must
		// reflect CURRENTLY RC==0, so the entry goes away in this same
		// txn — which is only possible because the record carries the
		// timestamp the key was built from.
		mutations = append(mutations, ChunkRefRCMutation{
			Key:    ChunkBlobGCQueueKey(existing.QueuedAtTS, delta.ContentSHA256),
			Delete: true,
		})
	}

	return append(mutations, ChunkRefRCMutation{
		Key:   ChunkRefRCKey(delta.ContentSHA256),
		Value: EncodeChunkRefRC(next),
	}), nil
}

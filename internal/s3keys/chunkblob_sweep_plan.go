package s3keys

// Sweep classification for the §3.5 blob GC.
//
// This is the decision half of the node-local sweeper: given one queue
// entry and the reference-count record as of the sweeper's read
// timestamp, it says what the sweeper is permitted to do. Pure, so the
// correctness-critical classification is testable without a Raft group
// or a Pebble store; the two-phase execution (Raft conditional delete,
// then local unlink) is the caller's.
//
// Why the classification carries the weight: §3.5 notes that an
// UNCONDITIONAL queue delete would let the sweeper proceed to
// local-delete a chunkblob that is currently live — a correctness bug,
// not a space leak. The verdicts below are what the caller turns into
// a conditional Raft txn, so getting them wrong is exactly that bug.

// ChunkBlobSweepVerdict is what a sweeper may do with one queue entry.
type ChunkBlobSweepVerdict int

const (
	// SweepSkip leaves both the queue entry and the blob alone. Used
	// when the record cannot be trusted, so the sweeper declines
	// rather than guessing.
	SweepSkip ChunkBlobSweepVerdict = iota

	// SweepReclaim deletes the queue entry (conditionally, through
	// Raft) and then the local chunkblob. Only reachable when the
	// reference count is zero AND the record still points at THIS
	// queue entry.
	SweepReclaim

	// SweepDropQueueEntryOnly deletes the queue entry and leaves the
	// chunkblob in place. This is §3.5(c): the entry is stale, either
	// because the blob is referenced again or because a newer entry
	// supersedes this one.
	SweepDropQueueEntryOnly
)

func (v ChunkBlobSweepVerdict) String() string {
	switch v {
	case SweepReclaim:
		return "reclaim"
	case SweepDropQueueEntryOnly:
		return "drop_queue_entry_only"
	case SweepSkip:
		return "skip"
	default:
		return "unknown"
	}
}

// ChunkBlobSweepDecision is a verdict plus the reason behind it, so a
// sweeper can log and meter why a blob was or was not reclaimed
// without re-deriving the logic.
type ChunkBlobSweepDecision struct {
	Verdict ChunkBlobSweepVerdict
	Reason  string
}

// Reasons, a closed set so a sweeper can use them as a metric label.
const (
	SweepReasonUnreferenced     = "unreferenced"
	SweepReasonReferencedAgain  = "referenced_again"
	SweepReasonSupersededEntry  = "superseded_entry"
	SweepReasonRecordUnreadable = "record_unreadable"
	SweepReasonRecordNotQueued  = "record_not_queued"
)

// ClassifyChunkBlobSweep decides the fate of the queue entry stamped
// entryTS for this SHA.
//
// rcValue is the raw stored reference-count value, and rcFound reports
// whether the key existed. The raw bytes are taken rather than a
// decoded record so an undecodable value is distinguishable from an
// absent one: the first means the sweeper cannot reason about
// reachability and must decline, while the second is a legitimate
// "never referenced" state.
func ClassifyChunkBlobSweep(entryTS uint64, rcValue []byte, rcFound bool) ChunkBlobSweepDecision {
	if !rcFound {
		// No reference-count record at all. The blob is unreachable
		// through any chunkref, and the queue entry is the only thing
		// tracking it — reclaim. This is also the PUT-abort orphan
		// shape §3.5 describes, except those never reach the queue and
		// are the orphan scan's job instead.
		return ChunkBlobSweepDecision{Verdict: SweepReclaim, Reason: SweepReasonUnreferenced}
	}

	rc, ok := DecodeChunkRefRC(rcValue)
	if !ok {
		// A malformed count must never be read as zero: that would
		// reclaim a blob on the strength of corruption. Decline and
		// leave the entry for an operator.
		return ChunkBlobSweepDecision{Verdict: SweepSkip, Reason: SweepReasonRecordUnreadable}
	}

	if rc.Count > 0 {
		// §3.5(c): referenced again. The entry is stale — either a
		// re-reference txn failed to remove it or this sweeper raced
		// one. Drop the entry, keep the blob.
		return ChunkBlobSweepDecision{
			Verdict: SweepDropQueueEntryOnly,
			Reason:  SweepReasonReferencedAgain,
		}
	}

	switch {
	case !rc.Queued():
		// Count is zero but the record claims no queue entry. The
		// entry cannot be matched to the record, so reclaiming on it
		// would be acting on state the record does not corroborate.
		return ChunkBlobSweepDecision{Verdict: SweepSkip, Reason: SweepReasonRecordNotQueued}
	case rc.QueuedAtTS != entryTS:
		// A newer queueing superseded this entry: the blob went
		// unreferenced, was referenced again, and went unreferenced
		// once more. The later entry owns the grace window, so this
		// one is garbage — but the BLOB is not, because the newer
		// entry has not served its own grace yet.
		return ChunkBlobSweepDecision{
			Verdict: SweepDropQueueEntryOnly,
			Reason:  SweepReasonSupersededEntry,
		}
	default:
		return ChunkBlobSweepDecision{Verdict: SweepReclaim, Reason: SweepReasonUnreferenced}
	}
}

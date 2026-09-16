package s3keys

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
)

// The §3.5 node-local sweeper loop.
//
// Each node runs this independently; correctness across nodes comes
// from the Raft-replicated queue key, whose single-writer-per-key
// property serialises concurrent sweepers — only the sweeper whose
// conditional delete commits proceeds to the local phase.
//
// The collaborators are narrow interfaces rather than a store handle
// so the loop's ordering guarantees are testable without a Raft group
// or Pebble.
const (
	// DefaultChunkBlobGCInterval is the §3.5 proposed sweep cadence.
	DefaultChunkBlobGCInterval = 5 * time.Minute

	// DefaultChunkBlobGCGracePeriod is how long a blob must sit
	// unreferenced before it may be reclaimed. It has to exceed the
	// longest window in which a live reader could still be holding a
	// chunkref it read before the dereferencing txn committed.
	DefaultChunkBlobGCGracePeriod = time.Hour
)

// ErrQueueEntryChanged reports that a conditional queue delete lost
// its precondition: the entry was already gone, or the reference count
// is no longer zero.
//
// This is the load-bearing error of the whole design. §3.5 is explicit
// that an UNCONDITIONAL delete would silently succeed on an
// already-absent entry and let the sweeper go on to local-delete a
// chunkblob that is currently live. Receiving this means another actor
// won the race and the sweeper MUST NOT touch the blob.
var ErrQueueEntryChanged = errors.New("s3keys: gc queue entry changed before the conditional delete")

// ChunkBlobGCQueueEntry is one entry returned by a queue scan.
type ChunkBlobGCQueueEntry struct {
	CommitTS      uint64
	ContentSHA256 [chunkBlobSHA256Bytes]byte
}

// ChunkBlobSweepStore is the replicated half: the GC queue and the
// reference counts, both read and written through Raft.
type ChunkBlobSweepStore interface {
	// ScanGCQueue returns up to limit queue entries from [startKey, endKey),
	// plus the key to resume from when more remain.
	//
	// Paged, because the eligible range is unbounded: an outage or a large
	// object-deletion workload can leave millions of expired entries, and
	// materialising them all before the sweeper reclaims even the first one
	// can OOM the process -- leaving the backlog permanently untouched,
	// which is the opposite of what a GC pass is for.
	//
	// Each PAGE must still be all-or-error. A short page is fine -- the
	// remaining entries are picked up by the continuation key or the next
	// pass -- but a page that silently truncated while reporting success
	// would hide a persistent backlog.
	//
	// nextStartKey is nil when the range is exhausted.
	ScanGCQueue(
		ctx context.Context, startKey, endKey []byte, limit int,
	) (entries []ChunkBlobGCQueueEntry, nextStartKey []byte, err error)

	// ReadChunkRefRC returns the raw reference-count value and
	// whether the key exists. Raw bytes, so the classifier can tell
	// an undecodable record from an absent one.
	ReadChunkRefRC(ctx context.Context, sha [chunkBlobSHA256Bytes]byte) ([]byte, bool, error)

	// DeleteGCQueueEntryIfUnreferenced deletes the queue entry AND the
	// zero-count reference record, in ONE txn, only if the entry still
	// exists and the count is still zero; ErrQueueEntryChanged otherwise.
	// Concurrent sweepers serialise here on the queue key's write-write
	// conflict.
	//
	// The RC record goes with the entry because nothing else ever removed
	// it: the planner persists a count-zero record when the last reference
	// drops, and reclamation deleted only the queue entry and the local
	// payload. A workload that creates and deletes unique chunks therefore
	// left one permanent Raft-replicated key per content hash, so the live
	// MVCC state and every snapshot grew without bound despite blob GC
	// working correctly.
	//
	// Atomically with the entry, not as a second txn: a crash between two
	// txns would leave either a queue entry for a blob with no RC record,
	// or the same unbounded leak this removes.
	DeleteGCQueueEntryIfUnreferenced(ctx context.Context, entry ChunkBlobGCQueueEntry) error

	// DeleteGCQueueEntry deletes the entry unconditionally. Used only
	// for the §3.5(c) stale-entry path, where the blob is explicitly
	// being left in place.
	DeleteGCQueueEntry(ctx context.Context, entry ChunkBlobGCQueueEntry) error

	// ClearGCQueueMetadata deletes the entry AND clears QueuedAtTS on the
	// live RC record in ONE transaction.
	//
	// Atomic because the two halves are only consistent together. Between
	// them the record would claim a queue entry that does not exist, and a
	// dereference landing in that window takes
	// PlanChunkRefRCMutations' already-queued branch: it preserves the
	// stale timestamp and writes no replacement queue key, stranding the
	// blob outside the queue permanently.
	ClearGCQueueMetadata(ctx context.Context, entry ChunkBlobGCQueueEntry) error
}

// maxRetainedSweepFailures caps how many per-entry errors one pass keeps for
// its joined return value. Every failure is still counted; only the detail is
// sampled, because a backlog large enough to matter is also large enough for
// one error per entry to be the memory problem rather than the diagnosis.
// Exported so a test can assert the cap actually bites rather than
// hard-coding a number that could drift past the fixture size.
const MaxRetainedSweepFailures = 64

// ChunkBlobLocalStore is the node-local half: the chunkblob payload in
// Pebble, never written through Raft.
type ChunkBlobLocalStore interface {
	// ChunkBlobWrittenAt returns when this node wrote the payload, as an
	// HLC timestamp, and whether it is present. Read BEFORE the Raft
	// phase so the unlink afterwards can be conditioned on the state the
	// sweep actually observed.
	ChunkBlobWrittenAt(ctx context.Context, sha [chunkBlobSHA256Bytes]byte) (uint64, bool, error)

	// DeleteChunkBlobIfUnchanged unlinks the blob only if it is still the
	// payload written at writtenAtTS, reporting false when it is not.
	//
	// Conditional because DeleteGCQueueEntryIfUnreferenced guarantees a
	// zero reference count only up to ITS OWN commit. A PUT that reuses
	// the SHA immediately afterwards re-anchors the payload and commits a
	// reference, and an unconditional unlink then removes bytes that PUT
	// has already acknowledged as durable. Same contract as
	// ChunkBlobOrphanLocalStore: the comparison and the unlink must be
	// atomic with respect to the local writer.
	DeleteChunkBlobIfUnchanged(
		ctx context.Context, sha [chunkBlobSHA256Bytes]byte, writtenAtTS uint64,
	) (bool, error)
}

// ChunkBlobSweepObserver receives per-entry outcomes.
type ChunkBlobSweepObserver interface {
	ObserveChunkBlobSweep(verdict ChunkBlobSweepVerdict, reason string)
	ObserveChunkBlobSweepRaceLost()
}

type nopSweepObserver struct{}

func (nopSweepObserver) ObserveChunkBlobSweep(ChunkBlobSweepVerdict, string) {}
func (nopSweepObserver) ObserveChunkBlobSweepRaceLost()                      {}

// ChunkBlobSweeper reclaims chunkblobs whose references are gone.
type ChunkBlobSweeper struct {
	store    ChunkBlobSweepStore
	local    ChunkBlobLocalStore
	grace    time.Duration
	interval time.Duration
	nowTS    func() uint64
	observer ChunkBlobSweepObserver
	logger   *slog.Logger
	pageSize int
}

// DefaultChunkBlobGCPageSize bounds one queue-scan page.
//
// Chosen so the sweeper's memory is independent of the backlog: a
// million-entry queue becomes slow to drain rather than impossible to load.
const DefaultChunkBlobGCPageSize = 1024

// ChunkBlobSweeperOptions configures NewChunkBlobSweeper.
//
// NowTS returns the current HLC timestamp — not a wall clock — because
// the queue keys are stamped with commit timestamps and the grace
// boundary must be computed in that domain.
type ChunkBlobSweeperOptions struct {
	Store       ChunkBlobSweepStore
	Local       ChunkBlobLocalStore
	GracePeriod time.Duration
	Interval    time.Duration
	NowTS       func() uint64
	Observer    ChunkBlobSweepObserver
	Logger      *slog.Logger
	// PageSize bounds one queue-scan page; zero uses
	// DefaultChunkBlobGCPageSize.
	PageSize int
}

func NewChunkBlobSweeper(opts ChunkBlobSweeperOptions) (*ChunkBlobSweeper, error) {
	switch {
	case opts.Store == nil:
		return nil, errors.Wrap(ErrInvalidChunkRefPlan, "chunkblob sweeper requires a replicated store")
	case opts.Local == nil:
		return nil, errors.Wrap(ErrInvalidChunkRefPlan, "chunkblob sweeper requires a local blob store")
	case opts.NowTS == nil:
		return nil, errors.Wrap(ErrInvalidChunkRefPlan, "chunkblob sweeper requires an HLC clock")
	}
	timing := resolveGCLoopTiming(
		opts.GracePeriod, opts.Interval,
		DefaultChunkBlobGCGracePeriod, DefaultChunkBlobGCInterval, opts.Logger)
	observer := opts.Observer
	if observer == nil {
		observer = nopSweepObserver{}
	}
	return &ChunkBlobSweeper{
		store:    opts.Store,
		local:    opts.Local,
		grace:    timing.grace,
		interval: timing.interval,
		nowTS:    opts.NowTS,
		observer: observer,
		logger:   timing.logger,
		pageSize: pageSizeOrDefault(opts.PageSize),
	}, nil
}

// pageSizeOrDefault defaults and floors the scan page size. A non-positive
// value would make the scan return nothing and the sweeper spin without
// reclaiming, so it is treated as "unset".
func pageSizeOrDefault(size int) int {
	if size <= 0 {
		return DefaultChunkBlobGCPageSize
	}
	return size
}

// gcLoopTiming is the cadence/logging configuration both GC loops
// share. Factored out because the sweeper and the orphan scanner
// only in their defaults, and duplicating the resolution invites the
// two from drifting apart.
type gcLoopTiming struct {
	grace    time.Duration
	interval time.Duration
	logger   *slog.Logger
}

// resolveGCLoopTiming applies the caller's values, falling back to the
// supplied defaults for anything non-positive.
func resolveGCLoopTiming(
	grace, interval, defaultGrace, defaultInterval time.Duration, logger *slog.Logger,
) gcLoopTiming {
	out := gcLoopTiming{grace: grace, interval: interval, logger: logger}
	if out.grace <= 0 {
		out.grace = defaultGrace
	}
	if out.interval <= 0 {
		out.interval = defaultInterval
	}
	if out.logger == nil {
		out.logger = slog.Default()
	}
	return out
}

// Run sweeps on the configured interval until ctx is cancelled.
//
// A failing pass is retried next tick rather than tearing the loop
// down: the queue is durable, so a transient store error costs a delay,
// never a lost reclaim.
func (s *ChunkBlobSweeper) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.Wrap(ErrInvalidChunkRefPlan, "chunkblob sweeper context is required")
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if sweepCancelled(ctx) {
			return nil
		}
		// A failing pass is logged and retried next tick: the queue is
		// durable, so a transient store error costs a delay rather
		// than a lost reclaim. Cancellation is handled above and in
		// the select, so it is never reported as a sweep failure.
		if err := s.SweepOnce(ctx); err != nil && !sweepCancelled(ctx) {
			s.logger.WarnContext(ctx, "chunkblob gc sweep failed",
				slog.String("error", err.Error()))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// SweepOnce runs one pass over the entries whose grace window has
// elapsed.
func (s *ChunkBlobSweeper) SweepOnce(ctx context.Context) error {
	boundary := ChunkBlobGCGraceBoundary(s.nowTS(), s.grace)
	if boundary == 0 {
		// Nothing can have served a full grace window yet.
		return nil
	}
	return s.sweepPages(ctx, boundary)
}

// sweepPages walks the eligible range a page at a time.
//
// A bounded page keeps the sweeper's memory independent of the backlog, so a
// million-entry queue is slow to drain rather than fatal to load.
func (s *ChunkBlobSweeper) sweepPages(ctx context.Context, boundary uint64) error {
	var (
		failures     []error
		failureCount int
		scanned      int
		startKey     = ChunkBlobGCQueueScanStart()
		endKey       = ChunkBlobGCQueueScanEnd(boundary)
	)
	// Cancellation ends the walk between pages; the remaining entries stay
	// queued for the next pass, so an interrupted sweep costs a delay rather
	// than a lost reclaim.
	for !sweepCancelled(ctx) {
		entries, next, err := s.store.ScanGCQueue(ctx, startKey, endKey, s.pageSize)
		if err != nil {
			failures = append(failures, errors.Wrap(err, "chunkblob gc: scan queue"))
			failureCount++
			break
		}
		scanned += len(entries)
		pageFailures, cancelled := s.sweepPage(ctx, entries)
		failureCount += len(pageFailures)
		// Counted in full, retained as a bounded sample. Paging the scan
		// bounded the entries held at once but not the errors: a large
		// eligible backlog hitting a broad per-entry failure -- RC reads
		// failing while queue scans keep working -- accumulated one wrapped
		// error per queued entry across every page, so a million-entry
		// backlog could still exhaust memory before the join.
		if room := MaxRetainedSweepFailures - len(failures); room > 0 {
			failures = append(failures, pageFailures[:min(room, len(pageFailures))]...)
		}
		if cancelled || len(next) == 0 {
			break
		}
		startKey = next
	}
	if failureCount > 0 {
		return errors.Wrapf(errors.Join(failures...),
			"chunkblob gc: %d failures over %d queue entries (showing %d)",
			failureCount, scanned, len(failures))
	}
	return nil
}

// sweepPage sweeps one page, returning its failures and whether the context was
// cancelled mid-page.
func (s *ChunkBlobSweeper) sweepPage(
	ctx context.Context, entries []ChunkBlobGCQueueEntry,
) ([]error, bool) {
	// Per-entry failures are collected, not returned immediately. The scan
	// is time-ordered, so one entry that persistently fails its RC read,
	// its stale-entry delete or its conditional Raft delete came back
	// first on every pass and starved every later entry indefinitely. The
	// pass still reports failure afterwards. Same rule as the orphan scan.
	var failures []error
	for _, entry := range entries {
		// Stop cleanly on cancellation: the remaining entries stay
		// queued and the next pass picks them up, so an interrupted
		// sweep costs a delay rather than a lost reclaim.
		if sweepCancelled(ctx) {
			return failures, true
		}
		if err := s.sweepEntry(ctx, entry); err != nil {
			s.logger.WarnContext(ctx, "chunkblob gc: entry failed, continuing",
				slog.String("err", err.Error()))
			failures = append(failures, err)
		}
	}
	return failures, false
}

// sweepEntry classifies and executes one entry.
func (s *ChunkBlobSweeper) sweepEntry(ctx context.Context, entry ChunkBlobGCQueueEntry) error {
	rcValue, found, err := s.store.ReadChunkRefRC(ctx, entry.ContentSHA256)
	if err != nil {
		return errors.Wrapf(err, "chunkblob gc: read reference count for %x", entry.ContentSHA256[:4])
	}
	decision := ClassifyChunkBlobSweep(entry.CommitTS, rcValue, found)
	s.observer.ObserveChunkBlobSweep(decision.Verdict, decision.Reason)

	switch decision.Verdict {
	case SweepSkip:
		s.logger.WarnContext(ctx, "chunkblob gc declined to sweep",
			slog.String("reason", decision.Reason))
		return nil
	case SweepDropQueueEntryOnly:
		if err := s.store.DeleteGCQueueEntry(ctx, entry); err != nil {
			return errors.Wrapf(err, "chunkblob gc: drop stale queue entry for %x", entry.ContentSHA256[:4])
		}
		return nil
	case SweepClearStaleQueueMetadata:
		if err := s.store.ClearGCQueueMetadata(ctx, entry); err != nil {
			return errors.Wrapf(err, "chunkblob gc: clear stale queue metadata for %x", entry.ContentSHA256[:4])
		}
		return nil
	case SweepReclaim:
		return s.reclaim(ctx, entry)
	default:
		return nil
	}
}

// reclaim runs the two phases in the order §3.5 mandates: the Raft
// conditional delete FIRST, the local unlink second.
//
// The ordering is the load-bearing detail. Local-first would leave a
// crash window in which the blob is gone locally but the queue entry
// survives, so every later pass re-attempts a no-op local delete and
// the entry never clears without manual intervention. Raft-first
// inverts that into a bounded local space leak — the entry is gone but
// the blob is still on disk — which the orphan scan reclaims.
func (s *ChunkBlobSweeper) reclaim(ctx context.Context, entry ChunkBlobGCQueueEntry) error {
	// Observed BEFORE the Raft phase, so it describes the payload this
	// sweep decided about rather than whatever is on disk afterwards.
	writtenAtTS, present, err := s.local.ChunkBlobWrittenAt(ctx, entry.ContentSHA256)
	if err != nil {
		return errors.Wrapf(err, "chunkblob gc: stat local blob %x", entry.ContentSHA256[:4])
	}
	// A payload written at or after the moment this blob became reclaimable
	// belongs to a LATER PUT, whose reference may not have committed yet.
	//
	// DeleteChunkBlobIfUnchanged cannot catch that on its own: it only
	// detects a write that lands after this stat. A PUT that re-anchors the
	// SHA just BEFORE the stat and commits its chunkref afterwards passes
	// every check -- the stat sees the PUT's new timestamp, the conditional
	// Raft delete still reads the old zero count, and the conditional unlink
	// matches the very timestamp the PUT wrote. The bytes go, and the PUT
	// then commits a reference to nothing.
	//
	// Comparing against the queue entry's own commit timestamp closes that:
	// a blob that legitimately became unreferenced at entry.CommitTS was
	// written strictly before it. Leaving the entry in place is safe -- the
	// next pass reads the PUT's committed count and takes the stale path.
	if present && writtenAtTS >= entry.CommitTS {
		s.observer.ObserveChunkBlobSweepRaceLost()
		return nil
	}
	if err := s.store.DeleteGCQueueEntryIfUnreferenced(ctx, entry); err != nil {
		if errors.Is(err, ErrQueueEntryChanged) {
			// Another sweeper won, or a re-reference txn committed
			// between the classification and here. Either way the blob
			// may now be live: do NOT touch it.
			s.observer.ObserveChunkBlobSweepRaceLost()
			return nil
		}
		return errors.Wrapf(err, "chunkblob gc: conditional queue delete for %x", entry.ContentSHA256[:4])
	}
	if !present {
		// Nothing local to unlink; the queue entry is now gone, which is
		// the whole remaining work.
		return nil
	}
	// The conditional delete committing proves the reference count was
	// zero through ITS commit window -- and no further. A PUT reusing this
	// SHA can re-anchor the payload and commit a reference immediately
	// afterwards, so the unlink stays conditional on the state observed
	// above; a mismatch means exactly that happened.
	unlinked, err := s.local.DeleteChunkBlobIfUnchanged(ctx, entry.ContentSHA256, writtenAtTS)
	if err != nil {
		return errors.Wrapf(err, "chunkblob gc: local delete for %x", entry.ContentSHA256[:4])
	}
	if !unlinked {
		// A normal concurrent-update conflict, not a failure: the payload
		// was re-anchored and is live again. The queue entry is gone, and
		// the re-anchoring PUT owns the new reference.
		s.observer.ObserveChunkBlobSweepRaceLost()
		s.logger.InfoContext(ctx, "chunkblob gc: payload re-anchored under the sweep; left in place")
	}
	return nil
}

// sweepCancelled reports whether ctx is done. It exists as a bool
// predicate so the cancellation checks above read as control flow
// rather than as error handling — cancellation is an orderly stop, and
// the remaining queue entries are picked up by the next pass.
func sweepCancelled(ctx context.Context) bool {
	return ctx.Err() != nil
}

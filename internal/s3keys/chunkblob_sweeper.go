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
	// ScanGCQueue returns every queue entry in [startKey, endKey).
	// It must be all-or-error: a partial scan would simply delay
	// entries to the next pass, which is safe, but a scan that
	// silently truncated mid-range while reporting success would hide
	// a persistent backlog.
	ScanGCQueue(ctx context.Context, startKey, endKey []byte) ([]ChunkBlobGCQueueEntry, error)

	// ReadChunkRefRC returns the raw reference-count value and
	// whether the key exists. Raw bytes, so the classifier can tell
	// an undecodable record from an absent one.
	ReadChunkRefRC(ctx context.Context, sha [chunkBlobSHA256Bytes]byte) ([]byte, bool, error)

	// DeleteGCQueueEntryIfUnreferenced deletes the queue entry only if
	// it still exists AND the reference count is still zero, returning
	// ErrQueueEntryChanged otherwise. Concurrent sweepers serialise
	// here on the queue key's write-write conflict.
	DeleteGCQueueEntryIfUnreferenced(ctx context.Context, entry ChunkBlobGCQueueEntry) error

	// DeleteGCQueueEntry deletes the entry unconditionally. Used only
	// for the §3.5(c) stale-entry path, where the blob is explicitly
	// being left in place.
	DeleteGCQueueEntry(ctx context.Context, entry ChunkBlobGCQueueEntry) error
}

// ChunkBlobLocalStore is the node-local half: the chunkblob payload in
// Pebble, never written through Raft.
type ChunkBlobLocalStore interface {
	DeleteChunkBlob(ctx context.Context, sha [chunkBlobSHA256Bytes]byte) error
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
}

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
	}, nil
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
	entries, err := s.store.ScanGCQueue(ctx,
		ChunkBlobGCQueueScanStart(), ChunkBlobGCQueueScanEnd(boundary))
	if err != nil {
		return errors.Wrap(err, "chunkblob gc: scan queue")
	}
	for _, entry := range entries {
		// Stop cleanly on cancellation: the remaining entries stay
		// queued and the next pass picks them up, so an interrupted
		// sweep costs a delay rather than a lost reclaim.
		if sweepCancelled(ctx) {
			break
		}
		if err := s.sweepEntry(ctx, entry); err != nil {
			return err
		}
	}
	return nil
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
	// Reaching here means the conditional delete committed, which
	// implies the reference count was zero at its read timestamp and
	// stayed zero through its commit window — the blob is genuinely
	// unreachable.
	if err := s.local.DeleteChunkBlob(ctx, entry.ContentSHA256); err != nil {
		return errors.Wrapf(err, "chunkblob gc: local delete for %x", entry.ContentSHA256[:4])
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

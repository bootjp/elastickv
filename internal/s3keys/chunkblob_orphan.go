package s3keys

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
)

// The §3.5 orphan scan: the safety net behind two paths the queue scan
// structurally cannot see.
//
//   - A sweeper that crashed between the Raft conditional delete and
//     the local unlink. The queue entry is gone, so no queue scan will
//     ever revisit it, but the blob is still on disk.
//   - A PUT that wrote the chunkblob to local Pebble and then aborted
//     before dispatching its chunkref (admission 503, client
//     disconnect, push quorum failure, context cancel). Neither an RC
//     entry nor a queue entry was ever written.
//
// Because the second case is indistinguishable from an IN-FLIGHT PUT —
// the bytes land before the chunkref commits, so a healthy upload
// briefly has no RC entry at all — the scan is gated on the blob's own
// age. Without that gate it would delete the payload out from under
// every concurrent upload.
const (
	// DefaultChunkBlobOrphanScanInterval is the §3.5 proposed cadence.
	// Deliberately far longer than the sweep interval: this is a
	// safety net for crash paths, not a routine reclaim.
	DefaultChunkBlobOrphanScanInterval = time.Hour

	// DefaultChunkBlobOrphanGracePeriod is how long a local blob must
	// have existed before an absent RC entry is read as an abort
	// rather than an upload in progress. It must exceed the longest
	// plausible interval between writing chunkblob bytes and
	// committing the chunkref that references them.
	DefaultChunkBlobOrphanGracePeriod = 6 * time.Hour
)

// ChunkBlobOrphanVerdict is what the scan may do with one local blob.
type ChunkBlobOrphanVerdict int

const (
	// OrphanKeep leaves the blob alone.
	OrphanKeep ChunkBlobOrphanVerdict = iota
	// OrphanReclaim deletes the local blob. There is no Raft phase:
	// by construction an orphan has no replicated state referring to
	// it, which is exactly what makes it an orphan.
	OrphanReclaim
)

func (v ChunkBlobOrphanVerdict) String() string {
	if v == OrphanReclaim {
		return "reclaim"
	}
	return "keep"
}

// Reasons, a closed set suitable for a metric label.
const (
	OrphanReasonNoReferenceRecord = "no_reference_record"
	OrphanReasonSweeperCrashed    = "sweeper_crashed"
	OrphanReasonWithinGrace       = "within_grace"
	OrphanReasonReferenced        = "referenced"
	OrphanReasonQueueOwnsIt       = "queue_owns_it"
	OrphanReasonRecordUnreadable  = "record_unreadable"
)

// ChunkBlobOrphanDecision is a verdict plus its reason.
type ChunkBlobOrphanDecision struct {
	Verdict ChunkBlobOrphanVerdict
	Reason  string
}

// LocalChunkBlob is one blob found on local disk.
type LocalChunkBlob struct {
	ContentSHA256 [chunkBlobSHA256Bytes]byte
	// WrittenAtTS is when this node wrote the payload, as an HLC
	// timestamp. It is the blob's own age, not a reference timestamp:
	// the whole point of the grace gate is that a young blob with no
	// RC entry is an upload in progress.
	WrittenAtTS uint64
}

// ClassifyChunkBlobOrphan applies the §3.5 detection criterion plus the
// age gate.
//
// rcValue/rcFound are the raw reference-count record; queueFound
// reports whether a GC queue entry exists for this SHA. Raw bytes
// again, so an undecodable record stays distinguishable from an absent
// one — the first means reachability is unknown and the scan must
// decline, the second is the legitimate never-referenced state this
// scan exists to clean up.
func ClassifyChunkBlobOrphan(
	blob LocalChunkBlob, boundaryTS uint64, rcValue []byte, rcFound, queueFound bool,
) ChunkBlobOrphanDecision {
	// Age gate first: it is the cheapest check and the one that
	// protects in-flight uploads, so nothing else should be able to
	// reach a reclaim verdict ahead of it.
	if blob.WrittenAtTS == 0 || blob.WrittenAtTS >= boundaryTS {
		return ChunkBlobOrphanDecision{Verdict: OrphanKeep, Reason: OrphanReasonWithinGrace}
	}

	if !rcFound {
		// §3.5 case two: the PUT never dispatched its chunkref, and
		// the blob is old enough that it cannot still be in flight.
		return ChunkBlobOrphanDecision{
			Verdict: OrphanReclaim,
			Reason:  OrphanReasonNoReferenceRecord,
		}
	}

	rc, ok := DecodeChunkRefRC(rcValue)
	if !ok {
		return ChunkBlobOrphanDecision{Verdict: OrphanKeep, Reason: OrphanReasonRecordUnreadable}
	}
	if rc.Count > 0 {
		return ChunkBlobOrphanDecision{Verdict: OrphanKeep, Reason: OrphanReasonReferenced}
	}
	if queueFound {
		// The sweeper owns this one: it has a live queue entry serving
		// its grace window, and reclaiming here would bypass the
		// conditional-delete interlock the sweeper relies on.
		return ChunkBlobOrphanDecision{Verdict: OrphanKeep, Reason: OrphanReasonQueueOwnsIt}
	}
	// §3.5 case one: count zero and no queue entry means a sweeper
	// removed the entry through Raft and then died before the local
	// unlink.
	return ChunkBlobOrphanDecision{Verdict: OrphanReclaim, Reason: OrphanReasonSweeperCrashed}
}

// ChunkBlobOrphanStore is the replicated state the scan consults.
type ChunkBlobOrphanStore interface {
	ReadChunkRefRC(ctx context.Context, sha [chunkBlobSHA256Bytes]byte) ([]byte, bool, error)
	// GCQueueEntryExists reports whether any queue entry references
	// this SHA. The scan only needs presence, not the timestamp, so
	// this stays a cheaper question than a range scan.
	GCQueueEntryExists(ctx context.Context, sha [chunkBlobSHA256Bytes]byte) (bool, error)
}

// ChunkBlobOrphanLocalStore is the node-local half.
type ChunkBlobOrphanLocalStore interface {
	// ListLocalChunkBlobs enumerates this node's chunkblobs.
	ListLocalChunkBlobs(ctx context.Context) ([]LocalChunkBlob, error)
	DeleteChunkBlob(ctx context.Context, sha [chunkBlobSHA256Bytes]byte) error
}

// ChunkBlobOrphanObserver receives per-blob outcomes.
type ChunkBlobOrphanObserver interface {
	ObserveChunkBlobOrphan(verdict ChunkBlobOrphanVerdict, reason string)
}

type nopOrphanObserver struct{}

func (nopOrphanObserver) ObserveChunkBlobOrphan(ChunkBlobOrphanVerdict, string) {}

// ChunkBlobOrphanScanner reclaims local blobs no replicated state
// refers to.
type ChunkBlobOrphanScanner struct {
	store    ChunkBlobOrphanStore
	local    ChunkBlobOrphanLocalStore
	grace    time.Duration
	interval time.Duration
	nowTS    func() uint64
	observer ChunkBlobOrphanObserver
	logger   *slog.Logger
}

// ChunkBlobOrphanScannerOptions configures NewChunkBlobOrphanScanner.
type ChunkBlobOrphanScannerOptions struct {
	Store       ChunkBlobOrphanStore
	Local       ChunkBlobOrphanLocalStore
	GracePeriod time.Duration
	Interval    time.Duration
	NowTS       func() uint64
	Observer    ChunkBlobOrphanObserver
	Logger      *slog.Logger
}

func NewChunkBlobOrphanScanner(opts ChunkBlobOrphanScannerOptions) (*ChunkBlobOrphanScanner, error) {
	switch {
	case opts.Store == nil:
		return nil, errors.Wrap(ErrInvalidChunkRefPlan, "orphan scanner requires a replicated store")
	case opts.Local == nil:
		return nil, errors.Wrap(ErrInvalidChunkRefPlan, "orphan scanner requires a local blob store")
	case opts.NowTS == nil:
		return nil, errors.Wrap(ErrInvalidChunkRefPlan, "orphan scanner requires an HLC clock")
	}
	timing := resolveGCLoopTiming(
		opts.GracePeriod, opts.Interval,
		DefaultChunkBlobOrphanGracePeriod, DefaultChunkBlobOrphanScanInterval, opts.Logger)
	observer := opts.Observer
	if observer == nil {
		observer = nopOrphanObserver{}
	}
	return &ChunkBlobOrphanScanner{
		store:    opts.Store,
		local:    opts.Local,
		grace:    timing.grace,
		interval: timing.interval,
		nowTS:    opts.NowTS,
		observer: observer,
		logger:   timing.logger,
	}, nil
}

// Run scans on the configured interval until ctx is cancelled.
func (s *ChunkBlobOrphanScanner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.Wrap(ErrInvalidChunkRefPlan, "orphan scanner context is required")
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if sweepCancelled(ctx) {
			return nil
		}
		if err := s.ScanOnce(ctx); err != nil && !sweepCancelled(ctx) {
			s.logger.WarnContext(ctx, "chunkblob orphan scan failed",
				slog.String("error", err.Error()))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ScanOnce runs one pass over this node's local chunkblobs.
func (s *ChunkBlobOrphanScanner) ScanOnce(ctx context.Context) error {
	boundary := ChunkBlobGCGraceBoundary(s.nowTS(), s.grace)
	if boundary == 0 {
		// No blob can be older than the grace window yet, so every
		// local blob could still belong to an upload in progress.
		return nil
	}
	blobs, err := s.local.ListLocalChunkBlobs(ctx)
	if err != nil {
		return errors.Wrap(err, "orphan scan: list local chunkblobs")
	}
	for _, blob := range blobs {
		if sweepCancelled(ctx) {
			break
		}
		if err := s.scanBlob(ctx, blob, boundary); err != nil {
			return err
		}
	}
	return nil
}

func (s *ChunkBlobOrphanScanner) scanBlob(
	ctx context.Context, blob LocalChunkBlob, boundary uint64,
) error {
	// The age gate needs no replicated reads, so check it before
	// paying for them: on a healthy node most blobs are referenced and
	// this keeps the scan's cost proportional to real orphans.
	if blob.WrittenAtTS == 0 || blob.WrittenAtTS >= boundary {
		s.observer.ObserveChunkBlobOrphan(OrphanKeep, OrphanReasonWithinGrace)
		return nil
	}

	rcValue, rcFound, err := s.store.ReadChunkRefRC(ctx, blob.ContentSHA256)
	if err != nil {
		return errors.Wrapf(err, "orphan scan: read reference count for %x", blob.ContentSHA256[:4])
	}
	queueFound := false
	if rcFound {
		// Only consulted when a record exists: with no record at all
		// the §3.5 criterion is already satisfied and the extra read
		// would be wasted.
		queueFound, err = s.store.GCQueueEntryExists(ctx, blob.ContentSHA256)
		if err != nil {
			return errors.Wrapf(err, "orphan scan: queue lookup for %x", blob.ContentSHA256[:4])
		}
	}

	decision := ClassifyChunkBlobOrphan(blob, boundary, rcValue, rcFound, queueFound)
	s.observer.ObserveChunkBlobOrphan(decision.Verdict, decision.Reason)
	if decision.Verdict != OrphanReclaim {
		return nil
	}
	if err := s.local.DeleteChunkBlob(ctx, blob.ContentSHA256); err != nil {
		return errors.Wrapf(err, "orphan scan: delete local blob %x", blob.ContentSHA256[:4])
	}
	s.logger.InfoContext(ctx, "chunkblob orphan reclaimed",
		slog.String("reason", decision.Reason))
	return nil
}

package s3keys

import (
	"context"
	"log/slog"
	"math"
	"sync"
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

// chunkRefRCIsZero reports whether the RC record decodes to a count of zero.
//
// This is the only state in which the GC-queue lookup can change the verdict:
// an absent record is already reclaimable, a positive count is already
// retained, and an undecodable one is already kept. Callers use it to avoid
// paying for a replicated read that cannot matter.
func chunkRefRCIsZero(rcValue []byte, rcFound bool) bool {
	if !rcFound {
		return false
	}
	rc, ok := DecodeChunkRefRC(rcValue)
	return ok && rc.Count == 0
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
	// DeleteChunkBlobIfUnchanged unlinks the blob only if it is still the
	// payload written at writtenAtTS, reporting false when it is not.
	//
	// Conditional, not a plain delete, because a PUT that reuses this SHA
	// re-anchors the local payload before committing its chunkref: a
	// rewrite therefore moves WrittenAtTS and this refuses, sparing bytes
	// the PUT has already acknowledged as durable. An unconditional unlink
	// had no interlock at all -- the scan's reads are from before the PUT
	// committed, so it would remove a now-live payload.
	//
	// The implementation must compare and unlink atomically with respect to
	// the local writer; a read-then-delete would reopen the same window it
	// exists to close.
	DeleteChunkBlobIfUnchanged(
		ctx context.Context, sha [chunkBlobSHA256Bytes]byte, writtenAtTS uint64,
	) (bool, error)
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

	// marks is the two-pass sweep state, the same shape the snapshot-offload
	// retention GC uses. The first pass that finds a blob reclaimable records
	// what it saw; only a LATER pass that finds the same blob still
	// reclaimable and unchanged may unlink it.
	//
	// The delay is the protection. The scan's RC read happens before a
	// concurrent PUT commits its chunkref, so a single-pass scan could decide
	// "orphan" and then unlink bytes the PUT had just referenced. Requiring a
	// second pass means any reference committed between the two is read back
	// as a positive count and the blob is spared.
	//
	// The state is in-memory and per-process. Losing it on restart is safe in
	// the only direction that matters: reclamation is delayed by one more
	// pass, never advanced.
	marksMu sync.Mutex
	marks   map[[chunkBlobSHA256Bytes]byte]orphanMark
}

// orphanMark records what a pass saw when it first found a blob reclaimable.
type orphanMark struct {
	atTS        uint64
	writtenAtTS uint64
	reason      string
}

// matches reports whether blob is the same payload that was marked. A
// different WrittenAtTS means a PUT rewrote it since, which invalidates the
// mark outright.
func (m orphanMark) matches(blob LocalChunkBlob) bool {
	return m.writtenAtTS == blob.WrittenAtTS
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
		marks:    make(map[[chunkBlobSHA256Bytes]byte]orphanMark),
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
	// Per-blob failures are collected, not returned immediately. With a
	// stable listing order, returning here ended every hourly pass at the
	// same blob, so one blob that consistently failed an RC read, a queue
	// lookup or an unlink starved every later orphan indefinitely and local
	// disk grew without bound. The pass still reports failure afterwards, so
	// the error is surfaced rather than swallowed.
	var failures []error
	for _, blob := range blobs {
		if sweepCancelled(ctx) {
			break
		}
		if err := s.scanBlob(ctx, blob, boundary); err != nil {
			s.logger.WarnContext(ctx, "chunkblob orphan scan: blob failed, continuing",
				slog.String("err", err.Error()))
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		return errors.Wrapf(errors.Join(failures...),
			"orphan scan: %d of %d blobs failed", len(failures), len(blobs))
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
	if chunkRefRCIsZero(rcValue, rcFound) {
		// Consulted ONLY for a decodable zero count, because that is the
		// only state where the answer can change the verdict. Gating on
		// rcFound alone meant every old, healthy, still-referenced blob
		// paid for a replicated queue read before the classifier looked
		// at its count -- making the hourly scan's read cost
		// proportional to the whole retained dataset instead of to the
		// blobs that could actually be orphans.
		queueFound, err = s.store.GCQueueEntryExists(ctx, blob.ContentSHA256)
		if err != nil {
			return errors.Wrapf(err, "orphan scan: queue lookup for %x", blob.ContentSHA256[:4])
		}
	}

	decision := ClassifyChunkBlobOrphan(blob, boundary, rcValue, rcFound, queueFound)
	s.observer.ObserveChunkBlobOrphan(decision.Verdict, decision.Reason)
	if decision.Verdict != OrphanReclaim {
		s.dropMark(blob.ContentSHA256)
		return nil
	}
	if !s.reclaimable(blob, decision.Reason) {
		// Marked on this pass; a later pass decides. Counted as a keep so
		// the metric does not report a reclaim that has not happened.
		return nil
	}
	unlinked, err := s.local.DeleteChunkBlobIfUnchanged(ctx, blob.ContentSHA256, blob.WrittenAtTS)
	if err != nil {
		return errors.Wrapf(err, "orphan scan: delete local blob %x", blob.ContentSHA256[:4])
	}
	s.dropMark(blob.ContentSHA256)
	if !unlinked {
		// The payload was rewritten between the listing and the unlink, so
		// a PUT re-anchored it. Leaving it is the whole point of the
		// condition.
		s.logger.InfoContext(ctx, "chunkblob orphan spared: payload changed under the scan",
			slog.String("reason", decision.Reason))
		return nil
	}
	s.logger.InfoContext(ctx, "chunkblob orphan reclaimed",
		slog.String("reason", decision.Reason))
	return nil
}

// reclaimable implements the two-pass rule: true only when this blob was
// marked on an earlier pass, is unchanged since, and the mark has aged at
// least one scan interval. Otherwise it (re-)marks and reports false.
func (s *ChunkBlobOrphanScanner) reclaimable(blob LocalChunkBlob, reason string) bool {
	now := s.nowTS()

	s.marksMu.Lock()
	defer s.marksMu.Unlock()

	mark, marked := s.marks[blob.ContentSHA256]
	if !marked || !mark.matches(blob) {
		s.marks[blob.ContentSHA256] = orphanMark{
			atTS:        now,
			writtenAtTS: blob.WrittenAtTS,
			reason:      reason,
		}
		return false
	}
	// One full scan interval, measured in the HLC physical domain so it
	// compares against the same timestamps the age gate uses.
	return hlcElapsed(mark.atTS, now) >= s.interval
}

// hlcElapsed returns the wall time between two HLC timestamps from their
// physical halves. The logical counter is in-memory only and represents no
// duration, so it is deliberately discarded.
func hlcElapsed(fromTS, toTS uint64) time.Duration {
	fromMs := fromTS >> hlcLogicalBits
	toMs := toTS >> hlcLogicalBits
	if toMs <= fromMs {
		return 0
	}
	deltaMs := toMs - fromMs
	// A delta this large cannot arise inside one process lifetime; clamping
	// keeps the conversion to a signed Duration total rather than wrapping.
	if deltaMs > math.MaxInt64/uint64(time.Millisecond) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(deltaMs) * time.Millisecond //nolint:gosec // clamped above
}

func (s *ChunkBlobOrphanScanner) dropMark(sha [chunkBlobSHA256Bytes]byte) {
	s.marksMu.Lock()
	defer s.marksMu.Unlock()
	delete(s.marks, sha)
}

// MarkedOrphans returns the SHAs currently held under a sweep mark. Exposed
// for tests and operator tooling; the set is per-process.
func (s *ChunkBlobOrphanScanner) MarkedOrphans() [][chunkBlobSHA256Bytes]byte {
	s.marksMu.Lock()
	defer s.marksMu.Unlock()

	out := make([][chunkBlobSHA256Bytes]byte, 0, len(s.marks))
	for sha := range s.marks {
		out = append(out, sha)
	}
	return out
}

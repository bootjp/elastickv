package s3keys_test

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	orphanNowMs = uint64(1_700_000_000_000)
	orphanNowTS = orphanNowMs << 16
	orphanGrace = 6 * time.Hour
	// orphanScanInterval is the mark-aging window the two-pass rule uses,
	// with its millisecond twin for advancing the test clock.
	orphanScanInterval   = time.Hour
	orphanScanIntervalMs = uint64(60 * 60 * 1000)

	// hlcLogicalBitsForTest mirrors the package's hlcLogicalBits; the
	// external test cannot reach the unexported constant.
	hlcLogicalBitsForTest = 16
	orphanGraceMs         = uint64(6 * 60 * 60 * 1000)
	orphanBoundary        = (orphanNowMs - orphanGraceMs) << 16
)

// oldBlob is written comfortably before the grace boundary.
func oldBlob(seed string) s3keys.LocalChunkBlob {
	return s3keys.LocalChunkBlob{
		ContentSHA256: testSHA(seed),
		WrittenAtTS:   orphanBoundary - (1 << 16),
	}
}

// TestClassifyChunkBlobOrphanCoversBothDocumentedSources is the §3.5
// detection criterion plus the age gate the criterion implies but does
// not state.
func TestClassifyChunkBlobOrphanCoversBothDocumentedSources(t *testing.T) {
	t.Parallel()

	zeroRC := s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0})
	liveRC := s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 2})

	tests := []struct {
		name       string
		blob       s3keys.LocalChunkBlob
		rc         []byte
		rcFound    bool
		queueFound bool
		wantVerb   s3keys.ChunkBlobOrphanVerdict
		wantReason string
	}{
		{
			name:       "put aborted before chunkref dispatch",
			blob:       oldBlob("aborted"),
			rcFound:    false,
			wantVerb:   s3keys.OrphanReclaim,
			wantReason: s3keys.OrphanReasonNoReferenceRecord,
		},
		{
			name:       "sweeper crashed after the raft phase",
			blob:       oldBlob("crashed"),
			rc:         zeroRC,
			rcFound:    true,
			queueFound: false,
			wantVerb:   s3keys.OrphanReclaim,
			wantReason: s3keys.OrphanReasonSweeperCrashed,
		},
		{
			name:       "still referenced",
			blob:       oldBlob("live"),
			rc:         liveRC,
			rcFound:    true,
			wantVerb:   s3keys.OrphanKeep,
			wantReason: s3keys.OrphanReasonReferenced,
		},
		{
			name:       "queue entry still owns it",
			blob:       oldBlob("queued"),
			rc:         zeroRC,
			rcFound:    true,
			queueFound: true,
			wantVerb:   s3keys.OrphanKeep,
			wantReason: s3keys.OrphanReasonQueueOwnsIt,
		},
		{
			name:       "unreadable record",
			blob:       oldBlob("corrupt"),
			rc:         []byte{0x01},
			rcFound:    true,
			wantVerb:   s3keys.OrphanKeep,
			wantReason: s3keys.OrphanReasonRecordUnreadable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := s3keys.ClassifyChunkBlobOrphan(
				tc.blob, orphanBoundary, tc.rc, tc.rcFound, tc.queueFound)
			require.Equal(t, tc.wantVerb, got.Verdict)
			require.Equal(t, tc.wantReason, got.Reason)
		})
	}
}

// TestClassifyChunkBlobOrphanProtectsInFlightUploads is the guard the
// §3.5 text does not spell out but the PUT path requires: chunkblob
// bytes land BEFORE the chunkref commits, so a healthy upload briefly
// looks exactly like the abort case. Without the age gate the scan
// would delete the payload out from under every concurrent PUT.
func TestClassifyChunkBlobOrphanProtectsInFlightUploads(t *testing.T) {
	t.Parallel()

	for _, writtenAt := range []uint64{
		orphanBoundary,             // exactly at the boundary
		orphanBoundary + (1 << 16), // just inside
		orphanNowTS,                // written this instant
	} {
		blob := s3keys.LocalChunkBlob{ContentSHA256: testSHA("in-flight"), WrittenAtTS: writtenAt}
		got := s3keys.ClassifyChunkBlobOrphan(blob, orphanBoundary, nil, false, false)
		require.Equal(t, s3keys.OrphanKeep, got.Verdict,
			"a blob written at %d must not be reclaimed", writtenAt)
		require.Equal(t, s3keys.OrphanReasonWithinGrace, got.Reason)
	}
}

// TestClassifyChunkBlobOrphanKeepsABlobWithAnUnknownAge pins that a
// missing write timestamp is treated as "too young to judge" rather
// than as epoch-old, which would reclaim it immediately.
func TestClassifyChunkBlobOrphanKeepsABlobWithAnUnknownAge(t *testing.T) {
	t.Parallel()

	blob := s3keys.LocalChunkBlob{ContentSHA256: testSHA("no-timestamp")}
	got := s3keys.ClassifyChunkBlobOrphan(blob, orphanBoundary, nil, false, false)
	require.Equal(t, s3keys.OrphanKeep, got.Verdict)
	require.Equal(t, s3keys.OrphanReasonWithinGrace, got.Reason)
}

// fakeOrphanStore is the replicated half.
type fakeOrphanStore struct {
	rc         map[[32]byte][]byte
	queued     map[[32]byte]bool
	rcReads    int
	queueReads int
	// rcErrs fails the RC read for specific SHAs, modelling a blob that
	// consistently fails every pass.
	rcErrs map[[32]byte]error
}

func (f *fakeOrphanStore) ReadChunkRefRC(_ context.Context, sha [32]byte) ([]byte, bool, error) {
	f.rcReads++
	if err, failing := f.rcErrs[sha]; failing {
		return nil, false, err
	}
	v, ok := f.rc[sha]
	return v, ok, nil
}

func (f *fakeOrphanStore) GCQueueEntryExists(_ context.Context, sha [32]byte) (bool, error) {
	f.queueReads++
	return f.queued[sha], nil
}

type conditionalDeleteCall struct {
	sha         [32]byte
	writtenAtTS uint64
}

type fakeOrphanLocal struct {
	blobs   []s3keys.LocalChunkBlob
	deleted [][32]byte
	// calls records every conditional-delete attempt, refused ones
	// included, so a test can tell "never attempted" from "attempted and
	// refused".
	calls []conditionalDeleteCall
	// changedSince names SHAs whose payload was rewritten after the
	// listing, modelling a PUT that re-anchored the blob.
	changedSince map[[32]byte]struct{}
	// deleteErr fails every conditional delete.
	deleteErr error
}

func (f *fakeOrphanLocal) ListLocalChunkBlobs(_ context.Context) ([]s3keys.LocalChunkBlob, error) {
	return f.blobs, nil
}

func (f *fakeOrphanLocal) DeleteChunkBlobIfUnchanged(
	_ context.Context, sha [32]byte, writtenAtTS uint64,
) (bool, error) {
	f.calls = append(f.calls, conditionalDeleteCall{sha: sha, writtenAtTS: writtenAtTS})
	if f.deleteErr != nil {
		return false, f.deleteErr
	}
	if _, changed := f.changedSince[sha]; changed {
		return false, nil
	}
	f.deleted = append(f.deleted, sha)
	return true, nil
}

// orphanClock is a movable HLC clock, so the two-pass tests can age a mark
// past the scan interval without sleeping.
type orphanClock struct{ ts uint64 }

func (c *orphanClock) now() uint64 { return c.ts }

// advanceMillis moves the clock forward. Milliseconds rather than a
// time.Duration so there is no signed-to-unsigned conversion to justify.
func (c *orphanClock) advanceMillis(ms uint64) {
	c.ts += ms << hlcLogicalBitsForTest
}

func newOrphanScannerWithClock(
	t *testing.T, store *fakeOrphanStore, local *fakeOrphanLocal, clock *orphanClock,
) *s3keys.ChunkBlobOrphanScanner {
	t.Helper()
	s, err := s3keys.NewChunkBlobOrphanScanner(s3keys.ChunkBlobOrphanScannerOptions{
		Store:       store,
		Local:       local,
		GracePeriod: orphanGrace,
		Interval:    orphanScanInterval,
		NowTS:       clock.now,
	})
	require.NoError(t, err)
	return s
}

// scanTwice runs the two passes the mark-and-sweep rule requires, advancing the
// clock past the scan interval in between so the mark is old enough to act on.
func scanTwice(t *testing.T, s *s3keys.ChunkBlobOrphanScanner, clock *orphanClock) {
	t.Helper()
	require.NoError(t, s.ScanOnce(context.Background()))
	clock.advanceMillis(orphanScanIntervalMs)
	require.NoError(t, s.ScanOnce(context.Background()))
}

func newOrphanScanner(t *testing.T, store *fakeOrphanStore, local *fakeOrphanLocal) *s3keys.ChunkBlobOrphanScanner {
	t.Helper()
	s, err := s3keys.NewChunkBlobOrphanScanner(s3keys.ChunkBlobOrphanScannerOptions{
		Store:       store,
		Local:       local,
		GracePeriod: orphanGrace,
		NowTS:       func() uint64 { return orphanNowTS },
	})
	require.NoError(t, err)
	return s
}

func TestOrphanScannerReclaimsOnlyTheOrphans(t *testing.T) {
	t.Parallel()

	aborted := oldBlob("aborted")
	crashed := oldBlob("crashed")
	live := oldBlob("live")
	queued := oldBlob("queued")
	young := s3keys.LocalChunkBlob{ContentSHA256: testSHA("young"), WrittenAtTS: orphanNowTS}

	store := &fakeOrphanStore{
		rc: map[[32]byte][]byte{
			crashed.ContentSHA256: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0}),
			live.ContentSHA256:    s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 1}),
			queued.ContentSHA256:  s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0}),
			young.ContentSHA256:   s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0}),
		},
		queued: map[[32]byte]bool{queued.ContentSHA256: true},
	}
	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{aborted, crashed, live, queued, young}}

	// Two passes: the first marks, the second unlinks. See the two-pass
	// rationale on ChunkBlobOrphanScanner.marks.
	clock := &orphanClock{ts: orphanNowTS}
	scanTwice(t, newOrphanScannerWithClock(t, store, local, clock), clock)

	require.ElementsMatch(t,
		[][32]byte{aborted.ContentSHA256, crashed.ContentSHA256},
		local.deleted,
		"only the two §3.5 orphan shapes may be reclaimed")
}

// TestOrphanScannerSkipsReplicatedReadsForYoungBlobs pins that the age
// gate runs before the replicated lookups. On a healthy node most
// blobs are young or referenced, so paying two reads for each would
// make the scan's cost proportional to total blobs rather than to real
// orphans.
func TestOrphanScannerSkipsReplicatedReadsForYoungBlobs(t *testing.T) {
	t.Parallel()

	store := &fakeOrphanStore{}
	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{
		{ContentSHA256: testSHA("a"), WrittenAtTS: orphanNowTS},
		{ContentSHA256: testSHA("b"), WrittenAtTS: orphanNowTS},
	}}

	require.NoError(t, newOrphanScanner(t, store, local).ScanOnce(context.Background()))
	require.Zero(t, store.rcReads, "a young blob must not cost a replicated read")
	require.Zero(t, store.queueReads)
	require.Empty(t, local.deleted)
}

// TestOrphanScannerSkipsTheQueueLookupWhenNoRecordExists pins the other
// read-avoidance: with no RC record the §3.5 criterion is already
// satisfied, so the queue lookup would be wasted.
func TestOrphanScannerSkipsTheQueueLookupWhenNoRecordExists(t *testing.T) {
	t.Parallel()

	blob := oldBlob("aborted")
	store := &fakeOrphanStore{}
	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{blob}}

	clock := &orphanClock{ts: orphanNowTS}
	scanTwice(t, newOrphanScannerWithClock(t, store, local, clock), clock)
	require.Equal(t, 2, store.rcReads, "one RC read per pass")
	require.Zero(t, store.queueReads)
	require.Equal(t, [][32]byte{blob.ContentSHA256}, local.deleted)
}

// TestOrphanScannerReclaimsNothingBeforeTheFirstGraceWindow covers a
// freshly started cluster, where now-grace underflows to the epoch and
// every local blob could still be an upload in progress.
func TestOrphanScannerReclaimsNothingBeforeTheFirstGraceWindow(t *testing.T) {
	t.Parallel()

	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{oldBlob("whatever")}}
	s, err := s3keys.NewChunkBlobOrphanScanner(s3keys.ChunkBlobOrphanScannerOptions{
		Store:       &fakeOrphanStore{},
		Local:       local,
		GracePeriod: orphanGrace,
		NowTS:       func() uint64 { return uint64(1_000) << 16 },
	})
	require.NoError(t, err)

	require.NoError(t, s.ScanOnce(context.Background()))
	require.Empty(t, local.deleted)
}

func TestNewChunkBlobOrphanScannerValidatesItsCollaborators(t *testing.T) {
	t.Parallel()

	valid := s3keys.ChunkBlobOrphanScannerOptions{
		Store: &fakeOrphanStore{},
		Local: &fakeOrphanLocal{},
		NowTS: func() uint64 { return 1 << 16 },
	}
	for _, mutate := range []func(*s3keys.ChunkBlobOrphanScannerOptions){
		func(o *s3keys.ChunkBlobOrphanScannerOptions) { o.Store = nil },
		func(o *s3keys.ChunkBlobOrphanScannerOptions) { o.Local = nil },
		func(o *s3keys.ChunkBlobOrphanScannerOptions) { o.NowTS = nil },
	} {
		opts := valid
		mutate(&opts)
		_, err := s3keys.NewChunkBlobOrphanScanner(opts)
		require.Error(t, err)
	}
	_, err := s3keys.NewChunkBlobOrphanScanner(valid)
	require.NoError(t, err)
}

func TestChunkBlobOrphanVerdictStringsAreStable(t *testing.T) {
	t.Parallel()

	require.Equal(t, "reclaim", s3keys.OrphanReclaim.String())
	require.Equal(t, "keep", s3keys.OrphanKeep.String())
}

// TestOrphanScannerNeedsTwoPassesBeforeUnlinking is the interlock for a PUT
// that reuses an old SHA.
//
// The scan's RC read happens before such a PUT commits its chunkref, so a
// single-pass scanner could read zero, decide "orphan", and unlink bytes the
// PUT had just referenced and acknowledged as durable. The age gate cannot
// help: the payload really is old, it is the reference that is new.
//
// Requiring a second pass means a reference committed between the two is read
// back as a positive count, and the blob is spared.
func TestOrphanScannerNeedsTwoPassesBeforeUnlinking(t *testing.T) {
	t.Parallel()

	blob := oldBlob("reused")
	store := &fakeOrphanStore{}
	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{blob}}
	clock := &orphanClock{ts: orphanNowTS}
	scanner := newOrphanScannerWithClock(t, store, local, clock)

	require.NoError(t, scanner.ScanOnce(context.Background()))
	require.Empty(t, local.calls,
		"the first pass may only mark; unlinking on the first sighting is the race")
	require.Equal(t, [][32]byte{blob.ContentSHA256}, scanner.MarkedOrphans())

	// The PUT commits its chunkref in the window between the passes.
	store.rc = map[[32]byte][]byte{
		blob.ContentSHA256: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 1}),
	}

	clock.advanceMillis(orphanScanIntervalMs)
	require.NoError(t, scanner.ScanOnce(context.Background()))
	require.Empty(t, local.calls,
		"a reference committed between passes must spare the payload")
	require.Empty(t, scanner.MarkedOrphans(),
		"a blob that is no longer reclaimable must lose its mark")
}

// A mark younger than one scan interval must not be acted on, or the delay that
// gives a concurrent PUT time to commit does not exist.
func TestOrphanScannerWaitsOutTheMarkInterval(t *testing.T) {
	t.Parallel()

	blob := oldBlob("waiting")
	store := &fakeOrphanStore{}
	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{blob}}
	clock := &orphanClock{ts: orphanNowTS}
	scanner := newOrphanScannerWithClock(t, store, local, clock)

	require.NoError(t, scanner.ScanOnce(context.Background()))
	clock.advanceMillis(orphanScanIntervalMs / 2)
	require.NoError(t, scanner.ScanOnce(context.Background()))
	require.Empty(t, local.calls, "the mark has not aged a full interval yet")

	clock.advanceMillis(orphanScanIntervalMs)
	require.NoError(t, scanner.ScanOnce(context.Background()))
	require.Equal(t, [][32]byte{blob.ContentSHA256}, local.deleted)
}

// A payload rewritten after the listing must survive: the conditional unlink is
// the second half of the interlock, for a PUT that re-anchors the blob inside
// the gap between the pass's reads and the unlink itself.
func TestOrphanScannerSparesAPayloadRewrittenUnderIt(t *testing.T) {
	t.Parallel()

	blob := oldBlob("rewritten")
	store := &fakeOrphanStore{}
	local := &fakeOrphanLocal{
		blobs:        []s3keys.LocalChunkBlob{blob},
		changedSince: map[[32]byte]struct{}{blob.ContentSHA256: {}},
	}
	clock := &orphanClock{ts: orphanNowTS}
	scanTwice(t, newOrphanScannerWithClock(t, store, local, clock), clock)

	require.Len(t, local.calls, 1, "the unlink must be attempted")
	require.Equal(t, blob.WrittenAtTS, local.calls[0].writtenAtTS,
		"the condition must name the payload state the scan actually observed")
	require.Empty(t, local.deleted, "a rewritten payload must not be unlinked")
}

// TestOrphanScannerContinuesPastAFailingBlob is the starvation regression.
//
// With a stable listing order, returning on the first per-blob error ended
// every hourly pass at the same blob, so one blob that consistently failed its
// RC read, queue lookup or unlink starved every later orphan indefinitely and
// local disk grew without bound.
func TestOrphanScannerContinuesPastAFailingBlob(t *testing.T) {
	t.Parallel()

	failing := oldBlob("failing")
	reclaimable := oldBlob("reclaimable")
	store := &fakeOrphanStore{
		rcErrs: map[[32]byte]error{failing.ContentSHA256: errors.New("pebble: read failed")},
	}
	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{failing, reclaimable}}
	clock := &orphanClock{ts: orphanNowTS}
	scanner := newOrphanScannerWithClock(t, store, local, clock)

	// Both passes report the failure, and both still process the rest.
	require.Error(t, scanner.ScanOnce(context.Background()))
	clock.advanceMillis(orphanScanIntervalMs)
	err := scanner.ScanOnce(context.Background())
	require.Error(t, err, "the pass must still report the per-blob failure")

	require.Equal(t, [][32]byte{reclaimable.ContentSHA256}, local.deleted,
		"a blob listed after the failing one must still be reclaimed")
}

// A failing unlink must not stop the pass either.
func TestOrphanScannerContinuesPastAFailingUnlink(t *testing.T) {
	t.Parallel()

	first := oldBlob("first")
	second := oldBlob("second")
	store := &fakeOrphanStore{}
	local := &fakeOrphanLocal{
		blobs:     []s3keys.LocalChunkBlob{first, second},
		deleteErr: errors.New("unlink: permission denied"),
	}
	clock := &orphanClock{ts: orphanNowTS}
	scanner := newOrphanScannerWithClock(t, store, local, clock)

	require.NoError(t, scanner.ScanOnce(context.Background()))
	clock.advanceMillis(orphanScanIntervalMs)
	require.Error(t, scanner.ScanOnce(context.Background()))
	require.Len(t, local.calls, 2,
		"both blobs must be attempted even though the first unlink failed")
}

// TestOrphanScannerDoesNotReadTheQueueForAReferencedBlob pins the read-cost
// fix: the queue was consulted whenever an RC record existed, so every old,
// healthy, still-referenced blob paid a replicated read before the classifier
// looked at its count — making the hourly scan cost proportional to the whole
// retained dataset instead of to possible orphans.
func TestOrphanScannerDoesNotReadTheQueueForAReferencedBlob(t *testing.T) {
	t.Parallel()

	live := oldBlob("live")
	unreadable := oldBlob("unreadable")
	zero := oldBlob("zero")
	store := &fakeOrphanStore{
		rc: map[[32]byte][]byte{
			live.ContentSHA256:       s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 3}),
			unreadable.ContentSHA256: []byte("not an rc record"),
			zero.ContentSHA256:       s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0}),
		},
	}
	local := &fakeOrphanLocal{blobs: []s3keys.LocalChunkBlob{live, unreadable, zero}}
	clock := &orphanClock{ts: orphanNowTS}
	scanner := newOrphanScannerWithClock(t, store, local, clock)

	require.NoError(t, scanner.ScanOnce(context.Background()))
	require.Equal(t, 3, store.rcReads, "every old blob costs its RC read")
	require.Equal(t, 1, store.queueReads,
		"only the decodable zero-count record may cost a queue read")
}

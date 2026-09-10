package s3keys_test

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/stretchr/testify/require"
)

const (
	orphanNowMs    = uint64(1_700_000_000_000)
	orphanNowTS    = orphanNowMs << 16
	orphanGrace    = 6 * time.Hour
	orphanGraceMs  = uint64(6 * 60 * 60 * 1000)
	orphanBoundary = (orphanNowMs - orphanGraceMs) << 16
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
}

func (f *fakeOrphanStore) ReadChunkRefRC(_ context.Context, sha [32]byte) ([]byte, bool, error) {
	f.rcReads++
	v, ok := f.rc[sha]
	return v, ok, nil
}

func (f *fakeOrphanStore) GCQueueEntryExists(_ context.Context, sha [32]byte) (bool, error) {
	f.queueReads++
	return f.queued[sha], nil
}

type fakeOrphanLocal struct {
	blobs   []s3keys.LocalChunkBlob
	deleted [][32]byte
}

func (f *fakeOrphanLocal) ListLocalChunkBlobs(_ context.Context) ([]s3keys.LocalChunkBlob, error) {
	return f.blobs, nil
}

func (f *fakeOrphanLocal) DeleteChunkBlob(_ context.Context, sha [32]byte) error {
	f.deleted = append(f.deleted, sha)
	return nil
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

	require.NoError(t, newOrphanScanner(t, store, local).ScanOnce(context.Background()))

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

	require.NoError(t, newOrphanScanner(t, store, local).ScanOnce(context.Background()))
	require.Equal(t, 1, store.rcReads)
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

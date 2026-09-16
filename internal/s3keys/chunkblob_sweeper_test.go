package s3keys_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// fakeSweepStore records the order of every replicated operation so a
// test can assert the §3.5 phase ordering rather than just the effects.
type fakeSweepStore struct {
	entries []s3keys.ChunkBlobGCQueueEntry
	rc      map[[32]byte][]byte

	calls         []string
	scanErr       error
	condDeleteErr error
	unconDeletes  int
	condDeletes   int
	// rcErrs fails the RC read for specific SHAs, modelling an entry that
	// fails on every pass.
	rcErrs    map[[32]byte]error
	scanCalls int
	lastLimit int
	// metaClears records the SHAs whose stale queue metadata was cleared,
	// so a test can tell the atomic clear apart from a bare key delete.
	metaClears [][32]byte
}

// ScanGCQueue pages the fixture's entries, modelling a real store: the page
// boundary is the index encoded in the continuation key, so a test can assert
// the sweeper actually follows it rather than re-reading page one.
func (f *fakeSweepStore) ScanGCQueue(
	_ context.Context, startKey, _ []byte, limit int,
) ([]s3keys.ChunkBlobGCQueueEntry, []byte, error) {
	f.calls = append(f.calls, "scan")
	f.scanCalls++
	f.lastLimit = limit
	if f.scanErr != nil {
		return nil, nil, f.scanErr
	}
	from := 0
	if len(startKey) > 0 {
		if parsed, err := strconv.Atoi(string(startKey)); err == nil {
			from = parsed
		}
	}
	if from >= len(f.entries) {
		return nil, nil, nil
	}
	to := from + limit
	if to > len(f.entries) {
		to = len(f.entries)
	}
	var next []byte
	if to < len(f.entries) {
		next = []byte(strconv.Itoa(to))
	}
	return f.entries[from:to], next, nil
}

func (f *fakeSweepStore) ReadChunkRefRC(_ context.Context, sha [32]byte) ([]byte, bool, error) {
	f.calls = append(f.calls, "read-rc")
	if err, failing := f.rcErrs[sha]; failing {
		return nil, false, err
	}
	v, ok := f.rc[sha]
	return v, ok, nil
}

// DeleteGCQueueEntryIfUnreferenced models the real contract: ONE txn removes
// both the queue entry and the zero-count RC record, so a test can assert the
// record does not survive reclamation.
func (f *fakeSweepStore) DeleteGCQueueEntryIfUnreferenced(
	_ context.Context, entry s3keys.ChunkBlobGCQueueEntry,
) error {
	f.calls = append(f.calls, "raft-conditional-delete")
	f.condDeletes++
	if f.condDeleteErr != nil {
		return f.condDeleteErr
	}
	delete(f.rc, entry.ContentSHA256)
	return nil
}

func (f *fakeSweepStore) DeleteGCQueueEntry(_ context.Context, _ s3keys.ChunkBlobGCQueueEntry) error {
	f.calls = append(f.calls, "raft-unconditional-delete")
	f.unconDeletes++
	return nil
}

// ClearGCQueueMetadata models the real contract: one txn removes the queue key
// AND rewrites the RC record with QueuedAtTS zeroed, so a test can assert the
// record no longer claims a queue entry.
func (f *fakeSweepStore) ClearGCQueueMetadata(
	_ context.Context, entry s3keys.ChunkBlobGCQueueEntry,
) error {
	f.calls = append(f.calls, "raft-clear-queue-metadata")
	f.metaClears = append(f.metaClears, entry.ContentSHA256)
	if raw, ok := f.rc[entry.ContentSHA256]; ok {
		if rc, decoded := s3keys.DecodeChunkRefRC(raw); decoded {
			rc.QueuedAtTS = 0
			f.rc[entry.ContentSHA256] = s3keys.EncodeChunkRefRC(rc)
		}
	}
	return nil
}

type fakeLocalStore struct {
	calls   *[]string
	deletes [][32]byte
	// attempts records every conditional unlink, refused ones included, so
	// a test can tell "never attempted" from "attempted and refused".
	attempts []uint64
	// writtenAt is the payload's HLC write time; absent means no local blob.
	writtenAt map[[32]byte]uint64
	// reanchored names SHAs a PUT rewrote after the sweep observed them.
	reanchored map[[32]byte]struct{}
	statErr    error
	deleteErr  error
}

func (f *fakeLocalStore) ChunkBlobWrittenAt(_ context.Context, sha [32]byte) (uint64, bool, error) {
	*f.calls = append(*f.calls, "local-stat")
	if f.statErr != nil {
		return 0, false, f.statErr
	}
	if f.writtenAt == nil {
		// Default fixture: the blob exists with a fixed write time.
		return 1, true, nil
	}
	ts, ok := f.writtenAt[sha]
	return ts, ok, nil
}

func (f *fakeLocalStore) DeleteChunkBlobIfUnchanged(
	_ context.Context, sha [32]byte, writtenAtTS uint64,
) (bool, error) {
	*f.calls = append(*f.calls, "local-delete")
	f.attempts = append(f.attempts, writtenAtTS)
	if f.deleteErr != nil {
		return false, f.deleteErr
	}
	if _, changed := f.reanchored[sha]; changed {
		return false, nil
	}
	f.deletes = append(f.deletes, sha)
	return true, nil
}

// recordingSweepObserver captures what the sweeper concluded, as opposed to
// what the fakes were asked to do. That difference matters: a sweeper that
// ignores a refused conditional delete still leaves `deletes` empty, because
// the fake controls that field -- only the sweeper's own race-lost
// observation distinguishes "refused and handled" from "refused and ignored".
type recordingSweepObserver struct {
	verdicts []string
	raceLost int
}

func (o *recordingSweepObserver) ObserveChunkBlobSweep(_ s3keys.ChunkBlobSweepVerdict, reason string) {
	o.verdicts = append(o.verdicts, reason)
}

func (o *recordingSweepObserver) ObserveChunkBlobSweepRaceLost() {
	o.raceLost++
}

func newSweeperFixture(
	t *testing.T, store *fakeSweepStore,
) (*s3keys.ChunkBlobSweeper, *fakeLocalStore) {
	sweeper, local, _ := newObservedSweeperFixture(t, store)
	return sweeper, local
}

func newObservedSweeperFixtureWithPageSize(t *testing.T, store *fakeSweepStore, pageSize int) (
	*s3keys.ChunkBlobSweeper, *fakeLocalStore, *recordingSweepObserver,
) {
	t.Helper()
	return newSweeperFixtureWith(t, store, pageSize)
}

func newObservedSweeperFixture(t *testing.T, store *fakeSweepStore) (
	*s3keys.ChunkBlobSweeper, *fakeLocalStore, *recordingSweepObserver,
) {
	t.Helper()
	return newSweeperFixtureWith(t, store, 0)
}

func newSweeperFixtureWith(t *testing.T, store *fakeSweepStore, pageSize int) (
	*s3keys.ChunkBlobSweeper, *fakeLocalStore, *recordingSweepObserver,
) {
	t.Helper()
	local := &fakeLocalStore{calls: &store.calls}
	observer := &recordingSweepObserver{}
	// An HLC "now" far enough ahead that every fixture entry has served
	// its grace window.
	nowTS := (uint64(1_700_000_000_000) + 7_200_000) << 16
	sweeper, err := s3keys.NewChunkBlobSweeper(s3keys.ChunkBlobSweeperOptions{
		Store:       store,
		Local:       local,
		GracePeriod: time.Hour,
		NowTS:       func() uint64 { return nowTS },
		Observer:    observer,
		PageSize:    pageSize,
	})
	require.NoError(t, err)
	return sweeper, local, observer
}

func queuedEntry(sha [32]byte) s3keys.ChunkBlobGCQueueEntry {
	return s3keys.ChunkBlobGCQueueEntry{
		CommitTS:      uint64(1_700_000_000_000) << 16,
		ContentSHA256: sha,
	}
}

// TestSweeperRunsTheRaftPhaseBeforeTheLocalDelete is the §3.5 phase
// ordering. Local-first would leave a crash window where the blob is
// gone locally but the queue entry survives, so every later pass
// re-attempts a no-op local delete and the entry never clears without
// manual intervention. Raft-first inverts that into a bounded local
// space leak the orphan scan reclaims.
func TestSweeperRunsTheRaftPhaseBeforeTheLocalDelete(t *testing.T) {
	t.Parallel()

	sha := testSHA("orphaned")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local := newSweeperFixture(t, store)

	require.NoError(t, sweeper.SweepOnce(context.Background()))

	require.Equal(t,
		[]string{"scan", "read-rc", "local-stat", "raft-conditional-delete", "local-delete"},
		store.calls,
		"the replicated conditional delete must commit before the local unlink, "+
			"and the payload state the unlink is conditioned on must be observed "+
			"BEFORE that commit -- read afterwards it would already include a "+
			"re-anchoring PUT and the condition could not refuse it")
	require.Equal(t, [][32]byte{sha}, local.deletes)
}

// TestSweeperDoesNotTouchTheBlobWhenItLosesTheRace is the correctness
// property §3.5 calls out explicitly: an unconditional delete would
// silently succeed on an already-absent entry and let the sweeper
// local-delete a blob that is currently live.
func TestSweeperDoesNotTouchTheBlobWhenItLosesTheRace(t *testing.T) {
	t.Parallel()

	sha := testSHA("contended")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
		condDeleteErr: s3keys.ErrQueueEntryChanged,
	}
	sweeper, local := newSweeperFixture(t, store)

	require.NoError(t, sweeper.SweepOnce(context.Background()),
		"losing the race is a normal outcome, not a sweep failure")
	require.Empty(t, local.deletes,
		"a lost conditional delete means the blob may be live again; it must not be deleted")
	require.NotContains(t, store.calls, "local-delete")
}

// TestSweeperDropsAStaleEntryWithoutDeletingTheBlob covers §3.5(c):
// the blob is referenced again, so only the entry goes.
func TestSweeperDropsAStaleEntryWithoutDeletingTheBlob(t *testing.T) {
	t.Parallel()

	sha := testSHA("referenced-again")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 1}),
		},
	}
	sweeper, local := newSweeperFixture(t, store)

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	require.Equal(t, 1, store.unconDeletes)
	require.Zero(t, store.condDeletes)
	require.Empty(t, local.deletes, "a referenced blob must survive")
}

// TestSweeperDeclinesOnAnUnreadableRecord pins that corruption stops
// the sweep for that entry rather than reclaiming on a guess: neither
// the queue entry nor the blob is touched.
func TestSweeperDeclinesOnAnUnreadableRecord(t *testing.T) {
	t.Parallel()

	sha := testSHA("corrupt")
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{queuedEntry(sha)},
		rc:      map[[32]byte][]byte{sha: {0xAA}},
	}
	sweeper, local := newSweeperFixture(t, store)

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	require.Zero(t, store.condDeletes)
	require.Zero(t, store.unconDeletes)
	require.Empty(t, local.deletes)
}

// TestSweeperSkipsEntriesInsideTheGraceWindow pins that the scan
// boundary is applied: an entry stamped now has not served its grace.
func TestSweeperSkipsEntriesInsideTheGraceWindow(t *testing.T) {
	t.Parallel()

	nowMs := uint64(1_700_000_000_000)
	nowTS := nowMs << 16
	store := &fakeSweepStore{}
	local := &fakeLocalStore{calls: &store.calls}
	sweeper, err := s3keys.NewChunkBlobSweeper(s3keys.ChunkBlobSweeperOptions{
		Store:       store,
		Local:       local,
		GracePeriod: time.Hour,
		NowTS:       func() uint64 { return nowTS },
	})
	require.NoError(t, err)

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	// The scan happened, but bounded to entries older than now-1h.
	require.Equal(t, []string{"scan"}, store.calls)
}

// TestSweeperSweepsNothingBeforeTheFirstGraceWindowElapses covers a
// freshly started cluster, where now-grace underflows to the epoch.
func TestSweeperSweepsNothingBeforeTheFirstGraceWindowElapses(t *testing.T) {
	t.Parallel()

	store := &fakeSweepStore{}
	local := &fakeLocalStore{calls: &store.calls}
	sweeper, err := s3keys.NewChunkBlobSweeper(s3keys.ChunkBlobSweeperOptions{
		Store:       store,
		Local:       local,
		GracePeriod: time.Hour,
		NowTS:       func() uint64 { return uint64(1_000) << 16 },
	})
	require.NoError(t, err)

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	require.Empty(t, store.calls, "nothing can have served a grace window yet")
}

func TestSweeperPropagatesAScanFailure(t *testing.T) {
	t.Parallel()

	boom := errors.New("store unavailable")
	store := &fakeSweepStore{scanErr: boom}
	sweeper, local := newSweeperFixture(t, store)

	err := sweeper.SweepOnce(context.Background())
	require.ErrorIs(t, err, boom)
	require.Empty(t, local.deletes)
}

func TestNewChunkBlobSweeperValidatesItsCollaborators(t *testing.T) {
	t.Parallel()

	valid := s3keys.ChunkBlobSweeperOptions{
		Store: &fakeSweepStore{},
		Local: &fakeLocalStore{calls: &[]string{}},
		NowTS: func() uint64 { return 1 << 16 },
	}

	noStore := valid
	noStore.Store = nil
	_, err := s3keys.NewChunkBlobSweeper(noStore)
	require.Error(t, err)

	noLocal := valid
	noLocal.Local = nil
	_, err = s3keys.NewChunkBlobSweeper(noLocal)
	require.Error(t, err)

	noClock := valid
	noClock.NowTS = nil
	_, err = s3keys.NewChunkBlobSweeper(noClock)
	require.Error(t, err, "an HLC clock is required; a wall clock would be the wrong domain")

	_, err = s3keys.NewChunkBlobSweeper(valid)
	require.NoError(t, err)
}

// TestSweeperSparesAPayloadReAnchoredAfterTheRaftPhase closes the window
// DeleteGCQueueEntryIfUnreferenced cannot.
//
// That conditional delete proves the reference count was zero only through ITS
// OWN commit. A PUT that reuses the SHA immediately afterwards re-anchors the
// payload and commits a reference, and the unconditional local unlink then
// removed bytes the PUT had already acknowledged as durable.
func TestSweeperSparesAPayloadReAnchoredAfterTheRaftPhase(t *testing.T) {
	t.Parallel()

	sha := testSHA("re-anchored")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local, observer := newObservedSweeperFixture(t, store)
	local.writtenAt = map[[32]byte]uint64{sha: 4242}
	local.reanchored = map[[32]byte]struct{}{sha: {}}

	require.NoError(t, sweeper.SweepOnce(context.Background()),
		"a re-anchored payload is a normal conflict, not a sweep failure")

	require.Equal(t, []uint64{4242}, local.attempts,
		"the unlink must be conditioned on the state observed before the Raft phase")
	require.Empty(t, local.deletes,
		"a payload re-anchored after the Raft phase must not be unlinked")

	// The sweeper must also ACT on the refusal rather than ignore the
	// result: a caller that treats every conditional delete as successful
	// passes the assertions above, because the fake controls `deletes`.
	// The race-lost observation is the sweeper's own record of what it
	// concluded, so it distinguishes the two.
	require.Equal(t, 1, observer.raceLost,
		"a refused unlink must be recorded as a lost race, not treated as a reclaim")
}

// TestSweeperContinuesPastAFailingEntry is the starvation regression, the
// sibling of the orphan scan's.
//
// The queue scan is time-ordered, so returning on the first per-entry error
// came back to the same entry on every pass and starved every later entry
// indefinitely.
func TestSweeperContinuesPastAFailingEntry(t *testing.T) {
	t.Parallel()

	failing := testSHA("failing")
	ok := testSHA("reclaimable")
	failingEntry := queuedEntry(failing)
	okEntry := queuedEntry(ok)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{failingEntry, okEntry},
		rc: map[[32]byte][]byte{
			ok: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: okEntry.CommitTS}),
		},
		rcErrs: map[[32]byte]error{failing: errors.New("pebble: read failed")},
	}
	sweeper, local := newSweeperFixture(t, store)

	require.Error(t, sweeper.SweepOnce(context.Background()),
		"the pass must still report the per-entry failure")
	require.Equal(t, [][32]byte{ok}, local.deletes,
		"an entry queued after the failing one must still be reclaimed")
}

// A blob already absent locally still needs its queue entry cleared, and must
// not be reported as an unlink.
func TestSweeperClearsTheQueueEntryWhenNoLocalBlobRemains(t *testing.T) {
	t.Parallel()

	sha := testSHA("already-gone")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local := newSweeperFixture(t, store)
	local.writtenAt = map[[32]byte]uint64{}

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	require.Empty(t, local.attempts, "there is nothing to unlink")
	require.Contains(t, store.calls, "raft-conditional-delete",
		"the queue entry must still be cleared")
}

// TestSweeperPagesTheEligibleQueue pins the bounded scan.
//
// The scan API used to require the implementation to materialise the whole
// eligible range in one slice before the sweeper could reclaim even the first
// entry. An outage or a large object-deletion workload can leave millions of
// expired entries, so that spike can OOM the process and leave the backlog
// permanently untouched — the opposite of what a GC pass is for.
func TestSweeperPagesTheEligibleQueue(t *testing.T) {
	t.Parallel()

	const total = 7
	const pageSize = 3

	entries := make([]s3keys.ChunkBlobGCQueueEntry, 0, total)
	rc := make(map[[32]byte][]byte, total)
	for i := range total {
		sha := testSHA(fmt.Sprintf("queued-%d", i))
		entry := queuedEntry(sha)
		entries = append(entries, entry)
		rc[sha] = s3keys.EncodeChunkRefRC(
			s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS})
	}
	store := &fakeSweepStore{entries: entries, rc: rc}

	sweeper, local, _ := newObservedSweeperFixtureWithPageSize(t, store, pageSize)
	require.NoError(t, sweeper.SweepOnce(context.Background()))

	require.Equal(t, pageSize, store.lastLimit, "the scan must be bounded")
	require.Equal(t, 3, store.scanCalls,
		"7 entries at a page size of 3 is two full pages, a partial page, and no more")
	require.Len(t, local.deletes, total,
		"every entry must still be reclaimed: paging changes the memory profile, "+
			"not the work done")
}

// A non-positive page size must not make the scan return nothing and the
// sweeper spin without reclaiming.
func TestSweeperPageSizeDefaults(t *testing.T) {
	t.Parallel()

	sha := testSHA("one")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local, _ := newObservedSweeperFixtureWithPageSize(t, store, 0)
	require.NoError(t, sweeper.SweepOnce(context.Background()))

	require.Equal(t, s3keys.DefaultChunkBlobGCPageSize, store.lastLimit)
	require.Len(t, local.deletes, 1)
}

// TestSweeperReclaimDeletesTheZeroCountRecord documents the contract that stops
// the RC keyspace growing without bound.
//
// The planner persists a count-zero RC record when the last reference drops, and
// reclamation deleted only the queue entry and the local payload — so nothing
// ever removed that key. A workload creating and deleting unique chunks left one
// permanent Raft-replicated record per content hash, growing the live MVCC state
// and every snapshot despite blob GC working correctly.
//
// The deletion is part of DeleteGCQueueEntryIfUnreferenced's single txn rather
// than a second one: a crash between two txns would leave either a queue entry
// for a blob with no RC record, or the same leak.
func TestSweeperReclaimDeletesTheZeroCountRecord(t *testing.T) {
	t.Parallel()

	sha := testSHA("reclaimed")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local, _ := newObservedSweeperFixture(t, store)
	require.NoError(t, sweeper.SweepOnce(context.Background()))

	require.Equal(t, [][32]byte{sha}, local.deletes)
	require.Equal(t, 1, store.condDeletes,
		"exactly one conditional txn must carry both the queue entry and the "+
			"zero-count record")
	require.NotContains(t, store.rc, sha,
		"the zero-count reference record must not survive reclamation, or the RC "+
			"keyspace grows without bound")
}

// TestSweeperDoesNotUnlinkAPayloadWrittenAfterTheEntry closes the in-flight
// PUT window.
//
// The conditional unlink only detects a write that lands AFTER the sweeper's
// stat. A PUT that re-anchors the SHA just before the stat and commits its
// chunkref afterwards passes everything: the stat reads the PUT's new
// timestamp, the conditional Raft delete still sees the old zero count, and
// the conditional unlink matches the very timestamp the PUT wrote. The bytes
// go, and the PUT then commits a reference to nothing.
//
// A blob that legitimately became unreferenced at entry.CommitTS was written
// strictly before it, so a payload at or after that timestamp belongs to a
// later PUT.
func TestSweeperDoesNotUnlinkAPayloadWrittenAfterTheEntry(t *testing.T) {
	t.Parallel()

	sha := testSHA("reanchored-before-stat")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local, obs := newObservedSweeperFixture(t, store)
	// The PUT already wrote the payload; its chunkref has not committed.
	local.writtenAt = map[[32]byte]uint64{sha: entry.CommitTS + 1}

	require.NoError(t, sweeper.SweepOnce(context.Background()),
		"a re-anchored payload is a normal outcome, not a sweep failure")
	require.Empty(t, local.deletes, "the in-flight PUT's payload must survive")
	require.Empty(t, local.attempts, "the unlink must not even be attempted")
	require.Zero(t, store.condDeletes,
		"and the queue entry must stay, so the next pass sees the PUT's committed count")
	require.Positive(t, obs.raceLost)
}

// The boundary: a payload written strictly before the entry is the one the
// entry describes, and is reclaimed normally.
func TestSweeperReclaimsAPayloadWrittenBeforeTheEntry(t *testing.T) {
	t.Parallel()

	sha := testSHA("genuinely-dead")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 0, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local := newSweeperFixture(t, store)
	local.writtenAt = map[[32]byte]uint64{sha: entry.CommitTS - 1}

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	require.Equal(t, [][32]byte{sha}, local.deletes)
}

// TestSweeperClearsQueueMetadataOnALiveRecord keeps a re-referenced blob
// reachable by the sweeper.
//
// Removing only the queue key leaves the RC record claiming an entry that no
// longer exists. PlanChunkRefRCMutations reads that claim: when the last
// reference is later removed it takes the already-queued branch, preserves the
// stale timestamp and writes NO replacement queue key, so the blob never
// re-enters the queue and only the orphan scan can reclaim it -- on its own
// interval rather than the GC grace period.
func TestSweeperClearsQueueMetadataOnALiveRecord(t *testing.T) {
	t.Parallel()

	sha := testSHA("referenced-again-same-entry")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			// Live again, and the record still points at THIS entry.
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 1, QueuedAtTS: entry.CommitTS}),
		},
	}
	sweeper, local := newSweeperFixture(t, store)

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	require.Equal(t, [][32]byte{sha}, store.metaClears,
		"the timestamp must be cleared with the key, atomically")
	require.Zero(t, store.unconDeletes, "a bare key delete would strand the blob")
	require.Empty(t, local.deletes, "a referenced blob must survive")

	rc, ok := s3keys.DecodeChunkRefRC(store.rc[sha])
	require.True(t, ok)
	require.False(t, rc.Queued(),
		"the record must no longer claim a queue entry, or the next dereference "+
			"preserves the stale timestamp and inserts no replacement key")
}

// A SUPERSEDED entry is the opposite case: the record points at a NEWER entry
// that is still serving its own grace window, so its timestamp must survive.
func TestSweeperKeepsQueueMetadataForASupersededEntry(t *testing.T) {
	t.Parallel()

	sha := testSHA("superseded")
	entry := queuedEntry(sha)
	store := &fakeSweepStore{
		entries: []s3keys.ChunkBlobGCQueueEntry{entry},
		rc: map[[32]byte][]byte{
			sha: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 1, QueuedAtTS: entry.CommitTS + 99}),
		},
	}
	sweeper, _ := newSweeperFixture(t, store)

	require.NoError(t, sweeper.SweepOnce(context.Background()))
	require.Empty(t, store.metaClears, "the newer entry's timestamp is not ours to clear")
	require.Equal(t, 1, store.unconDeletes)

	rc, ok := s3keys.DecodeChunkRefRC(store.rc[sha])
	require.True(t, ok)
	require.Equal(t, entry.CommitTS+99, rc.QueuedAtTS)
}

// TestSweeperBoundsRetainedFailures pins that a broad failure over a large
// backlog does not retain one wrapped error per entry.
//
// Paging the scan bounded the entries held at once but not the errors, so a
// million-entry backlog whose RC reads all fail could still exhaust memory
// before the join. Every failure is still counted; only the detail is sampled.
func TestSweeperBoundsRetainedFailures(t *testing.T) {
	t.Parallel()

	const entries = 500
	store := &fakeSweepStore{rc: map[[32]byte][]byte{}, rcErrs: map[[32]byte]error{}}
	for i := range entries {
		sha := testSHA("bulk-" + strconv.Itoa(i))
		store.entries = append(store.entries, queuedEntry(sha))
		store.rcErrs[sha] = errors.New("rc read unavailable")
	}
	sweeper, _, _ := newObservedSweeperFixtureWithPageSize(t, store, 50)

	err := sweeper.SweepOnce(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "500 failures",
		"every failure must still be counted")
	// The retained detail must be strictly smaller than the failure count,
	// which is the whole property: "showing 500" would also contain the word
	// "showing" while retaining one error per entry.
	require.ErrorContains(t, err, fmt.Sprintf("(showing %d)", s3keys.MaxRetainedSweepFailures))
	require.Less(t, s3keys.MaxRetainedSweepFailures, entries,
		"the cap has to bite for this fixture to test anything")
}

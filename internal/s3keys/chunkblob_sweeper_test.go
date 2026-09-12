package s3keys_test

import (
	"context"
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
}

func (f *fakeSweepStore) ScanGCQueue(_ context.Context, _, _ []byte) ([]s3keys.ChunkBlobGCQueueEntry, error) {
	f.calls = append(f.calls, "scan")
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	return f.entries, nil
}

func (f *fakeSweepStore) ReadChunkRefRC(_ context.Context, sha [32]byte) ([]byte, bool, error) {
	f.calls = append(f.calls, "read-rc")
	v, ok := f.rc[sha]
	return v, ok, nil
}

func (f *fakeSweepStore) DeleteGCQueueEntryIfUnreferenced(_ context.Context, _ s3keys.ChunkBlobGCQueueEntry) error {
	f.calls = append(f.calls, "raft-conditional-delete")
	f.condDeletes++
	return f.condDeleteErr
}

func (f *fakeSweepStore) DeleteGCQueueEntry(_ context.Context, _ s3keys.ChunkBlobGCQueueEntry) error {
	f.calls = append(f.calls, "raft-unconditional-delete")
	f.unconDeletes++
	return nil
}

type fakeLocalStore struct {
	calls   *[]string
	deletes [][32]byte
}

func (f *fakeLocalStore) DeleteChunkBlob(_ context.Context, sha [32]byte) error {
	*f.calls = append(*f.calls, "local-delete")
	f.deletes = append(f.deletes, sha)
	return nil
}

func newSweeperFixture(t *testing.T, store *fakeSweepStore) (*s3keys.ChunkBlobSweeper, *fakeLocalStore) {
	t.Helper()
	local := &fakeLocalStore{calls: &store.calls}
	// An HLC "now" far enough ahead that every fixture entry has served
	// its grace window.
	nowTS := (uint64(1_700_000_000_000) + 7_200_000) << 16
	sweeper, err := s3keys.NewChunkBlobSweeper(s3keys.ChunkBlobSweeperOptions{
		Store:       store,
		Local:       local,
		GracePeriod: time.Hour,
		NowTS:       func() uint64 { return nowTS },
	})
	require.NoError(t, err)
	return sweeper, local
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
		[]string{"scan", "read-rc", "raft-conditional-delete", "local-delete"},
		store.calls,
		"the replicated conditional delete must commit before the local unlink")
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

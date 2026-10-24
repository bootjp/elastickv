package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func seedActivatedPinJournal(t *testing.T, st store.MVCCStore) {
	t.Helper()
	// Test-only activation fixture. Production activation must prove the
	// durable legacy drain and capability gate before writing this epoch.
	seedPinJournalRecord(t, st, replicatedPinStateRecord{
		Version: replicatedPinStateVersion, AppliedIndex: 1,
		Pins: replicatedBackupPins{Epoch: 1, Capacity: 4},
	})
}

func seedPinJournalRecord(t *testing.T, st store.MVCCStore, record replicatedPinStateRecord) {
	t.Helper()
	value, err := json.Marshal(record)
	require.NoError(t, err)
	writer, ok := st.(store.ReplicatedControlStore)
	require.True(t, ok)
	applied, err := writer.ApplyReplicatedControlState(context.Background(), replicatedPinStateKey, value, record.AppliedIndex)
	require.NoError(t, err)
	require.True(t, applied)
}

func TestReplicatedPinJournalAllocatesBeforeReserving(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := g17OpenStore(t, t.TempDir())
	seedPinJournalRecord(t, st, replicatedPinStateRecord{
		Version: replicatedPinStateVersion, AppliedIndex: 1, Allocator: true,
		Pins: replicatedBackupPins{Epoch: 1, Capacity: 1},
	})
	journal := newPinJournal(t, st)
	require.ErrorIs(t, journal.apply(ctx, 2, pinCommand(replicatedPinAdmit, 1)), ErrReplicatedBackupPin)
	first, err := journal.allocate(ctx, 2, 1)
	require.NoError(t, err)
	require.Equal(t, BackupIdentity{Epoch: 1, Generation: 1}, first)
	repeated, err := journal.allocate(ctx, 2, 1)
	require.NoError(t, err)
	require.Equal(t, first, repeated)
	require.NoError(t, journal.apply(ctx, 3, pinCommand(replicatedPinAdmit, first.Generation)))
	journal = newPinJournal(t, st)
	second, err := journal.allocate(ctx, 4, 1)
	require.NoError(t, err)
	require.Equal(t, first.Generation+1, second.Generation)
	_, err = journal.allocate(ctx, 2, 1)
	require.ErrorIs(t, err, ErrReplicatedBackupPin, "an old lost allocation response must not return a newer caller's identity")
	require.ErrorIs(t, journal.apply(ctx, 5, pinCommand(replicatedPinAdmit, second.Generation)), ErrTooManyActiveBackups)
	require.NoError(t, journal.apply(ctx, 6, pinCommand(replicatedPinRelease, second.Generation)))
	require.ErrorIs(t, journal.apply(ctx, 7, pinCommand(replicatedPinAdmit, second.Generation)), ErrReplicatedBackupPin)
	_, err = journal.allocate(ctx, 8, 2)
	require.ErrorIs(t, err, ErrReplicatedBackupPin)
	require.Equal(t, uint64(2), journal.snapshot().AllocatedThrough)
}

func TestReplicatedPinJournalAllocationFailureNeverReturnsIdentity(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	seedPinJournalRecord(t, st, replicatedPinStateRecord{
		Version: replicatedPinStateVersion, AppliedIndex: 1, Allocator: true,
		Pins: replicatedBackupPins{Epoch: 1, Capacity: 1},
	})
	failure := errors.New("disk unavailable")
	journal := newPinJournal(t, &failingPinControlStore{MVCCStore: st, err: failure})
	identity, err := journal.allocate(context.Background(), 2, 1)
	require.ErrorIs(t, err, failure)
	require.Zero(t, identity)
	require.Zero(t, journal.snapshot().AllocatedThrough)
	journal = newPinJournal(t, st)
	journal.state.AllocatedThrough = math.MaxUint64
	journal.state.LastAllocationIndex = 1
	identity, err = journal.allocate(context.Background(), 2, 1)
	require.ErrorIs(t, err, ErrReplicatedBackupPin)
	require.Zero(t, identity)
	require.Equal(t, uint64(math.MaxUint64), journal.snapshot().AllocatedThrough)
}

func TestReplicatedPinJournalAllocatorSurvivesSnapshotAndReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = st.Close()
		}
	})
	seedPinJournalRecord(t, st, replicatedPinStateRecord{
		Version: replicatedPinStateVersion, AppliedIndex: 1, Allocator: true,
		Pins: replicatedBackupPins{Epoch: 1, Capacity: 1},
	})
	journal := newPinJournal(t, st)
	first, err := journal.allocate(ctx, 2, 1)
	require.NoError(t, err)
	snapshot, err := st.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var data bytes.Buffer
	_, err = snapshot.WriteTo(&data)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	require.NoError(t, st.Close())
	closed = true

	reopened := newPinJournal(t, g17OpenStore(t, dir))
	target := g17OpenStore(t, t.TempDir())
	require.NoError(t, target.Restore(&data))
	restored := newPinJournal(t, target)
	for _, current := range []*replicatedPinJournal{reopened, restored} {
		require.Equal(t, journal.snapshot(), current.snapshot())
		second, err := current.allocate(ctx, 3, 1)
		require.NoError(t, err)
		require.Equal(t, first.Generation+1, second.Generation)
	}
}

func newPinJournal(t *testing.T, st store.MVCCStore) *replicatedPinJournal {
	t.Helper()
	j, err := openReplicatedPinJournal(context.Background(), st)
	require.NoError(t, err)
	return j
}

func TestReplicatedPinJournalSnapshotPreservesRetirement(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"memory", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			open := func() store.MVCCStore {
				if backend == "pebble" {
					return g17OpenStore(t, t.TempDir())
				}
				st := store.NewMVCCStore()
				t.Cleanup(func() { _ = st.Close() })
				return st
			}
			st := open()
			seedActivatedPinJournal(t, st)
			journal := newPinJournal(t, st)
			ctx := context.Background()
			require.NoError(t, journal.apply(ctx, 2, pinCommand(replicatedPinAdmit, 1)))
			require.NoError(t, journal.apply(ctx, 3, pinCommand(replicatedPinRelease, 5)))
			snapshot, err := st.Snapshot()
			require.NoError(t, err)
			defer snapshot.Close()
			var data bytes.Buffer
			_, err = snapshot.WriteTo(&data)
			require.NoError(t, err)
			target := open()
			require.NoError(t, target.Restore(&data))
			restored := newPinJournal(t, target)
			require.Equal(t, journal.snapshot(), restored.snapshot())
			require.ErrorIs(t, restored.apply(ctx, 4, pinCommand(replicatedPinAdmit, 5)), ErrReplicatedBackupPin)
			extend := pinCommand(replicatedPinExtend, 1)
			extend.Deadline = 200
			require.NoError(t, restored.apply(ctx, 5, extend))
			require.NoError(t, restored.apply(ctx, 6, pinCommand(replicatedPinExpire, 1)))
			require.Equal(t, uint64(10), restored.snapshot().Pins.oldest())
			require.Equal(t, uint64(5), restored.snapshot().Pins.RetiredThrough)
			require.Zero(t, target.LastCommitTS())
		})
	}
}

func TestReplicatedPinJournalReopenAndSkipStaleApply(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	seedActivatedPinJournal(t, st)
	older := newPinJournal(t, st)
	current := newPinJournal(t, st)
	require.NoError(t, current.apply(ctx, 3, pinCommand(replicatedPinAdmit, 3)))
	require.NoError(t, older.apply(ctx, 2, pinCommand(replicatedPinRelease, 5)))
	require.Equal(t, current.snapshot(), older.snapshot(), "stale durable index must reload, not install its computed replacement")
	require.NoError(t, st.Close())
	reopened := g17OpenStore(t, dir)
	journal := newPinJournal(t, reopened)
	require.Equal(t, current.snapshot(), journal.snapshot())
	require.NoError(t, journal.apply(ctx, 3, pinCommand(replicatedPinRelease, 3)))
	require.Len(t, journal.snapshot().Pins.Active, 1, "duplicate index must not apply a different transition")
	require.NoError(t, journal.apply(ctx, 4, pinCommand(replicatedPinRelease, 3)))
	require.Empty(t, journal.snapshot().Pins.Active)
	_, _, err = journal.snapshot().Pins.transition(pinCommand(replicatedPinAdmit, 3))
	require.ErrorIs(t, err, ErrReplicatedBackupPin)
}

type failingPinControlStore struct {
	store.MVCCStore
	err error
}

func (s *failingPinControlStore) ApplyReplicatedControlState(context.Context, []byte, []byte, uint64) (bool, error) {
	return false, s.err
}

func TestReplicatedPinJournalDoesNotPublishFailedWrite(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	seedActivatedPinJournal(t, st)
	failure := errors.New("disk unavailable")
	journal := newPinJournal(t, &failingPinControlStore{MVCCStore: st, err: failure})
	before := journal.snapshot()
	require.ErrorIs(t, journal.apply(context.Background(), 2, pinCommand(replicatedPinAdmit, 1)), failure)
	require.Equal(t, before, journal.snapshot())
	require.Equal(t, before, newPinJournal(t, st).snapshot())
}

func TestReplicatedPinJournalMissingOrCorruptStateStaysInactive(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	journal := newPinJournal(t, st)
	require.Zero(t, journal.snapshot().Pins.Epoch)
	require.ErrorIs(t, journal.apply(context.Background(), 1, pinCommand(replicatedPinAdmit, 1)), ErrReplicatedBackupPin)
	for _, value := range []string{
		`{}`, `{"Version":2}`, `null`, `{} {}`,
		`{"Version":1,"AppliedIndex":1,"Pins":{"Epoch":1},"Unexpected":true}`,
		`{"Version":1,"AppliedIndex":1,"Pins":{"Epoch":1,"Active":{"0":{}}}}`,
	} {
		_, err := decodeReplicatedPinState([]byte(value))
		require.Error(t, err, "corrupt state %s", value)
	}
}

func TestReplicatedPinJournalLeavesLegacyUserNamespaceUntouched(t *testing.T) {
	t.Parallel()
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "value", true: "tombstone"}[deleted], func(t *testing.T) {
			t.Parallel()
			st := g17OpenStore(t, t.TempDir())
			ctx := context.Background()
			legacyKey := []byte("!txn|control|backup-pins")
			require.NoError(t, st.PutAt(ctx, legacyKey, []byte("user"), 10, 0))
			if deleted {
				require.NoError(t, st.DeleteAt(ctx, legacyKey, 11))
			}
			seedActivatedPinJournal(t, st)
			journal := newPinJournal(t, st)
			require.NoError(t, journal.apply(ctx, 2, pinCommand(replicatedPinAdmit, 1)))
			snapshot, err := st.Snapshot()
			require.NoError(t, err)
			defer snapshot.Close()
			var data bytes.Buffer
			_, err = snapshot.WriteTo(&data)
			require.NoError(t, err)
			target := g17OpenStore(t, t.TempDir())
			require.NoError(t, target.Restore(&data))
			require.Equal(t, journal.snapshot(), newPinJournal(t, target).snapshot())
			for _, current := range []store.MVCCStore{st, target} {
				got, err := current.GetAt(ctx, legacyKey, ^uint64(0))
				if deleted {
					require.ErrorIs(t, err, store.ErrKeyNotFound)
				} else {
					require.NoError(t, err)
					require.Equal(t, []byte("user"), got)
				}
			}
		})
	}
}

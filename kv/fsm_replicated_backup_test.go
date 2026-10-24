package kv

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func applyReplicatedBackupCommand(t *testing.T, f *kvFSM, index uint64, command ReplicatedBackupCommand) any {
	t.Helper()
	payload, err := EncodeReplicatedBackupCommand(command)
	require.NoError(t, err)
	f.SetApplyIndex(index)
	require.False(t, f.IsVolatileOnlyPayload(payload), "pin commands persist their own state/index atomically")
	return f.Apply(payload)
}

func activatedPinFSM(t *testing.T, st store.MVCCStore) *kvFSM {
	t.Helper()
	f := g17FSM(t, st)
	// Test fixture only: production activation still requires its capability
	// certificate and the durable legacy drain, neither supplied by this flag.
	f.ttlInlineCapability.Store(true)
	return f
}

func TestFSMReplicatedBackupRequiresCapabilityAndActivatedEpoch(t *testing.T) {
	t.Parallel()
	st := g17OpenStore(t, t.TempDir())
	f := g17FSM(t, st)
	command := replicatedBackupWireFixtures()[0]
	response := applyReplicatedBackupCommand(t, f, 2, command)
	err, ok := response.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, ErrTTLInlineCapabilityRequired)
	f.ttlInlineCapability.Store(true)
	response = applyReplicatedBackupCommand(t, f, 3, command)
	err, ok = response.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, ErrTTLInlineCapabilityRequired, "binary support cannot manufacture an epoch")
}

func TestFSMReplicatedBackupCoexistsWithPrepareEvidence(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"memory", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			open := func() store.MVCCStore {
				if backend == "pebble" {
					return g17OpenStore(t, t.TempDir())
				}
				st := store.NewMVCCStore()
				t.Cleanup(func() { require.NoError(t, st.Close()) })
				return st
			}
			st := open()
			seedActivatedPinJournal(t, st)
			f := activatedPinFSM(t, st)
			pin := replicatedBackupWireFixtures()[0]
			payload, err := EncodeReplicatedBackupCommand(pin)
			require.NoError(t, err)
			commands := [][]byte{EncodePrepareEvidenceFence(0), EncodePrepareEvidenceSeal(), payload,
				EncodePrepareEvidenceFence(pin.ReadTS), EncodePrepareEvidenceRelease(pin.ReadTS)}
			for index, command := range commands {
				f.SetApplyIndex(uint64(index) + 2)
				require.Nil(t, f.Apply(command), "timestamp controls are not legacy backup lifecycle commands")
			}
			snapshot, err := f.Snapshot()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, snapshot.Close()) })
			var image bytes.Buffer
			_, err = snapshot.WriteTo(&image)
			require.NoError(t, err)
			restored := activatedPinFSM(t, open())
			require.NoError(t, restored.Restore(&image))
			for _, current := range []*kvFSM{f, restored} {
				require.True(t, current.prepareEvidenceEnabled.Load())
				require.False(t, current.prepareCreationSealed.Load())
				require.Equal(t, pin.ReadTS, current.prepareEvidenceReleaseFloor.Load())
				require.Equal(t, pin.ReadTS, current.prepareEvidenceTimestampFloor.Load())
				require.Equal(t, f.replicatedPins.snapshot(), current.replicatedPins.snapshot())
				legacy := EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: pin.PinID})
				response, ok := current.Apply(legacy).(error)
				require.True(t, ok)
				require.ErrorIs(t, response, ErrInvalidBackupPin)
			}
		})
	}
}

func TestFSMReplicatedBackupPreservesRetentionAndWriteFenceThroughRestore(t *testing.T) {
	t.Parallel()
	st := g17OpenStore(t, t.TempDir())
	seedActivatedPinJournal(t, st)
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	var observed uint64
	tracker.SetBackupTimestampFloorObserver(func(ts uint64) { observed = ts })
	f := activatedPinFSM(t, st)
	f.readTracker = tracker
	commands := replicatedBackupWireFixtures()
	require.Nil(t, applyReplicatedBackupCommand(t, f, 2, commands[0]))
	require.Equal(t, uint64(100), observed)
	require.Zero(t, tracker.ActiveBackupPinCount(), "the local sweeper must not own replicated retention")
	require.Nil(t, applyReplicatedBackupCommand(t, f, 3, commands[1]))
	require.Nil(t, applyReplicatedBackupCommand(t, f, 4, commands[3]))
	require.Equal(t, uint64(100), f.replicatedPins.snapshot().Pins.oldest(), "stale expiry must not remove an extended pin")
	require.Nil(t, applyReplicatedBackupCommand(t, f, 5, commands[2]))
	require.Empty(t, f.replicatedPins.snapshot().Pins.Active)
	require.Equal(t, uint64(100), f.replicatedPins.snapshot().Pins.WriteTimestampFloor)
	response := applyReplicatedBackupCommand(t, f, 6, commands[0])
	err, ok := response.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, ErrReplicatedBackupPin)
	snapshot, err := f.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var data bytes.Buffer
	_, err = snapshot.WriteTo(&data)
	require.NoError(t, err)
	target := activatedPinFSM(t, g17OpenStore(t, t.TempDir()))
	require.NoError(t, target.Restore(&data))
	require.Equal(t, f.replicatedPins.snapshot(), target.replicatedPins.snapshot())
	for _, current := range []*kvFSM{f, target} {
		require.Equal(t, uint64(100), current.backupTimestampFloor.Load())
		current.SetApplyIndex(7)
		response := current.Apply(g17Entry(t, g17RawPut(99, "late", "refused")))
		err, ok := response.(error)
		require.True(t, ok)
		require.ErrorIs(t, err, ErrBackupTimestampFenced)
		legacy := EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: commands[0].PinID})
		response = current.Apply(legacy)
		err, ok = response.(error)
		require.True(t, ok)
		require.ErrorIs(t, err, ErrInvalidBackupPin)
	}
}

func TestFSMReplicatedBackupAllocationAndOperationalFailure(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	seedPinJournalRecord(t, st, replicatedPinStateRecord{
		Version: replicatedPinStateVersion, AppliedIndex: 1, Allocator: true,
		Pins: replicatedBackupPins{Epoch: 1, Capacity: 1},
	})
	f := activatedPinFSM(t, st)
	response := applyReplicatedBackupCommand(t, f, 2, replicatedBackupWireFixtures()[4])
	allocation, ok := response.(*ReplicatedBackupAllocation)
	require.True(t, ok)
	require.Equal(t, BackupIdentity{Epoch: 1, Generation: 1}, allocation.Identity)
	failure := errors.New("disk failed")
	broken := activatedPinFSM(t, &failingPinControlStore{MVCCStore: st, err: failure})
	before := broken.replicatedPins.snapshot()
	response = applyReplicatedBackupCommand(t, broken, 3, replicatedBackupWireFixtures()[4])
	require.ErrorIs(t, haltApplyOf(response), failure)
	require.Equal(t, before, broken.replicatedPins.snapshot())
	require.Equal(t, before, newPinJournal(t, st).snapshot())
}

func TestFSMReplicatedBackupMalformedCommandAndCorruptStateHalt(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { _ = st.Close() })
	f := g17FSM(t, st)
	require.True(t, errors.Is(haltApplyOf(f.Apply([]byte{raftEncodeReplicatedBackup, 0xff})), ErrReplicatedBackupApply))
	writer, ok := st.(store.ReplicatedControlStore)
	require.True(t, ok)
	_, err := writer.ApplyReplicatedControlState(context.Background(), replicatedPinStateKey, []byte("corrupt"), 1)
	require.NoError(t, err)
	f = g17FSM(t, st)
	require.True(t, errors.Is(haltApplyOf(f.Apply(g17TwoPutEntry(t))), ErrReplicatedBackupApply))
}

func TestFSMReplicatedBackupReopenKeepsReleasedWriteFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = st.Close()
		}
	})
	seedActivatedPinJournal(t, st)
	f := activatedPinFSM(t, st)
	commands := replicatedBackupWireFixtures()
	require.Nil(t, applyReplicatedBackupCommand(t, f, 2, commands[0]))
	require.Nil(t, applyReplicatedBackupCommand(t, f, 3, commands[2]))
	require.NoError(t, st.Close())
	closed = true
	reopened := g17FSM(t, g17OpenStore(t, dir))
	require.False(t, reopened.ttlInlineCapability.Load(), "loading state must not manufacture a capability certificate")
	require.Empty(t, reopened.replicatedPins.snapshot().Pins.Active)
	require.Equal(t, uint64(100), reopened.backupTimestampFloor.Load())
	reopened.SetApplyIndex(4)
	response := reopened.Apply(g17Entry(t, g17RawPut(99, "late", "refused"), g17RawPut(101, "new", "accepted")))
	results, ok := response.(*fsmApplyResponse)
	require.True(t, ok)
	require.ErrorIs(t, results.results[0], ErrBackupTimestampFenced)
	require.NoError(t, results.results[1])
	require.True(t, g17Exists(t, reopened.store, "new"))
}

func TestFSMCompactorRefusesActivatedJournalWithoutFloorProtocol(t *testing.T) {
	t.Parallel()
	for _, withPin := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty active set", true: "active pin"}[withPin], func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			t.Cleanup(func() { _ = st.Close() })
			seedActivatedPinJournal(t, st)
			if withPin {
				f := activatedPinFSM(t, st)
				require.Nil(t, applyReplicatedBackupCommand(t, f, 2, replicatedBackupWireFixtures()[0]))
			}
			ctx := context.Background()
			require.NoError(t, st.PutAt(ctx, []byte("key"), []byte("old"), 10, 0))
			require.NoError(t, st.PutAt(ctx, []byte("key"), []byte("new"), 30, 0))
			compactor := NewFSMCompactor([]FSMCompactRuntime{{
				GroupID: 1, Store: st,
				StatusReader: fakeRaftStatus{status: raftengine.Status{
					State: raftengine.StateFollower, AppliedIndex: 10, CommitIndex: 10,
				}},
			}}, WithFSMCompactorRetentionWindow(time.Millisecond))
			require.ErrorIs(t, compactor.SyncOnce(ctx), ErrTTLInlineCapabilityRequired,
				"matching cached apply/commit indexes cannot replace a fresh quorum barrier")
			got, err := st.GetAt(ctx, []byte("key"), 10)
			require.NoError(t, err)
			require.Equal(t, []byte("old"), got)
		})
	}
}

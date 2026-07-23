package kv

import (
	"bytes"
	"context"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestApplyBackupUsesSharedTracker(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)
	pinID := backupTrackerTestPinID(1)
	now := time.Now()
	firstDeadline := time.UnixMilli(now.Add(time.Hour).UnixMilli())
	secondDeadline := time.UnixMilli(now.Add(2 * time.Hour).UnixMilli())

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: firstDeadline,
	}))))
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(42), tracker.Oldest())

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    pinID,
		Deadline: secondDeadline,
	}))))
	gotDeadline, ok := tracker.BackupPinDeadline(pinID)
	require.True(t, ok)
	require.Equal(t, secondDeadline, gotDeadline)

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: pinID}))))
	require.Equal(t, 0, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(0), tracker.Oldest())
}

func TestApplyBackupWithoutTrackerHalts(t *testing.T) {
	fsm, ok := NewKvFSMWithHLC(store.NewMVCCStore(), NewHLC()).(*kvFSM)
	require.True(t, ok)

	err := haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(1),
		ReadTS:   42,
		Deadline: time.UnixMilli(5000),
	})))
	require.True(t, errors.Is(err, ErrBackupApply), "err = %v", err)
}

func TestApplyBackupUnknownSubtypeHalts(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))

	err := haltApplyOf(fsm.Apply([]byte{raftEncodeBackup, 0xff}))
	require.True(t, errors.Is(err, ErrBackupApply), "err = %v", err)
	require.True(t, errors.Is(err, ErrBackupWireSubtype), "err = %v", err)
}

func TestApplyBackupInvalidPinReturnsNonFatalError(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))

	resp := fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    BackupPinID{},
		ReadTS:   42,
		Deadline: time.UnixMilli(5000),
	}))
	require.NoError(t, haltApplyOf(resp))
	respErr, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, respErr, ErrInvalidBackupPin)
}

func TestApplyBackupLimitDoesNotDropCommittedPins(t *testing.T) {
	tracker := NewActiveTimestampTracker(
		WithActiveTimestampTrackerSweepInterval(0),
		WithActiveTimestampTrackerMaxBackupPins(1),
	)
	fsm := newBackupTestFSM(t, tracker)
	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(1),
		ReadTS:   42,
		Deadline: time.Now().Add(time.Hour),
	}))))

	resp := fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(2),
		ReadTS:   43,
		Deadline: time.Now().Add(2 * time.Hour),
	}))

	require.NoError(t, haltApplyOf(resp))
	require.Nil(t, resp)
	require.Equal(t, 2, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(42), tracker.Oldest())
}

func TestApplyBackupMissingExtendIsNoop(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)

	resp := fsm.Apply(EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    backupTrackerTestPinID(1),
		Deadline: time.Now().Add(time.Hour),
	}))

	require.NoError(t, haltApplyOf(resp))
	require.Nil(t, resp)
	require.Equal(t, 0, tracker.ActiveBackupPinCount())
}

func TestApplyBackupExpiredExtendRestoresCommittedFence(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)
	pinID := backupTrackerTestPinID(1)
	require.NoError(t, tracker.PinWithDeadline(pinID, 42, time.Now().Add(-time.Millisecond)))

	resp := fsm.Apply(EncodeBackupExtendEntry(BackupExtendEntry{
		PinID:    pinID,
		Deadline: time.Now().Add(time.Hour),
	}))

	require.NoError(t, haltApplyOf(resp))
	require.Nil(t, resp)
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	require.Equal(t, uint64(42), tracker.Oldest())
}

func TestApplyBackupObservesPinnedReadTimestamp(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	hlc := NewHLC()
	fsm, ok := NewKvFSMWithHLCAndTracker(
		store.NewMVCCStore(), hlc, tracker, WithRouteHistory(nil, 1),
	).(*kvFSM)
	require.True(t, ok)
	readTS := hlc.Next() + 10_000

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: readTS, Deadline: time.Now().Add(time.Hour),
	}))))
	require.GreaterOrEqual(t, hlc.Current(), readTS)
}

func TestApplyBackupFencesPreallocatedWrites(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)
	readTS := uint64(100)
	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: readTS, Deadline: time.Now().Add(time.Hour),
	}))))

	tests := []struct {
		name string
		req  *pb.Request
	}{
		{
			name: "raw",
			req: &pb.Request{Ts: readTS - 1, Mutations: []*pb.Mutation{{
				Op: pb.Op_PUT, Key: []byte("raw"), Value: []byte("stale"),
			}}},
		},
		{
			name: "one phase",
			req: &pb.Request{IsTxn: true, Phase: pb.Phase_NONE, Ts: readTS - 2, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: []byte("one-phase"), CommitTS: readTS - 1,
				})},
				{Op: pb.Op_PUT, Key: []byte("one-phase"), Value: []byte("stale")},
			}},
		},
		{
			name: "prepare",
			req: &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: readTS - 1, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
					PrimaryKey: []byte("prepare"), LockTTLms: defaultTxnLockTTLms,
				})},
				{Op: pb.Op_PUT, Key: []byte("prepare"), Value: []byte("stale")},
			}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			requireBackupTimestampFenced(t, applyBackupTestRequest(t, fsm, tc.req))
		})
	}
}

func TestApplyBackupReserveDoesNotInstallTimestampFloor(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSM(t, tracker)
	readTS := uint64(100)
	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupReserveEntry(BackupReserveEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: readTS, Deadline: time.Now().Add(time.Hour),
	}))))

	require.Nil(t, applyBackupTestRequest(t, fsm, &pb.Request{Ts: readTS - 1, Mutations: []*pb.Mutation{{
		Op: pb.Op_PUT, Key: []byte("reserved-only"), Value: []byte("allowed"),
	}}}))

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: readTS, Deadline: time.Now().Add(time.Hour),
	}))))
	requireBackupTimestampFenced(t, applyBackupTestRequest(t, fsm, &pb.Request{Ts: readTS - 1, Mutations: []*pb.Mutation{{
		Op: pb.Op_PUT, Key: []byte("after-pin"), Value: []byte("blocked"),
	}}}))
}

func TestBackupTimestampFloorKeyRejectsRawMutation(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))
	resp := applyBackupTestRequest(t, fsm, &pb.Request{Ts: 10, Mutations: []*pb.Mutation{{
		Op: pb.Op_PUT, Key: bytes.Clone(backupTimestampFloorKey), Value: make([]byte, backupTimestampFloorValueSize),
	}}})
	err, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestApplyBackupAllowsResolutionOfPrePinTransaction(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))
	primary := []byte("primary")
	startTS := uint64(30)
	commitTS := uint64(40)
	prepare := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: startTS, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
			PrimaryKey: primary, LockTTLms: defaultTxnLockTTLms,
		})},
		{Op: pb.Op_PUT, Key: primary, Value: []byte("committed")},
	}}
	require.Nil(t, applyBackupTestRequest(t, fsm, prepare))
	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: 50, Deadline: time.Now().Add(time.Hour),
	}))))
	commit := &pb.Request{IsTxn: true, Phase: pb.Phase_COMMIT, Ts: startTS, Mutations: []*pb.Mutation{
		{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
			PrimaryKey: primary, CommitTS: commitTS,
		})},
		{Op: pb.Op_PUT, Key: primary},
	}}
	require.Nil(t, applyBackupTestRequest(t, fsm, commit))
	value, err := fsm.store.GetAt(context.Background(), primary, 50)
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), value)
}

func TestBackupTimestampFloorSurvivesSnapshotRestore(t *testing.T) {
	srcStore := store.NewMVCCStore()
	src, ok := NewKvFSMWithHLCAndTracker(
		srcStore, NewHLC(), NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)),
	).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, haltApplyOf(src.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: 75, Deadline: time.Now().Add(time.Hour),
	}))))
	snapshot, err := srcStore.Snapshot()
	require.NoError(t, err)
	defer snapshot.Close()
	var raw bytes.Buffer
	_, err = snapshot.WriteTo(&raw)
	require.NoError(t, err)

	dstStore := store.NewMVCCStore()
	dst, ok := NewKvFSMWithHLCAndTracker(
		dstStore, NewHLC(), NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)),
	).(*kvFSM)
	require.True(t, ok)
	require.NoError(t, dst.Restore(bytes.NewReader(raw.Bytes())))
	requireBackupTimestampFenced(t, applyBackupTestRequest(t, dst, &pb.Request{Ts: 74, Mutations: []*pb.Mutation{{
		Op: pb.Op_PUT, Key: []byte("late"), Value: []byte("stale"),
	}}}))
}

func TestFSMSnapshotRejectsActiveBackupPin(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm := newBackupTestFSMWithGroup(t, tracker, 7)
	pinID := backupTrackerTestPinID(1)
	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID: pinID, ReadTS: 75, Deadline: time.Now().Add(time.Hour),
	}))))

	_, err := fsm.Snapshot()
	require.ErrorIs(t, err, ErrBackupSnapshotBlocked)

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: pinID}))))
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
}

func TestBackupTimestampFloorRejectsDelayedRaftProposal(t *testing.T) {
	st := store.NewMVCCStore()
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	r, stop := newSingleRaft(t, "backup-floor-delayed", NewKvFSMWithHLCAndTracker(st, NewHLC(), tracker))
	t.Cleanup(stop)
	readTS := uint64(500)
	result, err := r.ProposeAdmin(context.Background(), EncodeBackupPinEntry(BackupPinEntry{
		PinID: backupTrackerTestPinID(1), ReadTS: readTS, Deadline: time.Now().Add(time.Hour),
	}))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Nil(t, result.Response)

	txn := NewTransactionWithProposer(r)
	_, err = txn.Commit(context.Background(), []*pb.Request{{
		Ts: readTS - 1,
		Mutations: []*pb.Mutation{{
			Op: pb.Op_PUT, Key: []byte("delayed"), Value: []byte("stale"),
		}},
	}})
	require.ErrorIs(t, err, ErrBackupTimestampFenced)
	_, err = st.GetAt(context.Background(), []byte("delayed"), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func applyBackupTestRequest(t *testing.T, fsm *kvFSM, req *pb.Request) any {
	t.Helper()
	payload, err := marshalRaftCommand([]*pb.Request{req})
	require.NoError(t, err)
	return fsm.Apply(payload)
}

func requireBackupTimestampFenced(t *testing.T, resp any) {
	t.Helper()
	err, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, err, ErrBackupTimestampFenced)
}

func TestApplyBackupReserveEnforcesCapacityAndUnreserveReleases(t *testing.T) {
	tracker := NewActiveTimestampTracker(
		WithActiveTimestampTrackerSweepInterval(0),
		WithActiveTimestampTrackerMaxBackupPins(1),
	)
	fsm := newBackupTestFSM(t, tracker)
	deadline := time.Now().Add(time.Hour)
	first := backupTrackerTestPinID(1)
	second := backupTrackerTestPinID(2)

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupReserveEntry(BackupReserveEntry{
		PinID: first, ReadTS: 42, Deadline: deadline,
	}))))
	resp := fsm.Apply(EncodeBackupReserveEntry(BackupReserveEntry{
		PinID: second, ReadTS: 43, Deadline: deadline,
	}))
	require.NoError(t, haltApplyOf(resp))
	respErr, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, respErr, ErrTooManyActiveBackups)

	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupUnreserveEntry(BackupUnreserveEntry{PinID: first}))))
	require.NoError(t, haltApplyOf(fsm.Apply(EncodeBackupReserveEntry(BackupReserveEntry{
		PinID: second, ReadTS: 43, Deadline: deadline,
	}))))
}

func TestApplyBackupZeroDeadlineReturnsNonFatalError(t *testing.T) {
	fsm := newBackupTestFSM(t, NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0)))

	resp := fsm.Apply(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    backupTrackerTestPinID(1),
		ReadTS:   42,
		Deadline: time.Time{},
	}))

	require.NoError(t, haltApplyOf(resp))
	respErr, ok := resp.(error)
	require.True(t, ok)
	require.ErrorIs(t, respErr, ErrInvalidBackupPin)
}

func TestApplyBackupPinsAreScopedByRaftGroup(t *testing.T) {
	tracker := NewActiveTimestampTracker(WithActiveTimestampTrackerSweepInterval(0))
	fsm1 := newBackupTestFSMWithGroup(t, tracker, 1)
	fsm2 := newBackupTestFSMWithGroup(t, tracker, 2)
	pinID := backupTrackerTestPinID(1)
	entry := BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: time.Now().Add(time.Hour),
	}

	require.NoError(t, haltApplyOf(fsm1.Apply(EncodeBackupPinEntry(entry))))
	require.NoError(t, haltApplyOf(fsm2.Apply(EncodeBackupPinEntry(entry))))
	require.Equal(t, 2, tracker.ActiveBackupPinCount())

	require.NoError(t, haltApplyOf(fsm1.Apply(EncodeBackupReleaseEntry(BackupReleaseEntry{PinID: pinID}))))
	require.Equal(t, 1, tracker.ActiveBackupPinCount())
	_, ok := tracker.BackupPinDeadlineForGroup(pinID, 1)
	require.False(t, ok)
	_, ok = tracker.BackupPinDeadlineForGroup(pinID, 2)
	require.True(t, ok)
}

func TestBackupPayloadIsVolatileOnly(t *testing.T) {
	fsm := &kvFSM{}
	pinID := backupTrackerTestPinID(1)

	require.True(t, fsm.IsVolatileOnlyPayload(EncodeBackupPinEntry(BackupPinEntry{
		PinID:    pinID,
		ReadTS:   42,
		Deadline: time.UnixMilli(5000),
	})))
	require.False(t, fsm.IsVolatileOnlyPayload(nil))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{raftEncodeSingle}))
	require.False(t, fsm.IsVolatileOnlyPayload([]byte{0x03}))
}

func newBackupTestFSM(t *testing.T, tracker *ActiveTimestampTracker) *kvFSM {
	t.Helper()
	fsm, ok := NewKvFSMWithHLCAndTracker(store.NewMVCCStore(), NewHLC(), tracker).(*kvFSM)
	require.True(t, ok)
	return fsm
}

func newBackupTestFSMWithGroup(t *testing.T, tracker *ActiveTimestampTracker, groupID uint64) *kvFSM {
	t.Helper()
	fsm, ok := NewKvFSMWithHLCAndTracker(
		store.NewMVCCStore(),
		NewHLC(),
		tracker,
		WithRouteHistory(nil, groupID),
	).(*kvFSM)
	require.True(t, ok)
	return fsm
}

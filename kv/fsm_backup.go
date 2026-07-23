package kv

import (
	"context"
	"encoding/binary"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrBackupApply = errors.New("backup fsm apply failed")
var ErrBackupTimestampFenced = errors.New("backup timestamp fence rejects stale write")
var ErrBackupSnapshotBlocked = errors.New("active backup pin blocks fsm snapshot")

var backupTimestampFloorKey = []byte(TxnKeyPrefix + "backup|timestamp_floor")

const backupTimestampFloorValueSize = 8

func (f *kvFSM) applyBackup(data []byte) any {
	if f.readTracker == nil {
		return haltErr(errors.Wrap(ErrBackupApply, "kv/fsm: backup entry arrived but no ActiveTimestampTracker is wired"))
	}
	entry, err := decodeBackupPayload(data)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrBackupApply), "kv/fsm: decode backup entry"))
	}
	err = f.applyDecodedBackup(entry)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInvalidBackupPin) || errors.Is(err, ErrTooManyActiveBackups) {
		return err
	}
	return haltErr(errors.Wrap(errors.Mark(err, ErrBackupApply), "kv/fsm: apply backup entry"))
}

func (f *kvFSM) applyDecodedBackup(entry backupEntry) error {
	switch entry.subtype {
	case backupSubtypePin:
		err := f.readTracker.ApplyPinWithDeadlineForGroup(entry.pin.PinID, f.shardGroupID, entry.pin.ReadTS, entry.pin.Deadline)
		return f.applyBackupTimestampFence(entry.pin.ReadTS, err)
	case backupSubtypeExtend:
		return f.readTracker.ApplyExtendForGroup(entry.extend.PinID, f.shardGroupID, entry.extend.Deadline)
	case backupSubtypeRelease:
		f.readTracker.ReleaseBackupPinForGroup(entry.release.PinID, f.shardGroupID)
		return nil
	case backupSubtypeReserve:
		return f.readTracker.PinWithDeadline(entry.pin.PinID, entry.pin.ReadTS, entry.pin.Deadline)
	case backupSubtypeUnreserve:
		f.readTracker.ReleaseBackupPin(entry.release.PinID)
		return nil
	default:
		return ErrBackupWireSubtype
	}
}

func (f *kvFSM) applyBackupTimestampFence(readTS uint64, applyErr error) error {
	if applyErr != nil {
		return applyErr
	}
	if err := f.persistBackupTimestampFloor(context.Background(), readTS); err != nil {
		return err
	}
	return f.observeBackupReadTimestamp(readTS, nil)
}

func (f *kvFSM) persistBackupTimestampFloor(ctx context.Context, readTS uint64) error {
	if readTS <= f.backupTimestampFloor.Load() {
		return nil
	}
	value := make([]byte, backupTimestampFloorValueSize)
	binary.BigEndian.PutUint64(value, readTS)
	err := f.store.ApplyMutationsRaftAt(ctx, []*store.KVPairMutation{{
		Op: store.OpTypePut, Key: backupTimestampFloorKey, Value: value,
	}}, nil, readTS, readTS, f.pendingApplyIdx)
	if err != nil {
		return errors.Wrap(err, "persist backup timestamp floor")
	}
	f.backupTimestampFloor.Store(readTS)
	return nil
}

func (f *kvFSM) reloadBackupTimestampFloor(ctx context.Context) error {
	if f == nil || f.store == nil {
		return nil
	}
	value, err := f.store.GetAt(ctx, backupTimestampFloorKey, ^uint64(0))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			f.backupTimestampFloor.Store(0)
			return nil
		}
		return errors.WithStack(err)
	}
	if len(value) != backupTimestampFloorValueSize {
		return errors.Wrapf(ErrBackupApply, "backup timestamp floor has %d bytes, want %d", len(value), backupTimestampFloorValueSize)
	}
	f.backupTimestampFloor.Store(binary.BigEndian.Uint64(value))
	return nil
}

func (f *kvFSM) verifyBackupTimestampFloor(r *pb.Request, commitTS uint64) error {
	floor := f.backupTimestampFloor.Load()
	if floor == 0 || commitTS > floor || r == nil {
		return nil
	}
	if r.IsTxn && (r.Phase == pb.Phase_COMMIT || r.Phase == pb.Phase_ABORT) {
		// A transaction prepared before the pin must remain resolvable. The
		// backup scanner resolves its existing lock or fails BeginBackup while
		// its primary remains pending; new PREPARE entries at this ts are fenced.
		return nil
	}
	return errors.Wrapf(ErrBackupTimestampFenced, "commit_ts %d is not above backup read_ts %d", commitTS, floor)
}

func (f *kvFSM) rejectSnapshotWithActiveBackupPin() error {
	if f == nil || f.readTracker == nil {
		return nil
	}
	if readTS := f.readTracker.OldestBackupForGroup(f.shardGroupID); readTS != 0 {
		return errors.Wrapf(ErrBackupSnapshotBlocked, "raft group %d has active backup read_ts %d", f.shardGroupID, readTS)
	}
	return nil
}

func (f *kvFSM) observeBackupReadTimestamp(readTS uint64, applyErr error) error {
	if applyErr == nil && f.hlc != nil {
		// The pin entry is also the per-group timestamp barrier. Once it
		// applies, later writes on this replica must issue above read_ts.
		f.hlc.Observe(readTS)
	}
	return applyErr
}

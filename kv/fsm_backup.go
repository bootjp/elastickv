package kv

import "github.com/cockroachdb/errors"

var ErrBackupApply = errors.New("backup fsm apply failed")

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
		return f.observeBackupReadTimestamp(entry.pin.ReadTS, err)
	case backupSubtypeExtend:
		return f.readTracker.ApplyExtendForGroup(entry.extend.PinID, f.shardGroupID, entry.extend.Deadline)
	case backupSubtypeRelease:
		f.readTracker.ReleaseBackupPinForGroup(entry.release.PinID, f.shardGroupID)
		return nil
	case backupSubtypeReserve:
		err := f.readTracker.PinWithDeadline(entry.pin.PinID, entry.pin.ReadTS, entry.pin.Deadline)
		return f.observeBackupReadTimestamp(entry.pin.ReadTS, err)
	case backupSubtypeUnreserve:
		f.readTracker.ReleaseBackupPin(entry.release.PinID)
		return nil
	default:
		return ErrBackupWireSubtype
	}
}

func (f *kvFSM) observeBackupReadTimestamp(readTS uint64, applyErr error) error {
	if applyErr == nil && f.hlc != nil {
		// The pin entry is also the per-group timestamp barrier. Once it
		// applies, later writes on this replica must issue above read_ts.
		f.hlc.Observe(readTS)
	}
	return applyErr
}

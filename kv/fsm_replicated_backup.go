package kv

import (
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrReplicatedBackupApply = errors.New("replicated backup state apply failed")

// ReplicatedBackupAllocation is returned only after the control-group journal
// has durably saved the identity at this entry's applied index.
type ReplicatedBackupAllocation struct {
	Identity BackupIdentity
}

func (f *kvFSM) applyBackupOpcode(ctx context.Context, opcode byte, payload []byte) (any, bool) {
	switch opcode {
	case raftEncodeBackup:
		return f.applyBackup(payload), true
	case raftEncodeReplicatedBackup:
		return f.applyReplicatedBackup(ctx, payload), true
	default:
		return nil, false
	}
}

func (f *kvFSM) reloadReplicatedBackupPins(ctx context.Context) error {
	if f.store == nil {
		f.replicatedPins = nil
		return nil
	}
	if _, supported := f.store.(store.ReplicatedControlStore); !supported {
		f.replicatedPins = nil
		return nil
	}
	journal, err := openReplicatedPinJournal(ctx, f.store)
	if err != nil {
		return err
	}
	f.replicatedPins = journal
	f.observeReplicatedBackupWriteFloor()
	return nil
}

func (f *kvFSM) observeReplicatedBackupWriteFloor() {
	floor := f.replicatedPins.snapshot().Pins.WriteTimestampFloor
	if floor <= f.backupTimestampFloor.Load() {
		return
	}
	f.backupTimestampFloor.Store(floor)
	_ = f.observeBackupReadTimestamp(floor, nil)
	f.readTracker.observeReplicatedBackupTimestampFloor(floor)
}

func (f *kvFSM) applyReplicatedBackup(ctx context.Context, payload []byte) any {
	command, err := decodeReplicatedBackupCommand(payload)
	if err != nil {
		return replicatedBackupHalt(err, "decode command")
	}
	if f.replicatedPins == nil || f.pendingApplyIdx == 0 {
		return replicatedBackupHalt(errors.New("missing journal or Raft applied index"), "apply command")
	}
	if !f.ttlInlineCapability.Load() || f.replicatedPins.snapshot().Pins.Epoch == 0 {
		// Binary support is not activation. Only a future configuration-bound
		// capability marker and complete durable drain may create the epoch.
		return errors.WithStack(ErrTTLInlineCapabilityRequired)
	}
	if command.Operation == ReplicatedBackupAllocate {
		identity, err := f.replicatedPins.allocate(ctx, f.pendingApplyIdx, command.Identity.Epoch)
		if err != nil {
			return replicatedBackupApplyError(err)
		}
		return &ReplicatedBackupAllocation{Identity: identity}
	}
	err = f.replicatedPins.apply(ctx, f.pendingApplyIdx, replicatedPinCommand{
		Operation: replicatedPinOperation(command.Operation), Identity: command.Identity,
		PinID: command.PinID, ReadTS: command.ReadTS, Deadline: command.DeadlineMillis,
		ExpectedDeadline: command.ExpectedDeadlineMillis,
	})
	if err == nil {
		f.observeReplicatedBackupWriteFloor()
	}
	return replicatedBackupApplyError(err)
}

func replicatedBackupApplyError(err error) any {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrReplicatedBackupPin) || errors.Is(err, ErrTooManyActiveBackups) {
		return err
	}
	return replicatedBackupHalt(err, "persist command")
}

func replicatedBackupHalt(err error, operation string) any {
	return haltErr(errors.Wrap(errors.Mark(err, ErrReplicatedBackupApply), "kv/fsm: replicated backup "+operation))
}

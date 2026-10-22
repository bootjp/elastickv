package kv

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
)

// 0x0f is an invalid protobuf wire type, not a valid legacy Request prefix.
// No producer may emit it until the configuration-bound capability gate and
// durable legacy drain activate the new backup epoch.
const raftEncodeReplicatedBackup byte = 0x0f

const (
	replicatedBackupWireVersion = byte(1)
	replicatedBackupPayloadSize = 58
)

type replicatedBackupWire struct {
	Version                byte
	Operation              ReplicatedBackupOperation
	Identity               BackupIdentity
	PinID                  BackupPinID
	ReadTS                 uint64
	DeadlineMillis         int64
	ExpectedDeadlineMillis int64
}

type ReplicatedBackupOperation byte

const (
	ReplicatedBackupPin ReplicatedBackupOperation = iota + 1
	ReplicatedBackupExtend
	ReplicatedBackupRelease
	ReplicatedBackupExpire
	ReplicatedBackupAllocate
)

// ReplicatedBackupCommand carries only ordered state; deadline milliseconds
// are identities for Extend/Expire, never a local apply-time expiry clock.
type ReplicatedBackupCommand struct {
	Operation              ReplicatedBackupOperation
	Identity               BackupIdentity
	PinID                  BackupPinID
	ReadTS                 uint64
	DeadlineMillis         int64
	ExpectedDeadlineMillis int64
}

func (c ReplicatedBackupCommand) validate() error {
	if c.Identity.Epoch == 0 {
		return errors.Wrap(ErrReplicatedBackupPin, "missing backup epoch")
	}
	if c.Operation == ReplicatedBackupAllocate {
		return c.validateAllocation()
	}
	if c.Identity.Generation == 0 || c.PinID.IsZero() {
		return errors.Wrap(ErrReplicatedBackupPin, "missing backup identity or pin ID")
	}
	return c.validatePinFields()
}

func (c ReplicatedBackupCommand) validateAllocation() error {
	if c.Identity.Generation != 0 || !c.PinID.IsZero() || c.ReadTS != 0 || c.DeadlineMillis != 0 || c.ExpectedDeadlineMillis != 0 {
		return errors.Wrap(ErrReplicatedBackupPin, "allocation carries unexpected pin fields")
	}
	return nil
}

func (c ReplicatedBackupCommand) validatePinFields() error {
	if err := c.validateReadTimestamp(); err != nil {
		return err
	}
	switch c.Operation {
	case ReplicatedBackupPin, ReplicatedBackupExtend:
		return c.validateLiveDeadline()
	case ReplicatedBackupRelease:
		return c.validateNoDeadline()
	case ReplicatedBackupExpire:
		return c.validateExpectedDeadline()
	case ReplicatedBackupAllocate:
		return c.validateAllocation()
	}
	return errors.Wrap(ErrReplicatedBackupPin, "unknown backup operation")
}

func (c ReplicatedBackupCommand) validateReadTimestamp() error {
	if c.Operation == ReplicatedBackupPin {
		if c.ReadTS == 0 || c.ReadTS == math.MaxUint64 {
			return errors.Wrap(ErrReplicatedBackupPin, "invalid pin read timestamp")
		}
	} else if c.ReadTS != 0 {
		return errors.Wrap(ErrReplicatedBackupPin, "only initial pins may carry a read timestamp")
	}
	return nil
}

func (c ReplicatedBackupCommand) validateLiveDeadline() error {
	if c.DeadlineMillis <= 0 || c.ExpectedDeadlineMillis != 0 {
		return errors.Wrap(ErrReplicatedBackupPin, "invalid pin deadline")
	}
	return nil
}

func (c ReplicatedBackupCommand) validateNoDeadline() error {
	if c.DeadlineMillis != 0 || c.ExpectedDeadlineMillis != 0 {
		return errors.Wrap(ErrReplicatedBackupPin, "release carries unexpected deadlines")
	}
	return nil
}

func (c ReplicatedBackupCommand) validateExpectedDeadline() error {
	if c.DeadlineMillis != 0 || c.ExpectedDeadlineMillis <= 0 {
		return errors.Wrap(ErrReplicatedBackupPin, "invalid expiry deadline identity")
	}
	return nil
}

func EncodeReplicatedBackupCommand(command ReplicatedBackupCommand) ([]byte, error) {
	if err := command.validate(); err != nil {
		return nil, err
	}
	out := make([]byte, 1+replicatedBackupPayloadSize)
	out[0] = raftEncodeReplicatedBackup
	wire := replicatedBackupWire{
		Version: replicatedBackupWireVersion, Operation: command.Operation,
		Identity: command.Identity, PinID: command.PinID, ReadTS: command.ReadTS,
		DeadlineMillis: command.DeadlineMillis, ExpectedDeadlineMillis: command.ExpectedDeadlineMillis,
	}
	if _, err := binary.Encode(out[1:], binary.BigEndian, wire); err != nil {
		return nil, errors.Wrap(err, "encode replicated backup command")
	}
	return out, nil
}

func decodeReplicatedBackupCommand(payload []byte) (ReplicatedBackupCommand, error) {
	if len(payload) != replicatedBackupPayloadSize || payload[0] != replicatedBackupWireVersion {
		return ReplicatedBackupCommand{}, errors.New("unknown or malformed replicated backup wire format")
	}
	var wire replicatedBackupWire
	if _, err := binary.Decode(payload, binary.BigEndian, &wire); err != nil {
		return ReplicatedBackupCommand{}, errors.Wrap(err, "decode replicated backup command")
	}
	command := ReplicatedBackupCommand{
		Operation: wire.Operation, Identity: wire.Identity, PinID: wire.PinID,
		ReadTS: wire.ReadTS, DeadlineMillis: wire.DeadlineMillis, ExpectedDeadlineMillis: wire.ExpectedDeadlineMillis,
	}
	if err := command.validate(); err != nil {
		return ReplicatedBackupCommand{}, err
	}
	return command, nil
}

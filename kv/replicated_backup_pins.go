package kv

import (
	"maps"
	"math"

	"github.com/cockroachdb/errors"
)

var ErrReplicatedBackupPin = errors.New("replicated backup pin rejected")

// BackupIdentity is allocated durably before a backup fans out to data groups.
// A generation is never reused within an epoch, including failed admissions.
type BackupIdentity struct {
	Epoch      uint64
	Generation uint64
}

type replicatedBackupPin struct {
	PinID  BackupPinID
	ReadTS uint64
	// Unix milliseconds; apply compares versions, never the local clock.
	Deadline int64
}

type replicatedBackupPins struct {
	Epoch          uint64
	RetiredThrough uint64
	PinGeneration  uint64
	ReadFloor      uint64
	// A released/expired backup never reopens admission of delayed writes.
	WriteTimestampFloor uint64
	Capacity            uint32
	Active              map[uint64]replicatedBackupPin
}

type replicatedPinOperation byte

const (
	replicatedPinAdmit replicatedPinOperation = iota + 1
	replicatedPinExtend
	replicatedPinRelease
	replicatedPinExpire
)

type replicatedPinCommand struct {
	Operation        replicatedPinOperation
	Identity         BackupIdentity
	PinID            BackupPinID
	ReadTS           uint64
	Deadline         int64
	ExpectedDeadline int64
}

// transition has no clock input. Deadlines only identify the expiry version;
// the leader decides when to propose an expiry, never an applying replica.
// The receiver remains unchanged so a failed durable write cannot publish it.
func (s replicatedBackupPins) transition(command replicatedPinCommand) (replicatedBackupPins, bool, error) {
	if err := s.validateCommandIdentity(command); err != nil {
		return s, false, err
	}
	next := s
	next.Active = maps.Clone(s.Active)
	if next.Active == nil {
		next.Active = make(map[uint64]replicatedBackupPin)
	}
	changed, err := next.apply(command)
	if err != nil || !changed {
		return s, false, err
	}
	if s.PinGeneration == math.MaxUint64 {
		return s, false, errors.Wrap(ErrReplicatedBackupPin, "pin generation exhausted")
	}
	next.PinGeneration++
	return next, true, nil
}

func (s replicatedBackupPins) validateCommandIdentity(command replicatedPinCommand) error {
	if s.Epoch == 0 || command.Identity.Epoch != s.Epoch || command.Identity.Generation == 0 || command.PinID.IsZero() {
		return errors.Wrap(ErrReplicatedBackupPin, "invalid or stale backup identity")
	}
	if active, ok := s.Active[command.Identity.Generation]; ok && active.PinID != command.PinID {
		return errors.Wrap(ErrReplicatedBackupPin, "generation already belongs to another pin")
	}
	return nil
}

func (s *replicatedBackupPins) apply(command replicatedPinCommand) (bool, error) {
	switch command.Operation {
	case replicatedPinAdmit:
		return s.admit(command)
	case replicatedPinExtend:
		return s.extend(command)
	case replicatedPinRelease:
		return s.retire(command.Identity.Generation), nil
	case replicatedPinExpire:
		return s.expire(command)
	default:
		return false, errors.Wrap(ErrReplicatedBackupPin, "unknown pin operation")
	}
}

func (s *replicatedBackupPins) admit(command replicatedPinCommand) (bool, error) {
	if command.ReadTS == 0 || command.ReadTS == math.MaxUint64 || command.Deadline <= 0 {
		return false, errors.Wrap(ErrReplicatedBackupPin, "invalid pin timestamp or deadline")
	}
	pin := replicatedBackupPin{PinID: command.PinID, ReadTS: command.ReadTS, Deadline: command.Deadline}
	if active, ok := s.Active[command.Identity.Generation]; ok {
		if active != pin {
			return false, errors.Wrap(ErrReplicatedBackupPin, "initial admission cannot replace an active pin")
		}
		return false, nil
	}
	if command.Identity.Generation <= s.RetiredThrough || command.ReadTS < s.ReadFloor {
		return false, errors.Wrap(ErrReplicatedBackupPin, "pin is behind retirement or read floor")
	}
	if s.Capacity != 0 && uint64(len(s.Active)) >= uint64(s.Capacity) {
		return false, errors.WithStack(ErrTooManyActiveBackups)
	}
	s.Active[command.Identity.Generation] = pin
	s.WriteTimestampFloor = max(s.WriteTimestampFloor, command.ReadTS)
	return true, nil
}

func (s *replicatedBackupPins) extend(command replicatedPinCommand) (bool, error) {
	active, ok := s.Active[command.Identity.Generation]
	if !ok || command.Deadline <= 0 {
		return false, errors.Wrap(ErrReplicatedBackupPin, "cannot extend a missing pin or invalid deadline")
	}
	if command.Deadline <= active.Deadline {
		return false, nil
	}
	active.Deadline = command.Deadline
	s.Active[command.Identity.Generation] = active
	return true, nil
}

func (s *replicatedBackupPins) expire(command replicatedPinCommand) (bool, error) {
	if command.ExpectedDeadline <= 0 {
		return false, errors.Wrap(ErrReplicatedBackupPin, "invalid expected deadline")
	}
	if active, ok := s.Active[command.Identity.Generation]; ok && active.Deadline != command.ExpectedDeadline {
		return false, nil
	}
	return s.retire(command.Identity.Generation), nil
}

func (s *replicatedBackupPins) retire(generation uint64) bool {
	_, active := s.Active[generation]
	delete(s.Active, generation)
	if generation > s.RetiredThrough {
		s.RetiredThrough = generation
		return true
	}
	return active
}

func (s replicatedBackupPins) oldest() uint64 {
	var oldest uint64
	for _, pin := range s.Active {
		if oldest == 0 || pin.ReadTS < oldest {
			oldest = pin.ReadTS
		}
	}
	return oldest
}

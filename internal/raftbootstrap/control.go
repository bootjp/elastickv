package raftbootstrap

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const (
	controlPrefix         byte = 0x1f
	controlVersion        byte = 1
	controlHeaderSize          = 1 + 2 + 1 + 1 + 4
	WitnessSize                = 16 + 8 + 8 + 8 + 8 + 32
	operationIDSize            = 16
	guardedBodyHeaderSize      = WitnessSize + operationIDSize + 4
)

var controlMagic = [2]byte{'B', 'C'}

type ControlKind uint8

const (
	ControlActivate         ControlKind = 1
	ControlReserve          ControlKind = 2
	ControlAbort            ControlKind = 3
	ControlGuardedWatermark ControlKind = 4
	ControlGuardedCatalog   ControlKind = 5
)

type OperationID [operationIDSize]byte

type Witness struct {
	Cluster             UUID
	Group               uint64
	ActivationIndex     uint64
	Generation          uint64
	ConfigurationIndex  uint64
	ConfigurationDigest Digest
}

type GuardedCommand struct {
	Kind        ControlKind
	Witness     Witness
	OperationID OperationID
	Inner       []byte
}

type GuardedInnerValidator func(ControlKind, []byte) error

// EncodeGuardedCommand encodes one protected watermark or catalog command.
// The inner payload remains opaque here and is validated by its owning FSM
// path after the replicated witness is accepted.
func EncodeGuardedCommand(command GuardedCommand, validateInner GuardedInnerValidator) ([]byte, error) {
	if err := validateGuardedCommand(command, validateInner); err != nil {
		return nil, err
	}
	bodyLen := guardedBodyHeaderSize + len(command.Inner)
	out := make([]byte, controlHeaderSize+bodyLen)
	out[0] = controlPrefix
	copy(out[1:3], controlMagic[:])
	out[3] = controlVersion
	out[4] = byte(command.Kind)
	binary.BigEndian.PutUint32(out[5:9], uint32(bodyLen)) //nolint:gosec // validateGuardedCommand bounds the complete frame to 16 MiB.
	encodeWitness(out[controlHeaderSize:controlHeaderSize+WitnessSize], command.Witness)
	offset := controlHeaderSize + WitnessSize
	copy(out[offset:offset+operationIDSize], command.OperationID[:])
	offset += operationIDSize
	binary.BigEndian.PutUint32(out[offset:offset+4], uint32(len(command.Inner))) //nolint:gosec // validateGuardedCommand bounds inner to 16 MiB.
	copy(out[offset+4:], command.Inner)
	return out, nil
}

// ParseGuardedCommand reports matched=true for every reserved bootstrap-control
// prefix. Malformed or unsupported control records must never fall through to
// legacy protobuf decoding.
func ParseGuardedCommand(payload []byte, validateInner GuardedInnerValidator) (command GuardedCommand, matched bool, err error) {
	if len(payload) == 0 || payload[0] != controlPrefix {
		return GuardedCommand{}, false, nil
	}
	kind, body, err := parseGuardedControlFrame(payload)
	if err != nil {
		return GuardedCommand{}, true, err
	}
	command, err = parseGuardedBody(kind, body, validateInner)
	return command, true, err
}

func parseGuardedControlFrame(payload []byte) (ControlKind, []byte, error) {
	if len(payload) < controlHeaderSize || !bytes.Equal(payload[1:3], controlMagic[:]) {
		return 0, nil, errors.WithStack(ErrInvalid)
	}
	if payload[3] != controlVersion {
		return 0, nil, errors.WithStack(ErrUnsupported)
	}
	kind := ControlKind(payload[4])
	if !guardedControlKind(kind) {
		return 0, nil, errors.WithStack(ErrUnsupported)
	}
	bodyLen := int(binary.BigEndian.Uint32(payload[5:9]))
	if bodyLen != len(payload)-controlHeaderSize || bodyLen < guardedBodyHeaderSize || len(payload) > MaxFileSize {
		return 0, nil, errors.WithStack(ErrInvalid)
	}
	return kind, payload[controlHeaderSize:], nil
}

func parseGuardedBody(kind ControlKind, body []byte, validateInner GuardedInnerValidator) (GuardedCommand, error) {
	command := GuardedCommand{Kind: kind, Witness: decodeWitness(body[:WitnessSize])}
	offset := WitnessSize
	copy(command.OperationID[:], body[offset:offset+operationIDSize])
	offset += operationIDSize
	innerLen := int(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4
	if innerLen != len(body)-offset || innerLen == 0 {
		return GuardedCommand{}, errors.WithStack(ErrInvalid)
	}
	command.Inner = bytes.Clone(body[offset:])
	if err := validateGuardedCommand(command, validateInner); err != nil {
		return GuardedCommand{}, err
	}
	return command, nil
}

func validateGuardedCommand(command GuardedCommand, validateInner GuardedInnerValidator) error {
	if !guardedControlKind(command.Kind) {
		return errors.WithStack(ErrUnsupported)
	}
	if !validActiveWitness(command.Witness) || command.OperationID == (OperationID{}) || len(command.Inner) == 0 {
		return errors.WithStack(ErrInvalid)
	}
	if len(command.Inner) > MaxFileSize-controlHeaderSize-guardedBodyHeaderSize {
		return errors.WithStack(ErrInvalid)
	}
	if validateInner == nil {
		return errors.WithStack(ErrInvalid)
	}
	return errors.WithStack(validateInner(command.Kind, command.Inner))
}

func guardedControlKind(kind ControlKind) bool {
	return kind == ControlGuardedWatermark || kind == ControlGuardedCatalog
}

func validActiveWitness(w Witness) bool {
	return w.Cluster != (UUID{}) && w.ActivationIndex != 0 && w.Generation != 0 &&
		w.ConfigurationIndex != 0 && w.ConfigurationDigest != (Digest{})
}

func encodeWitness(dst []byte, witness Witness) {
	copy(dst[:16], witness.Cluster[:])
	binary.BigEndian.PutUint64(dst[16:24], witness.Group)
	binary.BigEndian.PutUint64(dst[24:32], witness.ActivationIndex)
	binary.BigEndian.PutUint64(dst[32:40], witness.Generation)
	binary.BigEndian.PutUint64(dst[40:48], witness.ConfigurationIndex)
	copy(dst[48:80], witness.ConfigurationDigest[:])
}

func decodeWitness(src []byte) Witness {
	var witness Witness
	copy(witness.Cluster[:], src[:16])
	witness.Group = binary.BigEndian.Uint64(src[16:24])
	witness.ActivationIndex = binary.BigEndian.Uint64(src[24:32])
	witness.Generation = binary.BigEndian.Uint64(src[32:40])
	witness.ConfigurationIndex = binary.BigEndian.Uint64(src[40:48])
	copy(witness.ConfigurationDigest[:], src[48:80])
	return witness
}

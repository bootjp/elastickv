package main

import (
	"bytes"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var errBootstrapAdmissionPayload = errors.New("bootstrap proposal payload is malformed, unsupported, or outside its admission lane")

const (
	bootstrapPayloadSingle           byte = 0x00
	bootstrapPayloadBatch            byte = 0x01
	bootstrapPayloadHLC              byte = 0x02
	bootstrapPayloadImport           byte = 0x09
	bootstrapPayloadPromote          byte = 0x0b
	bootstrapPayloadRetire           byte = 0x0c
	bootstrapPayloadCleanup          byte = 0x0d
	bootstrapPayloadBackup           byte = 0x0e
	bootstrapPayloadReadiness        byte = 0x0f
	bootstrapPayloadReplicatedBackup byte = 0x16
	bootstrapPayloadScalarSize            = 8
	bootstrapPayloadMaximumSize           = 64 * 1024 * 1024
)

// validateBootstrapAdmissionPayload checks the COMPLETE final native bytes,
// outside A. A tag selects a decoder, never permission by itself. There is no
// legacy bare-protobuf fallback or unverified encrypted-envelope exception.
// This structural lane check does not replace applied policy/receipt checks.
func validateBootstrapAdmissionPayload(kind raftadmission.Kind, payload []byte) error {
	if len(payload) == 0 || len(payload) > bootstrapPayloadMaximumSize {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	validate := bootstrapAdmissionPayloadValidator(kind)
	if validate == nil {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	if err := validate(payload); err != nil {
		return errors.Join(errBootstrapAdmissionPayload, err)
	}
	return nil
}

func bootstrapAdmissionPayloadValidator(kind raftadmission.Kind) func([]byte) error {
	switch kind {
	case raftadmission.Data, raftadmission.Maintenance:
		return validateBootstrapDataPayload
	case raftadmission.HLC:
		return validateBootstrapHLCPayload
	case raftadmission.Allocator:
		return validateBootstrapAllocatorPayload
	case raftadmission.Catalog:
		return validateBootstrapCatalogPayload
	case raftadmission.Registration:
		return validateBootstrapRegistrationPayload
	case raftadmission.Control:
		return validateBootstrapControlPayload
	case raftadmission.Recovery:
		return validateBootstrapActivationPayload
	case raftadmission.Rotation:
		return validateBootstrapRotationPayload
	default:
		return nil
	}
}

func validateBootstrapActivationPayload(payload []byte) error {
	_, matched, err := raftbootstrap.ParseActivationCommand(payload)
	if err != nil || !matched {
		return errors.Join(errBootstrapAdmissionPayload, err)
	}
	return nil
}

func validateBootstrapControlPayload(payload []byte) error {
	if !bootstrapPreparePayload(payload) {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return nil
}

func validateBootstrapHLCPayload(payload []byte) error {
	if len(payload) != 1+bootstrapPayloadScalarSize || payload[0] != bootstrapPayloadHLC {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	// HLC has 48 physical bits. Both KV/TSO accept the same positive ceiling;
	// values outside the timestamp domain must not become bootstrap progress.
	ceiling := binary.BigEndian.Uint64(payload[1:])
	if ceiling == 0 || ceiling >= uint64(1)<<48 {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return nil
}

func validateBootstrapAllocatorPayload(payload []byte) error {
	// These versioned envelopes are private in kv/tso_fsm.go and tso_pending.go.
	// Compare the complete frame after decoding its scalar, never just its tag.
	if bytes.Equal(payload, []byte("\x07TSOC\x01")) {
		return nil
	}
	const headerSize = len("\x07TSOF\x01")
	if len(payload) != headerSize+bootstrapPayloadScalarSize {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	floor := binary.BigEndian.Uint64(payload[headerSize:])
	if floor == 0 || floor == ^uint64(0) {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	for _, header := range []string{"\x07TSOF\x01", "\x07TSOD\x01", "\x07TSOP\x01"} {
		canonical := binary.BigEndian.AppendUint64([]byte(header), floor)
		if bytes.Equal(payload, canonical) {
			return nil
		}
	}
	return errors.WithStack(errBootstrapAdmissionPayload)
}

func validateBootstrapRegistrationPayload(payload []byte) error {
	if payload[0] != fsmwire.OpRegistration {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	registration, err := fsmwire.DecodeRegistration(payload[1:])
	if err != nil {
		return errors.Wrap(err, "decode bootstrap writer registration")
	}
	if registration.DEKID == 0 || registration.FullNodeID == 0 {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return nil
}

func validateBootstrapCatalogPayload(payload []byte) error {
	command, matched, err := kv.ParseGuardedBootstrapCommand(payload)
	if err != nil || !matched {
		return errors.Join(errBootstrapAdmissionPayload, err)
	}
	switch command.Kind {
	case raftbootstrap.ControlGuardedWatermark:
		return nil // The existing guarded parser fully validates the inner scalar.
	case raftbootstrap.ControlGuardedCatalog:
		// The guarded parser validates the catalog shape and witness. Also
		// reject unknown nested protobuf fields/enums in the decoded request.
		var request pb.Request
		if err := decodeBootstrapAdmissionProto(command.Inner[1:], &request); err != nil {
			return err
		}
		return validateBootstrapKVRequest(&request)
	case raftbootstrap.ControlActivate, raftbootstrap.ControlReserve, raftbootstrap.ControlAbort:
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return errors.WithStack(errBootstrapAdmissionPayload)
}

func validateBootstrapDataPayload(payload []byte) error {
	if _, matched, err := kv.DecodeWatermarkAdvance(payload); matched {
		return errors.Wrap(err, "decode data read watermark")
	}
	switch payload[0] {
	case bootstrapPayloadSingle:
		var request pb.Request
		if err := decodeBootstrapAdmissionProto(payload[1:], &request); err != nil {
			return err
		}
		return validateBootstrapKVRequest(&request)
	case bootstrapPayloadBatch:
		return validateBootstrapKVBatch(payload[1:])
	case bootstrapPayloadBackup:
		return validateBootstrapBackupPayload(payload)
	case bootstrapPayloadReplicatedBackup:
		return validateBootstrapReplicatedBackupPayload(payload)
	default:
		return validateBootstrapMigrationPayload(payload)
	}
}

func validateBootstrapKVBatch(payload []byte) error {
	var command pb.RaftCommand
	if err := decodeBootstrapAdmissionProto(payload, &command); err != nil {
		return err
	}
	if len(command.Requests) == 0 {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	for _, request := range command.Requests {
		if err := validateBootstrapKVRequest(request); err != nil {
			return err
		}
	}
	return nil
}

func validateBootstrapKVRequest(request *pb.Request) error {
	if request == nil || request.Ts == 0 || request.Ts == ^uint64(0) || len(request.Mutations) == 0 ||
		(!request.IsTxn && request.Phase != pb.Phase_NONE) {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return nil
}

func validateBootstrapBackupPayload(payload []byte) error {
	// This existing decoder checks every legacy backup/prepare subtype and its
	// complete length. Only after that parse do we distinguish the two lanes.
	if err := kv.ValidateForwardedAdminPayload(payload); err != nil {
		return errors.Wrap(err, "decode data backup control")
	}
	if bootstrapPreparePayload(payload) {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return nil
}

func validateBootstrapReplicatedBackupPayload(payload []byte) error {
	var command kv.ReplicatedBackupCommand
	const headerSize = 2 // opcode and version
	if len(payload) != headerSize+binary.Size(command) {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	if _, err := binary.Decode(payload[headerSize:], binary.BigEndian, &command); err != nil {
		return errors.Wrap(err, "decode replicated backup control")
	}
	canonical, err := kv.EncodeReplicatedBackupCommand(command)
	if err != nil || !bytes.Equal(payload, canonical) {
		return errors.Join(errBootstrapAdmissionPayload, err)
	}
	return nil
}

func validateBootstrapMigrationPayload(payload []byte) error {
	var message proto.Message
	switch payload[0] {
	case bootstrapPayloadImport:
		message = &pb.ImportRangeVersionsRequest{}
	case bootstrapPayloadPromote:
		message = &pb.PromoteStagedVersionsRequest{}
	case bootstrapPayloadCleanup:
		message = &pb.CleanupMigrationRequest{}
	case bootstrapPayloadReadiness:
		message = &pb.TargetStagedReadinessRequest{}
	case bootstrapPayloadRetire:
		if len(payload) == 1+bootstrapPayloadScalarSize && binary.BigEndian.Uint64(payload[1:]) != 0 {
			return nil
		}
		return errors.WithStack(errBootstrapAdmissionPayload)
	default:
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	if len(payload) == 1 {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return decodeBootstrapAdmissionProto(payload[1:], message)
}

func decodeBootstrapAdmissionProto(payload []byte, message proto.Message) error {
	// Discarding unknowns and comparing the FULL deterministic re-encoding
	// rejects unknown fields at every nesting level, duplicates, concatenated
	// commands, overlong varints and trailing bytes instead of silently accepting
	// protobuf's usual last-value-wins/unknown-field semantics.
	if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload, message); err != nil {
		return errors.Wrap(err, "decode bootstrap proposal protobuf")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || !bytes.Equal(payload, canonical) || !bootstrapAdmissionKnownFields(message.ProtoReflect()) {
		return errors.Join(errBootstrapAdmissionPayload, err)
	}
	return nil
}

func bootstrapAdmissionKnownFields(message protoreflect.Message) bool {
	valid := true
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		valid = bootstrapAdmissionKnownField(field, value)
		return valid
	})
	return valid
}

func bootstrapAdmissionKnownField(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
	if field.IsMap() {
		return false // None of the supported native command schemas has maps.
	}
	if field.IsList() {
		list := value.List()
		for i := 0; i < list.Len(); i++ {
			if !bootstrapAdmissionKnownValue(field, list.Get(i)) {
				return false
			}
		}
		return true
	}
	return bootstrapAdmissionKnownValue(field, value)
}

func bootstrapAdmissionKnownValue(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
	if field.Enum() != nil {
		return field.Enum().Values().ByNumber(value.Enum()) != nil
	}
	if field.Message() != nil {
		return bootstrapAdmissionKnownFields(value.Message())
	}
	return true
}

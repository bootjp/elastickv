package main

import (
	"bytes"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/cockroachdb/errors"
)

// Matches EncryptionAdminServer's wrapped-DEK boundary; projection validation
// must not authorize a frame the production receiver would refuse to create.
const bootstrapRotationWrappedKeyLimit = 4096

// This is only the complete structural lane decoder. Actual rotation
// admission additionally requires a local exact recipe in the projection.
// Bootstrap, registration, future subtags and unrelated admin remain outside
// this lane; an opcode/prefix alone never authorizes their effects.
func validateBootstrapRotationPayload(payload []byte) error {
	rotation, err := decodeBootstrapRotationPayload(payload)
	if err != nil {
		return err
	}
	switch rotation.SubTag {
	case fsmwire.RotateSubRotateDEK:
		if len(rotation.Wrapped) == 0 {
			return errors.WithStack(errBootstrapAdmissionPayload)
		}
	case fsmwire.RotateSubEnableStorageEnvelope:
		return validateBootstrapEnvelopeActivation(rotation, fsmwire.PurposeStorage)
	case fsmwire.RotateSubEnableRaftEnvelope:
		return validateBootstrapEnvelopeActivation(rotation, fsmwire.PurposeRaft)
	default:
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return nil
}

func decodeBootstrapRotationPayload(payload []byte) (fsmwire.RotationPayload, error) {
	if len(payload) == 0 || len(payload) > bootstrapPayloadMaximumSize || payload[0] != fsmwire.OpRotation {
		return fsmwire.RotationPayload{}, errors.WithStack(errBootstrapAdmissionPayload)
	}
	rotation, err := fsmwire.DecodeRotation(payload[1:])
	if err != nil {
		return rotation, errors.Join(errBootstrapAdmissionPayload, err)
	}
	if !validBootstrapRotationRegistration(rotation) || !bytes.Equal(payload[1:], fsmwire.EncodeRotation(rotation)) {
		return rotation, errors.WithStack(errBootstrapAdmissionPayload)
	}
	return rotation, nil
}

func validBootstrapRotationRegistration(rotation fsmwire.RotationPayload) bool {
	return rotation.DEKID != encryption.ReservedKeyID && rotation.ProposerRegistration.DEKID == rotation.DEKID &&
		rotation.ProposerRegistration.FullNodeID != 0 && len(rotation.Wrapped) <= bootstrapRotationWrappedKeyLimit
}

func validateBootstrapEnvelopeActivation(rotation fsmwire.RotationPayload, purpose fsmwire.Purpose) error {
	if rotation.Purpose != purpose || len(rotation.Wrapped) != 0 {
		return errors.WithStack(errBootstrapAdmissionPayload)
	}
	return nil
}

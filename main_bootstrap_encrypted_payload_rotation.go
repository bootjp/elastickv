package main

import (
	"math"

	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// Encode the exact frame EncryptionAdminServer.RotateDEK proposes. Refuse
// caller fields the protobuf/RPC boundary would reject, rather than narrowing
// an epoch or treating unknown protobuf fields as part of an authorization.
func bootstrapRotationPayloadFromRequest(request *pb.RotateDEKRequest) ([]byte, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.WithStack(errBootstrapRotationRecipe)
	}
	epoch := request.GetProposerLocalEpoch()
	if epoch > math.MaxUint16 || len(request.GetWrappedNewDek()) > bootstrapRotationWrappedKeyLimit {
		return nil, errors.WithStack(errBootstrapRotationRecipe)
	}
	var purpose fsmwire.Purpose
	switch request.GetPurpose() {
	case pb.RotateDEKRequest_PURPOSE_STORAGE:
		purpose = fsmwire.PurposeStorage
	case pb.RotateDEKRequest_PURPOSE_RAFT:
		purpose = fsmwire.PurposeRaft
	case pb.RotateDEKRequest_PURPOSE_UNSPECIFIED:
		return nil, errors.WithStack(errBootstrapRotationRecipe)
	default:
		return nil, errors.WithStack(errBootstrapRotationRecipe)
	}
	rotation := fsmwire.RotationPayload{SubTag: fsmwire.RotateSubRotateDEK, DEKID: request.GetNewDekId(), Purpose: purpose,
		Wrapped: request.GetWrappedNewDek(), ProposerRegistration: fsmwire.RegistrationPayload{
			DEKID: request.GetNewDekId(), FullNodeID: request.GetProposerNodeId(), LocalEpoch: uint16(epoch)}}
	payload := append([]byte{fsmwire.OpRotation}, fsmwire.EncodeRotation(rotation)...)
	if err := validateBootstrapRotationPayload(payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func bootstrapRotationPayloadGrants(group uint64, requests ...*pb.RotateDEKRequest) ([]bootstrapRotationPayloadGrant, error) {
	grants := make([]bootstrapRotationPayloadGrant, 0, len(requests))
	for _, request := range requests {
		payload, err := bootstrapRotationPayloadFromRequest(request)
		if err != nil {
			return nil, err
		}
		grant, err := newBootstrapRotationPayloadGrant(group, payload)
		if err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, nil
}

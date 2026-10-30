package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// IsTxn and the initial phase distinguish raw, 1PC and prepared creation.
// Resolution is authorized separately by a retained PREPARE capsule.
func validClaimCreationPhase(isTxn bool, phase pb.Phase) bool {
	switch phase {
	case pb.Phase_NONE:
		return true
	case pb.Phase_PREPARE:
		return isTxn
	case pb.Phase_COMMIT, pb.Phase_ABORT:
		return false
	default:
		return false
	}
}

func claimRequestModeMatches(request *pb.Request, intent *pb.TimestampClaimIntent) bool {
	return request.IsTxn == intent.IsTxn && request.Phase == intent.InitialPhase
}

func validateClaimIntentMode(intent *pb.TimestampClaimIntent) error {
	if !validClaimCreationPhase(intent.IsTxn, intent.InitialPhase) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	if !intent.IsTxn && (intent.ReadTimestamp != 0 || len(intent.PrimaryKey) != 0 || intent.PreviousCommitTimestamp != 0) {
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

// The two persistence timestamp slots are not interchangeable, even when
// both contain the same concrete claim timestamp.
func validateClaimCreationMeta(phase pb.Phase, meta TxnMeta) error {
	switch phase {
	case pb.Phase_NONE:
		if meta.PrepareCommitTS != 0 {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
	case pb.Phase_PREPARE:
		if meta.CommitTS != 0 {
			return errors.WithStack(ErrTSOClaimInvalid)
		}
	case pb.Phase_COMMIT, pb.Phase_ABORT:
		return errors.WithStack(ErrTSOClaimInvalid)
	default:
		return errors.WithStack(ErrTSOClaimInvalid)
	}
	return nil
}

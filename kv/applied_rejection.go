package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// AppliedRequestRejection proves that one Raft entry reached an apply-time
// verdict. It does not rule out an earlier attempt of the logical operation.
type AppliedRequestRejection struct {
	commitIndex uint64
	cause       error
}

func (r *AppliedRequestRejection) Error() string {
	return "raft request rejected at apply"
}

func (r *AppliedRequestRejection) Unwrap() error {
	return r.cause
}

func (r *AppliedRequestRejection) CommitIndex() uint64 {
	return r.commitIndex
}

func singleRequestApplyVerdict(req *pb.Request, requestCount int, commitIndex uint64, err error) bool {
	if requestCount != 1 || req == nil || commitIndex == 0 ||
		(req.Phase != pb.Phase_NONE && req.Phase != pb.Phase_PREPARE) {
		return false
	}
	return recognizedApplyVerdict(err)
}

func recognizedApplyVerdict(err error) bool {
	return errors.Is(err, store.ErrWriteConflict) ||
		errors.Is(err, ErrTxnLocked) ||
		errors.Is(err, ErrComposed1Violation) ||
		errors.Is(err, ErrComposed1VersionGCd) ||
		errors.Is(err, ErrRouteWriteFenced) ||
		errors.Is(err, ErrRouteWriteTimestampTooLow)
}

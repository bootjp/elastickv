package kv

import (
	"context"
	"crypto/sha256"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

func timestampBindingForCreationRequest(request *pb.Request) (TimestampBinding, error) {
	var binding TimestampBinding
	if err := validateTimestampBindingRequest(request); err != nil {
		return binding, err
	}
	copy(binding.OperationID[:], request.TimestampBindingOperationId)
	timestamp, err := prepareCreationPersistenceTimestamp(request)
	if err != nil {
		return binding, errors.WithStack(err)
	}
	binding.Timestamp = timestamp
	binding.IsTxn = request.IsTxn
	binding.Phase = request.Phase
	manifest := proto.CloneOf(request)
	manifest.TimestampBindingOperationId = nil
	manifest.TimestampAdmissionFloor = 0
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(manifest)
	if err != nil {
		return binding, errors.Wrap(err, "encode timestamp binding manifest")
	}
	binding.ManifestDigest = sha256.Sum256(raw)
	if !binding.valid() {
		return binding, errors.WithStack(ErrTimestampBindingInvalid)
	}
	return binding, nil
}

func validateTimestampBindingRequest(request *pb.Request) error {
	var operationID [16]byte
	if request == nil || !timestampAdmissionCreation(request) ||
		len(request.TimestampBindingOperationId) != len(operationID) || len(request.ProtoReflect().GetUnknown()) != 0 {
		return errors.WithStack(ErrTimestampBindingInvalid)
	}
	for _, mutation := range request.Mutations {
		if mutation == nil || len(mutation.ProtoReflect().GetUnknown()) != 0 {
			return errors.WithStack(ErrTimestampBindingInvalid)
		}
	}
	return nil
}

func (f *kvFSM) applyTimestampBoundCreation(ctx context.Context, request *pb.Request) any {
	if !f.timestampBindingEnabled.Load() || f.pendingApplyIdx == 0 {
		return errors.WithStack(ErrTimestampBindingInvalid)
	}
	binding, err := timestampBindingForCreationRequest(request)
	if err != nil {
		return err
	}
	f.beginDeferredApplyNotices()
	var commitTS uint64
	already, err := ApplyTimestampBoundCreation(ctx, f.store, f.pendingApplyIdx, binding, func(entryCtx context.Context) error {
		var applyErr error
		commitTS, applyErr = f.applyRequestData(entryCtx, request)
		return applyErr
	})
	if err != nil {
		f.discardDeferredApplyNotices()
		if isTimestampBoundCreationVerdict(err) {
			return err
		}
		return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "apply timestamp-bound creation entry"))
	}
	if !already {
		f.observeAppliedCommitTS(commitTS)
	}
	f.flushDeferredApplyNotices()
	return nil
}

func isTimestampBoundCreationVerdict(err error) bool {
	return errors.Is(err, ErrTimestampBindingConflict) ||
		errors.Is(err, ErrTimestampBindingInvalid) ||
		errors.Is(err, ErrTxnInvalidMeta) ||
		errors.Is(err, ErrTxnCommitTSRequired) ||
		errors.Is(err, ErrTxnPrimaryKeyRequired) ||
		errors.Is(err, ErrPrepareAdmissionFenced) ||
		errors.Is(err, ErrComposed1Violation) ||
		isRawApplyVerdict(err)
}

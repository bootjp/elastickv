package kv

import (
	"context"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const timestampAdmissionValidationBudget = 5 * time.Second

// StampTimestampAdmission binds creation to the data group's released epoch
// only after validating its unchanged persistence timestamp against group 0.
// Resolution is exempt. Incoming creation epochs are never trusted, even when
// the group has not yet activated the protocol.
func StampTimestampAdmission(ctx context.Context, allocator TimestampAllocator, st store.MVCCStore, reqs []*pb.Request) error {
	creation := clearTimestampAdmission(reqs)
	if !creation || st == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(nonNilTSOContext(ctx), timestampAdmissionValidationBudget)
	defer cancel()
	sealed, floor, err := readPrepareEvidenceCreationGate(ctx, st)
	if err != nil {
		return errors.Wrap(err, "read timestamp admission epoch")
	}
	if sealed {
		return errors.WithStack(ErrPrepareCreationSealed)
	}
	if floor == 0 {
		return nil
	}
	if err := validateTimestampAdmission(ctx, allocator, reqs, floor); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "timestamp admission budget exhausted")
	}
	for _, request := range reqs {
		if timestampAdmissionCreation(request) {
			request.TimestampAdmissionFloor = floor
		}
	}
	return nil
}

func timestampAdmissionCreation(request *pb.Request) bool {
	return request != nil && (!request.IsTxn || request.Phase == pb.Phase_NONE || request.Phase == pb.Phase_PREPARE)
}

func clearTimestampAdmission(reqs []*pb.Request) bool {
	creation := false
	for _, request := range reqs {
		if timestampAdmissionCreation(request) {
			request.TimestampAdmissionFloor = 0
			creation = true
		}
	}
	return creation
}

func validateTimestampAdmission(ctx context.Context, allocator TimestampAllocator, reqs []*pb.Request, floor uint64) error {
	validator, ok := allocator.(DurableTimestampValidator)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	validated := make(map[uint64]struct{})
	for _, request := range reqs {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "timestamp admission budget exhausted")
		}
		if !timestampAdmissionCreation(request) {
			continue
		}
		timestamp, err := prepareCreationPersistenceTimestamp(request)
		if err != nil {
			return errors.WithStack(err)
		}
		if timestamp <= floor {
			return errors.Wrapf(ErrTSOTimestampInvalid, "creation timestamp %d does not exceed admission floor %d", timestamp, floor)
		}
		if _, exists := validated[timestamp]; exists {
			continue
		}
		if err := validator.ValidateDurableTimestamp(ctx, timestamp); err != nil {
			return errors.Wrap(err, "validate creation timestamp admission")
		}
		validated[timestamp] = struct{}{}
	}
	return nil
}

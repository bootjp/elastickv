package kv

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const timestampBindingAdmissionBudget = 5 * time.Second

// PrepareHomeRawTimestampBinding prepares one raw write for the opt-in atomic
// data-group apply path. The issuer must be a leader-owned authenticated TSO
// route, not a numeric timestamp supplied by the caller.
func PrepareHomeRawTimestampBinding(
	ctx context.Context,
	leader raftengine.LeaderView,
	metadata store.RaftControlMetadataStore,
	issue func(context.Context) (uint64, error),
	request *pb.Request,
) (*pb.Request, error) {
	operationID, err := validateHomeRawBindingInputs(leader, metadata, issue, request)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingAdmissionBudget)
	defer cancel()
	bound, found, err := readHomeBinding(ctx, leader, metadata, operationID)
	if err != nil {
		return nil, err
	}
	owned := proto.CloneOf(request)
	if found {
		owned.Ts = bound.Timestamp
		candidate, err := timestampBindingForCreationRequest(owned)
		if err != nil {
			return nil, err
		}
		if candidate != bound {
			return nil, errors.WithStack(ErrTimestampBindingConflict)
		}
		return owned, nil
	}
	timestamp, err := issue(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "issue home write timestamp")
	}
	if timestamp == 0 || leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(ErrTimestampBindingInvalid)
	}
	owned.Ts = timestamp
	if _, err := timestampBindingForCreationRequest(owned); err != nil {
		return nil, err
	}
	return owned, nil
}

func validateHomeRawBindingInputs(leader raftengine.LeaderView, metadata store.RaftControlMetadataStore,
	issue func(context.Context) (uint64, error), request *pb.Request) ([16]byte, error) {
	var operationID [16]byte
	if leader == nil || metadata == nil || issue == nil || !rawBindingRequestUnstamped(request) {
		return operationID, errors.WithStack(ErrTimestampBindingInvalid)
	}
	if len(request.TimestampBindingOperationId) != len(operationID) {
		return operationID, errors.WithStack(ErrTimestampBindingInvalid)
	}
	copy(operationID[:], request.TimestampBindingOperationId)
	if operationID == ([16]byte{}) || leader.State() != raftengine.StateLeader {
		return operationID, errors.WithStack(ErrTimestampBindingInvalid)
	}
	return operationID, nil
}

func rawBindingRequestUnstamped(request *pb.Request) bool {
	return request != nil && !request.IsTxn && request.Phase == pb.Phase_NONE &&
		request.Ts == 0 && request.TimestampAdmissionFloor == 0
}

func readHomeBinding(ctx context.Context, leader raftengine.LeaderView, metadata store.RaftControlMetadataStore,
	operationID [16]byte) (TimestampBinding, bool, error) {
	if _, err := leader.LinearizableRead(ctx); err != nil {
		return TimestampBinding{}, false, errors.Wrap(err, "read home binding barrier")
	}
	bound, found, err := LookupTimestampBinding(ctx, metadata, operationID)
	if err != nil {
		return TimestampBinding{}, false, errors.Wrap(err, "lookup home binding")
	}
	if err := ctx.Err(); err != nil {
		return TimestampBinding{}, false, errors.WithStack(err)
	}
	if leader.State() != raftengine.StateLeader {
		return TimestampBinding{}, false, errors.WithStack(ErrLeaderNotFound)
	}
	return bound, found, nil
}

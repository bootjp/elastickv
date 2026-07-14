package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// MarshalTargetStagedReadinessCommand encodes a target-group readiness guard
// as a Raft FSM command. The target Internal RPC handler uses this instead of
// mutating its local store directly so an acknowledged guard has been applied
// by the target group's voters.
func MarshalTargetStagedReadinessCommand(req *pb.TargetStagedReadinessRequest) ([]byte, error) {
	if req == nil {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(b) >= maxMarshaledCommandSize {
		return nil, errors.New("marshaled target readiness request too large")
	}
	return prependByte(raftEncodeTargetReadiness, b), nil
}

func (f *kvFSM) applyTargetStagedReadiness(ctx context.Context, data []byte) any {
	req := &pb.TargetStagedReadinessRequest{}
	if err := proto.Unmarshal(data, req); err != nil {
		return errors.WithStack(err)
	}
	writer, ok := f.store.(store.MigrationTargetReadinessWriter)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	if err := writer.ApplyTargetStagedReadiness(ctx, targetStagedReadinessStateFromProto(req)); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func targetStagedReadinessStateFromProto(req *pb.TargetStagedReadinessRequest) store.TargetStagedReadinessState {
	if req == nil {
		return store.TargetStagedReadinessState{}
	}
	return store.TargetStagedReadinessState{
		JobID:                  req.GetJobId(),
		RouteStart:             bytes.Clone(req.GetRouteStart()),
		RouteEnd:               bytes.Clone(req.GetRouteEnd()),
		ExpectedCutoverVersion: req.GetExpectedCutoverVersion(),
		MigrationJobID:         req.GetMigrationJobId(),
		MinWriteTSExclusive:    req.GetMinWriteTsExclusive(),
		Armed:                  req.GetArmed(),
	}
}

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
	state := targetStagedReadinessStateFromProto(req)
	state = f.preserveMigrationTrackerMinimum(ctx, state)
	if err := writer.ApplyTargetStagedReadiness(ctx, state); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (f *kvFSM) preserveMigrationTrackerMinimum(ctx context.Context, state store.TargetStagedReadinessState) store.TargetStagedReadinessState {
	if !state.TrackWrites {
		return state
	}
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return state
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return state
	}
	for _, current := range states {
		if current.JobID == state.JobID && (state.MinAdmittedTS == 0 || current.MinAdmittedTS < state.MinAdmittedTS) {
			state.MinAdmittedTS = current.MinAdmittedTS
		}
	}
	return state
}

func (f *kvFSM) recordMigrationWrite(ctx context.Context, muts []*pb.Mutation, commitTS uint64) error {
	if commitTS == 0 {
		return nil
	}
	reader, writer, ok := f.migrationReadinessStore()
	if !ok {
		return nil
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	return recordMigrationWriteInStates(ctx, writer, states, muts, commitTS)
}

func (f *kvFSM) migrationReadinessStore() (store.MigrationTargetReadinessReader, store.MigrationTargetReadinessWriter, bool) {
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return nil, nil, false
	}
	writer, ok := f.store.(store.MigrationTargetReadinessWriter)
	if !ok {
		return nil, nil, false
	}
	return reader, writer, true
}

func recordMigrationWriteInStates(ctx context.Context, writer store.MigrationTargetReadinessWriter, states []store.TargetStagedReadinessState, muts []*pb.Mutation, commitTS uint64) error {
	for _, state := range states {
		if !state.Armed || !state.TrackWrites || !migrationMutationsIntersect(muts, state.RouteStart, state.RouteEnd) {
			continue
		}
		if state.MinAdmittedTS != 0 && state.MinAdmittedTS <= commitTS {
			continue
		}
		state.MinAdmittedTS = commitTS
		if err := writer.ApplyTargetStagedReadiness(ctx, state); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func migrationMutationsIntersect(muts []*pb.Mutation, routeStart []byte, routeEnd []byte) bool {
	for _, mut := range muts {
		if mut == nil || len(mut.Key) == 0 || isTxnInternalKey(mut.Key) {
			continue
		}
		var start, end []byte
		if mut.GetOp() == pb.Op_DEL_PREFIX {
			start, end = routePrefixRange(mut.Key)
		} else {
			start, end = readinessRouteRangeForScan(mut.Key, nextScanCursor(mut.Key))
		}
		if routeRangeIntersects(start, end, routeStart, routeEnd) {
			return true
		}
	}
	return false
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
		SourceWriteFence:       req.GetSourceWriteFence(),
		SourceReadFence:        req.GetSourceReadFence(),
		RetentionPinTS:         req.GetRetentionPinTs(),
		TrackWrites:            req.GetTrackWrites(),
		MinAdmittedTS:          req.GetMinAdmittedTs(),
	}
}

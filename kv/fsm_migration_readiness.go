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
	if req.GetTransferSizeGuardVersion() != 0 &&
		(!req.GetRequireConfigurationIndex() || req.GetExpectedConfigurationIndex() == 0 ||
			req.GetExpectedConfigurationIndex() != f.AppliedConfigurationIndex()) {
		return errors.WithStack(ErrMigrationConfigurationChanged)
	}
	return f.applyTargetStagedReadinessRequest(ctx, req)
}

func (f *kvFSM) applyTargetStagedReadinessRequest(ctx context.Context, req *pb.TargetStagedReadinessRequest) any {
	writer, ok := f.store.(store.MigrationTargetReadinessWriter)
	if !ok {
		return haltErr(errors.Wrap(errors.Mark(store.ErrNotSupported, ErrTargetReadinessApply), "kv/fsm: target staged readiness store"))
	}
	state := targetStagedReadinessStateFromProto(req)
	state, err := f.preserveMigrationTrackerMinimum(ctx, state)
	if err != nil {
		if errors.Is(err, store.ErrMigrationOwnership) {
			return err
		}
		return haltErr(errors.Wrap(errors.Mark(err, ErrTargetReadinessApply), "kv/fsm: inspect source migration control"))
	}
	if err := applyTargetStagedReadinessAt(ctx, writer, state, f.pendingApplyIdx); err != nil {
		// Validation is a pure function of the entry, so every replica rejects
		// the same malformed request. Halting on it would let one bad internal
		// RPC stop the group instead of returning InvalidArgument to its caller.
		if errors.Is(err, store.ErrInvalidReadinessState) || errors.Is(err, store.ErrMigrationRetired) {
			return errors.Wrap(err, "kv/fsm: target staged readiness")
		}
		// Anything else is a local Pebble write. A replica that returns an
		// ordinary error here advances past the entry without installing the
		// source fence, write tracker or target guard that healthy replicas
		// installed, and then accepts user writes they reject -- divergence the
		// runner's voter probe cannot repair after the fact.
		return haltErr(errors.Wrap(errors.Mark(err, ErrTargetReadinessApply), "kv/fsm: persist target staged readiness"))
	}
	if state.Armed && f.hlc != nil {
		f.hlc.Observe(max(state.MinWriteTSExclusive, f.store.LastCommitTS()))
	}
	return nil
}

func (f *kvFSM) preserveMigrationTrackerMinimum(ctx context.Context, state store.TargetStagedReadinessState) (store.TargetStagedReadinessState, error) {
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return state, nil
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return state, errors.WithStack(err)
	}
	for _, current := range states {
		if current.JobID == state.JobID {
			return preserveSourceMigrationControl(state, current)
		}
	}
	return state, nil
}

func preserveSourceMigrationControl(state, current store.TargetStagedReadinessState) (store.TargetStagedReadinessState, error) {
	if armedSourceMigrationControl(current) {
		if !sameSourceMigrationControlRange(state, current) {
			return state, errors.WithStack(store.ErrMigrationOwnership)
		}
		state = mergeSourceMigrationControl(state, current)
	}
	if state.TrackWrites && (state.MinAdmittedTS == 0 || current.MinAdmittedTS < state.MinAdmittedTS) {
		state.MinAdmittedTS = current.MinAdmittedTS
	}
	return state, nil
}

func armedSourceMigrationControl(state store.TargetStagedReadinessState) bool {
	return state.Armed && (state.TrackWrites || state.SourceWriteFence || state.SourceReadFence)
}

func mergeSourceMigrationControl(state, current store.TargetStagedReadinessState) store.TargetStagedReadinessState {
	state.SourceWriteFence = state.SourceWriteFence || current.SourceWriteFence
	state.SourceReadFence = state.SourceReadFence || current.SourceReadFence
	state.TrackWrites = state.TrackWrites || current.TrackWrites
	state.TransferSizeGuardVersion = max(state.TransferSizeGuardVersion, current.TransferSizeGuardVersion)
	state.MinWriteTSExclusive = max(state.MinWriteTSExclusive, current.MinWriteTSExclusive)
	if state.RetentionPinTS == 0 || current.RetentionPinTS < state.RetentionPinTS {
		state.RetentionPinTS = current.RetentionPinTS
	}
	return state
}

func sameSourceMigrationControlRange(state, current store.TargetStagedReadinessState) bool {
	return state.Armed && state.MigrationJobID == current.MigrationJobID &&
		bytes.Equal(state.RouteStart, current.RouteStart) && bytes.Equal(state.RouteEnd, current.RouteEnd)
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
		return errors.WithStack(errors.Mark(err, errMigrationTrackerPersistence))
	}
	// Tracker updates share the Raft entry with the following user mutation.
	// Leave metaAppliedIndex to the data batch so replay cannot skip a write if
	// the process stops after only the tracker record is persisted.
	if err := recordMigrationWriteInStates(ctx, writer, states, muts, commitTS, 0); err != nil {
		return errors.WithStack(errors.Mark(err, errMigrationTrackerPersistence))
	}
	return nil
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

func recordMigrationWriteInStates(ctx context.Context, writer store.MigrationTargetReadinessWriter, states []store.TargetStagedReadinessState, muts []*pb.Mutation, commitTS uint64, appliedIndex uint64) error {
	for _, state := range states {
		if !state.Armed || !state.TrackWrites || !migrationMutationsIntersect(muts, state.RouteStart, state.RouteEnd) {
			continue
		}
		if state.MinAdmittedTS != 0 && state.MinAdmittedTS <= commitTS {
			continue
		}
		state.MinAdmittedTS = commitTS
		if err := applyTargetStagedReadinessAt(ctx, writer, state, appliedIndex); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func applyTargetStagedReadinessAt(ctx context.Context, writer store.MigrationTargetReadinessWriter, state store.TargetStagedReadinessState, appliedIndex uint64) error {
	if raftWriter, ok := writer.(store.MigrationTargetReadinessRaftWriter); ok {
		if err := raftWriter.ApplyTargetStagedReadinessAt(ctx, state, appliedIndex); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}
	if err := writer.ApplyTargetStagedReadiness(ctx, state); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func migrationMutationsIntersect(muts []*pb.Mutation, routeStart []byte, routeEnd []byte) bool {
	for _, mut := range muts {
		if mut == nil || isTxnInternalKey(mut.Key) {
			continue
		}
		var start, end []byte
		if mut.GetOp() == pb.Op_DEL_PREFIX {
			start, end = routePrefixRange(mut.Key)
		} else {
			if len(mut.Key) == 0 {
				continue
			}
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
		JobID:                    req.GetJobId(),
		RouteStart:               bytes.Clone(req.GetRouteStart()),
		RouteEnd:                 bytes.Clone(req.GetRouteEnd()),
		ExpectedCutoverVersion:   req.GetExpectedCutoverVersion(),
		MigrationJobID:           req.GetMigrationJobId(),
		MinWriteTSExclusive:      req.GetMinWriteTsExclusive(),
		Armed:                    req.GetArmed(),
		SourceWriteFence:         req.GetSourceWriteFence(),
		SourceReadFence:          req.GetSourceReadFence(),
		RetentionPinTS:           req.GetRetentionPinTs(),
		TrackWrites:              req.GetTrackWrites(),
		MinAdmittedTS:            req.GetMinAdmittedTs(),
		TransferSizeGuardVersion: req.GetTransferSizeGuardVersion(),
	}
}

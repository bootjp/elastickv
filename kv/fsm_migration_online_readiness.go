package kv

import (
	"bytes"
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (f *kvFSM) applyOnlineMigrationReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest) any {
	if req.GetOnlineCaptureVersion() != store.OnlineMigrationVersion || !req.GetRequireConfigurationIndex() || req.GetExpectedConfigurationIndex() == 0 || req.GetJobId() == 0 {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	if req.GetExpectedConfigurationIndex() != f.AppliedConfigurationIndex() {
		return errors.WithStack(ErrMigrationConfigurationChanged)
	}
	applier, ok := f.store.(store.RaftEntryApplier)
	if !ok || !store.OnlineMigrationSupported(f.store) {
		return haltErr(errors.Mark(store.ErrNotSupported, ErrRaftEntryApply))
	}
	err := applier.ApplyRaftEntry(ctx, f.pendingApplyIdx, func(tx context.Context) error {
		return f.applyOnlineReadinessControl(tx, req)
	})
	if err != nil {
		if onlineMigrationControlVerdict(err) {
			return errors.WithStack(err)
		}
		return haltErr(errors.Mark(err, ErrRaftEntryApply))
	}
	return nil
}

func (f *kvFSM) applyOnlineReadinessControl(ctx context.Context, req *pb.TargetStagedReadinessRequest) error {
	if !req.GetOnlineSeal() && !req.GetOnlineReady() {
		// A prejoin authorized before capture started did not have to prove
		// online support. ConfChange consumes that authorization.
		state, err := readNamespaceConfiguration(ctx, f.store)
		if err != nil {
			return err
		}
		if len(state.Joining) != 0 {
			return errors.WithStack(store.ErrOnlineMigrationIdentity)
		}
	}
	if len(req.GetOnlineTargetManifest()) != 0 {
		return f.applyOnlineTargetReadiness(ctx, req)
	}
	return f.applyOnlineSourceReadiness(ctx, req)
}

func onlineMigrationControlVerdict(err error) bool {
	return errors.Is(err, store.ErrOnlineMigrationSealed) || errors.Is(err, store.ErrOnlineMigrationIdentity) || errors.Is(err, store.ErrOnlineMigrationInvalid) || errors.Is(err, store.ErrOnlineMigrationTargetNotEmpty) || errors.Is(err, store.ErrOnlineMigrationTargetClosed) || errors.Is(err, ErrTxnLocked) || errors.Is(err, store.ErrInvalidReadinessState) || errors.Is(err, store.ErrMigrationOwnership) || errors.Is(err, store.ErrMigrationRetired)
}

func (f *kvFSM) applyOnlineSourceReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest) error {
	if !validOnlineTrackerRequest(req) || f.shardGroupID == 0 {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	lineage := [sha256LineageBytes]byte(req.GetOnlineLineage())
	generation := req.GetOnlineSourceGeneration()
	if (!req.GetOnlineSeal() || generation != "") && !store.ValidOnlineOwnershipGeneration(generation, lineage) {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	if req.GetOnlineSeal() {
		return f.applyOnlineSourceSeal(ctx, req, lineage)
	}
	return f.startOnlineSourceTracker(ctx, req, lineage)
}

func validOnlineTrackerRequest(req *pb.TargetStagedReadinessRequest) bool {
	return req.GetArmed() && req.GetTrackWrites() && req.GetRetentionPinTs() != 0 &&
		req.GetMigrationJobId() == req.GetJobId() && len(req.GetOnlineLineage()) == sha256LineageBytes
}

func (f *kvFSM) startOnlineSourceTracker(ctx context.Context, req *pb.TargetStagedReadinessRequest, lineage [sha256LineageBytes]byte) error {
	if err := f.checkOnlineMigrationLocks(ctx, req); err != nil {
		return err
	}
	identity := store.MigrationCaptureIdentity{SourceGroupID: f.shardGroupID, JobID: req.GetJobId(), Generation: 1, Index: f.pendingApplyIdx, Lineage: lineage}
	session := store.OnlineMigrationSession{
		Version: store.OnlineMigrationVersion, Identity: identity, OwnershipGeneration: req.GetOnlineSourceGeneration(),
		StartIndex: f.pendingApplyIdx, LastIndex: f.pendingApplyIdx, ConfigurationIndex: f.AppliedConfigurationIndex(),
		RouteStart: bytes.Clone(req.GetRouteStart()), RouteEnd: bytes.Clone(req.GetRouteEnd()),
	}
	if err := store.StartOnlineMigrationSession(ctx, f.store, session); err != nil {
		return errors.WithStack(err)
	}
	return onlineReadinessResponseError(f.applyTargetStagedReadinessRequest(ctx, req))
}

func (f *kvFSM) applyOnlineSourceSeal(ctx context.Context, req *pb.TargetStagedReadinessRequest, lineage [sha256LineageBytes]byte) error {
	if !req.GetSourceWriteFence() {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	session, err := store.ReadOnlineMigrationSession(ctx, f.store, req.GetJobId())
	if err != nil {
		return errors.WithStack(err)
	}
	if session.Identity.Lineage != lineage || !onlineRangeMatches(req.GetRouteStart(), req.GetRouteEnd(), session) {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	if err := f.requireOnlineSourceWriteFence(ctx, req.GetJobId()); err != nil {
		return err
	}
	// Locks elsewhere in the group may have appeared since the durable seal.
	// Drain the whole group on first seal only.
	if !session.Sealed {
		if err := f.checkOnlineMigrationLocks(ctx, req); err != nil {
			return err
		}
	}
	if _, err := store.SealOnlineMigrationSession(ctx, f.store, req.GetJobId(), f.AppliedConfigurationIndex()); err != nil {
		return errors.WithStack(err)
	}
	return onlineReadinessResponseError(f.applyTargetStagedReadinessRequest(ctx, req))
}

func (f *kvFSM) requireOnlineSourceWriteFence(ctx context.Context, jobID uint64) error {
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	guards, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, guard := range guards {
		if guard.JobID == jobID && guard.Armed && guard.SourceWriteFence && guard.TrackWrites {
			return nil
		}
	}
	return errors.WithStack(store.ErrOnlineMigrationIdentity)
}

const sha256LineageBytes = 32

func onlineReadinessResponseError(response any) error {
	if response == nil {
		return nil
	}
	if halt, ok := response.(interface{ HaltApply() error }); ok {
		return errors.WithStack(halt.HaltApply())
	}
	if err, ok := response.(error); ok {
		return errors.WithStack(err)
	}
	return errors.WithStack(ErrInvalidRequest)
}

func (f *kvFSM) applyOnlineTargetReadiness(ctx context.Context, req *pb.TargetStagedReadinessRequest) error {
	source, err := store.DecodeOnlineMigrationManifest(req.GetOnlineTargetManifest())
	if err != nil {
		return errors.Wrap(store.ErrOnlineMigrationIdentity, "invalid target manifest in command")
	}
	if source.Identity.JobID != req.GetJobId() || source.Identity.SourceGroupID == f.shardGroupID || !onlineRangeMatches(req.GetRouteStart(), req.GetRouteEnd(), source) {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	if err := f.checkOnlineMigrationLocks(ctx, req); err != nil {
		return err
	}
	if !req.GetOnlineReady() {
		return f.prepareOnlineTarget(ctx, req, source)
	}
	if err := store.ReadyOnlineMigrationTarget(ctx, f.store, source, f.AppliedConfigurationIndex(), req.GetOnlineSequence()); err != nil {
		return errors.WithStack(err)
	}
	return onlineReadinessResponseError(f.applyTargetStagedReadinessRequest(ctx, req))
}

func (f *kvFSM) prepareOnlineTarget(ctx context.Context, req *pb.TargetStagedReadinessRequest, source store.OnlineMigrationSession) error {
	if !req.GetArmed() || req.GetMigrationJobId() != req.GetJobId() || req.GetTrackWrites() || req.GetSourceReadFence() || req.GetSourceWriteFence() {
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	if source.Sealed {
		return errors.WithStack(store.ReattestOnlineMigrationTarget(ctx, f.store, source, f.AppliedConfigurationIndex()))
	}
	if err := store.PrepareOnlineMigrationTarget(ctx, f.store, source, f.AppliedConfigurationIndex(), func(key []byte) bool { return !isTxnPeerLocalControlKey(key) }); err != nil {
		return errors.WithStack(err)
	}
	return onlineReadinessResponseError(f.applyTargetStagedReadinessRequest(ctx, req))
}

func (f *kvFSM) checkOnlineMigrationLocks(ctx context.Context, req *pb.TargetStagedReadinessRequest) error {
	pending, err := PendingTxnLocksInRoute(ctx, f.store, nil, nil, ^uint64(0), 1)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(pending) != 0 {
		if len(req.GetOnlineTargetManifest()) != 0 {
			return errors.WithStack(store.ErrOnlineMigrationTargetNotEmpty)
		}
		return errors.WithStack(ErrTxnLocked)
	}
	return nil
}

// Read the same replicated manifests after snapshot installation and when
// authorizing prospective apply members. Both roles have an armed guard.
func (f *kvFSM) hasActiveOnlineMigration(ctx context.Context) (bool, error) {
	if _, ok := f.store.(store.RaftControlMetadataStore); !ok {
		return false, nil
	}
	r, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return false, nil
	}
	guards, err := r.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return false, errors.WithStack(err)
	}
	for _, g := range guards {
		if !g.Armed {
			continue
		}
		var closed bool
		if g.TrackWrites {
			source, e := store.ReadOnlineMigrationSession(ctx, f.store, g.JobID)
			closed, err = source.Closed, e
		} else {
			target, e := store.ReadOnlineMigrationTarget(ctx, f.store, g.JobID)
			closed, err = target.Closed, e
		}
		if errors.Is(err, store.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if !closed {
			return true, nil
		}
	}
	return false, nil
}

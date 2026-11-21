package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
)

func (i *Internal) exportOnlineMigration(req *pb.ExportRangeVersionsRequest, stream pb.Internal_ExportRangeVersionsServer) error {
	if err := i.verifyInternalLeaderApplied(stream.Context()); err != nil {
		return err
	}
	session, err := store.ReadOnlineMigrationSession(stream.Context(), i.store, req.GetOnlineJobId())
	if err != nil {
		return errors.WithStack(err)
	}
	if session.Invalid != "" || session.Closed {
		return grpcStatusError(codes.FailedPrecondition, store.ErrOnlineMigrationInvalid.Error())
	}
	if err := i.validateOnlineExport(req, session); err != nil {
		return err
	}
	manifest, err := store.EncodeOnlineMigrationManifest(session)
	if err != nil {
		return errors.WithStack(err)
	}
	response := &pb.ExportRangeVersionsResponse{OnlineManifest: manifest, Done: req.GetOnlineAfterSequence() == session.Sequence}
	if err := i.fillOnlineExportFrame(stream.Context(), req, session, response); err != nil {
		return err
	}
	if err := stream.Send(response); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (i *Internal) probeOnlineMigration(ctx context.Context, req *pb.ProbeMigrationStateRequest) error {
	if req.GetOnlineCaptureVersion() != store.OnlineMigrationVersion || !store.OnlineMigrationSupported(i.store) {
		return grpcStatusError(codes.FailedPrecondition, store.ErrOnlineMigrationIdentity.Error())
	}
	if req.GetOnlineReady() {
		return i.probeOnlineMigrationTarget(ctx, req)
	}
	return i.probeOnlineMigrationSource(ctx, req)
}

func (i *Internal) probeOnlineMigrationSource(ctx context.Context, req *pb.ProbeMigrationStateRequest) error {
	session, err := store.ReadOnlineMigrationSession(ctx, i.store, req.GetJobId())
	if err != nil {
		return errors.WithStack(err)
	}
	if !onlineSealMatches(req, session) || session.Invalid != "" || session.Closed || (!session.Sealed && session.ConfigurationIndex != i.migrationAppliedConfigurationIndex()) {
		return grpcStatusError(codes.FailedPrecondition, store.ErrOnlineMigrationInvalid.Error())
	}
	return nil
}

func onlineMigrationRPCError(err error) error {
	if errors.Is(err, kv.ErrMigrationConfigurationChanged) {
		return grpcStatusError(codes.Aborted, kv.ErrMigrationConfigurationChanged.Error())
	}
	if errors.Is(err, kv.ErrTxnLocked) {
		return grpcStatusError(codes.Aborted, kv.ErrTxnLocked.Error())
	}
	for _, verdict := range []error{store.ErrOnlineMigrationInvalid, store.ErrOnlineMigrationIdentity, store.ErrOnlineMigrationTargetNotEmpty, store.ErrOnlineMigrationTargetClosed, store.ErrMigrationCaptureConflict} {
		if errors.Is(err, verdict) {
			return grpcStatusError(codes.FailedPrecondition, verdict.Error())
		}
	}
	return errors.WithStack(err)
}

// With a configuration-bound cutover probe every member must hold exactly the
// seal the catalog recorded, including its original capture configuration.
func onlineSealMatches(req *pb.ProbeMigrationStateRequest, session store.OnlineMigrationSession) bool {
	if len(req.GetOnlineSealedManifest()) == 0 {
		return true
	}
	if !session.Sealed || req.GetOnlineSequence() != session.Sequence {
		return false
	}
	local, err := store.EncodeOnlineMigrationManifest(session)
	return err == nil && bytes.Equal(local, req.GetOnlineSealedManifest())
}

func (i *Internal) validateOnlineExport(req *pb.ExportRangeVersionsRequest, session store.OnlineMigrationSession) error {
	if !bytes.Equal(session.RouteStart, req.GetRouteStart()) || !bytes.Equal(session.RouteEnd, req.GetRouteEnd()) || (!session.Sealed && session.ConfigurationIndex != i.migrationAppliedConfigurationIndex()) || req.GetOnlineAfterSequence() > session.Sequence {
		return grpcStatusError(codes.FailedPrecondition, store.ErrOnlineMigrationIdentity.Error())
	}
	return nil
}

func (i *Internal) fillOnlineExportFrame(ctx context.Context, req *pb.ExportRangeVersionsRequest, session store.OnlineMigrationSession, response *pb.ExportRangeVersionsResponse) error {
	if !response.GetDone() {
		frame, err := store.ReadOnlineMigrationFrame(ctx, i.store, session, req.GetOnlineAfterSequence()+1)
		if err != nil {
			return errors.WithStack(err)
		}
		response.OnlineFrame, err = store.EncodeOnlineMigrationFrame(req.GetOnlineAfterSequence()+1, frame)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (i *Internal) probeOnlineMigrationTarget(ctx context.Context, req *pb.ProbeMigrationStateRequest) error {
	target, err := store.ReadOnlineMigrationTarget(ctx, i.store, req.GetJobId())
	if err != nil {
		return errors.WithStack(err)
	}
	if !onlineSealMatches(req, target.Source) || target.Closed || !target.Ready || target.Sequence != req.GetOnlineSequence() || target.ConfigurationIndex != i.migrationAppliedConfigurationIndex() {
		return grpcStatusError(codes.FailedPrecondition, store.ErrOnlineMigrationIdentity.Error())
	}
	return nil
}

// Capture activation precedes the source ownership certificate. Its committed
// nonce must be discoverable even when the activation response is lost.
func (i *Internal) decorateOnlineSourceGeneration(ctx context.Context, req *pb.ProbeMigrationStateRequest, resp *pb.ProbeMigrationStateResponse) error {
	if resp.GetOwnershipGeneration() != "" {
		return nil
	}
	if req.GetKind() != pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY && req.GetKind() != pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_APPLIED {
		return nil
	}
	source, err := store.ReadOnlineMigrationSession(ctx, i.store, req.GetJobId())
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return errors.WithStack(err)
	}
	if source.Closed {
		return nil
	}
	resp.OnlineSourceGeneration = source.OwnershipGeneration
	return nil
}

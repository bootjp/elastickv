package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WithInternalTwoGroupTerminalAdmission is a trusted-runtime opt-in. It must
// remain unset until all replicas can apply terminal entries.
func WithInternalTwoGroupTerminalAdmission(metadata store.RaftControlMetadataStore,
	secondary func(uint64) (*TwoGroupTerminalPeer, error)) InternalOption {
	return func(i *Internal) {
		i.twoGroupTerminalMetadata = metadata
		i.twoGroupTerminalSecondary = secondary
	}
}

func (i *Internal) VerifyTwoGroupPrepare(ctx context.Context, req *pb.VerifyTwoGroupPrepareRequest) (*pb.VerifyTwoGroupPrepareResponse, error) {
	receipt, err := i.authenticateTwoGroupPrepareSession(ctx)
	if err != nil {
		return nil, err
	}
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 || !kv.ValidTwoGroupPrepareManifest(req.GetManifest()) ||
		!twoGroupManifestHasGroup(req.GetManifest(), i.twoGroupPrepareGroupID) {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "canonical two-group manifest required"))
	}
	if i.kvForwardRejected || i.leader == nil || i.twoGroupTerminalMetadata == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "two-group PREPARE verification unavailable"))
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingPeerBudget)
	defer cancel()
	index, term, err := kv.VerifyTwoGroupPrepareOnLeader(ctx, i.twoGroupPrepareGroupID, req.GetManifest(),
		i.leader, i.twoGroupTerminalMetadata, receipt.Check)
	if err != nil {
		return nil, errors.Wrap(err, "verify two-group PREPARE")
	}
	return &pb.VerifyTwoGroupPrepareResponse{AppliedIndex: index, LeaderTerm: term}, nil
}

func (i *Internal) ProposeTwoGroupTerminal(ctx context.Context, req *pb.ProposeTwoGroupTerminalRequest) (*pb.ProposeTwoGroupTerminalResponse, error) {
	receipt, err := i.authenticateTwoGroupPrepareSession(ctx)
	if err != nil {
		return nil, err
	}
	if err := i.checkTwoGroupTerminalRequest(req); err != nil {
		return nil, err
	}
	manifest := req.GetManifest()
	home, err := i.twoGroupTerminalHomeLookup(manifest)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingPeerBudget)
	defer cancel()
	index, term, err := kv.ProposeTwoGroupTerminalOnLeader(ctx, i.twoGroupPrepareGroupID, manifest,
		req.GetDecision(), i.leader, i.twoGroupPrepareProposer, i.twoGroupTerminalMetadata,
		home, i.verifyTwoGroupTerminalSecondary, receipt.Check)
	if err != nil {
		return nil, errors.Wrap(err, "propose two-group terminal")
	}
	return &pb.ProposeTwoGroupTerminalResponse{CommitIndex: index, LeaderTerm: term}, nil
}

func (i *Internal) checkTwoGroupTerminalRequest(req *pb.ProposeTwoGroupTerminalRequest) error {
	if !validTwoGroupTerminalRPCRequest(req, i.twoGroupPrepareGroupID) {
		return errors.WithStack(status.Error(codes.InvalidArgument, "canonical two-group terminal request required"))
	}
	if i.kvForwardRejected || i.leader == nil || i.twoGroupPrepareProposer == nil || i.twoGroupTerminalMetadata == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "two-group terminal unavailable"))
	}
	return nil
}

func validTwoGroupTerminalRPCRequest(req *pb.ProposeTwoGroupTerminalRequest, group uint64) bool {
	return req != nil && len(req.ProtoReflect().GetUnknown()) == 0 &&
		kv.ValidTwoGroupPrepareManifest(req.GetManifest()) && twoGroupManifestHasGroup(req.GetManifest(), group) &&
		(req.GetDecision() == pb.Phase_COMMIT || req.GetDecision() == pb.Phase_ABORT)
}

func (i *Internal) twoGroupTerminalHomeLookup(manifest *pb.TwoGroupPrepareManifest) (kv.TwoGroupDecisionLookup, error) {
	if i.twoGroupPrepareGroupID == manifest.GetPrimaryGroupId() {
		return nil, nil
	}
	if i.twoGroupPrepareHomeLookup == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "home decision lookup unavailable"))
	}
	peerLookup, err := i.twoGroupPrepareHomeLookup(manifest.GetPrimaryGroupId())
	if err != nil || peerLookup == nil || peerLookup.Handle == nil ||
		peerLookup.Handle.GroupID() != manifest.GetPrimaryGroupId() {
		return nil, errors.Join(kv.ErrTransactionOutcomeUnknown, err)
	}
	return peerLookup, nil
}

func (i *Internal) verifyTwoGroupTerminalSecondary(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest) error {
	if i.twoGroupTerminalSecondary == nil {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	peer, err := i.twoGroupTerminalSecondary(group)
	if err != nil || peer == nil {
		return errors.Join(kv.ErrTransactionOutcomeUnknown, err)
	}
	return errors.Wrap(peer.VerifyTwoGroupPrepareOnLeader(ctx, group, manifest), "verify remote secondary PREPARE")
}

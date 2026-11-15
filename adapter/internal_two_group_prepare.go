package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// WithInternalTwoGroupPrepareAdmission is a trusted-runtime opt-in. It is not
// installed by production bootstrap before the all-replica activation gate.
func WithInternalTwoGroupPrepareAdmission(groupID uint64, token string, proposer raftengine.TermFencedProposer,
	homeLookup func(uint64) (*TwoGroupHomePeerLookup, error)) InternalOption {
	return func(i *Internal) {
		i.twoGroupPrepareGroupID = groupID
		i.twoGroupPrepareToken = token
		i.twoGroupPrepareProposer = proposer
		i.twoGroupPrepareHomeLookup = homeLookup
	}
}

func (i *Internal) ProposeTwoGroupPrepare(ctx context.Context, req *pb.ProposeTwoGroupPrepareRequest) (*pb.ProposeTwoGroupPrepareResponse, error) {
	receipt, err := i.checkTwoGroupPrepareIngress(ctx, req)
	if err != nil {
		return nil, err
	}
	manifest := req.GetManifest()
	var lookup kv.TwoGroupHomeLookup
	if i.twoGroupPrepareGroupID != manifest.GetPrimaryGroupId() {
		if i.twoGroupPrepareHomeLookup == nil {
			return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "home lookup unavailable"))
		}
		peerLookup, lookupErr := i.twoGroupPrepareHomeLookup(manifest.GetPrimaryGroupId())
		if lookupErr != nil || peerLookup == nil || peerLookup.Handle == nil ||
			peerLookup.Handle.GroupID() != manifest.GetPrimaryGroupId() {
			return nil, errors.Join(kv.ErrTwoGroupAdmission, lookupErr)
		}
		lookup = peerLookup
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingPeerBudget)
	defer cancel()
	index, term, err := kv.ProposeTwoGroupPrepareOnLeader(ctx, i.twoGroupPrepareGroupID, manifest,
		i.leader, i.twoGroupPrepareProposer, lookup, receipt.Check)
	if err != nil {
		return nil, errors.Wrap(err, "propose two-group PREPARE")
	}
	return &pb.ProposeTwoGroupPrepareResponse{CommitIndex: index, LeaderTerm: term}, nil
}

func (i *Internal) checkTwoGroupPrepareIngress(ctx context.Context, req *pb.ProposeTwoGroupPrepareRequest) (*peersession.Receipt, error) {
	receipt, err := i.authenticateTwoGroupPrepareSession(ctx)
	if err != nil {
		return nil, err
	}
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 || !kv.ValidTwoGroupPrepareManifest(req.GetManifest()) ||
		!twoGroupManifestHasGroup(req.GetManifest(), i.twoGroupPrepareGroupID) {
		return nil, errors.WithStack(status.Error(codes.InvalidArgument, "canonical two-group manifest required"))
	}
	if i.kvForwardRejected || i.leader == nil || i.twoGroupPrepareProposer == nil {
		return nil, errors.WithStack(status.Error(codes.FailedPrecondition, "two-group PREPARE unavailable"))
	}
	return receipt, nil
}

func (i *Internal) authenticateTwoGroupPrepareSession(ctx context.Context) (*peersession.Receipt, error) {
	if err := checkBearerToken(ctx, i.twoGroupPrepareToken, "two-group PREPARE"); err != nil {
		return nil, err
	}
	p, ok := peer.FromContext(ctx)
	if !ok || !splitMigrationTLSProtectedTransport(p.AuthInfo) {
		return nil, errors.WithStack(status.Error(codes.Unauthenticated, "two-group PREPARE requires TLS"))
	}
	receipt := peersession.CapturedReceipt(ctx)
	if i.twoGroupPrepareGroupID == 0 || receipt == nil || receipt.Message().Traffic != peersession.Control ||
		receipt.Message().Group != i.twoGroupPrepareGroupID || receipt.Check(ctx) != nil {
		return nil, errors.WithStack(status.Error(codes.PermissionDenied, "two-group PREPARE requires a data-group peer session"))
	}
	return receipt, nil
}

func twoGroupManifestHasGroup(manifest *pb.TwoGroupPrepareManifest, group uint64) bool {
	for _, participant := range manifest.GetParticipants() {
		if participant.GetGroupId() == group {
			return true
		}
	}
	return false
}

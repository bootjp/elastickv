package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This proposer is separate from the general Admin surface. The shared listener
// must independently enforce its typed mTLS session, startup token and A handle.
func WithInternalBootstrapPolicyProposer(proposer raftengine.Proposer) InternalOption {
	return func(server *Internal) { server.bootstrapPolicyProposer = proposer }
}

// The startup owner must verify the typed peer receipt, dedicated credential
// and current private admission handle. A header or context boolean is not proof.
func WithInternalPrepareEvidenceStartupAuthorization(authorize func(context.Context, *pb.PrepareEvidenceStateRequest) bool) InternalOption {
	return func(server *Internal) { server.prepareStartupAuthorization = authorize }
}

func (i *Internal) authorizePrepareEvidenceRequest(ctx context.Context, req *pb.PrepareEvidenceStateRequest) error {
	if i.prepareStartupAuthorization != nil && i.prepareStartupAuthorization(ctx, req) {
		return nil
	}
	return checkBearerToken(ctx, i.prepareEvidenceToken, "prepare evidence peer")
}

func (i *Internal) forwardBootstrapActivation(ctx context.Context, req *pb.ForwardAdminProposalRequest) (*pb.ForwardAdminProposalResponse, bool, error) {
	command, matched, err := raftbootstrap.ParseActivationCommand(req.GetPayload())
	if !matched {
		return nil, false, nil
	}
	if err != nil || req.GetForceEntry() || i.bootstrapPolicyProposer == nil {
		return nil, true, errors.WithStack(status.Error(codes.PermissionDenied, "bootstrap activation unavailable or malformed"))
	}
	receipt := peersession.CapturedReceipt(ctx)
	if !bootstrapActivationReceiptMatches(receipt, command) {
		return nil, true, errors.WithStack(status.Error(codes.PermissionDenied, "typed bootstrap policy session required"))
	}
	if err := receipt.Check(ctx); err != nil {
		return nil, true, errors.WithStack(err)
	}
	result, err := i.bootstrapPolicyProposer.ProposeAdmin(ctx, req.GetPayload())
	if err != nil {
		return nil, true, errors.WithStack(err)
	}
	if err := forwardedAdminProposalResponseError(result); err != nil {
		return nil, true, err
	}
	return &pb.ForwardAdminProposalResponse{CommitIndex: result.CommitIndex}, true, nil
}

func bootstrapActivationReceiptMatches(receipt *peersession.Receipt, command raftbootstrap.ActivationCommand) bool {
	if receipt == nil {
		return false
	}
	message := receipt.Message()
	return message.Traffic == peersession.PolicyControl && message.Group == command.Witness.Group
}

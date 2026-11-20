package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TwoGroupEvidencePeer reads the remote leader's replicated PREPARE-evidence
// marker through a group-scoped, revocable private peer session.
type TwoGroupEvidencePeer struct {
	Handle *peersession.Handle
	Conn   *grpc.ClientConn
	Token  string
}

func (p TwoGroupEvidencePeer) RequireTwoGroupPrepareEvidence(ctx context.Context, group uint64) error {
	if !p.validEvidenceRoute(group) {
		return errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	response := new(pb.ProbeTwoGroupPrepareEvidenceResponse)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.Token)
	if err := p.Handle.Invoke(ctx, p.Conn, pb.Internal_ProbeTwoGroupPrepareEvidence_FullMethodName,
		&pb.ProbeTwoGroupPrepareEvidenceRequest{Version: kv.TwoGroupPrepareEvidenceProbeVersion}, response); err != nil {
		return errors.Join(kv.ErrTwoGroupAdmission, err)
	}
	if !validTwoGroupEvidenceReply(response, group) {
		return errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	return nil
}

func (p TwoGroupEvidencePeer) validEvidenceRoute(group uint64) bool {
	return p.Handle != nil && group != 0 && p.Handle.GroupID() == group && p.Conn != nil && p.Token != ""
}

func validTwoGroupEvidenceReply(response *pb.ProbeTwoGroupPrepareEvidenceResponse, group uint64) bool {
	return response != nil && len(response.ProtoReflect().GetUnknown()) == 0 &&
		response.GetVersion() == kv.TwoGroupPrepareEvidenceProbeVersion && response.GetGroupId() == group &&
		response.GetProofEnabled() && response.GetAppliedIndex() != 0 && response.GetLeaderTerm() != 0
}

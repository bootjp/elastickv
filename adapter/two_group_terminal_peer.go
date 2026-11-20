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

// TwoGroupTerminalPeer carries only a manifest and decision in a private
// data-group session. The destination constructs all durable evidence.
type TwoGroupTerminalPeer struct {
	Handle *peersession.Handle
	Conn   *grpc.ClientConn
	Token  string
}

func (p TwoGroupTerminalPeer) validRoute(group uint64, manifest *pb.TwoGroupPrepareManifest) bool {
	return p.Handle != nil && p.Handle.GroupID() == group && group != 0 && p.Conn != nil && p.Token != "" &&
		kv.ValidTwoGroupPrepareManifest(manifest) && twoGroupManifestHasGroup(manifest, group)
}

func (p TwoGroupTerminalPeer) VerifyTwoGroupPrepareOnLeader(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest) error {
	if !p.validRoute(group, manifest) {
		return errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	response := new(pb.VerifyTwoGroupPrepareResponse)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.Token)
	if err := p.Handle.Invoke(ctx, p.Conn, pb.Internal_VerifyTwoGroupPrepare_FullMethodName,
		&pb.VerifyTwoGroupPrepareRequest{Manifest: manifest}, response); err != nil {
		return errors.Join(kv.ErrTransactionOutcomeUnknown, err)
	}
	if response.GetAppliedIndex() == 0 || response.GetLeaderTerm() == 0 || len(response.ProtoReflect().GetUnknown()) != 0 {
		return errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	return nil
}

func (p TwoGroupTerminalPeer) DecideTwoGroupOnLeader(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest, decision pb.Phase) error {
	if !p.validRoute(group, manifest) || decision != pb.Phase_COMMIT && decision != pb.Phase_ABORT {
		return errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	response := new(pb.ProposeTwoGroupTerminalResponse)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.Token)
	if err := p.Handle.Invoke(ctx, p.Conn, pb.Internal_ProposeTwoGroupTerminal_FullMethodName,
		&pb.ProposeTwoGroupTerminalRequest{Manifest: manifest, Decision: decision}, response); err != nil {
		return errors.Join(kv.ErrTransactionOutcomeUnknown, err)
	}
	if response.GetCommitIndex() == 0 || response.GetLeaderTerm() == 0 || len(response.ProtoReflect().GetUnknown()) != 0 {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return nil
}

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

// TwoGroupPreparePeer is the private peer-session route to a group leader.
// It cannot carry a home observation or raw Raft entry.
type TwoGroupPreparePeer struct {
	Handle *peersession.Handle
	Conn   *grpc.ClientConn
	Token  string
}

func (p TwoGroupPreparePeer) PrepareTwoGroupOnLeader(ctx context.Context, group uint64, manifest *pb.TwoGroupPrepareManifest) error {
	if !p.validRoute(group, manifest) {
		return errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	request := &pb.ProposeTwoGroupPrepareRequest{Manifest: manifest}
	response := new(pb.ProposeTwoGroupPrepareResponse)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.Token)
	if err := p.Handle.Invoke(ctx, p.Conn, pb.Internal_ProposeTwoGroupPrepare_FullMethodName, request, response); err != nil {
		return errors.Wrap(err, "propose authenticated two-group PREPARE")
	}
	if response.GetCommitIndex() == 0 || response.GetLeaderTerm() == 0 || len(response.ProtoReflect().GetUnknown()) != 0 {
		return errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	return nil
}

func (p TwoGroupPreparePeer) validRoute(group uint64, manifest *pb.TwoGroupPrepareManifest) bool {
	return p.Handle != nil && group != 0 && p.Handle.GroupID() == group && p.Conn != nil && p.Token != "" &&
		kv.ValidTwoGroupPrepareManifest(manifest) && twoGroupManifestHasGroup(manifest, group)
}

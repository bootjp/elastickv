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

// TwoGroupHomePeerLookup is an opt-in private peer-session client. Production
// Forward cannot supply or select this lookup route.
type TwoGroupHomePeerLookup struct {
	Handle *peersession.Handle
	Conn   *grpc.ClientConn
	Token  string
}

func (l TwoGroupHomePeerLookup) LookupTwoGroupHome(ctx context.Context, id [16]byte) ([]byte, uint64, uint64, error) {
	if l.Handle == nil || l.Conn == nil || l.Token == "" || id == ([16]byte{}) {
		return nil, 0, 0, errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	request := &pb.LookupTimestampBindingRequest{OperationId: id[:], HomeAdmissionVersion: kv.TwoGroupHomeAdmissionVersion}
	response := new(pb.LookupTimestampBindingResponse)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+l.Token)
	if err := l.Handle.Invoke(ctx, l.Conn, pb.Internal_LookupTimestampBinding_FullMethodName, request, response); err != nil {
		return nil, 0, 0, errors.Wrap(err, "read authenticated home admission")
	}
	if !validTwoGroupHomeReply(response) {
		return nil, 0, 0, errors.WithStack(kv.ErrTwoGroupAdmission)
	}
	return response.GetHomeAdmissionRecord(), response.GetAppliedIndex(), response.GetLeaderTerm(), nil
}

func validTwoGroupHomeReply(response *pb.LookupTimestampBindingResponse) bool {
	return response.GetFound() && response.GetHomeAdmissionVersion() == kv.TwoGroupHomeAdmissionVersion &&
		response.GetAppliedIndex() != 0 && response.GetLeaderTerm() != 0 &&
		len(response.GetHomeAdmissionRecord()) != 0 && len(response.ProtoReflect().GetUnknown()) == 0
}

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

// BoundTimestampPeerIssuer is the opt-in home-leader client for a group-0
// native session. Runtime routing and all-member activation are not wired yet.
type BoundTimestampPeerIssuer struct {
	Handle *peersession.Handle
	Conn   *grpc.ClientConn
	Token  string
}

func (i BoundTimestampPeerIssuer) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	if i.Handle == nil || i.Conn == nil || i.Token == "" || min == ^uint64(0) {
		return 0, errors.WithStack(kv.ErrTSOProtocolUnsupported)
	}
	request := &pb.IssueBoundTimestampRequest{MinTimestamp: min, PhysicalBandProtocolVersion: kv.TSOPhysicalBandProtocolVersion}
	response := new(pb.IssueBoundTimestampResponse)
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+i.Token)
	if err := i.Handle.Invoke(ctx, i.Conn, pb.Internal_IssueBoundTimestamp_FullMethodName, request, response); err != nil {
		return 0, errors.Wrap(err, "request authenticated physical-band timestamp")
	}
	if err := validateBoundTimestampResponse(request, response); err != nil {
		return 0, err
	}
	return response.GetTimestamp(), nil
}

func validateBoundTimestampResponse(request *pb.IssueBoundTimestampRequest, response *pb.IssueBoundTimestampResponse) error {
	if request == nil || response == nil || response.GetPhysicalBandProtocolVersion() != kv.TSOPhysicalBandProtocolVersion ||
		!validBoundTimestampBand(response.GetTimestamp(), request.GetMinTimestamp(), response.GetCommittedBandEnd(), response.GetPreviousIssuedCursor()) ||
		len(response.ProtoReflect().GetUnknown()) != 0 {
		return errors.WithStack(kv.ErrTSOProtocolUnsupported)
	}
	return nil
}

func validBoundTimestampBand(timestamp, minimum, committedEnd, previousCursor uint64) bool {
	return timestamp != 0 && timestamp > minimum && committedEnd >= timestamp &&
		committedEnd>>16 == timestamp>>16 && committedEnd&0xffff == 0xffff &&
		previousCursor < timestamp &&
		(previousCursor == 0 || previousCursor>>16 == timestamp>>16)
}

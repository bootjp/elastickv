package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// StartupControlRPC selects a per-node typed startup control transport before
// sending. Only (false, nil) selects the existing public/auth path; a handled
// private call or any error must never retry through public transport.
type StartupControlRPC func(ctx context.Context, group uint64, address, method string, request, reply any) (handled bool, err error)

// StartupOnlyControlRPC keeps the bounded private lane off steady-state traffic.
// processDataReady must check every protected group, not just the target group.
// Receiver admission and the caller's existing operation context remain intact
// across readiness changes; the predicate is transport selection, not authority.
func StartupOnlyControlRPC(invoke func(context.Context, uint64, string, string, any, any) error, processDataReady func() bool) StartupControlRPC {
	return func(ctx context.Context, group uint64, address, method string, request, reply any) (bool, error) {
		if invoke == nil || processDataReady == nil {
			return true, errors.New("startup control transport requires private invoker and process readiness")
		}
		if processDataReady() {
			return false, nil
		}
		return true, errors.WithStack(invoke(ctx, group, address, method, request, reply))
	}
}

// SetStartupControlRPC must be called before receiver/background work starts.
func (s *ShardStore) SetStartupControlRPC(call StartupControlRPC) {
	s.startupControlRPC = call
}

func WithTSOStartupControlRPC(call StartupControlRPC) LeaderRoutedTSOAllocatorOption {
	return func(a *LeaderRoutedTSOAllocator) { a.startupControlRPC = call }
}

// WithLeaderAdminStartupControlRPC is installed only for prepare controls,
// never for backup or read-watermark forwarding sharing the same RPC method.
func WithLeaderAdminStartupControlRPC(group uint64, call StartupControlRPC) LeaderAdminProposerOption {
	return func(p *leaderAdminProposer) {
		p.startupControlGroup = group
		p.startupControlRPC = call
	}
}

func invokeStartupControlRPC(ctx context.Context, call StartupControlRPC, cache *GRPCConnCache, group uint64, address, method string, request, reply any) error {
	if call != nil {
		handled, err := call(ctx, group, address, method, request, reply)
		if handled || err != nil {
			return errors.WithStack(err)
		}
	}
	if cache == nil {
		return errors.New("startup control public connection cache is unavailable")
	}
	conn, err := cache.ConnFor(address)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(conn.Invoke(ctx, method, request, reply))
}

func (s *ShardStore) remoteStartupGroupFloor(ctx context.Context, group uint64, address string) (*pb.RawLatestCommitTSResponse, error) {
	reply := &pb.RawLatestCommitTSResponse{}
	err := invokeStartupControlRPC(ctx, s.startupControlRPC, &s.connCache, group, address, pb.RawKV_RawLatestCommitTS_FullMethodName,
		&pb.RawLatestCommitTSRequest{GroupId: group}, reply)
	return reply, err
}

func (s *ShardStore) remoteStartupPrepareState(ctx context.Context, group uint64, address string, request *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
	reply := &pb.PrepareEvidenceStateResponse{}
	err := invokeStartupControlRPC(ctx, s.startupControlRPC, &s.connCache, group, address, pb.Internal_GetPrepareEvidenceState_FullMethodName, request, reply)
	return reply, err
}

func (a *LeaderRoutedTSOAllocator) remoteStartupTimestamp(ctx context.Context, address string, request *pb.GetTimestampRequest) (*pb.GetTimestampResponse, error) {
	reply := &pb.GetTimestampResponse{}
	err := invokeStartupControlRPC(ctx, a.startupControlRPC, &a.connCache, 0, address, pb.Distribution_GetTimestamp_FullMethodName, request, reply)
	return reply, err
}

func (p *leaderAdminProposer) remoteStartupAdminProposal(ctx context.Context, address string, request *pb.ForwardAdminProposalRequest) (*pb.ForwardAdminProposalResponse, error) {
	reply := &pb.ForwardAdminProposalResponse{}
	err := invokeStartupControlRPC(ctx, p.startupControlRPC, p.connCache, p.startupControlGroup, address, pb.Internal_ForwardAdminProposal_FullMethodName, request, reply)
	return reply, err
}

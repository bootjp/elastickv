package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WithInternalTwoGroupEvidenceProbe enables only the private, read-only probe.
// Production does not install it before the all-replica activation gate.
func WithInternalTwoGroupEvidenceProbe() InternalOption {
	return func(i *Internal) { i.twoGroupEvidenceProbeEnabled = true }
}

func (i *Internal) ProbeTwoGroupPrepareEvidence(ctx context.Context,
	req *pb.ProbeTwoGroupPrepareEvidenceRequest) (*pb.ProbeTwoGroupPrepareEvidenceResponse, error) {
	receipt, err := i.authenticateTwoGroupPrepareSession(ctx)
	if err != nil {
		return nil, err
	}
	if err := i.checkTwoGroupEvidenceProbeRequest(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingPeerBudget)
	defer cancel()
	reader, term, err := i.twoGroupEvidenceLeader(ctx)
	if err != nil {
		return nil, err
	}
	index, err := i.leader.LinearizableRead(ctx)
	if err != nil || index == 0 {
		return nil, errors.Join(kv.ErrTwoGroupAdmission, err)
	}
	enabled, err := kv.PrepareEvidenceEnabled(ctx, i.store)
	if err != nil {
		return nil, errors.Wrap(err, "read two-group evidence marker")
	}
	if err := checkTwoGroupEvidenceFence(ctx, reader, term, receipt.Check); err != nil {
		return nil, err
	}
	return &pb.ProbeTwoGroupPrepareEvidenceResponse{Version: kv.TwoGroupPrepareEvidenceProbeVersion, GroupId: i.twoGroupPrepareGroupID,
		ProofEnabled: enabled, AppliedIndex: index, LeaderTerm: term}, nil
}

func (i *Internal) checkTwoGroupEvidenceProbeRequest(req *pb.ProbeTwoGroupPrepareEvidenceRequest) error {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 || req.GetVersion() != kv.TwoGroupPrepareEvidenceProbeVersion {
		return errors.WithStack(status.Error(codes.InvalidArgument, "two-group evidence probe version required"))
	}
	if !i.twoGroupEvidenceProbeEnabled || i.kvForwardRejected || i.store == nil || i.leader == nil {
		return errors.WithStack(status.Error(codes.FailedPrecondition, "two-group evidence probe unavailable"))
	}
	return nil
}

func (i *Internal) twoGroupEvidenceLeader(ctx context.Context) (raftengine.StatusReader, uint64, error) {
	reader, ok := i.leader.(raftengine.StatusReader)
	if !ok || i.leader.State() != raftengine.StateLeader {
		return nil, 0, errors.WithStack(raftengine.ErrNotLeader)
	}
	term := reader.Status().Term
	if term == 0 {
		return nil, 0, errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	if err := i.leader.VerifyLeader(ctx); err != nil {
		return nil, 0, errors.Wrap(err, "verify two-group evidence leader")
	}
	return reader, term, nil
}

func checkTwoGroupEvidenceFence(ctx context.Context, reader raftengine.StatusReader, term uint64, checkSession func(context.Context) error) error {
	if err := checkSession(ctx); err != nil {
		return errors.Wrap(err, "two-group evidence session changed")
	}
	if current := reader.Status(); current.State != raftengine.StateLeader || current.Term != term {
		return errors.WithStack(raftengine.ErrStaleAdmissionTerm)
	}
	return nil
}

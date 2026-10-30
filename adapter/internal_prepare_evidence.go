package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const prepareEvidenceInspectionTimeout = 5 * time.Second

// WithInternalPrepareEvidenceToken supplies the existing forwarded-control
// credential. An omitted or empty credential leaves this peer RPC disabled,
// independently of whether the Admin service/interceptor is installed.
func WithInternalPrepareEvidenceToken(token string) InternalOption {
	return func(i *Internal) { i.prepareEvidenceToken = token }
}

// GetPrepareEvidenceState serves read-only protocol negotiation on every
// replica, and authoritative drain inspection only on an applied leader.
func (i *Internal) GetPrepareEvidenceState(ctx context.Context, req *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
	if err := checkBearerToken(ctx, i.prepareEvidenceToken, "prepare evidence peer"); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, errors.WithStack(kv.ErrInvalidRequest)
	}
	if req.GetCapabilityOnly() {
		return i.prepareEvidenceCapabilities(), nil
	}
	if i.kvForwardRejected || i.store == nil {
		return nil, errors.WithStack(ErrKVForwardNotSupported)
	}
	return i.inspectPrepareEvidence(ctx, req.GetFloor())
}

func (i *Internal) inspectPrepareEvidence(ctx context.Context, requestedFloor uint64) (*pb.PrepareEvidenceStateResponse, error) {
	if i.leader == nil || i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(ErrNotLeader)
	}
	if !i.prepareEvidenceScanning.CompareAndSwap(false, true) {
		return nil, errors.WithStack(status.Error(codes.ResourceExhausted, "prepare evidence inspection already running"))
	}
	defer i.prepareEvidenceScanning.Store(false)
	ctx, cancel := context.WithTimeout(ctx, prepareEvidenceInspectionTimeout)
	defer cancel()
	index, err := i.leader.LinearizableRead(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(status.FromContextError(err).Err())
	}
	enabled, unproven, floor, err := kv.InspectPrepareEvidence(ctx, i.store, requestedFloor)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(status.FromContextError(err).Err())
	}
	if i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(ErrNotLeader)
	}
	return &pb.PrepareEvidenceStateResponse{
		Supported: true, ProofEnabled: enabled, UnprovenLocks: unproven,
		FenceFloor: floor, AppliedIndex: index,
	}, nil
}

func (i *Internal) prepareEvidenceCapabilities() *pb.PrepareEvidenceStateResponse {
	return &pb.PrepareEvidenceStateResponse{
		Supported: true, DedicatedTsoConfigured: i.tsoCutoverState != nil && i.tsAllocator != nil,
		CutoverApplied: i.tsoCutoverState != nil && i.tsoCutoverState.CutoverActive(),
	}
}

func (i *Internal) validateForwardedPrepareEvidence(ctx context.Context, requests []*pb.Request, startTS uint64) error {
	ctx, cancel := context.WithTimeout(ctx, legacyCommitEvidenceTimeout)
	defer cancel()
	seen := make(map[uint64]struct{})
	for _, request := range requests {
		timestamp, err := kv.PrepareRequestCommitTimestamp(request)
		if err != nil {
			return errors.WithStack(err)
		}
		if timestamp == 0 {
			continue
		}
		if timestamp <= startTS {
			return errors.WithStack(kv.ErrTxnInvalidMeta)
		}
		if _, exists := seen[timestamp]; exists {
			continue
		}
		if len(seen) == kv.MaxPrimaryCommitEvidenceKeys {
			return errors.WithStack(kv.ErrInvalidRequest)
		}
		if err := kv.ReservePrepareEvidenceTimestamp(ctx, i.tsAllocator, timestamp); err != nil {
			return errors.WithStack(err)
		}
		seen[timestamp] = struct{}{}
	}
	return nil
}

func forwardedTxnCreationsHavePrepareProof(requests []*pb.Request) bool {
	hasPrepare := false
	for _, request := range requests {
		if request == nil || request.Phase == pb.Phase_COMMIT || request.Phase == pb.Phase_ABORT {
			continue
		}
		timestamp, err := kv.PrepareRequestCommitTimestamp(request)
		if err != nil || timestamp == 0 {
			return false
		}
		hasPrepare = true
	}
	return hasPrepare
}

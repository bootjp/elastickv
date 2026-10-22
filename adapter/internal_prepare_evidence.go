package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// GetPrepareEvidenceState serves read-only protocol negotiation on every
// replica, and authoritative drain inspection only on an applied leader.
func (i *Internal) GetPrepareEvidenceState(ctx context.Context, req *pb.PrepareEvidenceStateRequest) (*pb.PrepareEvidenceStateResponse, error) {
	if req == nil {
		return nil, errors.WithStack(kv.ErrInvalidRequest)
	}
	if req.GetCapabilityOnly() {
		return i.prepareEvidenceCapabilities(), nil
	}
	if i.kvForwardRejected || i.store == nil {
		return nil, errors.WithStack(ErrKVForwardNotSupported)
	}
	if i.leader == nil || i.leader.State() != raftengine.StateLeader {
		return nil, errors.WithStack(ErrNotLeader)
	}
	index, err := i.leader.LinearizableRead(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	enabled, unproven, floor, err := kv.InspectPrepareEvidence(ctx, i.store, req.GetFloor())
	if err != nil {
		return nil, errors.WithStack(err)
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
		Supported: true, DedicatedTsoConfigured: i.tsoCutoverState != nil,
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

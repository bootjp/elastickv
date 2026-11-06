package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// AdmissionControl is a synchronous typed control child, not a fresh data root.
// The process arbiter must preserve and validate the allocator parent lineage.
const AdmissionControl AdmissionPurpose = AdmissionMaintenance + 1

func (s *ShardStore) proposePhaseDControl(ctx context.Context, id uint64, proposer raftengine.Proposer, command []byte) (*raftengine.ProposalResult, error) {
	return admittedOperation(ctx, s.operationAdmission, id, AdmissionControl, func(owned context.Context) (*raftengine.ProposalResult, error) {
		result, err := proposer.ProposeAdmin(owned, command)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if result == nil {
			return nil, errors.WithStack(ErrTSOProtocolUnsupported)
		}
		if applyErr, ok := result.Response.(error); ok && applyErr != nil {
			return nil, errors.WithStack(applyErr)
		}
		return result, nil
	})
}

package main

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

type bootstrapRequestOwner struct {
	ctx       context.Context
	op        *raftadmission.Operation
	admission *raftadmission.Arbiter
}

func bootstrapInternalPolicyOptions(runtime *raftGroupRuntime, proposer raftengine.Proposer) []adapter.InternalOption {
	if runtime.bootstrapDomain == nil {
		return nil
	}
	domain := runtime.bootstrapDomain
	return []adapter.InternalOption{adapter.WithInternalBootstrapPolicyProposer(proposer),
		adapter.WithInternalPrepareEvidenceStartupAuthorization(func(ctx context.Context, req *pb.PrepareEvidenceStateRequest) bool {
			receipt := peersession.CapturedReceipt(ctx)
			if receipt == nil {
				return false
			}
			descriptor, _, found := bootstrapStartupControlDescriptor(receipt.Message().Group, pb.Internal_GetPrepareEvidenceState_FullMethodName)
			if !found {
				return false
			}
			if validateBootstrapStartupControl(ctx, domain.startupToken, pb.Internal_GetPrepareEvidenceState_FullMethodName, req, receipt, descriptor) != nil {
				return false
			}
			return domain.admission.ValidateResult(ctx) == nil
		})}
}

func (o *bootstrapRequestOwner) Context() context.Context { return o.ctx }
func (o *bootstrapRequestOwner) Validate() error {
	return errors.WithStack(o.admission.ValidateResult(o.ctx))
}
func (o *bootstrapRequestOwner) CancelAndJoin() { o.op.CancelAndJoin() }
func (o *bootstrapRequestOwner) Finish(original error) error {
	return finishBootstrapIngress(o.op, original)
}

func bootstrapRequestAdmissionForCoordinator(coordinate kv.Coordinator) adapter.RequestAdmission {
	var gate *startupPublicKVGate
	switch c := coordinate.(type) {
	case startupGatedCoordinator:
		gate = c.gate
	case *startupGatedCoordinator:
		if c != nil {
			gate = c.gate
		}
	}
	if gate == nil || gate.domain == nil {
		return nil
	}
	return func(ctx context.Context) (adapter.RequestOwnership, error) {
		if gate.blocked() {
			return nil, bootstrapAdmissionUnavailable()
		}
		owned, op, err := gate.domain.admission.Begin(ctx, raftadmission.Data)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return &bootstrapRequestOwner{ctx: owned, op: op, admission: gate.domain.admission}, nil
	}
}

package clusterbootstrap

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
	Ctx       context.Context
	op        *raftadmission.Operation
	Admission *raftadmission.Arbiter
}

func BootstrapInternalPolicyOptions(runtime *GroupRuntime, proposer raftengine.Proposer) []adapter.InternalOption {
	if runtime.BootstrapDomain == nil {
		return nil
	}
	domain := runtime.BootstrapDomain
	return []adapter.InternalOption{adapter.WithInternalBootstrapPolicyProposer(proposer),
		adapter.WithInternalPrepareEvidenceStartupAuthorization(func(ctx context.Context, req *pb.PrepareEvidenceStateRequest) bool {
			receipt := peersession.CapturedReceipt(ctx)
			if receipt == nil {
				return false
			}
			descriptor, _, found := BootstrapStartupControlDescriptor(receipt.Message().Group, pb.Internal_GetPrepareEvidenceState_FullMethodName)
			if !found {
				return false
			}
			if ValidateBootstrapStartupControl(ctx, domain.StartupToken, pb.Internal_GetPrepareEvidenceState_FullMethodName, req, receipt, descriptor) != nil {
				return false
			}
			return domain.Admission.ValidateResult(ctx) == nil
		})}
}

func (o *bootstrapRequestOwner) Context() context.Context { return o.Ctx }
func (o *bootstrapRequestOwner) Validate() error {
	return errors.WithStack(o.Admission.ValidateResult(o.Ctx))
}
func (o *bootstrapRequestOwner) CancelAndJoin() { o.op.CancelAndJoin() }
func (o *bootstrapRequestOwner) Finish(original error) error {
	return FinishBootstrapIngress(o.op, original)
}

func BootstrapRequestAdmissionForCoordinator(coordinate kv.Coordinator) adapter.RequestAdmission {
	var gate *StartupPublicKVGate
	switch c := coordinate.(type) {
	case StartupGatedCoordinator:
		gate = c.gate
	case *StartupGatedCoordinator:
		if c != nil {
			gate = c.gate
		}
	}
	if gate == nil || gate.Domain == nil {
		return nil
	}
	return func(ctx context.Context) (adapter.RequestOwnership, error) {
		if gate.Blocked() {
			return nil, bootstrapAdmissionUnavailable()
		}
		owned, op, err := gate.Domain.Admission.Begin(ctx, raftadmission.Data)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return &bootstrapRequestOwner{Ctx: owned, op: op, Admission: gate.Domain.Admission}, nil
	}
}

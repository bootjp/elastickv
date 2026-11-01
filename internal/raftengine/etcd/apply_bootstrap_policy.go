package etcd

import (
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// Policy transitions revoke local handles before the FSM commits a new policy.
// Reconciliation after apply independently recovers P at the durable boundary.
// A caller can lose its reply and must reconcile the exact entry receipt; an
// old session must not keep authorizing work across activation.
func (e *Engine) invalidateBeforeBootstrapPolicy(payload []byte) error {
	if e.recoveredIdentity == nil {
		return nil
	}
	_, matched, err := raftbootstrap.ParseActivationCommand(payload)
	if matched && err == nil {
		aware, ok := e.fsm.(raftengine.ApplyAdmissionChangeAware)
		if !ok {
			return errors.WithStack(raftengine.ErrNativeAdmissionDenied)
		}
		changed, err := aware.ApplyChangesAdmission(payload)
		if err != nil {
			return errors.WithStack(err)
		}
		if changed {
			e.invalidateAdmission()
		}
	}
	return nil
}

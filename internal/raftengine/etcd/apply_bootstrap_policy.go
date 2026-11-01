package etcd

import (
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// Policy transitions revoke local handles before the FSM commits a new policy.
// Reconciliation after apply independently recovers P at the durable boundary.
// A caller can lose its reply and must reconcile the exact entry receipt; an
// old session must not keep authorizing work across activation.
func (e *Engine) invalidateBeforeBootstrapPolicy(entry raftpb.Entry, payload []byte) error {
	if e.recoveredIdentity == nil {
		return nil
	}
	aware, ok := e.fsm.(raftengine.ApplyAdmissionChangeAware)
	if !ok {
		_, matched, _ := raftbootstrap.ParseActivationCommand(payload)
		if matched || implementedEncryptionOpcode(payload) {
			return errors.WithStack(raftengine.ErrNativeAdmissionDenied)
		}
		return nil
	}
	changed, err := aware.ApplyChangesAdmission(payload)
	if err != nil {
		return errors.WithStack(err)
	}
	if !changed {
		return nil
	}
	e.invalidateAdmission()
	if e.beforeAdmissionChange != nil {
		identity := raftengine.NewAppliedEntryIdentity(entry.GetIndex(), entry.GetTerm(), int32(entry.GetType()), payload)
		return errors.WithStack(e.beforeAdmissionChange(identity, payload))
	}
	return nil
}

// This predicate rejects a missing verifier; it never permits any effects.
func implementedEncryptionOpcode(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	switch payload[0] {
	case fsmwire.OpRegistration, fsmwire.OpBootstrap, fsmwire.OpRotation:
		return true
	default:
		return false
	}
}

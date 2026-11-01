package peersession

import (
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

// PublishRecoveredPolicy opens only the identity lane after the startup owner
// independently recovers policy from the selected native Raft snapshot/log.
// Never pass a local policy-cache record as recovery evidence. Control/data
// admission requires a later revocable runtime projection via UpdateGroup.
// The startup owner must serialize this call with other group publications.
func (o *Owner) PublishRecoveredPolicy(files *raftbootstrap.Store, groupID uint64, policy raftbootstrap.PolicyRecord, state raftbootstrap.RaftState, generation uint64) (err error) {
	defer func() {
		if err != nil {
			o.SuspendGroup(groupID)
		}
	}()
	if files == nil || state != raftbootstrap.Intact || policy.Identity.Cluster != o.local.Cluster || policy.Identity.Group != groupID {
		return errors.WithStack(ErrDenied)
	}
	digest, err := raftbootstrap.PolicyDigest(policy)
	if err != nil {
		return errors.WithStack(err)
	}
	floor := raftbootstrap.Floor{ActivationIndex: policy.ActivationIndex, Generation: policy.Generation}
	authoritative := raftbootstrap.Policy{
		Identity: policy.Identity, BaselineDigest: policy.BaselineDigest, Minimum: policy.Minimum, Floor: floor,
	}
	inspection, err := files.Inspect(raftbootstrap.Evidence{
		Identity: &policy.Identity, BaselineDigest: &policy.BaselineDigest, RaftState: state, Policy: &authoritative,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if inspection.Class != raftbootstrap.Required {
		return errors.WithStack(ErrDenied)
	}
	group := Group{
		ID: policy.Identity.Group, Generation: generation, Configuration: policy.Configuration,
		Policy: digest, Floor: floor, Minimum: policy.Minimum, State: Intact,
		Stage: IdentityReady, Pending: PendingClear, MarkerReady: true,
	}
	if !memberMatches(o.local, group) {
		return errors.WithStack(ErrDenied)
	}
	return errors.WithStack(o.UpdateGroup(group))
}

// SuspendGroup closes current sessions AND future restricted admission before
// the runtime publishes changed term/role/configuration state. Unlike mere
// connection revocation, a fresh handshake cannot regain the old permission.
// Reopening requires a strictly newer validated projection via UpdateGroup.
func (o *Owner) SuspendGroup(id uint64) {
	o.mu.Lock()
	transports := o.revokeGroupLocked(id)
	if group, ok := o.groups[id]; ok {
		group.Stage = Reconciling
		group.Pending = PendingTransition
		o.groups[id] = group
	}
	o.mu.Unlock()
	closeTransports(transports)
}

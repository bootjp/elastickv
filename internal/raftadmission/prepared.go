package raftadmission

import (
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

func (a *Arbiter) NeedsActivation(id uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[id]
	return !a.closed && a.fatal == nil && g != nil && g.reconciled && g.activation == 0
}

// PublishPreparedWith opens only the intact identity lane from an independently
// verified local preparation. A signed probe is not itself native lineage proof;
// the caller owes selected WAL, marker and store reconciliation before this call.
func (a *Arbiter) PublishPreparedWith(ticket RecoveryTicket, probe raftbootstrap.BaselineProbe, manifest raftbootstrap.Digest, publish Publisher) error {
	if _, err := raftbootstrap.MarshalBaselineProbe(probe); err != nil {
		return errors.WithStack(err)
	}
	retire, err := func() (func(), error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if !a.currentTicketLocked(ticket) || !a.preparedMatchesLocked(ticket, probe, manifest) || publish == nil {
			return nil, errors.WithStack(ErrStale)
		}
		g := a.groups[ticket.group]
		next := g.sessionGeneration + 1
		if next == 0 {
			a.latchFatalLocked(ErrClosed)
			return nil, errors.WithStack(ErrClosed)
		}
		retire, err := publish(peersession.IdentityReady, next)
		if err != nil {
			a.invalidateLocked(ticket.group)
			return retire, errors.WithStack(err)
		}
		a.invalidateLocked(ticket.group)
		g.policy, g.activation, g.policyGeneration = manifest, 0, 0
		g.reconciled, g.joined = true, true
		g.reservation, g.loaded = false, false
		g.registration = RegistrationUnknown
		g.peerStage, g.sessionGeneration = peersession.IdentityReady, next
		return retire, nil
	}()
	if retire != nil {
		retire()
	}
	return err
}

func (a *Arbiter) preparedMatchesLocked(ticket RecoveryTicket, probe raftbootstrap.BaselineProbe, manifest raftbootstrap.Digest) bool {
	g := a.groups[ticket.group]
	return g != nil && g.activation == 0 && manifest != (raftbootstrap.Digest{}) &&
		probe.Identity.Cluster == a.config.Cluster && probe.Identity.Group == ticket.group &&
		probe.Configuration.Index == g.native.ConfigurationIndex
}

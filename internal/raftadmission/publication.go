package raftadmission

import (
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

// Publisher changes only the peer owner's in-memory projection under A. The
// returned physical cleanup runs after A is released. It must not call an
// engine, perform I/O, or wait for a child.
type Publisher func(peersession.Stage, uint64) (func(), error)

func (a *Arbiter) PublishRecoveryWith(ticket RecoveryTicket, policy raftbootstrap.PolicyRecord, registration RegistrationState, publish Publisher) error {
	digest, err := raftbootstrap.PolicyDigest(policy)
	if err != nil {
		return errors.WithStack(err)
	}
	retire, err := func() (func(), error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		g := a.groups[ticket.group]
		if g == nil || publish == nil {
			return nil, errors.WithStack(ErrClosed)
		}
		next := g.sessionGeneration + 1
		if next == 0 {
			a.latchFatalLocked(ErrClosed)
			return nil, errors.WithStack(ErrClosed)
		}
		if err := a.checkRecoveryLocked(ticket, policy, next, registration); err != nil {
			return nil, err
		}
		retire, err := publish(peersession.IdentityReady, next)
		if err != nil {
			a.invalidateLocked(ticket.group)
			return retire, errors.WithStack(err)
		}
		a.commitRecoveryLocked(ticket, policy, digest, next, registration)
		return retire, nil
	}()
	if retire != nil {
		retire()
	}
	return err
}

func (a *Arbiter) PublishStage(id uint64, publish Publisher) error {
	retire, err := func() (func(), error) {
		a.mu.Lock()
		defer a.mu.Unlock()
		g := a.groups[id]
		if g == nil || g.sessionGeneration == 0 || publish == nil {
			return nil, errors.WithStack(ErrClosed)
		}
		stage := a.stageLocked(id)
		if stage == peersession.Reconciling {
			return nil, nil
		}
		if stage == g.peerStage {
			return nil, nil
		}
		next := g.sessionGeneration + 1
		if next == 0 {
			a.latchFatalLocked(ErrClosed)
			return nil, errors.WithStack(ErrClosed)
		}
		retire, err := publish(stage, next)
		if err != nil {
			a.invalidateLocked(id)
			return retire, errors.WithStack(err)
		}
		g.sessionGeneration, g.peerStage = next, stage
		return retire, nil
	}()
	if retire != nil {
		retire()
	}
	return err
}

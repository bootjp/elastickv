// Package raftadmission owns process-local, revocable Raft operation handles.
// Handles never cross a network or become replicated apply authority.
package raftadmission

import (
	"context"
	"crypto/rand"
	"math"
	"sync"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

var (
	ErrClosed = errors.New("raft admission is closed")
	ErrStale  = errors.New("raft admission handle is stale")
)

type Kind uint8

const (
	Data Kind = iota + 1
	Control
	Catalog
	Allocator
	HLC
	Registration
	Recovery
	Rotation
	Maintenance
)

type RegistrationState uint8

const (
	RegistrationUnknown RegistrationState = iota
	RegistrationNotConfigured
	RegistrationVerified
)

type Config struct {
	Cluster           raftbootstrap.UUID
	DefaultGroup      uint64
	Groups            []uint64
	RotationRequested bool
	// ValidatePayload parses the complete final native payload outside A.
	// It must reject commands outside the receiver-owned operation's lane.
	ValidatePayload func(Kind, uint64, []byte) error
}

type group struct {
	native                                          raftengine.NativeState
	generation                                      uint64
	observed, reconciled, joined, loaded            bool
	policy                                          raftbootstrap.Digest
	activation, policyGeneration, sessionGeneration uint64
	reservation                                     bool
	registration                                    RegistrationState
	draining                                        map[*Operation]struct{}
	peerStage                                       peersession.Stage
}

type Arbiter struct {
	mu                   sync.Mutex
	config               Config
	boot                 [32]byte
	groups               map[uint64]*group
	permissionGeneration uint64
	fatal                error
	closed               bool
	operations           map[*Operation]struct{}
}

// RecoveryTicket binds I/O performed outside A to the native image preceding it.
type RecoveryTicket struct {
	owner             *Arbiter
	group, generation uint64
	native            raftengine.NativeState
}

func New(config Config) (*Arbiter, error) {
	if config.Cluster == (raftbootstrap.UUID{}) || len(config.Groups) == 0 || config.ValidatePayload == nil {
		return nil, errors.WithStack(ErrClosed)
	}
	a := &Arbiter{config: config, groups: make(map[uint64]*group), permissionGeneration: 1,
		operations: make(map[*Operation]struct{})}
	for _, id := range config.Groups {
		if _, exists := a.groups[id]; exists {
			return nil, errors.WithStack(ErrClosed)
		}
		a.groups[id] = &group{generation: 1, draining: make(map[*Operation]struct{})}
	}
	if a.groups[config.DefaultGroup] == nil {
		return nil, errors.WithStack(ErrClosed)
	}
	if _, err := rand.Read(a.boot[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	return a, nil
}

func (a *Arbiter) Ticket(id uint64) (RecoveryTicket, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[id]
	if a.closed || a.fatal != nil || g == nil || !g.observed {
		return RecoveryTicket{}, errors.WithStack(ErrClosed)
	}
	return RecoveryTicket{owner: a, group: id, generation: g.generation, native: g.native}, nil
}

// PublishRecovery accepts only independently verified applied policy. Validation
// and durable reads precede this call; publication itself never performs I/O.
func (a *Arbiter) PublishRecovery(ticket RecoveryTicket, policy raftbootstrap.PolicyRecord, session uint64, registration RegistrationState) error {
	digest, err := raftbootstrap.PolicyDigest(policy)
	if err != nil {
		return errors.WithStack(err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.checkRecoveryLocked(ticket, policy, session, registration); err != nil {
		return err
	}
	a.commitRecoveryLocked(ticket, policy, digest, session, registration)
	return nil
}

func (a *Arbiter) checkRecoveryLocked(ticket RecoveryTicket, policy raftbootstrap.PolicyRecord, session uint64, registration RegistrationState) error {
	g := a.groups[ticket.group]
	if !a.currentTicketLocked(ticket) {
		return errors.WithStack(ErrStale)
	}
	if registration > RegistrationVerified || !a.recoveryPolicyMatchesLocked(ticket.group, policy) ||
		session == 0 || session <= g.sessionGeneration {
		return errors.WithStack(ErrClosed)
	}
	return nil
}

func (a *Arbiter) currentTicketLocked(ticket RecoveryTicket) bool {
	g := a.groups[ticket.group]
	return !a.closed && a.fatal == nil && ticket.owner == a && g != nil &&
		ticket.generation == g.generation && ticket.native == g.native
}

func (a *Arbiter) recoveryPolicyMatchesLocked(id uint64, policy raftbootstrap.PolicyRecord) bool {
	return policy.Identity.Cluster == a.config.Cluster && policy.Identity.Group == id &&
		policy.LastConfigurationIndex == a.groups[id].native.ConfigurationIndex
}

func (a *Arbiter) commitRecoveryLocked(ticket RecoveryTicket, policy raftbootstrap.PolicyRecord, digest raftbootstrap.Digest, session uint64, registration RegistrationState) {
	g := a.groups[ticket.group]
	a.invalidateLocked(ticket.group)
	g.policy, g.activation, g.policyGeneration = digest, policy.ActivationIndex, policy.Generation
	g.sessionGeneration, g.registration = session, registration
	g.reservation = policy.Reservation != nil
	g.reconciled, g.joined = true, true
	g.peerStage = peersession.IdentityReady
}

func (a *Arbiter) MarkCatalogLoaded(ticket RecoveryTicket) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[ticket.group]
	if ticket.owner != a || g == nil || ticket.generation != g.generation || !g.reconciled || a.closed || a.fatal != nil {
		return errors.WithStack(ErrStale)
	}
	g.loaded = true
	return nil
}

func (a *Arbiter) Stage(id uint64) peersession.Stage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stageLocked(id)
}

func (a *Arbiter) stageLocked(id uint64) peersession.Stage {
	g := a.groups[id]
	if a.closed || a.fatal != nil || g == nil || !g.reconciled || !g.observed {
		return peersession.Reconciling
	}
	if !a.permissionLocked() || !groupControlReady(g) {
		return peersession.IdentityReady
	}
	if !a.groups[a.config.DefaultGroup].loaded {
		return peersession.ControlReady
	}
	return peersession.DataReady
}

func groupControlReady(g *group) bool {
	return len(g.draining) == 0 && g.joined && g.registration != RegistrationUnknown &&
		g.activation != 0 && !g.reservation && stableRole(g.native.Role)
}

func stableRole(role raftengine.State) bool {
	return role == raftengine.StateLeader || role == raftengine.StateFollower
}

func (a *Arbiter) permissionLocked() bool {
	g := a.groups[a.config.DefaultGroup]
	// Requested rotation needs its owned completion/F protocol. It must never
	// accidentally acquire the explicit not-requested permission.
	return !a.config.RotationRequested && g != nil && g.reconciled && stableRole(g.native.Role) &&
		g.registration != RegistrationUnknown && !g.reservation && len(g.draining) == 0
}

func (a *Arbiter) invalidateLocked(id uint64) {
	g := a.groups[id]
	if g == nil {
		return
	}
	g.generation++
	if g.generation == 0 || (id == a.config.DefaultGroup && a.permissionGeneration == math.MaxUint64) {
		a.latchFatalLocked(ErrClosed)
	}
	g.reconciled = false
	if id == a.config.DefaultGroup {
		g.loaded = false
		a.permissionGeneration++
	}
	for op := range a.operations {
		if id == a.config.DefaultGroup || op.used[id] || op.globalDataDependency() {
			g.draining[op] = struct{}{}
			op.draining[id] = true
			op.cancel()
		}
	}
}

func (a *Arbiter) forgetLocked(op *Operation) {
	delete(a.operations, op)
	for id := range op.draining {
		delete(a.groups[id].draining, op)
	}
	if op.ancestor != nil {
		op.ancestorDone.Do(op.ancestor.children.Done)
	}
	op.doneOnce.Do(func() { close(op.done) })
}

func (a *Arbiter) Suspend(id uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.invalidateLocked(id)
}

func (a *Arbiter) LatchFatal(err error) {
	if err == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.latchFatalLocked(err)
}

func (a *Arbiter) latchFatalLocked(err error) {
	if a.fatal == nil {
		a.fatal = err
	}
	for op := range a.operations {
		op.cancel()
	}
}

func (a *Arbiter) Close() {
	a.mu.Lock()
	a.closed = true
	operations := make([]*Operation, 0, len(a.operations))
	for op := range a.operations {
		op.cancel()
		operations = append(operations, op)
	}
	a.mu.Unlock()
	for _, op := range operations {
		op.children.Wait()
		<-op.done
		op.stopParent()
	}
}

type nativeScope struct {
	owner    *Arbiter
	group    uint64
	heldPeer *peersession.Receipt
}

func (a *Arbiter) WithNative(ctx context.Context, id uint64, action func(raftengine.NativeScope) error) error {
	if action == nil || ctx == nil {
		return errors.WithStack(ErrClosed)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.groups[id] == nil {
		return errors.WithStack(ErrClosed)
	}
	scope := nativeScope{owner: a, group: id}
	if op := operationFrom(ctx); op != nil && op.peer != nil {
		scope.heldPeer = op.peer
		return errors.WithStack(op.peer.WithAdmission(ctx, func() error { return action(scope) }))
	}
	return action(scope)
}

func (s nativeScope) Observe(state raftengine.NativeState) {
	g := s.owner.groups[s.group]
	if !g.observed || g.native != state {
		s.owner.invalidateLocked(s.group)
		g.native, g.observed = state, true
	}
}

func (s nativeScope) Invalidate() { s.owner.invalidateLocked(s.group) }

func (s nativeScope) Check(ctx context.Context, request raftengine.NativeRequest) error {
	a, g := s.owner, s.owner.groups[s.group]
	if a.closed || a.fatal != nil {
		return errors.WithStack(ErrClosed)
	}
	if request.Operation == raftengine.NativePeerStep {
		return checkPeerGeneration(g, request.SessionGeneration)
	}
	op := operationFrom(ctx)
	if err := a.validateWithHeldPeerLocked(op, s.group, s.heldPeer); err != nil {
		return err
	}
	if g.native.Role != raftengine.StateLeader {
		return errors.WithStack(raftengine.ErrNotLeader)
	}
	if err := s.checkOwnedRequest(ctx, op, request); err != nil {
		return err
	}
	a.markUsedLocked(op, s.group)
	return nil
}

func checkPeerGeneration(g *group, generation uint64) error {
	if !g.reconciled || generation == 0 || generation != g.sessionGeneration {
		return errors.WithStack(ErrStale)
	}
	return nil
}

func (s nativeScope) checkOwnedRequest(ctx context.Context, op *Operation, request raftengine.NativeRequest) error {
	switch request.Operation {
	case raftengine.NativeProposal:
		if !proposalSealMatches(ctx, op, s.group, request.PayloadDigest) {
			return errors.WithStack(ErrClosed)
		}
	case raftengine.NativeReadIndex:
	case raftengine.NativeTransfer:
		if op.kind != Control {
			return errors.WithStack(ErrClosed)
		}
	case raftengine.NativeMembership, raftengine.NativePeerStep:
		return errors.WithStack(ErrClosed)
	default:
		return errors.WithStack(ErrClosed)
	}
	return nil
}

func proposalSealMatches(ctx context.Context, op *Operation, group uint64, digest [32]byte) bool {
	bound, ok := ctx.Value(payloadKey{}).(boundPayload)
	return ok && bound.operation == op && bound.group == group && bound.digest == digest
}

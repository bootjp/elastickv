package raftadmission

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"slices"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

type operationKey struct{}
type payloadKey struct{}

type boundPayload struct {
	operation *Operation
	group     uint64
	digest    [32]byte
}

type Operation struct {
	owner              *Arbiter
	kind               Kind
	ctx                context.Context
	parent             context.Context
	ancestor           *Operation
	ancestorDone       sync.Once
	done               chan struct{}
	doneOnce           sync.Once
	cancel             context.CancelFunc
	permission         uint64
	rotationRecipe     raftbootstrap.Digest
	snapshot           map[uint64]uint64
	used               map[uint64]bool
	peer               *peersession.Receipt
	draining           map[uint64]bool
	children           sync.WaitGroup
	finished, retiring bool
	id                 [16]byte
	stopParent         func()
}

type deadlineContext struct {
	context.Context
	parent context.Context
}

func (c deadlineContext) Deadline() (time.Time, bool) { return c.parent.Deadline() }

func operationFrom(ctx context.Context) *Operation {
	if ctx == nil {
		return nil
	}
	op, _ := ctx.Value(operationKey{}).(*Operation)
	return op
}

// Begin captures the local generations before work starts, not when a delayed
// child finally reaches a proposal queue. Native checks never renew this image.
func (a *Arbiter) Begin(ctx context.Context, kind Kind) (context.Context, *Operation, error) {
	if !validBegin(ctx, kind) {
		return nil, nil, errors.WithStack(ErrClosed)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.fatal != nil || !a.kindReadyLocked(kind, a.config.DefaultGroup) {
		return nil, nil, errors.WithStack(ErrClosed)
	}
	ancestor := operationFrom(ctx)
	if err := a.validateAncestorLocked(ancestor, kind); err != nil {
		return nil, nil, err
	}
	peer := peersession.CapturedReceipt(ctx)
	if err := a.validatePeerLocked(ctx, peer); err != nil {
		return nil, nil, err
	}
	return a.newOperationLocked(ctx, kind, id, ancestor, peer)
}

func validBegin(ctx context.Context, kind Kind) bool {
	return ctx != nil && ctx.Err() == nil && validKind(kind) && kind != Rotation
}

func validKind(kind Kind) bool { return kind >= Data && kind <= Maintenance }

func (a *Arbiter) newOperationLocked(ctx context.Context, kind Kind, id [16]byte, ancestor *Operation, peer *peersession.Receipt) (context.Context, *Operation, error) {
	// Parent cancellation enters A before canceling the owned context. A
	// terminal native action and its cancellation therefore have one order.
	child, cancel := context.WithCancel(deadlineContext{Context: context.WithoutCancel(ctx), parent: ctx})
	op := &Operation{owner: a, kind: kind, ctx: child, parent: ctx, cancel: cancel,
		permission: a.permissionGeneration, snapshot: make(map[uint64]uint64), used: make(map[uint64]bool), draining: make(map[uint64]bool), id: id, peer: peer, ancestor: ancestor, done: make(chan struct{})}
	for id, g := range a.groups {
		op.snapshot[id] = g.generation
		if ancestor != nil {
			op.snapshot[id] = ancestor.snapshot[id]
		}
	}
	if ancestor != nil {
		op.permission = ancestor.permission
		ancestor.children.Add(1)
	}
	parentDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		a.mu.Lock()
		op.cancel()
		a.mu.Unlock()
		close(parentDone)
	})
	var stopOnce sync.Once
	op.stopParent = func() {
		stopOnce.Do(func() {
			if !stop() {
				<-parentDone
			}
		})
	}
	a.operations[op] = struct{}{}
	return context.WithValue(child, operationKey{}, op), op, nil
}

func (a *Arbiter) validateAncestorLocked(ancestor *Operation, kind Kind) error {
	if ancestor == nil {
		return nil
	}
	if err := a.validateLocked(ancestor, a.config.DefaultGroup); err != nil {
		return err
	}
	if ancestor.retiring || !allowedChild(ancestor.kind, kind) {
		return errors.WithStack(ErrClosed)
	}
	return nil
}

func (a *Arbiter) validatePeerLocked(ctx context.Context, peer *peersession.Receipt) error {
	if peer == nil {
		return nil
	}
	g := a.groups[peer.Message().Group]
	if g == nil || !g.reconciled || peer.LocalGeneration() != g.sessionGeneration {
		return errors.WithStack(ErrStale)
	}
	return errors.WithStack(peer.WithAdmission(ctx, func() error { return nil }))
}

func allowedChild(parent, child Kind) bool {
	if parent == child {
		return true
	}
	switch parent {
	case Data, Maintenance:
		return slices.Contains([]Kind{Data, Allocator, HLC, Control, Recovery}, child)
	case Catalog:
		return slices.Contains([]Kind{Allocator, HLC, Control, Recovery}, child)
	case Allocator:
		return slices.Contains([]Kind{HLC, Control, Recovery}, child)
	case Control, HLC, Registration:
		return child == Recovery
	case Rotation:
		return slices.Contains([]Kind{Registration, Control, Recovery}, child)
	case Recovery:
		return false
	default:
		return false
	}
}

func (a *Arbiter) kindReadyLocked(kind Kind, id uint64) bool {
	stage := a.stageLocked(id)
	switch kind {
	case Data, Maintenance:
		return a.allDataReadyLocked()
	case Control, Catalog, Allocator, HLC:
		return stage >= peersession.ControlReady
	case Registration, Recovery:
		return stage >= peersession.IdentityReady
	case Rotation:
		return a.rotationReadyLocked() && a.rotationTargetReadyLocked(id)
	default:
		return false
	}
}

func (a *Arbiter) allDataReadyLocked() bool {
	for id := range a.groups {
		if a.stageLocked(id) != peersession.DataReady {
			return false
		}
	}
	return true
}

func (op *Operation) globalDataDependency() bool {
	return op.kind == Data || op.kind == Maintenance
}

func (a *Arbiter) validateLocked(op *Operation, id uint64) error {
	return a.validateWithHeldPeerLocked(op, id, nil)
}

func (a *Arbiter) validateWithHeldPeerLocked(op *Operation, id uint64, held *peersession.Receipt) error {
	g := a.groups[id]
	if !a.operationLiveLocked(op) || g == nil || op.snapshot[id] != g.generation ||
		!a.kindReadyLocked(op.kind, id) {
		return errors.WithStack(ErrStale)
	}
	if op.peer != nil && op.peer != held {
		if err := op.peer.WithAdmission(op.ctx, func() error { return nil }); err != nil {
			return errors.WithStack(ErrStale)
		}
	}
	if op.ancestor != nil {
		return a.validateWithHeldPeerLocked(op.ancestor, id, held)
	}
	return nil
}

func (a *Arbiter) operationLiveLocked(op *Operation) bool {
	return !a.closed && a.fatal == nil && op != nil && op.owner == a && !op.finished &&
		op.ctx.Err() == nil && op.parent.Err() == nil && op.permission == a.permissionGeneration
}

func (a *Arbiter) markUsedLocked(op *Operation, id uint64) {
	for current := op; current != nil; current = current.ancestor {
		current.used[id] = true
	}
}

// Enter reuses a matching owned operation, or creates an owned child inheriting
// the parent's original images. It never refreshes a stale parent generation.
func (a *Arbiter) Enter(ctx context.Context, kind Kind, id uint64) (context.Context, func(error) error, error) {
	op := operationFrom(ctx)
	if op != nil && op.kind == kind {
		if err := a.reuseOperation(op, id); err != nil {
			return nil, nil, err
		}
		return ctx, func(original error) error {
			if original != nil {
				return original
			}
			return a.ValidateResult(ctx)
		}, nil
	}
	owned, op, err := a.Begin(ctx, kind)
	if err != nil {
		return nil, nil, err
	}
	a.mu.Lock()
	err = a.validateLocked(op, id)
	if err == nil {
		a.markUsedLocked(op, id)
	}
	a.mu.Unlock()
	if err != nil {
		op.CancelAndJoin()
		return nil, nil, err
	}
	return owned, func(original error) error {
		if original != nil {
			op.CancelAndJoin()
			return original
		}
		return op.Finish()
	}, nil
}

func (a *Arbiter) reuseOperation(op *Operation, id uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateLocked(op, id); err != nil {
		return err
	}
	if op.retiring {
		return errors.WithStack(ErrClosed)
	}
	a.markUsedLocked(op, id)
	return nil
}

func (a *Arbiter) BindProposal(ctx context.Context, id uint64, payload []byte) (context.Context, error) {
	op := operationFrom(ctx)
	a.mu.Lock()
	err := a.validateLocked(op, id)
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	inspection, err := a.inspectProposal(op, id, payload)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	digest := sha256.Sum256(payload)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateLocked(op, id); err != nil {
		return nil, err
	}
	if inspection.ProjectionGeneration != 0 && inspection.ProjectionGeneration != op.permission {
		return nil, errors.WithStack(ErrStale)
	}
	if err := registrationDeclarationMatches(op, inspection.PlaintextDigest); err != nil {
		return nil, err
	}
	a.markUsedLocked(op, id)
	return context.WithValue(ctx, payloadKey{}, boundPayload{operation: op, group: id, digest: digest}), nil
}

func (a *Arbiter) inspectProposal(op *Operation, id uint64, payload []byte) (PayloadInspection, error) {
	if a.config.InspectPayload != nil {
		return a.config.InspectPayload(op.kind, id, payload, op.rotationRecipe)
	}
	if err := a.config.ValidatePayload(op.kind, id, payload); err != nil {
		return PayloadInspection{}, errors.WithStack(err)
	}
	return PayloadInspection{PlaintextDigest: sha256.Sum256(payload)}, nil
}

func registrationDeclarationMatches(op *Operation, plain raftbootstrap.Digest) error {
	if op.peer == nil {
		return nil
	}
	message := op.peer.Message()
	if op.kind == Registration {
		if message.Traffic != peersession.Registration || message.Declaration == (raftbootstrap.Digest{}) || message.Declaration != plain {
			return errors.WithStack(ErrClosed)
		}
	} else if message.Traffic == peersession.Registration {
		return errors.WithStack(ErrClosed)
	}
	return nil
}

func (a *Arbiter) ValidateResult(ctx context.Context) error {
	op := operationFrom(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateLocked(op, a.config.DefaultGroup); err != nil {
		return err
	}
	for id := range op.used {
		if err := a.validateLocked(op, id); err != nil {
			return err
		}
	}
	return nil
}

// Go owns a child until it returns. Invalidation cancels its context; retirement
// and shutdown join outside A, never while blocking another engine's loop.
func (op *Operation) Go(action func(context.Context)) error {
	if op == nil || action == nil {
		return errors.WithStack(ErrClosed)
	}
	a := op.owner
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.validateLocked(op, a.config.DefaultGroup); err != nil {
		return err
	}
	if op.retiring {
		return errors.WithStack(ErrClosed)
	}
	op.children.Add(1)
	go func() {
		defer op.children.Done()
		action(context.WithValue(op.ctx, operationKey{}, op))
	}()
	return nil
}

func (op *Operation) Finish() error {
	if op == nil {
		return errors.WithStack(ErrClosed)
	}
	a := op.owner
	a.mu.Lock()
	op.retiring = true
	a.mu.Unlock()
	op.children.Wait()
	op.stopParent()
	a.mu.Lock()
	defer a.mu.Unlock()
	err := a.validateLocked(op, a.config.DefaultGroup)
	if err == nil {
		for id := range op.used {
			if err = a.validateLocked(op, id); err != nil {
				break
			}
		}
	}
	op.finished = true
	a.forgetLocked(op)
	op.cancel()
	return err
}

func (op *Operation) CancelAndJoin() {
	if op == nil {
		return
	}
	a := op.owner
	a.mu.Lock()
	op.finished, op.retiring = true, true
	op.cancel()
	a.mu.Unlock()
	op.children.Wait()
	op.stopParent()
	a.mu.Lock()
	a.forgetLocked(op)
	a.mu.Unlock()
}

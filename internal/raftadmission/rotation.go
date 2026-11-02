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

type encryptionState struct {
	generation uint64
	digest     raftbootstrap.Digest
	observed   bool
	mutations  uint64
}

type rotationPermissionKind uint8

const (
	rotationPermissionNone rotationPermissionKind = iota
	rotationPermissionLeader
	rotationPermissionFollower
)

type rotationPermission struct {
	kind       rotationPermissionKind
	native     raftengine.NativeState
	group      uint64
	projection uint64
	sequence   uint64
}

type rotationState struct {
	sequence       uint64
	recipe         raftbootstrap.Digest
	active         *RotationRun
	responsibility bool
	permission     rotationPermission
}

// EncryptionTicket fences observations made outside A. A digest is an
// observation from the trusted local verifier, never a peer-supplied authority.
type EncryptionTicket struct {
	recovery   RecoveryTicket
	projection uint64
	permission uint64
	sequence   uint64
}

type RotationStatus struct {
	Native         raftengine.NativeState
	Recipe         raftbootstrap.Digest
	Responsibility bool
	Running        bool
	Observed       bool
}

type RotationRun struct {
	owner      *Arbiter
	op         *Operation
	ctx        context.Context
	sequence   uint64
	recipe     raftbootstrap.Digest
	joined     bool
	result     error
	finishOnce sync.Once
}

func (r *RotationRun) Context() context.Context { return r.ctx }

func (a *Arbiter) requiresEncryption() bool {
	return a.config.EncryptionConfigured || a.config.RotationRequested
}

func (a *Arbiter) rotationPermissionLocked(g *group) bool {
	if !a.requiresEncryption() {
		return true
	}
	if !a.encryption.observed || g.registration != RegistrationVerified || a.rotation.active != nil || a.rotation.responsibility {
		return false
	}
	if !a.config.RotationRequested {
		return true
	}
	p := a.rotation.permission
	if !a.rotationPermissionCurrentLocked(p, g) {
		return false
	}
	return p.matchesRole(g.native.Role)
}

func (a *Arbiter) rotationPermissionCurrentLocked(p rotationPermission, g *group) bool {
	return p.native == g.native && p.group == g.generation && p.projection == a.encryption.generation && p.sequence == a.rotation.sequence
}

func (p rotationPermission) matchesRole(role raftengine.State) bool {
	return (role == raftengine.StateLeader && p.kind == rotationPermissionLeader) ||
		(role == raftengine.StateFollower && p.kind == rotationPermissionFollower)
}

func (a *Arbiter) RotationStatus() (RotationStatus, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.fatal != nil {
		return RotationStatus{}, errors.Join(ErrClosed, a.fatal)
	}
	g := a.groups[a.config.DefaultGroup]
	return RotationStatus{Native: g.native, Recipe: a.rotation.recipe, Responsibility: a.rotation.responsibility,
		Running: a.rotation.active != nil, Observed: a.encryption.observed}, nil
}

func (a *Arbiter) LocalPermissionReady() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.closed && a.fatal == nil && a.permissionLocked()
}

func (a *Arbiter) EncryptionTicket() (EncryptionTicket, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[a.config.DefaultGroup]
	if a.closed || a.fatal != nil || !g.observed || !g.reconciled || a.encryption.mutations != 0 {
		return EncryptionTicket{}, errors.WithStack(ErrClosed)
	}
	return EncryptionTicket{recovery: RecoveryTicket{owner: a, group: a.config.DefaultGroup, generation: g.generation, native: g.native},
		projection: a.encryption.generation, permission: a.permissionGeneration, sequence: a.rotation.sequence}, nil
}

func (a *Arbiter) encryptionTicketCurrentLocked(t EncryptionTicket) bool {
	return a.encryption.mutations == 0 && a.currentTicketLocked(t.recovery) && t.projection == a.encryption.generation &&
		t.permission == a.permissionGeneration && t.sequence == a.rotation.sequence
}

// InvalidateEncryption runs BEFORE changing a local key/wrapper/registration
// projection. It performs no I/O and retains policy/catalog recovery evidence.
func (a *Arbiter) InvalidateEncryption() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.invalidateEncryptionLocked()
}

// BeginEncryptionMutation brackets an actual local publication. The returned
// end is called after releasing its store/wrapper locks. Observers cannot
// republish the coherent OLD image while that publication is still pending.
func (a *Arbiter) BeginEncryptionMutation() func() {
	a.mu.Lock()
	if a.encryption.mutations == math.MaxUint64 {
		a.latchFatalLocked(ErrClosed)
		a.mu.Unlock()
		return func() {}
	}
	a.invalidateEncryptionLocked()
	a.encryption.mutations++
	a.mu.Unlock()
	return sync.OnceFunc(func() {
		a.mu.Lock()
		a.encryption.mutations--
		a.mu.Unlock()
	})
}

func (a *Arbiter) EncryptionFence() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[a.config.DefaultGroup]
	if a.closed || a.fatal != nil || !g.observed || !g.reconciled || a.encryption.mutations != 0 {
		return 0, errors.WithStack(ErrClosed)
	}
	return a.permissionGeneration, nil
}

func (a *Arbiter) invalidateEncryptionLocked() {
	if a.permissionGeneration == math.MaxUint64 || a.encryption.generation == math.MaxUint64 {
		a.latchFatalLocked(ErrClosed)
		return
	}
	a.permissionGeneration++
	a.encryption.generation++
	a.encryption.observed = false
	a.rotation.permission = rotationPermission{}
	for op := range a.operations {
		for id, g := range a.groups {
			g.draining[op] = struct{}{}
			op.draining[id] = true
		}
		op.cancel()
	}
}

func (a *Arbiter) PublishEncryption(t EncryptionTicket, digest raftbootstrap.Digest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.requiresEncryption() || digest == (raftbootstrap.Digest{}) || !a.encryptionTicketCurrentLocked(t) {
		return errors.WithStack(ErrStale)
	}
	if a.encryption.observed && a.encryption.digest != digest {
		a.invalidateEncryptionLocked()
		return errors.WithStack(ErrStale)
	}
	a.encryption.digest, a.encryption.observed = digest, true
	a.groups[a.config.DefaultGroup].registration = RegistrationVerified
	return nil
}

func (a *Arbiter) rotationReadyLocked() bool {
	g := a.groups[a.config.DefaultGroup]
	return a.requiresEncryption() && g.reconciled && g.observed && g.joined &&
		g.native.Role == raftengine.StateLeader && g.registration == RegistrationVerified &&
		!g.reservation && a.encryption.observed && a.encryption.mutations == 0 && len(g.draining) == 0
}

func (a *Arbiter) rotationTargetReadyLocked(id uint64) bool {
	g := a.groups[id]
	return g != nil && g.reconciled && g.observed && g.joined && !g.reservation && len(g.draining) == 0
}

// BeginRotation is local-controller-only. Generic Begin cannot mint this lane.
// A new run captures current generations, never refreshes an old queued run.
func (a *Arbiter) BeginRotation(ctx context.Context, recipe raftbootstrap.Digest) (*RotationRun, error) {
	if !validRotationContext(ctx, recipe) {
		return nil, errors.WithStack(ErrClosed)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rotation.sequence == math.MaxUint64 {
		a.latchFatalLocked(ErrClosed)
	}
	if !a.canBeginRotationLocked() {
		return nil, errors.WithStack(ErrClosed)
	}
	if a.rotation.responsibility && a.rotation.recipe != (raftbootstrap.Digest{}) && a.rotation.recipe != recipe {
		return nil, errors.WithStack(ErrClosed)
	}
	return a.beginRotationLocked(ctx, recipe, id)
}

func (a *Arbiter) beginRotationLocked(ctx context.Context, recipe raftbootstrap.Digest, id [16]byte) (*RotationRun, error) {
	// Close dependents before assigning responsibility or exposing the run.
	a.invalidateEncryptionLocked()
	if a.fatal != nil {
		return nil, errors.WithStack(ErrClosed)
	}
	// The key projection did not change. Its already verified observation is
	// retained under the new generation; all previous handles remain revoked.
	a.encryption.observed = true
	a.rotation.sequence++
	owned, op, err := a.newOperationLocked(ctx, Rotation, id, nil, nil)
	if err != nil {
		return nil, err
	}
	run := &RotationRun{owner: a, op: op, ctx: owned, sequence: a.rotation.sequence, recipe: recipe}
	op.rotationRecipe = recipe
	a.rotation.active, a.rotation.recipe, a.rotation.responsibility = run, recipe, true
	return run, nil
}

func (a *Arbiter) canBeginRotationLocked() bool {
	return !a.closed && a.fatal == nil && a.rotation.active == nil && a.rotationReadyLocked()
}

func validRotationContext(ctx context.Context, recipe raftbootstrap.Digest) bool {
	return ctx != nil && ctx.Err() == nil && operationFrom(ctx) == nil &&
		peersession.CapturedReceipt(ctx) == nil && recipe != (raftbootstrap.Digest{})
}

func (r *RotationRun) Finish(original error) error {
	r.finishOnce.Do(func() {
		if original == nil {
			original = r.op.Finish()
		} else {
			r.op.CancelAndJoin()
		}
		a := r.owner
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.rotation.active == r {
			a.rotation.active = nil
		}
		r.joined, r.result = true, original
	})
	return r.result
}

func (a *Arbiter) CompleteRotation(r *RotationRun, t EncryptionTicket, digest raftbootstrap.Digest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[a.config.DefaultGroup]
	if !a.rotationRunCurrentLocked(r) || !a.encryptionTicketCurrentLocked(t) ||
		!a.encryption.observed || a.encryption.digest != digest || g.native.Role != raftengine.StateLeader || len(g.draining) != 0 {
		return errors.WithStack(ErrStale)
	}
	a.rotation.responsibility = false
	a.rotation.permission = a.currentRotationPermissionLocked(rotationPermissionLeader)
	return nil
}

func (a *Arbiter) rotationRunCurrentLocked(r *RotationRun) bool {
	if r == nil || r.owner != a || !r.joined || r.result != nil || r.sequence != a.rotation.sequence ||
		a.rotation.active != nil || r.recipe != a.rotation.recipe || r.op.parent.Err() != nil || r.op.permission != a.permissionGeneration {
		return false
	}
	return a.rotationGroupsCurrentLocked(r)
}

func (a *Arbiter) rotationGroupsCurrentLocked(r *RotationRun) bool {
	if r.op.snapshot[a.config.DefaultGroup] != a.groups[a.config.DefaultGroup].generation {
		return false
	}
	for id := range r.op.used {
		if r.op.snapshot[id] != a.groups[id].generation {
			return false
		}
	}
	return true
}

// ResolveRotationResponsibility accepts a freshly fenced, read-only exact
// recipe reconciliation. It grants no completion and cannot complete a leader.
func (a *Arbiter) ResolveRotationResponsibility(t EncryptionTicket, recipe, digest raftbootstrap.Digest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[a.config.DefaultGroup]
	if !a.encryptionTicketCurrentLocked(t) || !stableRole(g.native.Role) ||
		a.rotation.active != nil || a.rotation.recipe != recipe || !a.encryption.observed || a.encryption.digest != digest || !a.allOwnersJoinedLocked() {
		return errors.WithStack(ErrStale)
	}
	a.rotation.responsibility = false
	return nil
}

func (a *Arbiter) PublishFollowerPermission(t EncryptionTicket, digest raftbootstrap.Digest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.groups[a.config.DefaultGroup]
	if !a.encryptionTicketCurrentLocked(t) || g.native.Role != raftengine.StateFollower || a.rotation.responsibility ||
		a.rotation.active != nil || !a.encryption.observed || a.encryption.digest != digest || !a.allOwnersJoinedLocked() {
		return errors.WithStack(ErrStale)
	}
	a.rotation.permission = a.currentRotationPermissionLocked(rotationPermissionFollower)
	return nil
}

func (a *Arbiter) allOwnersJoinedLocked() bool {
	for _, g := range a.groups {
		if len(g.draining) != 0 {
			return false
		}
	}
	return true
}

func (a *Arbiter) currentRotationPermissionLocked(kind rotationPermissionKind) rotationPermission {
	g := a.groups[a.config.DefaultGroup]
	return rotationPermission{kind: kind, native: g.native, group: g.generation,
		projection: a.encryption.generation, sequence: a.rotation.sequence}
}

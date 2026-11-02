package raftadmission

import (
	"context"
	"crypto/rand"
	"math"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

// RotationRequest owns the original caller's generation while planning or queued.
// It cannot be used as proposal authority or renewed after native invalidation.
type RotationRequest struct {
	owner *Arbiter
	op    *Operation
	ctx   context.Context
}

func (r *RotationRequest) Context() context.Context { return r.ctx }
func (r *RotationRequest) CancelAndJoin()           { r.op.CancelAndJoin() }

func (a *Arbiter) CaptureRotationRequest(ctx context.Context) (*RotationRequest, error) {
	if !validExternalRotationContext(ctx) {
		return nil, errors.WithStack(ErrClosed)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stageLocked(a.config.DefaultGroup) != peersession.DataReady || !a.rotationReadyLocked() || a.rotation.responsibility || a.rotation.active != nil {
		return nil, errors.WithStack(ErrClosed)
	}
	owned, op, err := a.newOperationLocked(ctx, Recovery, id, nil, nil)
	if err != nil {
		return nil, err
	}
	return &RotationRequest{owner: a, op: op, ctx: owned}, nil
}

func validExternalRotationContext(ctx context.Context) bool {
	return ctx != nil && ctx.Err() == nil && operationFrom(ctx) == nil && peersession.CapturedReceipt(ctx) == nil
}

// BeginRotationFromRequest transfers the captured owner under A, then joins the
// cancelled planning tree outside A before exposing the private mutation lane.
func (a *Arbiter) BeginRotationFromRequest(request *RotationRequest, recipe raftbootstrap.Digest) (*RotationRun, error) {
	if request == nil || request.owner != a || recipe == (raftbootstrap.Digest{}) {
		return nil, errors.WithStack(ErrClosed)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, errors.WithStack(err)
	}
	a.mu.Lock()
	if a.rotation.sequence == math.MaxUint64 {
		a.latchFatalLocked(ErrClosed)
	}
	if !a.rotationRequestCurrentLocked(request) {
		a.mu.Unlock()
		return nil, errors.WithStack(ErrStale)
	}
	run, err := a.beginRotationLocked(request.op.parent, recipe, id)
	a.mu.Unlock()
	request.CancelAndJoin()
	if err != nil {
		return nil, err
	}
	if run.Context().Err() != nil {
		return nil, errors.Join(ErrStale, run.Finish(run.Context().Err()))
	}
	return run, nil
}

func (a *Arbiter) rotationRequestCurrentLocked(request *RotationRequest) bool {
	if !a.operationLiveLocked(request.op) || !a.canBeginRotationLocked() || a.rotation.responsibility {
		return false
	}
	for group, generation := range request.op.snapshot {
		if a.groups[group] == nil || a.groups[group].generation != generation {
			return false
		}
	}
	return true
}

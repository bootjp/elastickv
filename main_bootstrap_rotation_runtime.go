package main

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const bootstrapRotationAttemptTimeout = 5 * time.Second

type bootstrapRotationRuntimeConfig struct {
	admission    *raftadmission.Arbiter
	group        uint64
	requested    bool
	barrier      func(context.Context) (uint64, error)
	observe      func(context.Context) (bootstrapEncryptionProjection, error)
	plan         func(context.Context) (*bootstrapRotationRecipe, error)
	planExternal func(context.Context, *pb.RotateDEKRequest) (*bootstrapRotationRecipe, error)
	authorize    func(*bootstrapRotationRecipe) error
	refresh      func() error
}

type bootstrapRotationRuntime struct {
	config           bootstrapRotationRuntimeConfig
	recipe           *bootstrapRotationRecipe
	superseded       bool
	external         bool
	previousRecipe   *bootstrapRotationRecipe
	previousExternal bool
	requests         chan bootstrapExternalRotationCall
	done             chan struct{}
}

type bootstrapRotationObservation struct {
	status     raftadmission.RotationStatus
	ticket     raftadmission.EncryptionTicket
	projection bootstrapEncryptionProjection
	receipt    bootstrapRotationReceipt
	reconciled bool
	planned    *bootstrapRotationRecipe
}

func newBootstrapRotationRuntime(config bootstrapRotationRuntimeConfig) (*bootstrapRotationRuntime, error) {
	if config.admission == nil || config.barrier == nil || config.observe == nil ||
		config.plan == nil || config.authorize == nil || config.refresh == nil {
		return nil, errors.New("protected rotation requires complete receiver-owned wiring")
	}
	return &bootstrapRotationRuntime{config: config, requests: make(chan bootstrapExternalRotationCall, 1), done: make(chan struct{})}, nil
}

// One goroutine serializes recipe ownership. All disk, wrapper, RPC and join
// work happens outside A. Native mutation closes permission before callbacks.
func (r *bootstrapRotationRuntime) Run(ctx context.Context) error {
	defer close(r.done)
	ticker := time.NewTicker(bootstrapReadinessRetryInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if _, err := r.config.admission.RotationStatus(); err != nil {
			return errors.WithStack(err)
		}
		attempt, cancel := context.WithTimeout(ctx, bootstrapRotationAttemptTimeout)
		err := r.step(attempt)
		cancel()
		if err != nil && !bootstrapRotationTransient(err) {
			r.config.admission.LatchFatal(err)
			return errors.Wrap(err, "protected rotation terminal failure")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case call := <-r.requests:
			r.handleExternal(ctx, call)
		}
	}
}

func bootstrapRotationTransient(err error) bool {
	if bootstrapRotationInvalidEvidence(err) {
		return false
	}
	return errors.Is(err, raftadmission.ErrClosed) || errors.Is(err, raftadmission.ErrStale) ||
		errors.Is(err, errBootstrapEncryptionProjectionPending) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kv.ErrTransactionOutcomeUnknown) ||
		errors.Is(err, raftengine.ErrNotLeader) || errors.Is(err, raftengine.ErrLeadershipLost) ||
		retryableEncryptionRotateOnStartupError(err)
}

func bootstrapRotationInvalidEvidence(err error) bool {
	return errors.Is(err, errBootstrapRotationInvalidEvidence) || errors.Is(err, encryption.ErrLocalEpochRollback) || errors.Is(err, encryption.ErrRegistryValueMalformed)
}

func (r *bootstrapRotationRuntime) step(ctx context.Context) error {
	status, err := r.config.admission.RotationStatus()
	if err != nil {
		return errors.Wrap(err, "read protected rotation state")
	}
	if r.config.admission.LocalPermissionReady() {
		return nil
	}
	observation, err := r.observe(ctx, status)
	if err != nil {
		return err
	}
	if observation.planned != nil {
		r.recipe = observation.planned
	}
	if observation.reconciled && observation.receipt.Outcome == bootstrapRotationSuperseded {
		r.superseded = true
	}
	if err := r.config.admission.PublishEncryption(observation.ticket, observation.projection.digest()); err != nil {
		return errors.WithStack(err)
	}
	if err := r.resolvePrevious(observation); err != nil {
		return err
	}
	return r.advance(ctx, observation)
}

func (r *bootstrapRotationRuntime) observe(ctx context.Context, state raftadmission.RotationStatus) (out bootstrapRotationObservation, err error) {
	out.status = state
	out.ticket, err = r.config.admission.EncryptionTicket()
	if err != nil {
		return out, errors.WithStack(err)
	}
	owned, finish, err := r.config.admission.Enter(ctx, raftadmission.Recovery, r.config.group)
	if err != nil {
		return out, errors.WithStack(err)
	}
	returned := false
	defer func() {
		if !returned {
			_ = finish(raftadmission.ErrClosed)
			return
		}
		validated := finish(err)
		err = errors.Join(err, validated)
	}()
	err = r.collect(owned, &out)
	returned = true
	return out, err
}

func (r *bootstrapRotationRuntime) collect(ctx context.Context, out *bootstrapRotationObservation) error {
	if _, err := r.config.barrier(ctx); err != nil {
		return err
	}
	if err := r.prepareRecipe(ctx, out); err != nil {
		return err
	}
	var err error
	out.projection, err = r.config.observe(ctx)
	return err
}

func (r *bootstrapRotationRuntime) prepareRecipe(ctx context.Context, out *bootstrapRotationObservation) error {
	var err error
	if r.recipe != nil {
		out.receipt, err = r.recipe.Reconcile(ctx)
		out.reconciled = err == nil && out.receipt.Outcome != bootstrapRotationPending
		if err != nil {
			return err
		}
	}
	if r.recipe == nil && r.config.requested && out.status.Native.Role == raftengine.StateLeader {
		out.planned, err = r.config.plan(ctx)
		if err != nil {
			return err
		}
		if out.planned == nil {
			return errors.New("protected rotation planner returned no recipe")
		}
		// Repair current wrappers before asking the projection verifier to
		// accept them. A repair may revoke this owner; the next attempt then
		// observes the settled image with a new ticket, never refreshes it.
		if _, err = out.planned.Reconcile(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *bootstrapRotationRuntime) resolvePrevious(observation bootstrapRotationObservation) error {
	state := observation.status
	if !state.Responsibility || (state.Native.Role != raftengine.StateFollower && !r.superseded && !r.external) {
		return nil
	}
	if state.Recipe != (raftbootstrap.Digest{}) && (!observation.reconciled || observation.receipt.RecipeDigest != state.Recipe) {
		return errors.Wrap(raftadmission.ErrClosed, "owned rotation outcome still unresolved")
	}
	if err := r.config.admission.ResolveRotationResponsibility(observation.ticket, state.Recipe, observation.projection.digest()); err != nil {
		return errors.WithStack(err)
	}
	r.restoreAfterResolution(observation.receipt)
	return nil
}

func (r *bootstrapRotationRuntime) restoreAfterResolution(receipt bootstrapRotationReceipt) {
	if r.external && !r.superseded && !receipt.Complete {
		r.recipe, r.external = r.previousRecipe, r.previousExternal
	} else if r.superseded {
		r.recipe, r.superseded, r.external = nil, false, false
	}
	r.previousRecipe, r.previousExternal = nil, false
}

func (r *bootstrapRotationRuntime) advance(ctx context.Context, observation bootstrapRotationObservation) error {
	switch observation.status.Native.Role {
	case raftengine.StateFollower:
		if err := r.config.admission.PublishFollowerPermission(observation.ticket, observation.projection.digest()); err != nil {
			return errors.WithStack(err)
		}
	case raftengine.StateLeader:
		if r.config.requested || observation.status.Responsibility {
			if err := r.runLeader(ctx, observation.projection.digest()); err != nil {
				return err
			}
		}
	case raftengine.StateUnknown, raftengine.StateCandidate, raftengine.StateShutdown:
		return errors.WithStack(raftadmission.ErrClosed)
	default:
		return errors.WithStack(raftadmission.ErrClosed)
	}
	return r.config.refresh()
}

func (r *bootstrapRotationRuntime) runLeader(ctx context.Context, projection raftbootstrap.Digest) error {
	if r.recipe == nil {
		// A superseded run was resolved in this iteration. Plan only after a
		// new independently fenced observation, not from an old reply.
		return nil
	}
	if err := r.config.authorize(r.recipe); err != nil {
		return err
	}
	run, err := r.config.admission.BeginRotation(ctx, r.recipe.Digest())
	if err != nil {
		return errors.WithStack(err)
	}
	finished := false
	defer func() {
		if !finished {
			_ = run.Finish(raftadmission.ErrClosed)
		}
	}()
	result, attemptErr := r.executeRecipe(run.Context())
	finishErr := run.Finish(attemptErr)
	finished = true
	if finishErr != nil {
		return errors.Join(attemptErr, finishErr)
	}
	if result.Outcome == bootstrapRotationSuperseded {
		r.superseded = true
		return nil
	}
	if !result.Complete || result.RecipeDigest != r.recipe.Digest() {
		return errors.Wrap(raftadmission.ErrClosed, "rotation recipe not completed")
	}
	ticket, err := r.config.admission.EncryptionTicket()
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(r.config.admission.CompleteRotation(run, ticket, projection))
}

func (r *bootstrapRotationRuntime) executeRecipe(ctx context.Context) (bootstrapRotationReceipt, error) {
	if r.external {
		return r.recipe.Reconcile(ctx)
	}
	return r.recipe.Run(ctx)
}

func (r *bootstrapRotationRuntime) Wait(ctx context.Context) error {
	ticker := time.NewTicker(bootstrapReadinessRetryInterval)
	defer ticker.Stop()
	for {
		if _, err := r.config.admission.RotationStatus(); err != nil {
			return errors.WithStack(err)
		}
		if r.config.admission.LocalPermissionReady() {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

package clusterbootstrap

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const bootstrapRotationAttemptTimeout = 5 * time.Second

type BootstrapRotationRuntimeConfig struct {
	Admission    *raftadmission.Arbiter
	Group        uint64
	Requested    bool
	Barrier      func(context.Context) (uint64, error)
	Observe      func(context.Context) (encryptionwiring.BootstrapEncryptionProjection, error)
	Plan         func(context.Context) (*BootstrapRotationRecipe, error)
	PlanExternal func(context.Context, *pb.RotateDEKRequest) (*BootstrapRotationRecipe, error)
	Authorize    func(*BootstrapRotationRecipe) error
	Refresh      func() error
}

type BootstrapRotationRuntime struct {
	Config           BootstrapRotationRuntimeConfig
	Recipe           *BootstrapRotationRecipe
	superseded       bool
	external         bool
	previousRecipe   *BootstrapRotationRecipe
	previousExternal bool
	Requests         chan BootstrapExternalRotationCall
	Done             chan struct{}
}

type bootstrapRotationObservation struct {
	status     raftadmission.RotationStatus
	ticket     raftadmission.EncryptionTicket
	projection encryptionwiring.BootstrapEncryptionProjection
	receipt    bootstrapRotationReceipt
	reconciled bool
	planned    *BootstrapRotationRecipe
}

func NewBootstrapRotationRuntime(config BootstrapRotationRuntimeConfig) (*BootstrapRotationRuntime, error) {
	if config.Admission == nil || config.Barrier == nil || config.Observe == nil ||
		config.Plan == nil || config.Authorize == nil || config.Refresh == nil {
		return nil, errors.New("protected rotation requires complete receiver-owned wiring")
	}
	return &BootstrapRotationRuntime{Config: config, Requests: make(chan BootstrapExternalRotationCall, 1), Done: make(chan struct{})}, nil
}

// One goroutine serializes recipe ownership. All disk, wrapper, RPC and join
// work happens outside A. Native mutation closes permission before callbacks.
func (r *BootstrapRotationRuntime) Run(ctx context.Context) error {
	defer close(r.Done)
	ticker := time.NewTicker(BootstrapReadinessRetryInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		if _, err := r.Config.Admission.RotationStatus(); err != nil {
			return errors.WithStack(err)
		}
		attempt, cancel := context.WithTimeout(ctx, bootstrapRotationAttemptTimeout)
		err := r.Step(attempt)
		cancel()
		if err != nil && !BootstrapRotationTransient(err) {
			r.Config.Admission.LatchFatal(err)
			return errors.Wrap(err, "protected rotation terminal failure")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case call := <-r.Requests:
			r.HandleExternal(ctx, call)
		}
	}
}

func BootstrapRotationTransient(err error) bool {
	if bootstrapRotationInvalidEvidence(err) {
		return false
	}
	return errors.Is(err, raftadmission.ErrClosed) || errors.Is(err, raftadmission.ErrStale) ||
		errors.Is(err, encryptionwiring.ErrBootstrapEncryptionProjectionPending) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kv.ErrTransactionOutcomeUnknown) ||
		errors.Is(err, raftengine.ErrNotLeader) || errors.Is(err, raftengine.ErrLeadershipLost) ||
		encryptionwiring.RetryableRotateOnStartupError(err)
}

func bootstrapRotationInvalidEvidence(err error) bool {
	return errors.Is(err, ErrBootstrapRotationInvalidEvidence) || errors.Is(err, encryption.ErrLocalEpochRollback) || errors.Is(err, encryption.ErrRegistryValueMalformed)
}

func (r *BootstrapRotationRuntime) Step(ctx context.Context) error {
	status, err := r.Config.Admission.RotationStatus()
	if err != nil {
		return errors.Wrap(err, "read protected rotation state")
	}
	if r.Config.Admission.LocalPermissionReady() {
		return nil
	}
	observation, err := r.observe(ctx, status)
	if err != nil {
		return err
	}
	if observation.planned != nil {
		r.Recipe = observation.planned
	}
	if observation.reconciled && observation.receipt.Outcome == BootstrapRotationSuperseded {
		r.superseded = true
	}
	if err := r.Config.Admission.PublishEncryption(observation.ticket, observation.projection.Digest()); err != nil {
		return errors.WithStack(err)
	}
	if err := r.resolvePrevious(observation); err != nil {
		return err
	}
	return r.advance(ctx, observation)
}

func (r *BootstrapRotationRuntime) observe(ctx context.Context, state raftadmission.RotationStatus) (out bootstrapRotationObservation, err error) {
	out.status = state
	out.ticket, err = r.Config.Admission.EncryptionTicket()
	if err != nil {
		return out, errors.WithStack(err)
	}
	owned, finish, err := r.Config.Admission.Enter(ctx, raftadmission.Recovery, r.Config.Group)
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

func (r *BootstrapRotationRuntime) collect(ctx context.Context, out *bootstrapRotationObservation) error {
	if _, err := r.Config.Barrier(ctx); err != nil {
		return err
	}
	if err := r.prepareRecipe(ctx, out); err != nil {
		return err
	}
	var err error
	out.projection, err = r.Config.Observe(ctx)
	return err
}

func (r *BootstrapRotationRuntime) prepareRecipe(ctx context.Context, out *bootstrapRotationObservation) error {
	var err error
	if r.Recipe != nil {
		out.receipt, err = r.Recipe.Reconcile(ctx)
		out.reconciled = err == nil && out.receipt.Outcome != BootstrapRotationPending
		if err != nil {
			return err
		}
	}
	if r.Recipe == nil && r.Config.Requested && out.status.Native.Role == raftengine.StateLeader {
		out.planned, err = r.Config.Plan(ctx)
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

func (r *BootstrapRotationRuntime) resolvePrevious(observation bootstrapRotationObservation) error {
	state := observation.status
	if !state.Responsibility || (state.Native.Role != raftengine.StateFollower && !r.superseded && !r.external) {
		return nil
	}
	if state.Recipe != (raftbootstrap.Digest{}) && (!observation.reconciled || observation.receipt.RecipeDigest != state.Recipe) {
		return errors.Wrap(raftadmission.ErrClosed, "owned rotation outcome still unresolved")
	}
	if err := r.Config.Admission.ResolveRotationResponsibility(observation.ticket, state.Recipe, observation.projection.Digest()); err != nil {
		return errors.WithStack(err)
	}
	r.restoreAfterResolution(observation.receipt)
	return nil
}

func (r *BootstrapRotationRuntime) restoreAfterResolution(receipt bootstrapRotationReceipt) {
	if r.external && !r.superseded && !receipt.Complete {
		r.Recipe, r.external = r.previousRecipe, r.previousExternal
	} else if r.superseded {
		r.Recipe, r.superseded, r.external = nil, false, false
	}
	r.previousRecipe, r.previousExternal = nil, false
}

func (r *BootstrapRotationRuntime) advance(ctx context.Context, observation bootstrapRotationObservation) error {
	switch observation.status.Native.Role {
	case raftengine.StateFollower:
		if err := r.Config.Admission.PublishFollowerPermission(observation.ticket, observation.projection.Digest()); err != nil {
			return errors.WithStack(err)
		}
	case raftengine.StateLeader:
		if r.Config.Requested || observation.status.Responsibility {
			if err := r.runLeader(ctx, observation.projection.Digest()); err != nil {
				return err
			}
		}
	case raftengine.StateUnknown, raftengine.StateCandidate, raftengine.StateShutdown:
		return errors.WithStack(raftadmission.ErrClosed)
	default:
		return errors.WithStack(raftadmission.ErrClosed)
	}
	return r.Config.Refresh()
}

func (r *BootstrapRotationRuntime) runLeader(ctx context.Context, projection raftbootstrap.Digest) error {
	if r.Recipe == nil {
		// A superseded run was resolved in this iteration. Plan only after a
		// new independently fenced observation, not from an old reply.
		return nil
	}
	if err := r.Config.Authorize(r.Recipe); err != nil {
		return err
	}
	run, err := r.Config.Admission.BeginRotation(ctx, r.Recipe.Digest())
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
	if result.Outcome == BootstrapRotationSuperseded {
		r.superseded = true
		return nil
	}
	if !result.Complete || result.RecipeDigest != r.Recipe.Digest() {
		return errors.Wrap(raftadmission.ErrClosed, "rotation recipe not completed")
	}
	ticket, err := r.Config.Admission.EncryptionTicket()
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(r.Config.Admission.CompleteRotation(run, ticket, projection))
}

func (r *BootstrapRotationRuntime) executeRecipe(ctx context.Context) (bootstrapRotationReceipt, error) {
	if r.external {
		return r.Recipe.Reconcile(ctx)
	}
	return r.Recipe.Run(ctx)
}

func (r *BootstrapRotationRuntime) Wait(ctx context.Context) error {
	ticker := time.NewTicker(BootstrapReadinessRetryInterval)
	defer ticker.Stop()
	for {
		if _, err := r.Config.Admission.RotationStatus(); err != nil {
			return errors.WithStack(err)
		}
		if r.Config.Admission.LocalPermissionReady() {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

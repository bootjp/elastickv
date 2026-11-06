package kv

import (
	"context"
	"errors"
)

// AdmissionPurpose identifies the receiver operation, not a Raft entry type.
// Data and Maintenance require DataReady; HLC must not depend on the catalog.
type AdmissionPurpose uint8

const (
	AdmissionData AdmissionPurpose = iota
	AdmissionAllocator
	AdmissionHLC
	AdmissionMaintenance
)

// OperationAdmission is installed at startup, before any operation starts.
// It must validate and inherit an existing private handle in ctx without
// refreshing it, or mint one for this receiver operation if none exists.
// The returned context must retain caller cancellation and reach proposals.
// finish is called exactly once after synchronous work and child joins; it
// must revoke ownership and validate the result, preserving unknown outcomes.
// No handle or catalog policy is implemented by kv itself. Nil is legacy mode.
type OperationAdmission func(ctx context.Context, group uint64, purpose AdmissionPurpose) (ownedCtx context.Context, finish func(error) error, err error)

func admitOperation(ctx context.Context, hook OperationAdmission, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
	if hook == nil {
		return ctx, func(err error) error { return err }, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	owned, finish, err := hook(ctx, group, purpose)
	if err != nil {
		if finish != nil {
			err = finishOperation(err, finish)
		}
		return nil, nil, err
	}
	if owned == nil || finish == nil {
		err = errors.New("operation admission returned incomplete ownership")
		if finish != nil {
			err = errors.Join(err, finish(err))
		}
		return nil, nil, err
	}
	return owned, func(err error) error {
		return finishOperation(err, finish)
	}, nil
}

func finishOperation(err error, finish func(error) error) error {
	validated := finish(err)
	if validated == nil {
		return err
	}
	if err == nil {
		// Work already returned success. Revocation cannot prove that its
		// submitted effects were absent, even if the result is now withheld.
		return errors.Join(ErrTransactionOutcomeUnknown, validated)
	}
	if errors.Is(validated, err) {
		return validated
	}
	return errors.Join(err, validated)
}

func admittedOperation[T any](ctx context.Context, hook OperationAdmission, group uint64, purpose AdmissionPurpose, run func(context.Context) (T, error)) (result T, err error) {
	if hook == nil {
		return run(ctx)
	}
	var zero T
	owned, finish, err := admitOperation(ctx, hook, group, purpose)
	if err != nil {
		return zero, err
	}
	completed := false
	defer func() {
		if !completed {
			err = errors.New("operation did not return")
		}
		err = finish(err)
		if err != nil {
			result = zero
		}
	}()
	result, err = run(owned)
	completed = true
	return result, err
}

// WithOperationAdmission installs the single-group coordinator's startup hook.
func WithOperationAdmission(hook OperationAdmission) CoordinatorOption {
	return func(c *Coordinate) { c.operationAdmission = hook }
}

// SetOperationAdmission must be called before starting receiver operations.
func (c *Coordinate) SetOperationAdmission(hook OperationAdmission) {
	c.operationAdmission = hook
}

// WithOperationAdmission configures the coordinator before receiver startup.
func (c *ShardedCoordinator) WithOperationAdmission(hook OperationAdmission) *ShardedCoordinator {
	c.operationAdmission = hook
	return c
}

// LockResolverOption configures the resolver before its worker starts.
type LockResolverOption func(*LockResolver)

func WithLockResolverContext(ctx context.Context) LockResolverOption {
	return func(lr *LockResolver) {
		if ctx != nil {
			lr.rootContext = ctx
		}
	}
}

func WithLockResolverOperationAdmission(hook OperationAdmission) LockResolverOption {
	return func(lr *LockResolver) { lr.operationAdmission = hook }
}

func WithFSMCompactorOperationAdmission(hook OperationAdmission) FSMCompactorOption {
	return func(c *FSMCompactor) { c.operationAdmission = hook }
}

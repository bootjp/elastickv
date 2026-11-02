package etcd

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

func (e *Engine) ownedNativeProposal(ctx context.Context, data []byte) (proposalRequest, error) {
	payload := append([]byte(nil), data...)
	var digest [32]byte
	if e.nativeAdmission != nil {
		digest = sha256.Sum256(payload)
		bound, err := e.nativeAdmission.BindProposal(ctx, e.groupID, payload)
		if err != nil {
			return proposalRequest{}, errors.WithStack(err)
		}
		if bound == nil {
			return proposalRequest{}, errors.WithStack(raftengine.ErrNativeAdmissionDenied)
		}
		ctx = bound
	}
	return proposalRequest{ctx: ctx, id: e.nextID(), payload: payload, payloadDigest: digest,
		done: make(chan proposalResult, 1)}, nil
}

// Only the engine loop may call native mutation helpers. Scope callbacks are
// memory-only; persistence, FSM application and owner callbacks stay outside A.
func (e *Engine) nativeState() raftengine.NativeState {
	basic := e.rawNode.BasicStatus()
	return raftengine.NativeState{Term: basic.GetTerm(), Role: translateState(basic.RaftState),
		LeaderID: basic.Lead, ConfigurationIndex: e.nativeConfigurationIndex}
}

func (e *Engine) applyNativeConfChange(change raftpb.ConfChangeI, index uint64) (*raftpb.ConfState, error) {
	var conf *raftpb.ConfState
	err := e.withNative(context.Background(), nil, func() error {
		conf = e.rawNode.ApplyConfChange(change)
		e.nativeConfigurationIndex = index
		return nil
	})
	return conf, err
}

func (e *Engine) withNative(ctx context.Context, request *raftengine.NativeRequest, action func() error) error {
	return e.mutateNative(ctx, request, false, action)
}

func (e *Engine) mutateNative(ctx context.Context, request *raftengine.NativeRequest, invalidate bool, action func() error) error {
	if e.nativeAdmission == nil {
		return action()
	}
	var before, after raftengine.NativeState
	attempted := false
	invalidated := false
	err := e.nativeAdmission.WithNative(ctx, e.groupID, func(scope raftengine.NativeScope) error {
		before = e.nativeState()
		scope.Observe(before)
		defer func() {
			after = e.nativeState()
			scope.Observe(after)
		}()
		if request != nil {
			if err := scope.Check(ctx, *request); err != nil {
				return errors.WithStack(err)
			}
		}
		if invalidate {
			scope.Invalidate()
			invalidated = true
		}
		attempted = true
		return action()
	})
	if before != after || invalidated {
		// Observe already closed A's authority; owner cancellation follows only
		// after A and any receipt/owner mutex have been released.
		e.invalidateAdmission()
	}
	if err != nil && !attempted {
		return fmt.Errorf("%w: %w", raftengine.ErrNativeAdmissionDenied, err)
	}
	return errors.WithStack(err)
}

func validateNativeAdmissionConfig(cfg OpenConfig) error {
	if cfg.NativeIdentity == nil && cfg.RecoveredAdmission == nil && cfg.AdmissionChanged == nil && cfg.BeforeAdmissionChange == nil {
		return nil
	}
	if cfg.NativeIdentity == nil {
		return errors.WithStack(ErrWALIdentity)
	}
	if cfg.NativeAdmission == nil || cfg.GroupID != cfg.NativeIdentity.Identity.Group {
		return errors.New("protected native engine requires admission and matching group identity")
	}
	return nil
}

func (e *Engine) invalidateNativeAdmission() {
	if e.nativeAdmission == nil {
		return
	}
	// Cancellation must not prevent fail-closed invalidation.
	_ = e.nativeAdmission.WithNative(context.Background(), e.groupID, func(scope raftengine.NativeScope) error {
		scope.Invalidate()
		return nil
	})
}

func (e *Engine) validateNativeResult(ctx context.Context) error {
	if e.nativeAdmission == nil {
		return nil
	}
	if err := e.nativeAdmission.ValidateResult(ctx); err != nil {
		// Submission may already be durable. Preserve the existing ambiguous
		// outcome sentinel, never turn invalidation into a definitive rejection.
		return errors.Wrapf(raftengine.ErrLeadershipLost, "native result admission expired: %v", err)
	}
	return nil
}

func (e *Engine) nativeProposalResult(ctx context.Context, result proposalResult) (*raftengine.ProposalResult, error) {
	if result.err != nil {
		return nil, result.err
	}
	if err := e.validateNativeResult(ctx); err != nil {
		return nil, err
	}
	return result.result, nil
}

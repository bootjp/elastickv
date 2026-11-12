package main

import (
	"context"

	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type bootstrapExternalRotationCall struct {
	intent  *raftadmission.RotationRequest
	request *pb.RotateDEKRequest
	result  chan bootstrapExternalRotationResult
	cancel  context.CancelFunc
}

type bootstrapExternalRotationResult struct {
	response *pb.RotateDEKResponse
	err      error
}

func (r *bootstrapRotationRuntime) RotateDEK(ctx context.Context, request *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
	if r.config.planExternal == nil {
		return nil, bootstrapRotationRPCError(codes.Unavailable, "external rotation is not configured")
	}
	if _, err := bootstrapRotationPayloadFromRequest(request); err != nil {
		return nil, bootstrapRotationRPCError(codes.InvalidArgument, "invalid external rotation request")
	}
	ctx, cancel := context.WithTimeout(ctx, bootstrapRotationAttemptTimeout)
	defer cancel()
	intent, err := r.config.admission.CaptureRotationRequest(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer intent.CancelAndJoin()
	call := bootstrapExternalRotationCall{intent: intent, request: cloneBootstrapRotationRequest(request), result: make(chan bootstrapExternalRotationResult, 1), cancel: cancel}
	select {
	case r.requests <- call:
	case <-intent.Context().Done():
		return nil, errors.WithStack(intent.Context().Err())
	default:
		return nil, bootstrapRotationRPCError(codes.ResourceExhausted, "rotation queue is full")
	}
	// Keep the RPC owner alive through transfer and all child joins. Caller
	// cancellation propagates via its original context, never a renewed owner.
	select {
	case result := <-call.result:
		return result.response, result.err
	case <-r.done:
		return nil, errors.WithStack(raftadmission.ErrClosed)
	}
}

func (r *bootstrapRotationRuntime) handleExternal(ctx context.Context, call bootstrapExternalRotationCall) {
	stop := context.AfterFunc(ctx, call.cancel)
	defer stop()
	response, err := r.executeExternal(call)
	if bootstrapRotationInvalidEvidence(err) {
		r.config.admission.LatchFatal(err)
	}
	call.result <- bootstrapExternalRotationResult{response: response, err: err}
}

func (r *bootstrapRotationRuntime) executeExternal(call bootstrapExternalRotationCall) (*pb.RotateDEKResponse, error) {
	recipe, projection, err := r.planExternal(call)
	if err != nil {
		return nil, err
	}
	run, err := r.config.admission.BeginRotationFromRequest(call.intent, recipe.Digest())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.previousRecipe, r.previousExternal = r.recipe, r.external
	r.recipe, r.external, r.superseded = recipe, true, false
	return r.finishExternal(run, recipe, projection)
}

func (r *bootstrapRotationRuntime) planExternal(call bootstrapExternalRotationCall) (*bootstrapRotationRecipe, encryptionwiring.BootstrapEncryptionProjection, error) {
	var projection encryptionwiring.BootstrapEncryptionProjection
	ctx := call.intent.Context()
	if err := ctx.Err(); err != nil {
		return nil, projection, errors.WithStack(err)
	}
	recipe, err := r.config.planExternal(ctx, call.request)
	if err != nil {
		if errors.Is(err, errBootstrapExternalRotationRequest) {
			return nil, projection, bootstrapRotationRPCError(codes.InvalidArgument, "invalid external rotation key, identity or epoch")
		}
		return nil, projection, errors.WithStack(err)
	}
	projection, err = r.config.observe(ctx)
	if err != nil {
		return nil, projection, err
	}
	if err := r.config.authorize(recipe); err != nil {
		return nil, projection, err
	}
	return recipe, projection, nil
}

func (r *bootstrapRotationRuntime) finishExternal(run *raftadmission.RotationRun, recipe *bootstrapRotationRecipe, projection encryptionwiring.BootstrapEncryptionProjection) (*pb.RotateDEKResponse, error) {
	finished := false
	defer func() {
		if !finished {
			_ = run.Finish(raftadmission.ErrClosed)
		}
	}()
	// Retain even an unknown outcome, but only reconcile it read-only on later
	// ticks. A cancelled external command is never proposed by the supervisor.
	receipt, attemptErr := recipe.Run(run.Context())
	finishErr := run.Finish(attemptErr)
	finished = true
	if attemptErr != nil || finishErr != nil {
		return nil, bootstrapExternalRotationUnknown(errors.Join(attemptErr, finishErr))
	}
	if !receipt.Complete || receipt.RecipeDigest != recipe.Digest() {
		return nil, bootstrapExternalRotationUnknown(raftadmission.ErrStale)
	}
	ticket, err := r.config.admission.EncryptionTicket()
	if err == nil {
		err = r.config.admission.CompleteRotation(run, ticket, projection.Digest())
	}
	if err != nil {
		return nil, bootstrapExternalRotationUnknown(err)
	}
	r.previousRecipe, r.previousExternal = nil, false
	return &pb.RotateDEKResponse{AppliedIndex: receipt.AppliedIndex}, nil
}

// Preserve the local cause without allowing an embedded Canceled gRPC status
// from a self-revoking apply to misrepresent an already attempted operation.
type bootstrapExternalRotationOutcomeUnknown struct{ cause error }

func (e *bootstrapExternalRotationOutcomeUnknown) Error() string {
	return "external rotation outcome unknown"
}
func (e *bootstrapExternalRotationOutcomeUnknown) Unwrap() error {
	return errors.Join(kv.ErrTransactionOutcomeUnknown, e.cause)
}
func (e *bootstrapExternalRotationOutcomeUnknown) GRPCStatus() *status.Status {
	return status.New(codes.Unknown, e.Error())
}
func bootstrapExternalRotationUnknown(cause error) error {
	return errors.WithStack(&bootstrapExternalRotationOutcomeUnknown{cause: cause})
}

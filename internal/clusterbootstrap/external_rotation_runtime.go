package clusterbootstrap

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

type BootstrapExternalRotationCall struct {
	Intent  *raftadmission.RotationRequest
	Request *pb.RotateDEKRequest
	Result  chan BootstrapExternalRotationResult
	Cancel  context.CancelFunc
}

type BootstrapExternalRotationResult struct {
	Response *pb.RotateDEKResponse
	Err      error
}

func (r *BootstrapRotationRuntime) RotateDEK(ctx context.Context, request *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
	if r.Config.PlanExternal == nil {
		return nil, bootstrapRotationRPCError(codes.Unavailable, "external rotation is not configured")
	}
	if _, err := BootstrapRotationPayloadFromRequest(request); err != nil {
		return nil, bootstrapRotationRPCError(codes.InvalidArgument, "invalid external rotation request")
	}
	ctx, cancel := context.WithTimeout(ctx, bootstrapRotationAttemptTimeout)
	defer cancel()
	intent, err := r.Config.Admission.CaptureRotationRequest(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer intent.CancelAndJoin()
	call := BootstrapExternalRotationCall{Intent: intent, Request: CloneBootstrapRotationRequest(request), Result: make(chan BootstrapExternalRotationResult, 1), Cancel: cancel}
	select {
	case r.Requests <- call:
	case <-intent.Context().Done():
		return nil, errors.WithStack(intent.Context().Err())
	default:
		return nil, bootstrapRotationRPCError(codes.ResourceExhausted, "rotation queue is full")
	}
	// Keep the RPC owner alive through transfer and all child joins. Caller
	// cancellation propagates via its original context, never a renewed owner.
	select {
	case result := <-call.Result:
		return result.Response, result.Err
	case <-r.Done:
		return nil, errors.WithStack(raftadmission.ErrClosed)
	}
}

func (r *BootstrapRotationRuntime) HandleExternal(ctx context.Context, call BootstrapExternalRotationCall) {
	stop := context.AfterFunc(ctx, call.Cancel)
	defer stop()
	response, err := r.ExecuteExternal(call)
	if bootstrapRotationInvalidEvidence(err) {
		r.Config.Admission.LatchFatal(err)
	}
	call.Result <- BootstrapExternalRotationResult{Response: response, Err: err}
}

func (r *BootstrapRotationRuntime) ExecuteExternal(call BootstrapExternalRotationCall) (*pb.RotateDEKResponse, error) {
	recipe, projection, err := r.planExternal(call)
	if err != nil {
		return nil, err
	}
	run, err := r.Config.Admission.BeginRotationFromRequest(call.Intent, recipe.Digest())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.previousRecipe, r.previousExternal = r.Recipe, r.external
	r.Recipe, r.external, r.superseded = recipe, true, false
	return r.finishExternal(run, recipe, projection)
}

func (r *BootstrapRotationRuntime) planExternal(call BootstrapExternalRotationCall) (*BootstrapRotationRecipe, encryptionwiring.BootstrapEncryptionProjection, error) {
	var projection encryptionwiring.BootstrapEncryptionProjection
	ctx := call.Intent.Context()
	if err := ctx.Err(); err != nil {
		return nil, projection, errors.WithStack(err)
	}
	recipe, err := r.Config.PlanExternal(ctx, call.Request)
	if err != nil {
		if errors.Is(err, ErrBootstrapExternalRotationRequest) {
			return nil, projection, bootstrapRotationRPCError(codes.InvalidArgument, "invalid external rotation key, identity or epoch")
		}
		return nil, projection, errors.WithStack(err)
	}
	projection, err = r.Config.Observe(ctx)
	if err != nil {
		return nil, projection, err
	}
	if err := r.Config.Authorize(recipe); err != nil {
		return nil, projection, err
	}
	return recipe, projection, nil
}

func (r *BootstrapRotationRuntime) finishExternal(run *raftadmission.RotationRun, recipe *BootstrapRotationRecipe, projection encryptionwiring.BootstrapEncryptionProjection) (*pb.RotateDEKResponse, error) {
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
		return nil, BootstrapExternalRotationUnknown(errors.Join(attemptErr, finishErr))
	}
	if !receipt.Complete || receipt.RecipeDigest != recipe.Digest() {
		return nil, BootstrapExternalRotationUnknown(raftadmission.ErrStale)
	}
	ticket, err := r.Config.Admission.EncryptionTicket()
	if err == nil {
		err = r.Config.Admission.CompleteRotation(run, ticket, projection.Digest())
	}
	if err != nil {
		return nil, BootstrapExternalRotationUnknown(err)
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
func BootstrapExternalRotationUnknown(cause error) error {
	return errors.WithStack(&bootstrapExternalRotationOutcomeUnknown{cause: cause})
}

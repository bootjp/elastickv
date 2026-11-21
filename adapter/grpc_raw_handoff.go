package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/semaphore"
)

const (
	rawHandoffResponseReserve = 100 * time.Millisecond
	rawHandoffReserveFraction = 4
	rawHandoffMaxWait         = 2 * time.Second
	rawHandoffRetryDelay      = 10 * time.Millisecond
	rawHandoffMaxWaiters      = 256
	rawHandoffMaxBytes        = 8 << 20
	rawHandoffWaiterOverhead  = 256
)

var rawHandoffWaiters = make(chan struct{}, rawHandoffMaxWaiters)
var rawHandoffBytes = semaphore.NewWeighted(rawHandoffMaxBytes)

type onlineHandoffReader interface {
	OnlineMigrationHandoffActive(context.Context, []byte) bool
}

// A retry is permitted only for a complete single-point dispatch with proof
// that it did not mutate data. Unknown outcomes must return to the caller.
func (r *GRPCServer) dispatchRawHandoff(ctx context.Context, key []byte, payloadBytes int, build func() (*kv.OperationGroup[kv.OP], error)) (*kv.CoordinateResponse, error) {
	dispatch := func(attempt context.Context) (*kv.CoordinateResponse, error) {
		request, err := build()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		response, err := r.coordinator.Dispatch(attempt, request)
		return response, errors.WithStack(err)
	}
	response, err := dispatch(ctx)
	if !r.rawHandoffCanWait(ctx, key, err) {
		return response, err
	}
	release, acquired := reserveRawHandoff(payloadBytes)
	if !acquired {
		return response, err
	}
	defer release()
	wait := rawHandoffWaitBudget(ctx)
	if wait <= 0 {
		return response, err
	}
	waiting, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	return waitRawHandoff(waiting, dispatch, response, err)
}

func (r *GRPCServer) rawHandoffCanWait(ctx context.Context, key []byte, err error) bool {
	reader, ok := r.store.(onlineHandoffReader)
	return ok && retryableRawHandoff(err) && reader.OnlineMigrationHandoffActive(ctx, key)
}

func reserveRawHandoff(payloadBytes int) (func(), bool) {
	select {
	case rawHandoffWaiters <- struct{}{}:
	default:
		return nil, false
	}
	weight := int64(payloadBytes) + rawHandoffWaiterOverhead
	if !rawHandoffBytes.TryAcquire(weight) {
		<-rawHandoffWaiters
		return nil, false
	}
	return func() { rawHandoffBytes.Release(weight); <-rawHandoffWaiters }, true
}

func waitRawHandoff(ctx context.Context, dispatch func(context.Context) (*kv.CoordinateResponse, error), response *kv.CoordinateResponse, last error) (*kv.CoordinateResponse, error) {
	retry := time.NewTimer(rawHandoffRetryDelay)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return response, last
		case <-retry.C:
			// Cancellation before submission retains the last proof. Once a
			// new attempt starts, return its ACTUAL outcome, including Unknown.
			if ctx.Err() != nil {
				return response, last
			}
			response, last = dispatch(ctx)
			if !retryableRawHandoff(last) {
				return response, last
			}
			retry.Reset(rawHandoffRetryDelay)
		}
	}
}

func retryableRawHandoff(err error) bool {
	return kv.IsMigrationAdmissionRejection(err) && (kv.IsMutationNotAdmitted(err) || kv.IsMutationApplyRejected(err))
}

// Leave response time after a proof-preserving wait. The client deadline can
// otherwise expire while a known noncommit result is still waiting to return.
func rawHandoffWaitBudget(ctx context.Context) time.Duration {
	wait := rawHandoffMaxWait
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		reserve := min(rawHandoffResponseReserve, remaining/rawHandoffReserveFraction)
		wait = min(wait, remaining-reserve)
	}
	return wait
}

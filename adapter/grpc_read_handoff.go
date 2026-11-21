package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const rawReadHandoffMaxWait = 250 * time.Millisecond

// Each attempt owns and closes its read request. Keeping a pin across the
// retry would prevent cutover from draining the preceding ownership epoch.
func retryRawReadHandoff(ctx context.Context, req *pb.RawGetRequest, attempt func(context.Context) (*pb.RawGetResponse, error)) (*pb.RawGetResponse, error) {
	response, err := attempt(ctx)
	if !retryableRawReadHandoff(err) || req.GetGroupId() != 0 || req.GetReadRouteVersion() != 0 {
		return response, err
	}
	waiting, cancel := context.WithTimeout(ctx, min(rawReadHandoffMaxWait, rawHandoffWaitBudget(ctx)))
	defer cancel()
	timer := time.NewTimer(rawHandoffRetryDelay)
	defer timer.Stop()
	for {
		select {
		case <-waiting.Done():
			return nil, rawReadHandoffTimeout(ctx)
		case <-timer.C:
			response, err = attempt(waiting)
			if waiting.Err() != nil {
				return nil, rawReadHandoffTimeout(ctx)
			}
			if !retryableRawReadHandoff(err) {
				return response, err
			}
			timer.Reset(rawHandoffRetryDelay)
		}
	}
}
func retryableRawReadHandoff(err error) bool {
	return errors.Is(err, kv.ErrRouteCutoverPending) && !mixedMutationDispatchFailure(err)
}

func rawReadHandoffTimeout(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(status.FromContextError(err).Err())
	}
	return errors.WithStack(status.Error(codes.Unavailable, "Route unavailable"))
}

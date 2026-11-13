package adapter

import (
	"context"
	"io"
	"net"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/status"
)

// A failed mutation dispatch without terminal evidence cannot prove noncommit.
func isIndeterminateMutationDispatchFailure(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := status.FromError(err); ok {
		return true
	}
	var netErr net.Error
	return errors.Is(err, raftengine.ErrLeadershipLost) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr)
}

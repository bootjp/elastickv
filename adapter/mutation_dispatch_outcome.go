package adapter

import (
	"context"
	"io"
	"net"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/status"
)

var secondaryDispatchFailure = errors.WithSecondaryError(io.EOF, io.EOF)

// A failed mutation dispatch without terminal evidence cannot prove noncommit.
func isIndeterminateMutationDispatchFailure(err error) bool {
	if err == nil {
		return false
	}
	if mixedMutationDispatchFailure(err) {
		return true
	}
	if _, ok := status.FromError(err); ok {
		return true
	}
	var netErr net.Error
	return errors.Is(err, raftengine.ErrLeadershipLost) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr)
}

// Secondary errors are hidden from errors.Is; branched errors can also carry
// an outcome unrelated to their first recognizable sentinel. Neither proves a
// single terminal decision for this logical dispatch.
func mixedMutationDispatchFailure(err error) bool {
	if errors.HasType(err, secondaryDispatchFailure) {
		return true
	}
	for current := err; current != nil; current = errors.UnwrapOnce(current) {
		if _, branched := current.(interface{ Unwrap() []error }); branched {
			return true
		}
	}
	return false
}

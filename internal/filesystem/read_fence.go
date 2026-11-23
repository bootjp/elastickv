package filesystem

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrReadFenceUnavailable marks a failed read barrier while its leader or
// transport was unavailable. No filesystem mutation was submitted by the read.
var ErrReadFenceUnavailable = errors.New("filesystem read fence unavailable")

func markReadFenceError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, raftengine.ErrNotLeader) ||
		errors.Is(err, raftengine.ErrLeadershipLost) || errors.Is(err, raftengine.ErrLeadershipTransferInProgress) {
		return errors.Join(err, ErrReadFenceUnavailable)
	}
	// Read the original wire status. The group and filesystem wrappers add
	// context to Error(), which must not change the legacy status match.
	var wire interface{ GRPCStatus() *status.Status }
	if errors.As(err, &wire) {
		st := wire.GRPCStatus()
		if st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded ||
			(st.Code() == codes.Unknown && st.Message() == "not leader") {
			return errors.Join(err, ErrReadFenceUnavailable)
		}
	}
	return err
}

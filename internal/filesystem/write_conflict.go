package filesystem

import (
	"strings"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsWriteConflict reports whether err is an OCC write conflict, either the
// typed store.ErrWriteConflict raised by a local commit or the wire envelope a
// follower receives when its proposal was forwarded to the leader: Forward
// returns transaction failures as a gRPC status, which strips the typed error
// chain and leaves only the "key: <key>: write conflict" message.
//
// Callers that retry on it must be idempotent: a lost forwarding response can
// mean the write was already applied.
func IsWriteConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, store.ErrWriteConflict) {
		return true
	}
	return isWireWriteConflict(err)
}

// isRootInitializationConflict reports whether root initialization may retry
// err. Retrying is safe because initializeRootOnce re-reads the root inode
// before proposing and returns nil when another node already created it, so a
// lost forwarding response for an applied write converges on the next pass.
func isRootInitializationConflict(err error) bool {
	return IsWriteConflict(err)
}

func isWireWriteConflict(err error) bool {
	type grpcStatusCarrier interface {
		GRPCStatus() *status.Status
	}
	var carrier grpcStatusCarrier
	if !errors.As(err, &carrier) {
		return false
	}
	st := carrier.GRPCStatus()
	if st == nil || (st.Code() != codes.Unknown && st.Code() != codes.Aborted) {
		return false
	}
	msg := st.Message()
	return strings.HasPrefix(msg, "key: ") &&
		strings.HasSuffix(msg, ": "+store.ErrWriteConflict.Error())
}

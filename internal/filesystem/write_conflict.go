package filesystem

import (
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsWriteConflict reports a definitive OCC rejection. The leader proxy
// reconstructs store.ErrWriteConflict from a typed forwarded Aborted status.
// Textual gRPC errors cannot certify that an attempt did not commit.
func IsWriteConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return false
	}
	return errors.Is(err, store.ErrWriteConflict)
}

// isRootInitializationConflict reports whether root initialization may retry
// err. Retrying is safe because initializeRootOnce re-reads the root inode
// before proposing and returns nil when another node already created it, so a
// lost forwarding response for an applied write converges on the next pass.
// This exception is specific to root creation and never changes the general
// IsWriteConflict classification of an uncertain transaction.
func isRootInitializationConflict(err error) bool {
	return IsWriteConflict(err) || isWireWriteConflict(err)
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

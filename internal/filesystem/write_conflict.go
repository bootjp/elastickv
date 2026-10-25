package filesystem

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
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

// Root initialization may retry only a definitive conflict. An unknown reply
// is handled by a separate read-only check of the root inode.
func isRootInitializationConflict(err error) bool {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return false
	}
	return IsWriteConflict(err) || kv.IsForwardWriteConflict(err)
}

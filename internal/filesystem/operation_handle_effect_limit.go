package filesystem

import (
	"math"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/cockroachdb/errors"
)

const minHandleOperationEffectLimit = 256

// handleLeaseEffectLimit reserves the encoded key bytes that the operation
// may publish. The client ID uses ordered-byte escaping in RefKey, so its
// encoded length must be measured rather than inferred from len(clientID).
func handleLeaseEffectLimit(inode uint64, clientID []byte, withFence bool) (uint32, error) {
	keyBytes := uint64(len(fskeys.RefKey(inode, clientID, 0)))
	if withFence {
		keyBytes += uint64(len(fskeys.RefFenceKey(inode)))
	}
	limit := max(uint64(minHandleOperationEffectLimit), keyBytes)
	if limit > math.MaxUint32 {
		return 0, errors.WithStack(ErrOperationQuota)
	}
	return uint32(limit), nil
}

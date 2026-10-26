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
	keys := [][]byte{fskeys.RefKey(inode, clientID, 0)}
	if withFence {
		keys = append(keys, fskeys.RefFenceKey(inode))
	}
	return operationEffectLimitForKeys(minHandleOperationEffectLimit, keys...)
}

func operationEffectLimitForKeys(minimum uint32, keys ...[]byte) (uint32, error) {
	keyBytes, err := operationEffectLimitWithSlack(0, keys...)
	if err != nil {
		return 0, err
	}
	return max(minimum, keyBytes), nil
}

func operationEffectLimitWithSlack(slack uint32, keys ...[]byte) (uint32, error) {
	limit := uint64(slack)
	for _, key := range keys {
		limit += uint64(len(key))
	}
	if limit > math.MaxUint32 {
		return 0, errors.WithStack(ErrOperationQuota)
	}
	return uint32(limit), nil
}

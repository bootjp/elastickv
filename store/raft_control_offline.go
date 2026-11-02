package store

import (
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

// EncodeOfflinePlainControlMetadata encodes the physical store value for an
// explicitly unencrypted offline transformation. It performs no writes and
// grants no metadata/admission authority. Online and encrypted paths must keep
// using StageRaftControlMetadata and its indexed transaction/encryption gates.
func EncodeOfflinePlainControlMetadata(value []byte) ([]byte, error) {
	if len(value) == 0 || len(value) > raftbootstrap.MaxFileSize {
		return nil, errors.WithStack(ErrRaftControlProjectionKey)
	}
	return encodeValue(value, false, 0, 0), nil
}

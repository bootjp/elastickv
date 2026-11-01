package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

// WALIdentity is independently provisioned, never inferred from a policy cache.
type WALIdentity struct {
	Identity       raftbootstrap.Identity
	NodeID         uint64
	BaselineDigest raftbootstrap.Digest
}

var ErrWALIdentity = errors.New("native WAL identity invalid or mismatched")

const walIdentitySize = 8 + 16 + 8 + 8 + 32 + sha256.Size

func validateNativeIdentityConfig(cfg OpenConfig) error {
	if _, err := encodeWALIdentity(cfg.NativeIdentity); err != nil {
		return err
	}
	if cfg.NativeIdentity != nil && cfg.NativeIdentity.NodeID != cfg.NodeID {
		return ErrWALIdentity
	}
	return nil
}

func encodeWALIdentity(id *WALIdentity) ([]byte, error) {
	if id == nil {
		return nil, nil
	}
	if id.Identity.Cluster == (raftbootstrap.UUID{}) || id.NodeID == 0 || id.BaselineDigest == (raftbootstrap.Digest{}) {
		return nil, ErrWALIdentity
	}
	b := make([]byte, walIdentitySize)
	copy(b, "EKVWID01")
	copy(b[8:24], id.Identity.Cluster[:])
	binary.BigEndian.PutUint64(b[24:32], id.Identity.Group)
	binary.BigEndian.PutUint64(b[32:40], id.NodeID)
	copy(b[40:72], id.BaselineDigest[:])
	digest := sha256.Sum256(b[:72])
	copy(b[72:], digest[:])
	return b, nil
}

func verifyWALIdentity(metadata []byte, expected *WALIdentity) error {
	if expected == nil {
		// A bound WAL cannot be downgraded by removing operator configuration.
		// Recognize the frame even when its payload is truncated or corrupt.
		if bytes.HasPrefix(metadata, []byte("EKVWID01")) {
			return ErrWALIdentity
		}
		return nil
	}
	canonical, err := encodeWALIdentity(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(metadata, canonical) {
		return ErrWALIdentity
	}
	return nil
}

func copyWALIdentity(ids []*WALIdentity) *WALIdentity {
	if len(ids) == 0 || ids[0] == nil {
		return nil
	}
	id := *ids[0]
	return &id
}

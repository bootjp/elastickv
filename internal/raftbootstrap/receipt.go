package raftbootstrap

import (
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

const (
	entryIdentitySize = 16 + 8 + 8 + 8 + 32
	receiptKeyPrefix  = "R/"
	receiptKeySize    = len(receiptKeyPrefix) + 16 + 8 + 8
)

// EntryIdentity is the durable identity of one committed protected entry.
// Digest covers the entry type and canonical plaintext payload. Group zero is
// valid; index and term zero are not committed-entry identities.
type EntryIdentity struct {
	Cluster UUID
	Group   uint64
	Index   uint64
	Term    uint64
	Digest  Digest
}

func EncodeEntryIdentity(identity EntryIdentity) ([]byte, error) {
	if !validEntryIdentity(identity) {
		return nil, errors.WithStack(ErrInvalid)
	}
	out := make([]byte, entryIdentitySize)
	copy(out[:16], identity.Cluster[:])
	binary.BigEndian.PutUint64(out[16:24], identity.Group)
	binary.BigEndian.PutUint64(out[24:32], identity.Index)
	binary.BigEndian.PutUint64(out[32:40], identity.Term)
	copy(out[40:], identity.Digest[:])
	return out, nil
}

func ParseEntryIdentity(data []byte) (EntryIdentity, error) {
	if len(data) != entryIdentitySize {
		return EntryIdentity{}, errors.WithStack(ErrInvalid)
	}
	var identity EntryIdentity
	copy(identity.Cluster[:], data[:16])
	identity.Group = binary.BigEndian.Uint64(data[16:24])
	identity.Index = binary.BigEndian.Uint64(data[24:32])
	identity.Term = binary.BigEndian.Uint64(data[32:40])
	copy(identity.Digest[:], data[40:])
	if !validEntryIdentity(identity) {
		return EntryIdentity{}, errors.WithStack(ErrInvalid)
	}
	return identity, nil
}

// ReceiptKey returns the protected metadata key for an entry slot. Term and
// digest deliberately remain in the value identity: a replay at the same
// cluster/group/index must find the same key and compare the complete identity,
// so a conflicting committed entry is detected as corruption rather than a
// second receipt.
func ReceiptKey(identity EntryIdentity) ([]byte, error) {
	if !validEntryIdentity(identity) {
		return nil, errors.WithStack(ErrInvalid)
	}
	out := make([]byte, receiptKeySize)
	copy(out, receiptKeyPrefix)
	copy(out[len(receiptKeyPrefix):], identity.Cluster[:])
	offset := len(receiptKeyPrefix) + len(identity.Cluster)
	binary.BigEndian.PutUint64(out[offset:offset+8], identity.Group)
	binary.BigEndian.PutUint64(out[offset+8:], identity.Index)
	return out, nil
}

func validEntryIdentity(identity EntryIdentity) bool {
	return identity.Cluster != (UUID{}) && identity.Index != 0 && identity.Term != 0 && identity.Digest != (Digest{})
}

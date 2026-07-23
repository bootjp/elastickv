//nolint:dupl // Hash and Set helpers are intentionally parallel implementations for distinct types.
package store

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// Set wide-column key layout:
//
//	Base Metadata: !st|meta|<userKeyLen(4)><userKey>                 → [Len(8)][ExpireAtMs(8)]
//	Member Key:    !st|mem|<userKeyLen(4)><userKey><member>           → (empty value)
//	Delta Key:     !st|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)> → [LenDelta(8)]
const (
	SetMetaPrefix      = "!st|meta|"
	SetMemberPrefix    = "!st|mem|"
	SetMetaDeltaPrefix = "!st|meta|d|"

	// setMetaLegacySizeBytes is the pre-inline-TTL binary size of a SetMeta.
	setMetaLegacySizeBytes = 8
	// setMetaSizeBytes is the current binary size of a SetMeta (Len + ExpireAtMs).
	setMetaSizeBytes = 16
	// setMetaDeltaSizeBytes is the fixed binary size of a SetMetaDelta (one int64).
	setMetaDeltaSizeBytes = 8
)

// SetMeta is the base metadata for a set collection.
type SetMeta struct {
	Len      int64
	ExpireAt uint64
}

// SetMetaDelta holds a signed change in member count.
type SetMetaDelta struct {
	LenDelta int64
}

// MarshalSetMeta encodes SetMeta into a fixed 16-byte binary format.
func MarshalSetMeta(m SetMeta) []byte {
	buf := make([]byte, setMetaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(m.Len)) //nolint:gosec
	binary.BigEndian.PutUint64(buf[8:16], m.ExpireAt)
	return buf
}

// UnmarshalSetMeta decodes SetMeta from the legacy 8-byte or current 16-byte binary format.
func UnmarshalSetMeta(b []byte) (SetMeta, error) {
	if len(b) != setMetaLegacySizeBytes && len(b) != setMetaSizeBytes {
		return SetMeta{}, errors.WithStack(errors.Newf("invalid set meta length: %d", len(b)))
	}
	meta := SetMeta{Len: int64(binary.BigEndian.Uint64(b[0:8]))} //nolint:gosec
	if len(b) == setMetaSizeBytes {
		meta.ExpireAt = binary.BigEndian.Uint64(b[8:16])
	}
	return meta, nil
}

// MarshalSetMetaDelta encodes SetMetaDelta into a fixed 8-byte binary format.
func MarshalSetMetaDelta(d SetMetaDelta) []byte {
	buf := make([]byte, setMetaDeltaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(d.LenDelta)) //nolint:gosec
	return buf
}

// UnmarshalSetMetaDelta decodes SetMetaDelta from the fixed 8-byte binary format.
func UnmarshalSetMetaDelta(b []byte) (SetMetaDelta, error) {
	if len(b) != setMetaDeltaSizeBytes {
		return SetMetaDelta{}, errors.WithStack(errors.Newf("invalid set meta delta length: %d", len(b)))
	}
	return SetMetaDelta{LenDelta: int64(binary.BigEndian.Uint64(b))}, nil //nolint:gosec
}

// SetMetaKey builds the metadata key for a set.
func SetMetaKey(userKey []byte) []byte {
	buf := make([]byte, 0, len(SetMetaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, SetMetaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// SetMemberKey builds the per-member key for a set member.
func SetMemberKey(userKey, member []byte) []byte {
	buf := make([]byte, 0, len(SetMemberPrefix)+wideColKeyLenSize+len(userKey)+len(member))
	buf = append(buf, SetMemberPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	buf = append(buf, member...)
	return buf
}

// SetMemberScanPrefix returns the prefix to scan all members of a set.
func SetMemberScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(SetMemberPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, SetMemberPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// ExtractSetMemberName extracts the member name from a set member key.
func ExtractSetMemberName(key, userKey []byte) []byte {
	prefix := SetMemberScanPrefix(userKey)
	if !bytes.HasPrefix(key, prefix) {
		return nil
	}
	return key[len(prefix):]
}

// SetMetaDeltaKey builds the delta key for a set metadata change.
func SetMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	buf := make([]byte, 0, len(SetMetaDeltaPrefix)+wideColKeyLenSize+len(userKey)+deltaKeyTSSize+deltaKeySeqSize)
	buf = append(buf, SetMetaDeltaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	buf = append(buf, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	buf = append(buf, seq[:]...)
	return buf
}

// SetMetaDeltaScanPrefix returns the prefix to scan all delta keys for a set.
func SetMetaDeltaScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(SetMetaDeltaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, SetMetaDeltaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// IsSetMetaKey reports whether the key is a set metadata key.
func IsSetMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(SetMetaPrefix))
}

// IsSetMemberKey reports whether the key is a set member key.
func IsSetMemberKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(SetMemberPrefix))
}

// IsSetMetaDeltaKey reports whether the key is a set metadata delta key.
func IsSetMetaDeltaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(SetMetaDeltaPrefix))
}

// ExtractSetUserKeyFromMeta extracts the logical user key from a set meta key.
func ExtractSetUserKeyFromMeta(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(SetMetaPrefix), 0, true)
}

// ExtractSetUserKeyFromMember extracts the logical user key from a set member key.
func ExtractSetUserKeyFromMember(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(SetMemberPrefix), 0, false)
}

// ExtractSetUserKeyFromDelta extracts the logical user key from a set delta key.
func ExtractSetUserKeyFromDelta(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(SetMetaDeltaPrefix), deltaKeyTSSize+deltaKeySeqSize, true)
}

// ExtractSetUserKeyFromDeltaScanPrefix extracts the user key from a set
// metadata delta scan start/prefix.
func ExtractSetUserKeyFromDeltaScanPrefix(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(SetMetaDeltaPrefix), 0, false)
}

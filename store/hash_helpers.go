//nolint:dupl // Hash and Set helpers are intentionally parallel implementations for distinct types.
package store

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// Hash wide-column key layout:
//
//	Base Metadata: !hs|meta|<userKeyLen(4)><userKey>               → [Len(8)][ExpireAtMs(8)]
//	Field Key:     !hs|fld|<userKeyLen(4)><userKey><fieldName>     → field value bytes
//	Delta Key:     !hs|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)> → [LenDelta(8)]
const (
	HashMetaPrefix      = "!hs|meta|"
	HashFieldPrefix     = "!hs|fld|"
	HashMetaDeltaPrefix = "!hs|meta|d|"

	// hashMetaLegacySizeBytes is the pre-inline-TTL binary size of a HashMeta.
	hashMetaLegacySizeBytes = 8
	// hashMetaSizeBytes is the current binary size of a HashMeta (Len + ExpireAtMs).
	hashMetaSizeBytes = 16
	// hashMetaDeltaSizeBytes is the fixed binary size of a HashMetaDelta (one int64).
	hashMetaDeltaSizeBytes = 8
)

// HashMeta is the base metadata for a hash collection.
type HashMeta struct {
	Len      int64
	ExpireAt uint64
}

// HashMetaDelta holds a signed change in field count.
type HashMetaDelta struct {
	LenDelta int64
}

// MarshalHashMeta encodes HashMeta into a fixed 16-byte binary format.
func MarshalHashMeta(m HashMeta) []byte {
	buf := make([]byte, hashMetaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(m.Len)) //nolint:gosec
	binary.BigEndian.PutUint64(buf[8:16], m.ExpireAt)
	return buf
}

// UnmarshalHashMeta decodes HashMeta from the legacy 8-byte or current 16-byte binary format.
func UnmarshalHashMeta(b []byte) (HashMeta, error) {
	if len(b) != hashMetaLegacySizeBytes && len(b) != hashMetaSizeBytes {
		return HashMeta{}, errors.WithStack(errors.Newf("invalid hash meta length: %d", len(b)))
	}
	meta := HashMeta{Len: int64(binary.BigEndian.Uint64(b[0:8]))} //nolint:gosec
	if len(b) == hashMetaSizeBytes {
		meta.ExpireAt = binary.BigEndian.Uint64(b[8:16])
	}
	return meta, nil
}

// MarshalHashMetaDelta encodes HashMetaDelta into a fixed 8-byte binary format.
func MarshalHashMetaDelta(d HashMetaDelta) []byte {
	buf := make([]byte, hashMetaDeltaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(d.LenDelta)) //nolint:gosec
	return buf
}

// UnmarshalHashMetaDelta decodes HashMetaDelta from the fixed 8-byte binary format.
func UnmarshalHashMetaDelta(b []byte) (HashMetaDelta, error) {
	if len(b) != hashMetaDeltaSizeBytes {
		return HashMetaDelta{}, errors.WithStack(errors.Newf("invalid hash meta delta length: %d", len(b)))
	}
	return HashMetaDelta{LenDelta: int64(binary.BigEndian.Uint64(b))}, nil //nolint:gosec
}

// HashMetaKey builds the metadata key for a hash.
func HashMetaKey(userKey []byte) []byte {
	buf := make([]byte, 0, len(HashMetaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, HashMetaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// HashFieldKey builds the per-field key for a hash field.
func HashFieldKey(userKey, fieldName []byte) []byte {
	buf := make([]byte, 0, len(HashFieldPrefix)+wideColKeyLenSize+len(userKey)+len(fieldName))
	buf = append(buf, HashFieldPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	buf = append(buf, fieldName...)
	return buf
}

// HashFieldScanPrefix returns the prefix to scan all fields of a hash.
func HashFieldScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(HashFieldPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, HashFieldPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// ExtractHashFieldName extracts the field name from a hash field key.
func ExtractHashFieldName(key, userKey []byte) []byte {
	prefix := HashFieldScanPrefix(userKey)
	if !bytes.HasPrefix(key, prefix) {
		return nil
	}
	return key[len(prefix):]
}

// HashMetaDeltaKey builds the delta key for a hash metadata change.
func HashMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	buf := make([]byte, 0, len(HashMetaDeltaPrefix)+wideColKeyLenSize+len(userKey)+deltaKeyTSSize+deltaKeySeqSize)
	buf = append(buf, HashMetaDeltaPrefix...)
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

// HashMetaDeltaScanPrefix returns the prefix to scan all delta keys for a hash.
func HashMetaDeltaScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(HashMetaDeltaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, HashMetaDeltaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// IsHashMetaKey reports whether the key is a hash metadata key.
func IsHashMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(HashMetaPrefix))
}

// IsHashFieldKey reports whether the key is a hash field key.
func IsHashFieldKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(HashFieldPrefix))
}

// IsHashMetaDeltaKey reports whether the key is a hash metadata delta key.
func IsHashMetaDeltaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(HashMetaDeltaPrefix))
}

// ExtractHashUserKeyFromMeta extracts the logical user key from a hash meta key.
func ExtractHashUserKeyFromMeta(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(HashMetaPrefix), 0, true)
}

// ExtractHashUserKeyFromField extracts the logical user key from a hash field key.
func ExtractHashUserKeyFromField(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(HashFieldPrefix), 0, false)
}

// ExtractHashUserKeyFromDelta extracts the logical user key from a hash delta key.
func ExtractHashUserKeyFromDelta(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(HashMetaDeltaPrefix), deltaKeyTSSize+deltaKeySeqSize, true)
}

// ExtractHashUserKeyFromDeltaScanPrefix extracts the user key from a hash
// metadata delta scan start/prefix.
func ExtractHashUserKeyFromDeltaScanPrefix(key []byte) []byte {
	return extractWideColumnUserKey(key, []byte(HashMetaDeltaPrefix), 0, false)
}

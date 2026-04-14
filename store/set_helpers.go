//nolint:dupl // Hash and Set helpers are intentionally parallel implementations for distinct types.
package store

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// Set wide-column key layout:
//
//	Base Metadata: !st|meta|b|<userKeyLen(4)><userKey>                 → [Len(8)]
//	Member Key:    !st|mem|<userKeyLen(4)><userKey><member>             → (empty value)
//	Delta Key:     !st|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)> → [LenDelta(8)]
//
// Note: the base prefix ("!st|meta|b|") and delta prefix ("!st|meta|d|") differ
// at the trailing discriminator byte ('b' vs 'd') so that IsSetMetaKey and
// IsSetMetaDeltaKey produce unambiguous results without ordering constraints.
const (
	SetMetaPrefix      = "!st|meta|b|"
	SetMemberPrefix    = "!st|mem|"
	SetMetaDeltaPrefix = "!st|meta|d|"

	// setMetaSizeBytes is the fixed binary size of a SetMeta or SetMetaDelta (one int64).
	setMetaSizeBytes = 8
)

// SetMeta is the base metadata for a set collection.
type SetMeta struct {
	Len int64
}

// SetMetaDelta holds a signed change in member count.
type SetMetaDelta struct {
	LenDelta int64
}

// MarshalSetMeta encodes SetMeta into a fixed 8-byte binary format.
func MarshalSetMeta(m SetMeta) []byte {
	buf := make([]byte, setMetaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(m.Len)) //nolint:gosec
	return buf
}

// UnmarshalSetMeta decodes SetMeta from the fixed 8-byte binary format.
func UnmarshalSetMeta(b []byte) (SetMeta, error) {
	if len(b) != setMetaSizeBytes {
		return SetMeta{}, errors.WithStack(errors.Newf("invalid set meta length: %d", len(b)))
	}
	return SetMeta{Len: int64(binary.BigEndian.Uint64(b))}, nil //nolint:gosec
}

// MarshalSetMetaDelta encodes SetMetaDelta into a fixed 8-byte binary format.
func MarshalSetMetaDelta(d SetMetaDelta) []byte {
	buf := make([]byte, setMetaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(d.LenDelta)) //nolint:gosec
	return buf
}

// UnmarshalSetMetaDelta decodes SetMetaDelta from the fixed 8-byte binary format.
func UnmarshalSetMetaDelta(b []byte) (SetMetaDelta, error) {
	if len(b) != setMetaSizeBytes {
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
	trimmed := bytes.TrimPrefix(key, []byte(SetMetaPrefix))
	if len(trimmed) < wideColKeyLenSize {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if uint32(len(trimmed)) < uint32(wideColKeyLenSize)+ukLen { //nolint:gosec // wideColKeyLenSize fits in uint32
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

// ExtractSetUserKeyFromMember extracts the logical user key from a set member key.
func ExtractSetUserKeyFromMember(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, []byte(SetMemberPrefix))
	if len(trimmed) < wideColKeyLenSize {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if uint32(len(trimmed)) < uint32(wideColKeyLenSize)+ukLen { //nolint:gosec // wideColKeyLenSize fits in uint32
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

// ExtractSetUserKeyFromDelta extracts the logical user key from a set delta key.
func ExtractSetUserKeyFromDelta(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, []byte(SetMetaDeltaPrefix))
	minLen := wideColKeyLenSize + deltaKeyTSSize + deltaKeySeqSize
	if len(trimmed) < minLen {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if uint32(len(trimmed)) < uint32(wideColKeyLenSize)+ukLen+uint32(deltaKeyTSSize+deltaKeySeqSize) { //nolint:gosec // constants fit in uint32
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

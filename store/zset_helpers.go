package store

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
)

// ZSet wide-column key layout:
//
//	Base Metadata: !zs|meta|b|<userKeyLen(4)><userKey>                               → [Len(8)]
//	Member Key:    !zs|mem|<userKeyLen(4)><userKey><member>                           → [Score(8)] IEEE 754
//	Score Index:   !zs|scr|<userKeyLen(4)><userKey><sortableScore(8)><member>         → (empty)
//	Delta Key:     !zs|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)>     → [LenDelta(8)]
//
// Note: the base prefix ("!zs|meta|b|") and delta prefix ("!zs|meta|d|") differ
// at the trailing discriminator byte ('b' vs 'd') so that IsZSetMetaKey and
// IsZSetMetaDeltaKey produce unambiguous results without ordering constraints.
const (
	ZSetMetaPrefix      = "!zs|meta|b|"
	ZSetMemberPrefix    = "!zs|mem|"
	ZSetScorePrefix     = "!zs|scr|"
	ZSetMetaDeltaPrefix = "!zs|meta|d|"

	// zsetMetaSizeBytes is the fixed binary size of a ZSetMeta, ZSetMetaDelta, or ZSet score (one int64/float64).
	zsetMetaSizeBytes = 8
)

// ZSetMeta is the base metadata for a sorted set collection.
type ZSetMeta struct {
	Len int64
}

// ZSetMetaDelta holds a signed change in member count.
type ZSetMetaDelta struct {
	LenDelta int64
}

// MarshalZSetMeta encodes ZSetMeta into a fixed 8-byte binary format.
func MarshalZSetMeta(m ZSetMeta) []byte {
	buf := make([]byte, zsetMetaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(m.Len)) //nolint:gosec
	return buf
}

// UnmarshalZSetMeta decodes ZSetMeta from the fixed 8-byte binary format.
func UnmarshalZSetMeta(b []byte) (ZSetMeta, error) {
	if len(b) != zsetMetaSizeBytes {
		return ZSetMeta{}, errors.WithStack(errors.Newf("invalid zset meta length: %d", len(b)))
	}
	return ZSetMeta{Len: int64(binary.BigEndian.Uint64(b))}, nil //nolint:gosec
}

// MarshalZSetMetaDelta encodes ZSetMetaDelta into a fixed 8-byte binary format.
func MarshalZSetMetaDelta(d ZSetMetaDelta) []byte {
	buf := make([]byte, zsetMetaSizeBytes)
	binary.BigEndian.PutUint64(buf, uint64(d.LenDelta)) //nolint:gosec
	return buf
}

// UnmarshalZSetMetaDelta decodes ZSetMetaDelta from the fixed 8-byte binary format.
func UnmarshalZSetMetaDelta(b []byte) (ZSetMetaDelta, error) {
	if len(b) != zsetMetaSizeBytes {
		return ZSetMetaDelta{}, errors.WithStack(errors.Newf("invalid zset meta delta length: %d", len(b)))
	}
	return ZSetMetaDelta{LenDelta: int64(binary.BigEndian.Uint64(b))}, nil //nolint:gosec
}

// MarshalZSetScore encodes a float64 score in IEEE 754 big-endian format.
func MarshalZSetScore(score float64) []byte {
	buf := make([]byte, zsetMetaSizeBytes)
	binary.BigEndian.PutUint64(buf, math.Float64bits(score))
	return buf
}

// UnmarshalZSetScore decodes a float64 score from IEEE 754 big-endian format.
func UnmarshalZSetScore(b []byte) (float64, error) {
	if len(b) != zsetMetaSizeBytes {
		return 0, errors.WithStack(errors.Newf("invalid zset score length: %d", len(b)))
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
}

// EncodeSortableFloat64 encodes a float64 into a sortable 8-byte representation.
// For positive floats: XOR the sign bit to make them sort above negative.
// For negative floats: XOR all bits to reverse the order.
// This produces a byte sequence that sorts correctly with standard byte comparison.
func EncodeSortableFloat64(f float64) [8]byte {
	bits := math.Float64bits(f)
	if bits>>63 == 0 {
		// Positive (or +0): flip the sign bit
		bits ^= 0x8000000000000000
	} else {
		// Negative (or -0): flip all bits
		bits ^= 0xFFFFFFFFFFFFFFFF
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	return b
}

// DecodeSortableFloat64 decodes a sortable 8-byte representation back to float64.
func DecodeSortableFloat64(b [8]byte) float64 {
	bits := binary.BigEndian.Uint64(b[:])
	if bits>>63 == 1 {
		// Was positive: flip only the sign bit back
		bits ^= 0x8000000000000000
	} else {
		// Was negative: flip all bits back
		bits ^= 0xFFFFFFFFFFFFFFFF
	}
	return math.Float64frombits(bits)
}

// ZSetMetaKey builds the metadata key for a sorted set.
func ZSetMetaKey(userKey []byte) []byte {
	buf := make([]byte, 0, len(ZSetMetaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, ZSetMetaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// ZSetMemberKey builds the per-member key storing the score for a sorted set.
func ZSetMemberKey(userKey, member []byte) []byte {
	buf := make([]byte, 0, len(ZSetMemberPrefix)+wideColKeyLenSize+len(userKey)+len(member))
	buf = append(buf, ZSetMemberPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	buf = append(buf, member...)
	return buf
}

// ZSetMemberScanPrefix returns the prefix to scan all members of a sorted set.
func ZSetMemberScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(ZSetMemberPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, ZSetMemberPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// ExtractZSetMemberName extracts the member name from a zset member key.
func ExtractZSetMemberName(key, userKey []byte) []byte {
	prefix := ZSetMemberScanPrefix(userKey)
	if !bytes.HasPrefix(key, prefix) {
		return nil
	}
	return key[len(prefix):]
}

// ZSetScoreKey builds the score index key for a sorted set entry.
// Layout: !zs|scr|<userKeyLen(4)><userKey><sortableScore(8)><member>
func ZSetScoreKey(userKey []byte, score float64, member []byte) []byte {
	sortable := EncodeSortableFloat64(score)
	buf := make([]byte, 0, len(ZSetScorePrefix)+wideColKeyLenSize+len(userKey)+zsetMetaSizeBytes+len(member))
	buf = append(buf, ZSetScorePrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	buf = append(buf, sortable[:]...)
	buf = append(buf, member...)
	return buf
}

// ZSetScoreScanPrefix returns the prefix to scan all score index keys for a sorted set.
func ZSetScoreScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(ZSetScorePrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, ZSetScorePrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// ZSetScoreRangeScanPrefix returns the prefix for scanning scores in [minScore, maxScore].
func ZSetScoreRangeScanPrefix(userKey []byte, score float64) []byte {
	sortable := EncodeSortableFloat64(score)
	buf := make([]byte, 0, len(ZSetScorePrefix)+wideColKeyLenSize+len(userKey)+zsetMetaSizeBytes)
	buf = append(buf, ZSetScorePrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	buf = append(buf, sortable[:]...)
	return buf
}

// ExtractZSetScoreAndMember extracts the score and member name from a zset score index key.
func ExtractZSetScoreAndMember(key, userKey []byte) (score float64, member []byte, ok bool) {
	prefix := ZSetScoreScanPrefix(userKey)
	if !bytes.HasPrefix(key, prefix) {
		return 0, nil, false
	}
	rest := key[len(prefix):]
	if len(rest) < zsetMetaSizeBytes {
		return 0, nil, false
	}
	var sortable [zsetMetaSizeBytes]byte
	copy(sortable[:], rest[:zsetMetaSizeBytes])
	score = DecodeSortableFloat64(sortable)
	member = rest[zsetMetaSizeBytes:]
	return score, member, true
}

// ZSetMetaDeltaKey builds the delta key for a sorted set metadata change.
func ZSetMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	buf := make([]byte, 0, len(ZSetMetaDeltaPrefix)+wideColKeyLenSize+len(userKey)+deltaKeyTSSize+deltaKeySeqSize)
	buf = append(buf, ZSetMetaDeltaPrefix...)
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

// ZSetMetaDeltaScanPrefix returns the prefix to scan all delta keys for a sorted set.
func ZSetMetaDeltaScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(ZSetMetaDeltaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, ZSetMetaDeltaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// IsZSetMetaKey reports whether the key is a sorted set metadata key.
func IsZSetMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ZSetMetaPrefix))
}

// IsZSetMemberKey reports whether the key is a sorted set member key.
func IsZSetMemberKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ZSetMemberPrefix))
}

// IsZSetScoreKey reports whether the key is a sorted set score index key.
func IsZSetScoreKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ZSetScorePrefix))
}

// IsZSetMetaDeltaKey reports whether the key is a sorted set metadata delta key.
func IsZSetMetaDeltaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ZSetMetaDeltaPrefix))
}

// ExtractZSetUserKeyFromDelta extracts the logical user key from a zset delta key.
func ExtractZSetUserKeyFromDelta(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, []byte(ZSetMetaDeltaPrefix))
	minLen := wideColKeyLenSize + deltaKeyTSSize + deltaKeySeqSize
	if len(trimmed) < minLen {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if ukLen > uint32(len(trimmed)-wideColKeyLenSize-deltaKeyTSSize-deltaKeySeqSize) { //nolint:gosec // minLen check above guarantees non-negative subtraction
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

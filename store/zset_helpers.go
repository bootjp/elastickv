package store

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
)

// Wide-column style ZSet storage using per-member keys with a score index.
//
// Meta key:    !zs|meta|<userKeyLen(4)><userKey>                              → [Len(8)]
// Member key:  !zs|mem|<userKeyLen(4)><userKey><member>                       → [Score(8)]
// Score index: !zs|scr|<userKeyLen(4)><userKey><sortableScore(8)><member>     → [] (empty)
//
// The 4-byte big-endian userKey length prefix prevents ambiguity when one
// userKey is a prefix of another, which is critical for correct prefix scans.

const (
	ZSetMetaPrefix   = "!zs|meta|"
	ZSetMemberPrefix = "!zs|mem|"
	ZSetScorePrefix  = "!zs|scr|"

	zsetMetaBinarySize  = 8
	zsetScoreBinarySize = 8
	zsetUserKeyLenSize  = 4
)

// ZSetMeta stores the cardinality of a sorted set.
type ZSetMeta struct {
	Len int64
}

// ---- Key construction ----

func zsetUserKeyComponent(userKey []byte) []byte {
	buf := make([]byte, zsetUserKeyLenSize+len(userKey))
	binary.BigEndian.PutUint32(buf[:zsetUserKeyLenSize], uint32(len(userKey))) //nolint:gosec // userKey length fits in uint32
	copy(buf[zsetUserKeyLenSize:], userKey)
	return buf
}

// ZSetMetaKey builds the metadata key for a user key.
func ZSetMetaKey(userKey []byte) []byte {
	prefix := []byte(ZSetMetaPrefix)
	comp := zsetUserKeyComponent(userKey)
	buf := make([]byte, 0, len(prefix)+len(comp))
	buf = append(buf, prefix...)
	buf = append(buf, comp...)
	return buf
}

// ZSetMemberKey builds the member→score lookup key.
func ZSetMemberKey(userKey, member []byte) []byte {
	prefix := []byte(ZSetMemberPrefix)
	comp := zsetUserKeyComponent(userKey)
	buf := make([]byte, 0, len(prefix)+len(comp)+len(member))
	buf = append(buf, prefix...)
	buf = append(buf, comp...)
	buf = append(buf, member...)
	return buf
}

// ZSetMemberScanPrefix returns the prefix for scanning all members of a ZSet.
func ZSetMemberScanPrefix(userKey []byte) []byte {
	prefix := []byte(ZSetMemberPrefix)
	comp := zsetUserKeyComponent(userKey)
	buf := make([]byte, 0, len(prefix)+len(comp))
	buf = append(buf, prefix...)
	buf = append(buf, comp...)
	return buf
}

// ZSetScoreKey builds a score-index key for ordered iteration.
func ZSetScoreKey(userKey []byte, score float64, member []byte) []byte {
	prefix := []byte(ZSetScorePrefix)
	comp := zsetUserKeyComponent(userKey)
	var scoreBuf [zsetScoreBinarySize]byte
	EncodeSortableFloat64(scoreBuf[:], score)
	buf := make([]byte, 0, len(prefix)+len(comp)+zsetScoreBinarySize+len(member))
	buf = append(buf, prefix...)
	buf = append(buf, comp...)
	buf = append(buf, scoreBuf[:]...)
	buf = append(buf, member...)
	return buf
}

// ZSetScoreScanPrefix returns the prefix for scanning all score-index entries.
func ZSetScoreScanPrefix(userKey []byte) []byte {
	prefix := []byte(ZSetScorePrefix)
	comp := zsetUserKeyComponent(userKey)
	buf := make([]byte, 0, len(prefix)+len(comp))
	buf = append(buf, prefix...)
	buf = append(buf, comp...)
	return buf
}

// ZSetScoreRangeStart returns the start key (inclusive) for a score range scan.
func ZSetScoreRangeStart(userKey []byte, minScore float64) []byte {
	prefix := []byte(ZSetScorePrefix)
	comp := zsetUserKeyComponent(userKey)
	var scoreBuf [zsetScoreBinarySize]byte
	EncodeSortableFloat64(scoreBuf[:], minScore)
	buf := make([]byte, 0, len(prefix)+len(comp)+zsetScoreBinarySize)
	buf = append(buf, prefix...)
	buf = append(buf, comp...)
	buf = append(buf, scoreBuf[:]...)
	return buf
}

// ZSetScoreRangeEnd returns the end key (exclusive) for a score range scan.
// It returns the prefix-end of the score key with maxScore, so all members
// at maxScore are included.
func ZSetScoreRangeEnd(userKey []byte, maxScore float64) []byte {
	prefix := []byte(ZSetScorePrefix)
	comp := zsetUserKeyComponent(userKey)
	var scoreBuf [zsetScoreBinarySize]byte
	EncodeSortableFloat64(scoreBuf[:], maxScore)
	buf := make([]byte, 0, len(prefix)+len(comp)+zsetScoreBinarySize)
	buf = append(buf, prefix...)
	buf = append(buf, comp...)
	buf = append(buf, scoreBuf[:]...)
	return PrefixEnd(buf)
}

// ---- Sortable float64 encoding ----
// Converts IEEE 754 float64 to a byte sequence that preserves sort order
// when compared as unsigned bytes.
// - Positive floats (including +0): flip sign bit
// - Negative floats (including -0): flip all bits

// EncodeSortableFloat64 encodes a float64 into 8 bytes preserving sort order.
func EncodeSortableFloat64(dst []byte, f float64) {
	if len(dst) < zsetScoreBinarySize {
		return
	}
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 { // negative
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	binary.BigEndian.PutUint64(dst, bits)
}

// DecodeSortableFloat64 decodes a sortable-encoded float64 from 8 bytes.
func DecodeSortableFloat64(src []byte) float64 {
	bits := binary.BigEndian.Uint64(src)
	if bits&(1<<63) != 0 { // was positive (sign bit set in encoded form)
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}

// ---- Meta encoding ----

// MarshalZSetMeta encodes ZSetMeta into a fixed 8-byte binary format.
func MarshalZSetMeta(meta ZSetMeta) ([]byte, error) {
	if meta.Len < 0 {
		return nil, errors.WithStack(errors.Newf("zset meta contains negative len: %d", meta.Len))
	}
	buf := make([]byte, zsetMetaBinarySize)
	binary.BigEndian.PutUint64(buf, uint64(meta.Len)) //nolint:gosec // Len is non-negative
	return buf, nil
}

// UnmarshalZSetMeta decodes ZSetMeta from the fixed 8-byte binary format.
func UnmarshalZSetMeta(b []byte) (ZSetMeta, error) {
	if len(b) != zsetMetaBinarySize {
		return ZSetMeta{}, errors.WithStack(errors.Newf("invalid zset meta length: %d", len(b)))
	}
	length := binary.BigEndian.Uint64(b)
	if length > math.MaxInt64 {
		return ZSetMeta{}, errors.New("zset meta length overflows int64")
	}
	return ZSetMeta{Len: int64(length)}, nil //nolint:gosec // checked above
}

// ---- Score value encoding (stored in member key values) ----

// MarshalZSetScore encodes a score as raw IEEE 754 float64 (8 bytes).
func MarshalZSetScore(score float64) []byte {
	buf := make([]byte, zsetScoreBinarySize)
	binary.BigEndian.PutUint64(buf, math.Float64bits(score))
	return buf
}

// UnmarshalZSetScore decodes a score from 8-byte IEEE 754 float64.
func UnmarshalZSetScore(b []byte) (float64, error) {
	if len(b) != zsetScoreBinarySize {
		return 0, errors.WithStack(errors.Newf("invalid zset score length: %d", len(b)))
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
}

// ---- Key type detection ----

// IsZSetMetaKey returns true if the key is a ZSet metadata key.
func IsZSetMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ZSetMetaPrefix))
}

// IsZSetMemberKey returns true if the key is a ZSet member key.
func IsZSetMemberKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ZSetMemberPrefix))
}

// IsZSetScoreKey returns true if the key is a ZSet score-index key.
func IsZSetScoreKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ZSetScorePrefix))
}

// IsZSetInternalKey returns true if the key belongs to ZSet wide-column storage.
func IsZSetInternalKey(key []byte) bool {
	return IsZSetMetaKey(key) || IsZSetMemberKey(key) || IsZSetScoreKey(key)
}

// ---- User key / member extraction ----

// ExtractZSetUserKey returns the logical user key from any ZSet internal key.
func ExtractZSetUserKey(key []byte) []byte {
	var prefixLen int
	switch {
	case IsZSetMetaKey(key):
		prefixLen = len(ZSetMetaPrefix)
	case IsZSetMemberKey(key):
		prefixLen = len(ZSetMemberPrefix)
	case IsZSetScoreKey(key):
		prefixLen = len(ZSetScorePrefix)
	default:
		return nil
	}
	rest := key[prefixLen:]
	if len(rest) < zsetUserKeyLenSize {
		return nil
	}
	ukLen := int(binary.BigEndian.Uint32(rest[:zsetUserKeyLenSize]))
	if len(rest) < zsetUserKeyLenSize+ukLen {
		return nil
	}
	return rest[zsetUserKeyLenSize : zsetUserKeyLenSize+ukLen]
}

// ExtractZSetScoreAndMember extracts the score and member from a score-index key.
// The caller must provide the userKey to determine the offset.
func ExtractZSetScoreAndMember(key []byte, userKeyLen int) (float64, []byte) {
	prefixLen := len(ZSetScorePrefix)
	offset := prefixLen + zsetUserKeyLenSize + userKeyLen + zsetScoreBinarySize
	if len(key) < offset {
		return 0, nil
	}
	scoreStart := prefixLen + zsetUserKeyLenSize + userKeyLen
	score := DecodeSortableFloat64(key[scoreStart : scoreStart+zsetScoreBinarySize])
	member := key[offset:]
	return score, member
}

// ExtractZSetMember extracts the member from a member key.
// The caller must provide the userKey to determine the offset.
func ExtractZSetMember(key []byte, userKeyLen int) []byte {
	prefixLen := len(ZSetMemberPrefix)
	offset := prefixLen + zsetUserKeyLenSize + userKeyLen
	if len(key) < offset {
		return nil
	}
	return key[offset:]
}

// ---- Prefix utilities ----

// PrefixEnd computes the exclusive upper bound for a prefix scan.
// Returns nil if the prefix is empty or all 0xFF bytes.
func PrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // all 0xff
}

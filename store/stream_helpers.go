package store

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/errors"
)

// Stream wide-column key layout:
//
//	Meta:  !stream|meta|<userKeyLen(4)><userKey>        → [Len(8)][LastMs(8)][LastSeq(8)]
//	Entry: !stream|entry|<userKeyLen(4)><userKey><StreamID(16)>
const (
	StreamMetaPrefix  = "!stream|meta|"
	StreamEntryPrefix = "!stream|entry|"

	streamMetaBinarySize = 24
	// StreamIDBytes is the fixed size of the binary StreamID suffix on an entry key:
	// 8 bytes big-endian ms || 8 bytes big-endian seq. Big-endian so lex order
	// over the raw key bytes matches the (ms, seq) numeric order used by XADD / XRANGE.
	StreamIDBytes = 16
	// streamMetaLengthSignBit is the bit count used to check that an on-disk
	// length value still fits in an int64 on decode ((1<<63)-1 == math.MaxInt64).
	// Pulled out as a named constant to keep the overflow check self-documenting.
	streamMetaLengthSignBit = 63
)

// Pre-computed byte-slice prefixes to avoid per-call []byte(string)
// allocations in hot-path predicates (IsStreamMetaKey, IsStreamEntryKey, etc.).
var (
	streamMetaPrefixBytes  = []byte(StreamMetaPrefix)
	streamEntryPrefixBytes = []byte(StreamEntryPrefix)
)

// StreamMeta is the per-stream metadata. Length is authoritative for XLEN;
// LastMs/LastSeq track the highest ID ever appended so XADD '*' stays
// strictly monotonic even after XTRIM removes the current tail.
type StreamMeta struct {
	Length  int64
	LastMs  uint64
	LastSeq uint64
}

// MarshalStreamMeta encodes StreamMeta into a fixed 24-byte binary format.
func MarshalStreamMeta(m StreamMeta) ([]byte, error) {
	if m.Length < 0 {
		return nil, errors.WithStack(errors.Newf("stream meta negative length: %d", m.Length))
	}
	buf := make([]byte, streamMetaBinarySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(m.Length)) //nolint:gosec
	binary.BigEndian.PutUint64(buf[8:16], m.LastMs)
	binary.BigEndian.PutUint64(buf[16:24], m.LastSeq)
	return buf, nil
}

// UnmarshalStreamMeta decodes StreamMeta from the fixed 24-byte binary format.
func UnmarshalStreamMeta(b []byte) (StreamMeta, error) {
	if len(b) != streamMetaBinarySize {
		return StreamMeta{}, errors.WithStack(errors.Newf("invalid stream meta length: %d", len(b)))
	}
	length := binary.BigEndian.Uint64(b[0:8])
	if length > (1<<streamMetaLengthSignBit)-1 {
		return StreamMeta{}, errors.New("stream meta length overflows int64")
	}
	return StreamMeta{
		Length:  int64(length), //nolint:gosec
		LastMs:  binary.BigEndian.Uint64(b[8:16]),
		LastSeq: binary.BigEndian.Uint64(b[16:24]),
	}, nil
}

// StreamMetaKey builds the per-stream metadata key.
func StreamMetaKey(userKey []byte) []byte {
	buf := make([]byte, 0, len(StreamMetaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, StreamMetaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// StreamEntryScanPrefix returns the common prefix for every entry belonging
// to userKey, used as the [start, end) prefix for XRANGE / XREAD scans.
func StreamEntryScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(StreamEntryPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, StreamEntryPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// EncodeStreamID writes a StreamID as 16 big-endian bytes: ms || seq.
func EncodeStreamID(ms, seq uint64) []byte {
	var buf [StreamIDBytes]byte
	binary.BigEndian.PutUint64(buf[0:8], ms)
	binary.BigEndian.PutUint64(buf[8:16], seq)
	return buf[:]
}

// DecodeStreamID parses a 16-byte big-endian ms||seq tuple. Returns false if
// the slice length is wrong.
func DecodeStreamID(b []byte) (ms, seq uint64, ok bool) {
	if len(b) != StreamIDBytes {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16]), true
}

// StreamEntryKey builds the per-entry key for a stream.
func StreamEntryKey(userKey []byte, ms, seq uint64) []byte {
	buf := make([]byte, 0, len(StreamEntryPrefix)+wideColKeyLenSize+len(userKey)+StreamIDBytes)
	buf = append(buf, StreamEntryPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	buf = append(buf, EncodeStreamID(ms, seq)...)
	return buf
}

// ExtractStreamEntryID extracts (ms, seq) from a stream entry key. Returns
// false if the key is not a valid entry key for the given userKey.
func ExtractStreamEntryID(entryKey, userKey []byte) (ms, seq uint64, ok bool) {
	prefix := StreamEntryScanPrefix(userKey)
	if !bytes.HasPrefix(entryKey, prefix) {
		return 0, 0, false
	}
	return DecodeStreamID(entryKey[len(prefix):])
}

// IsStreamMetaKey reports whether the key is a stream metadata key.
func IsStreamMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, streamMetaPrefixBytes)
}

// IsStreamEntryKey reports whether the key is a stream entry key.
func IsStreamEntryKey(key []byte) bool {
	return bytes.HasPrefix(key, streamEntryPrefixBytes)
}

// ExtractStreamUserKeyFromMeta extracts the logical user key from a stream meta key.
func ExtractStreamUserKeyFromMeta(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, streamMetaPrefixBytes)
	if len(trimmed) < wideColKeyLenSize {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if uint32(len(trimmed)) < uint32(wideColKeyLenSize)+ukLen { //nolint:gosec
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

// ExtractStreamUserKeyFromEntry extracts the logical user key from a stream entry key.
func ExtractStreamUserKeyFromEntry(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, streamEntryPrefixBytes)
	if len(trimmed) < wideColKeyLenSize+StreamIDBytes {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if uint32(len(trimmed)) < uint32(wideColKeyLenSize)+ukLen+uint32(StreamIDBytes) { //nolint:gosec
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

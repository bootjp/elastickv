package store

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
)

// Delta/Claim key constants.
const (
	// ListMetaDeltaPrefix is the prefix for all list metadata delta keys.
	// Layout: !lst|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)>
	ListMetaDeltaPrefix = "!lst|meta|d|"

	// ListClaimPrefix is the prefix for list claim keys used by POP operations.
	// Layout: !lst|claim|<userKeyLen(4)><userKey><seq(8-byte sortable)>
	ListClaimPrefix = "!lst|claim|"

	// MaxDeltaScanLimit is the hard limit on delta scan results.
	// resolveListMeta returns ErrDeltaScanTruncated if this is reached.
	MaxDeltaScanLimit = 256

	// wideColKeyLenSize is the number of bytes used to encode the user-key
	// length as a big-endian uint32 in wide-column storage keys.
	wideColKeyLenSize = 4

	// deltaKeyTSSize is the number of bytes used for the commit-timestamp
	// field (uint64) in wide-column delta keys.
	deltaKeyTSSize = 8

	// deltaKeySeqSize is the number of bytes used for the seqInTxn field
	// (uint32) in wide-column delta keys.
	deltaKeySeqSize = 4

	// listDeltaSizeBytes is the fixed binary size of a ListMetaDelta (two int64 fields).
	listDeltaSizeBytes = 16
)

// ListMetaDelta holds the signed deltas applied by a single PUSH/POP operation.
type ListMetaDelta struct {
	HeadDelta int64
	LenDelta  int64
}

// MarshalListMetaDelta encodes a ListMetaDelta into a fixed 16-byte binary format.
func MarshalListMetaDelta(d ListMetaDelta) []byte {
	buf := make([]byte, listDeltaSizeBytes)
	binary.BigEndian.PutUint64(buf[0:8], uint64(d.HeadDelta)) //nolint:gosec
	binary.BigEndian.PutUint64(buf[8:16], uint64(d.LenDelta)) //nolint:gosec
	return buf
}

// UnmarshalListMetaDelta decodes a ListMetaDelta from the fixed 16-byte binary format.
func UnmarshalListMetaDelta(b []byte) (ListMetaDelta, error) {
	if len(b) != listDeltaSizeBytes {
		return ListMetaDelta{}, errors.WithStack(errors.Newf("invalid list meta delta length: %d", len(b)))
	}
	return ListMetaDelta{
		HeadDelta: int64(binary.BigEndian.Uint64(b[0:8])),  //nolint:gosec
		LenDelta:  int64(binary.BigEndian.Uint64(b[8:16])), //nolint:gosec
	}, nil
}

// ListMetaDeltaKey builds the delta key for a list: prefix + 4-byte userKeyLen + userKey + 8-byte commitTS + 4-byte seqInTxn.
func ListMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	buf := make([]byte, 0, len(ListMetaDeltaPrefix)+wideColKeyLenSize+len(userKey)+deltaKeyTSSize+deltaKeySeqSize)
	buf = append(buf, ListMetaDeltaPrefix...)
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

// ListMetaDeltaScanPrefix returns the prefix used to scan all delta keys for a userKey.
func ListMetaDeltaScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(ListMetaDeltaPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, ListMetaDeltaPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// ListClaimKey builds the claim key for a list item at the given sequence number.
func ListClaimKey(userKey []byte, seq int64) []byte {
	var raw [8]byte
	encodeSortableInt64(raw[:], seq)
	buf := make([]byte, 0, len(ListClaimPrefix)+wideColKeyLenSize+len(userKey)+sortableInt64Bytes)
	buf = append(buf, ListClaimPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	buf = append(buf, raw[:]...)
	return buf
}

// ListClaimScanPrefix returns the prefix used to scan all claim keys for a userKey.
func ListClaimScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(ListClaimPrefix)+wideColKeyLenSize+len(userKey))
	buf = append(buf, ListClaimPrefix...)
	var kl [4]byte
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey))) //nolint:gosec // len is bounded by max slice size
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// IsListMetaDeltaKey reports whether the key is a list metadata delta key.
func IsListMetaDeltaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ListMetaDeltaPrefix))
}

// IsListClaimKey reports whether the key is a list claim key.
func IsListClaimKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ListClaimPrefix))
}

// ExtractListUserKeyFromDelta extracts the logical user key from a list delta key.
func ExtractListUserKeyFromDelta(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, []byte(ListMetaDeltaPrefix))
	if len(trimmed) < wideColKeyLenSize+deltaKeyTSSize+deltaKeySeqSize {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if uint32(len(trimmed)) < uint32(wideColKeyLenSize)+ukLen+uint32(deltaKeyTSSize+deltaKeySeqSize) { //nolint:gosec // constants fit in uint32
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

// ExtractListUserKeyFromClaim extracts the logical user key from a list claim key.
func ExtractListUserKeyFromClaim(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, []byte(ListClaimPrefix))
	if len(trimmed) < wideColKeyLenSize+sortableInt64Bytes {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:wideColKeyLenSize])
	if uint32(len(trimmed)) < uint32(wideColKeyLenSize)+ukLen+uint32(sortableInt64Bytes) { //nolint:gosec // constants fit in uint32
		return nil
	}
	return trimmed[wideColKeyLenSize : wideColKeyLenSize+ukLen]
}

// PrefixScanEnd returns the exclusive end key for a prefix scan.
// It increments the last byte of the prefix; if overflow occurs (all 0xFF),
// it returns a nil slice which callers must interpret as "scan to end of keyspace".
func PrefixScanEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil // overflow: all bytes were 0xFF
}

// Wide-column style list storage using per-element keys.
// Item keys: !lst|itm|<userKey><seq(8-byte sortable binary)>
// Meta key : !lst|meta|<userKey> -> [Head(8)][Tail(8)][Len(8)]

const (
	ListMetaPrefix = "!lst|meta|"
	ListItemPrefix = "!lst|itm|"

	listMetaBinarySize = 24
)

type ListMeta struct {
	Head int64 `json:"h"`
	Tail int64 `json:"t"`
	Len  int64 `json:"l"`
}

// ListMetaKey builds the metadata key for a user key.
func ListMetaKey(userKey []byte) []byte {
	return append([]byte(ListMetaPrefix), userKey...)
}

// ListItemKey builds the item key for a user key and sequence number.
func ListItemKey(userKey []byte, seq int64) []byte {
	var raw [8]byte
	encodeSortableInt64(raw[:], seq)

	buf := make([]byte, 0, len(ListItemPrefix)+len(userKey)+len(raw))
	buf = append(buf, ListItemPrefix...)
	buf = append(buf, userKey...)
	buf = append(buf, raw[:]...)
	return buf
}

// MarshalListMeta encodes ListMeta into a fixed 24-byte binary format.
func MarshalListMeta(meta ListMeta) ([]byte, error) { return marshalListMeta(meta) }

// UnmarshalListMeta decodes ListMeta from the fixed 24-byte binary format.
func UnmarshalListMeta(b []byte) (ListMeta, error) { return unmarshalListMeta(b) }

func marshalListMeta(meta ListMeta) ([]byte, error) {
	if meta.Len < 0 {
		return nil, errors.WithStack(errors.Newf("list meta contains negative len: %d", meta.Len))
	}
	// Recompute Tail from Head+Len to guarantee the invariant and detect overflow.
	if meta.Len > 0 && meta.Head > math.MaxInt64-meta.Len {
		return nil, errors.WithStack(errors.Newf("list meta Head+Len overflows int64: head=%d len=%d", meta.Head, meta.Len))
	}
	meta.Tail = meta.Head + meta.Len

	buf := make([]byte, listMetaBinarySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(meta.Head))  //nolint:gosec // Head can be negative after LPUSH; uint64 cast preserves bits
	binary.BigEndian.PutUint64(buf[8:16], uint64(meta.Tail)) //nolint:gosec // Tail = Head + Len, may be negative
	binary.BigEndian.PutUint64(buf[16:24], uint64(meta.Len))
	return buf, nil
}

func unmarshalListMeta(b []byte) (ListMeta, error) {
	if len(b) != listMetaBinarySize {
		return ListMeta{}, errors.Wrap(errors.Newf("invalid list meta length: %d", len(b)), "unmarshal list meta")
	}

	head := int64(binary.BigEndian.Uint64(b[0:8]))  //nolint:gosec // Head may be negative after LPUSH
	tail := int64(binary.BigEndian.Uint64(b[8:16])) //nolint:gosec // Tail = Head + Len, may be negative
	length := binary.BigEndian.Uint64(b[16:24])

	if length > math.MaxInt64 {
		return ListMeta{}, errors.New("list meta length overflows int64")
	}
	iLen := int64(length)
	// Recompute expectedTail with overflow check instead of relying on
	// tail-head subtraction which wraps on int64 overflow.
	if iLen > 0 && head > math.MaxInt64-iLen {
		return ListMeta{}, errors.WithStack(errors.Newf("list meta head+len overflows int64: head=%d len=%d", head, iLen))
	}
	expectedTail := head + iLen
	if tail != expectedTail {
		return ListMeta{}, errors.WithStack(errors.Newf("list meta invariant violated: tail (%d) != head+len (%d)", tail, expectedTail))
	}

	return ListMeta{
		Head: head,
		Tail: tail,
		Len:  iLen,
	}, nil
}

// encodeSortableInt64 writes seq with sign bit flipped (seq ^ minInt64) in big-endian order.
const sortableInt64Bytes = 8

func encodeSortableInt64(dst []byte, seq int64) {
	if len(dst) < sortableInt64Bytes {
		return
	}
	binary.BigEndian.PutUint64(dst, uint64(seq^math.MinInt64)) //nolint:gosec // XOR trick for sortable int64 encoding
}

// IsListMetaKey Exported helpers for other packages (e.g., Redis adapter).
func IsListMetaKey(key []byte) bool { return bytes.HasPrefix(key, []byte(ListMetaPrefix)) }

func IsListItemKey(key []byte) bool { return bytes.HasPrefix(key, []byte(ListItemPrefix)) }

// ExtractListUserKey returns the logical user key from a list meta or item key.
// If the key is not a list key, it returns nil.
func ExtractListUserKey(key []byte) []byte {
	switch {
	case IsListMetaKey(key):
		return bytes.TrimPrefix(key, []byte(ListMetaPrefix))
	case IsListItemKey(key):
		trimmed := bytes.TrimPrefix(key, []byte(ListItemPrefix))
		if len(trimmed) < sortableInt64Bytes {
			return nil
		}
		return trimmed[:len(trimmed)-sortableInt64Bytes]
	default:
		return nil
	}
}

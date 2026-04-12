package store

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
)

// Wide-column style list storage using per-element keys.
// Item keys  : !lst|itm|<userKey><seq(8-byte sortable binary)>
// Meta key   : !lst|meta|<userKey> -> [Head(8)][Tail(8)][Len(8)]
// Delta key  : !lst|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)> -> [HeadDelta(8)][LenDelta(8)]
// Claim key  : !lst|claim|<userKeyLen(4)><userKey><seq(8-byte sortable)> -> claimValue

const (
	ListMetaPrefix      = "!lst|meta|"
	ListItemPrefix      = "!lst|itm|"
	ListMetaDeltaPrefix = "!lst|meta|d|"
	ListClaimPrefix     = "!lst|claim|"

	listMetaBinarySize      = 24
	listMetaDeltaBinarySize = 16

	// userKeyLen(4) + commitTS(8) + seqInTxn(4)
	listDeltaSuffixSize = 4 + 8 + 4
	// userKeyLen(4) + seq(8)
	listClaimSuffixSize = 4 + 8

	maxByte = 0xff
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

// IsListMetaKey reports whether key is a list base metadata key (not a delta key).
func IsListMetaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ListMetaPrefix)) && !bytes.HasPrefix(key, []byte(ListMetaDeltaPrefix))
}

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

// ── Delta Key Helpers ──────────────────────────────────────────────

// ListMetaDelta represents an incremental change to list metadata.
type ListMetaDelta struct {
	HeadDelta int64 // LPUSH: negative, LPOP: +1
	LenDelta  int64 // PUSH: positive, POP: -1
}

// ListMetaDeltaKey builds a globally-unique Delta key for a list.
func ListMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
	buf := make([]byte, 0, len(ListMetaDeltaPrefix)+listDeltaSuffixSize+len(userKey))
	buf = append(buf, ListMetaDeltaPrefix...)
	buf = appendUserKeyLenPrefixed(buf, userKey)
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], commitTS)
	buf = append(buf, ts[:]...)
	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], seqInTxn)
	buf = append(buf, seq[:]...)
	return buf
}

// ListMetaDeltaScanPrefix returns the key prefix for scanning all Delta
// keys belonging to the given user key.
func ListMetaDeltaScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(ListMetaDeltaPrefix)+4+len(userKey))
	buf = append(buf, ListMetaDeltaPrefix...)
	buf = appendUserKeyLenPrefixed(buf, userKey)
	return buf
}

// MarshalListMetaDelta encodes a ListMetaDelta into a fixed 16-byte binary.
func MarshalListMetaDelta(d ListMetaDelta) []byte {
	buf := make([]byte, listMetaDeltaBinarySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(d.HeadDelta)) //nolint:gosec // HeadDelta can be negative
	binary.BigEndian.PutUint64(buf[8:16], uint64(d.LenDelta)) //nolint:gosec // LenDelta can be negative
	return buf
}

// UnmarshalListMetaDelta decodes a ListMetaDelta from the fixed 16-byte binary.
func UnmarshalListMetaDelta(b []byte) (ListMetaDelta, error) {
	if len(b) != listMetaDeltaBinarySize {
		return ListMetaDelta{}, errors.WithStack(errors.Newf("invalid list meta delta length: %d, want %d", len(b), listMetaDeltaBinarySize))
	}
	return ListMetaDelta{
		HeadDelta: int64(binary.BigEndian.Uint64(b[0:8])),  //nolint:gosec // HeadDelta can be negative
		LenDelta:  int64(binary.BigEndian.Uint64(b[8:16])), //nolint:gosec // LenDelta can be negative
	}, nil
}

// IsListMetaDeltaKey reports whether key is a list metadata Delta key.
func IsListMetaDeltaKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ListMetaDeltaPrefix))
}

// ExtractListUserKeyFromDelta extracts the user key from a Delta key.
func ExtractListUserKeyFromDelta(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, []byte(ListMetaDeltaPrefix))
	if len(trimmed) < listDeltaSuffixSize {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:4])
	//nolint:gosec // ukLen is bounded by key length check below
	if uint32(len(trimmed)) < 4+ukLen+8+4 {
		return nil
	}
	return trimmed[4 : 4+ukLen]
}

// ── Claim Key Helpers ──────────────────────────────────────────────

// ListClaimKey builds a Claim key for a list item sequence.
func ListClaimKey(userKey []byte, seq int64) []byte {
	var raw [8]byte
	encodeSortableInt64(raw[:], seq)
	buf := make([]byte, 0, len(ListClaimPrefix)+listClaimSuffixSize+len(userKey))
	buf = append(buf, ListClaimPrefix...)
	buf = appendUserKeyLenPrefixed(buf, userKey)
	buf = append(buf, raw[:]...)
	return buf
}

// ListClaimScanPrefix returns the key prefix for scanning all Claim
// keys belonging to the given user key.
func ListClaimScanPrefix(userKey []byte) []byte {
	buf := make([]byte, 0, len(ListClaimPrefix)+4+len(userKey))
	buf = append(buf, ListClaimPrefix...)
	buf = appendUserKeyLenPrefixed(buf, userKey)
	return buf
}

// IsListClaimKey reports whether key is a list Claim key.
func IsListClaimKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ListClaimPrefix))
}

// ExtractListUserKeyFromClaim extracts the user key from a Claim key.
func ExtractListUserKeyFromClaim(key []byte) []byte {
	trimmed := bytes.TrimPrefix(key, []byte(ListClaimPrefix))
	if len(trimmed) < listClaimSuffixSize {
		return nil
	}
	ukLen := binary.BigEndian.Uint32(trimmed[:4])
	//nolint:gosec // ukLen is bounded by key length check below
	if uint32(len(trimmed)) < 4+ukLen+8 {
		return nil
	}
	return trimmed[4 : 4+ukLen]
}

// IsListInternalKey reports whether a key belongs to any list namespace.
func IsListInternalKey(key []byte) bool {
	return IsListMetaKey(key) || IsListItemKey(key) || IsListMetaDeltaKey(key) || IsListClaimKey(key)
}

// ── Common helpers ─────────────────────────────────────────────────

// appendUserKeyLenPrefixed appends a 4-byte big-endian length prefix
// followed by the user key bytes.
func appendUserKeyLenPrefixed(buf, userKey []byte) []byte {
	var kl [4]byte
	//nolint:gosec // userKey length is bounded by practical limits
	binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
	buf = append(buf, kl[:]...)
	buf = append(buf, userKey...)
	return buf
}

// PrefixScanEnd returns an exclusive upper bound for scanning all keys
// with the given prefix. It increments the last byte of the prefix.
func PrefixScanEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < maxByte {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

package store

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
)

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
	if meta.Head < 0 || meta.Tail < 0 || meta.Len < 0 {
		return nil, errors.WithStack(errors.Newf("list meta contains negative value: head=%d tail=%d len=%d", meta.Head, meta.Tail, meta.Len))
	}

	buf := make([]byte, listMetaBinarySize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(meta.Head))
	binary.BigEndian.PutUint64(buf[8:16], uint64(meta.Tail))
	binary.BigEndian.PutUint64(buf[16:24], uint64(meta.Len))
	return buf, nil
}

func unmarshalListMeta(b []byte) (ListMeta, error) {
	if len(b) != listMetaBinarySize {
		return ListMeta{}, errors.Wrap(errors.Newf("invalid list meta length: %d", len(b)), "unmarshal list meta")
	}

	head := binary.BigEndian.Uint64(b[0:8])
	tail := binary.BigEndian.Uint64(b[8:16])
	length := binary.BigEndian.Uint64(b[16:24])

	if head > math.MaxInt64 || tail > math.MaxInt64 || length > math.MaxInt64 {
		return ListMeta{}, errors.New("list meta value overflows int64")
	}

	return ListMeta{
		Head: int64(head),
		Tail: int64(tail),
		Len:  int64(length),
	}, nil
}

// encodeSortableInt64 writes seq with sign bit flipped (seq ^ minInt64) in big-endian order.
const sortableInt64Bytes = 8

func encodeSortableInt64(dst []byte, seq int64) {
	if len(dst) < sortableInt64Bytes {
		return
	}
	sortable := seq ^ math.MinInt64
	for i := sortableInt64Bytes - 1; i >= 0; i-- {
		dst[i] = byte(sortable)
		sortable >>= 8
	}
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

package store

import (
	"bytes"
	"context"
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
	// limit per scan when deleting to avoid OOM.
	deleteBatchSize    = 1024
	listMetaBinarySize = 24
	scanAdvanceByte    = byte(0x00)
)

type ListMeta struct {
	Head int64 `json:"h"`
	Tail int64 `json:"t"`
	Len  int64 `json:"l"`
}

// ListStore requires ScanStore to fetch ranges efficiently.
type ListStore struct {
	store ScanStore
}

func NewListStore(base ScanStore) *ListStore {
	return &ListStore{store: base}
}

// IsList reports whether the given key has list metadata.
func (s *ListStore) IsList(ctx context.Context, key []byte) (bool, error) {
	_, exists, err := s.LoadMeta(ctx, key)
	return exists, err
}

// PutList replaces the entire list.
func (s *ListStore) PutList(ctx context.Context, key []byte, list []string) error {
	meta := ListMeta{Head: 0, Tail: int64(len(list)), Len: int64(len(list))}
	metaBytes, err := marshalListMeta(meta)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(s.store.Txn(ctx, func(ctx context.Context, txn Txn) error {
		scanTxn, ok := txn.(ScanTxn)
		if !ok {
			return errors.WithStack(ErrNotSupported)
		}

		if err := s.deleteListTxn(ctx, scanTxn, key); err != nil {
			return err
		}

		for i, v := range list {
			if err := scanTxn.Put(ctx, ListItemKey(key, int64(i)), []byte(v)); err != nil {
				return errors.WithStack(err)
			}
		}
		if err := scanTxn.Put(ctx, ListMetaKey(key), metaBytes); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}))
}

// GetList returns the whole list. It reconstructs via Scan; avoid for huge lists.
func (s *ListStore) GetList(ctx context.Context, key []byte) ([]string, error) {
	meta, exists, err := s.LoadMeta(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists || meta.Len == 0 {
		return nil, ErrKeyNotFound
	}
	return s.Range(ctx, key, 0, int(meta.Len)-1)
}

// RPush appends values and returns new length.
func (s *ListStore) RPush(ctx context.Context, key []byte, values ...string) (int, error) {
	if len(values) == 0 {
		return 0, nil
	}

	newLen := 0

	err := s.store.Txn(ctx, func(ctx context.Context, txn Txn) error {
		// load meta inside txn for correctness
		meta, exists, err := s.loadMetaTxn(ctx, txn, key)
		if err != nil {
			return err
		}
		if !exists {
			meta = ListMeta{Head: 0, Tail: 0, Len: 0}
		}

		startSeq := meta.Head + meta.Len

		for i, v := range values {
			seq := startSeq + int64(i)
			if err := txn.Put(ctx, ListItemKey(key, seq), []byte(v)); err != nil {
				return errors.WithStack(err)
			}
		}
		meta.Len += int64(len(values))
		meta.Tail = meta.Head + meta.Len
		metaBytes, err := marshalListMeta(meta)
		if err != nil {
			return errors.WithStack(err)
		}
		newLen = int(meta.Len)
		return errors.WithStack(txn.Put(ctx, ListMetaKey(key), metaBytes))
	})
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return newLen, nil
}

// Range returns elements between start and end (inclusive).
// Negative indexes follow Redis semantics.
func (s *ListStore) Range(ctx context.Context, key []byte, start, end int) ([]string, error) {
	meta, exists, err := s.LoadMeta(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists || meta.Len == 0 {
		return nil, ErrKeyNotFound
	}

	si, ei := clampRange(start, end, int(meta.Len))
	if ei < si {
		return []string{}, nil
	}

	startSeq := meta.Head + int64(si)
	endSeq := meta.Head + int64(ei)
	startKey := ListItemKey(key, startSeq)
	endKey := ListItemKey(key, endSeq+1) // exclusive

	kvs, err := s.store.Scan(ctx, startKey, endKey, ei-si+1)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]string, 0, len(kvs))
	for _, kvp := range kvs {
		out = append(out, string(kvp.Value))
	}
	return out, nil
}

// --- helpers ---

// LoadMeta returns metadata and whether the list exists.
func (s *ListStore) LoadMeta(ctx context.Context, key []byte) (ListMeta, bool, error) {
	val, err := s.store.Get(ctx, ListMetaKey(key))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return ListMeta{}, false, nil
		}
		return ListMeta{}, false, errors.WithStack(err)
	}
	if len(val) == 0 {
		return ListMeta{}, false, nil
	}
	meta, err := unmarshalListMeta(val)
	return meta, err == nil, errors.WithStack(err)
}

func (s *ListStore) loadMetaTxn(ctx context.Context, txn Txn, key []byte) (ListMeta, bool, error) {
	val, err := txn.Get(ctx, ListMetaKey(key))
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return ListMeta{}, false, nil
		}
		return ListMeta{}, false, errors.WithStack(err)
	}
	if len(val) == 0 {
		return ListMeta{}, false, nil
	}
	meta, err := unmarshalListMeta(val)
	return meta, err == nil, errors.WithStack(err)
}

// deleteListTxn deletes list items and metadata within the provided transaction.
func (s *ListStore) deleteListTxn(ctx context.Context, txn ScanTxn, key []byte) error {
	start := ListItemKey(key, mathMinInt64) // inclusive
	end := ListItemKey(key, mathMaxInt64)   // inclusive sentinel

	for {
		kvs, err := txn.Scan(ctx, start, end, deleteBatchSize)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return errors.WithStack(err)
		}
		if len(kvs) == 0 {
			break
		}

		for _, kvp := range kvs {
			if err := txn.Delete(ctx, kvp.Key); err != nil {
				return errors.WithStack(err)
			}
		}

		// advance start just after the last processed key to guarantee forward progress
		lastKey := kvs[len(kvs)-1].Key
		start = append(bytes.Clone(lastKey), scanAdvanceByte)

		if len(kvs) < deleteBatchSize {
			break
		}
	}

	// delete meta last (ignore missing)
	if err := txn.Delete(ctx, ListMetaKey(key)); err != nil && !errors.Is(err, ErrKeyNotFound) {
		return errors.WithStack(err)
	}
	return nil
}

// ListMetaKey builds the metadata key for a user key.
func ListMetaKey(userKey []byte) []byte {
	return append([]byte(ListMetaPrefix), userKey...)
}

// ListItemKey builds the item key for a user key and sequence number.
func ListItemKey(userKey []byte, seq int64) []byte {
	// Offset sign bit (seq ^ minInt64) to preserve order, then big-endian encode (8 bytes).
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

func clampRange(start, end, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if end < start {
		return 0, -1
	}
	return start, end
}

// sentinel seq for scan bounds
const (
	mathMinInt64 = -1 << 63
	mathMaxInt64 = 1<<63 - 1
)

// Exported helpers for other packages (e.g., Redis adapter).
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

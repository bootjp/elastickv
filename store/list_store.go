package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math"

	"github.com/cockroachdb/errors"
)

// Wide-column style list storage using per-element keys.
// Item keys: !lst|itm|<userKey>|%020d
// Meta key : !lst|meta|<userKey> -> {"h":head,"t":tail,"l":len}

const (
	ListMetaPrefix = "!lst|meta|"
	ListItemPrefix = "!lst|itm|"
	ListSeqWidth   = 20
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
	// delete existing meta/items (best-effort)
	if err := s.deleteList(ctx, key); err != nil {
		return err
	}

	meta := ListMeta{Head: 0, Tail: int64(len(list)), Len: int64(len(list))}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(s.store.Txn(ctx, func(ctx context.Context, txn Txn) error {
		for i, v := range list {
			if err := txn.Put(ctx, ListItemKey(key, int64(i)), []byte(v)); err != nil {
				return errors.WithStack(err)
			}
		}
		if err := txn.Put(ctx, ListMetaKey(key), metaBytes); err != nil {
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
		metaBytes, err := json.Marshal(meta)
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
	var meta ListMeta
	if err := json.Unmarshal(val, &meta); err != nil {
		return ListMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
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
	var meta ListMeta
	if err := json.Unmarshal(val, &meta); err != nil {
		return ListMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

func (s *ListStore) deleteList(ctx context.Context, key []byte) error {
	start := ListItemKey(key, mathMinInt64) // use smallest seq
	end := ListItemKey(key, mathMaxInt64)

	items, err := s.store.Scan(ctx, start, end, math.MaxInt)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return errors.WithStack(err)
	}

	return errors.WithStack(s.store.Txn(ctx, func(ctx context.Context, txn Txn) error {
		for _, kvp := range items {
			if err := txn.Delete(ctx, kvp.Key); err != nil {
				return errors.WithStack(err)
			}
		}
		_ = txn.Delete(ctx, ListMetaKey(key))
		return nil
	}))
}

// ListMetaKey builds the metadata key for a user key.
func ListMetaKey(userKey []byte) []byte {
	return append([]byte(ListMetaPrefix), userKey...)
}

// ListItemKey builds the item key for a user key and sequence number.
func ListItemKey(userKey []byte, seq int64) []byte {
	// Offset sign bit (seq ^ minInt64) to preserve order, then big-endian encode and hex.
	var raw [8]byte
	encodeSortableInt64(raw[:], seq)
	hexSeq := make([]byte, hex.EncodedLen(len(raw)))
	hex.Encode(hexSeq, raw[:])

	buf := make([]byte, 0, len(ListItemPrefix)+len(userKey)+1+len(hexSeq))
	buf = append(buf, ListItemPrefix...)
	buf = append(buf, userKey...)
	buf = append(buf, '|')
	buf = append(buf, hexSeq...)
	return buf
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
		idx := bytes.LastIndexByte(trimmed, '|')
		if idx == -1 {
			return nil
		}
		return trimmed[:idx]
	default:
		return nil
	}
}

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/emirpasic/gods/maps/treemap"
)

type rbMemoryStore struct {
	tree *treemap.Map
	mtx  sync.RWMutex
	// key -> value
	// key -> ttl
	ttl *treemap.Map
	log *slog.Logger

	expire *time.Ticker
}

func byteSliceComparator(a, b interface{}) int {
	aAsserted, aOk := a.([]byte)
	bAsserted, bOK := b.([]byte)
	if !aOk || !bOK {
		panic("not a byte slice")
	}
	return bytes.Compare(aAsserted, bAsserted)
}

func NewRbMemoryStore() ScanStore {
	m := &rbMemoryStore{
		mtx:  sync.RWMutex{},
		tree: treemap.NewWith(byteSliceComparator),
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),

		ttl: nil,
	}

	m.expire = nil

	return m
}

var _ TTLStore = (*rbMemoryStore)(nil)

func NewRbMemoryStoreWithExpire(interval time.Duration) TTLStore {
	//nolint:forcetypeassert
	m := NewRbMemoryStore().(*rbMemoryStore)
	m.expire = time.NewTicker(interval)
	m.ttl = treemap.NewWith(byteSliceComparator)

	go func() {
		for range m.expire.C {
			m.cleanExpired()
		}
	}()
	return m
}

func NewRbMemoryStoreDefaultTTL() TTLStore {
	return NewMemoryStoreWithExpire(defaultExpireInterval)
}

var _ Store = &rbMemoryStore{}

func (s *rbMemoryStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	v, ok := s.tree.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	vv, ok := v.([]byte)
	if !ok {
		return nil, errors.WithStack(ErrKeyNotFound)
	}

	return vv, nil
}

func (s *rbMemoryStore) Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var result []*KVPair

	s.tree.Each(func(key interface{}, value interface{}) {
		k, ok := key.([]byte)
		if !ok {
			return
		}
		v, ok := value.([]byte)
		if !ok {
			return
		}

		if start != nil && bytes.Compare(k, start) < 0 {
			return
		}

		if end != nil && bytes.Compare(k, end) > 0 {
			return
		}

		if len(result) >= limit {
			return
		}

		result = append(result, &KVPair{
			Key:   k,
			Value: v,
		})

	})
	return result, nil
}

func (s *rbMemoryStore) Put(ctx context.Context, key []byte, value []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.tree.Put(key, value)

	return nil
}

func (s *rbMemoryStore) Delete(ctx context.Context, key []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.tree.Remove(key)
	s.log.InfoContext(ctx, "Delete",
		slog.String("key", string(key)),
	)

	return nil
}
func (s *rbMemoryStore) Exists(ctx context.Context, key []byte) (bool, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	_, ok := s.tree.Get(key)

	return ok, nil
}

func (s *rbMemoryStore) Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	txn := s.NewTxn()

	err := f(ctx, txn)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, op := range txn.ops {
		switch op.opType {
		case OpTypePut:
			s.tree.Put(op.key, op.v)
		case OpTypeDelete:
			s.tree.Remove(op.key)
		default:
			return errors.WithStack(ErrUnknownOp)
		}
	}

	return nil
}

func (s *rbMemoryStore) TxnWithTTL(ctx context.Context, f func(ctx context.Context, txn TTLTxn) error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	txn := s.NewTTLTxn()

	err := f(ctx, txn)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, op := range txn.ops {
		switch op.opType {
		case OpTypePut:
			s.tree.Put(op.key, op.v)
			s.ttl.Put(op.key, op.ttl)
		case OpTypeDelete:
			s.tree.Remove(op.key)
			s.ttl.Remove(op.key)
		default:
			return errors.WithStack(ErrUnknownOp)
		}
	}

	return nil
}

func (s *rbMemoryStore) Close() error {
	return nil
}

func (s *rbMemoryStore) Snapshot() (io.ReadWriter, error) {
	s.mtx.RLock()
	cl := make(map[*[]byte][]byte, s.tree.Size())

	s.tree.Each(func(key interface{}, value interface{}) {
		k, ok := key.([]byte)
		if !ok {
			return
		}
		v, ok := value.([]byte)
		if !ok {
			return
		}
		cl[&k] = v
	})

	// early unlock
	s.mtx.RUnlock()

	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(cl); err != nil {
		return nil, errors.WithStack(err)
	}

	sum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.LittleEndian, sum); err != nil {
		return nil, errors.WithStack(err)
	}

	return buf, nil
}
func (s *rbMemoryStore) Restore(r io.Reader) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	data, err := io.ReadAll(r)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(data) < 4 {
		return errors.WithStack(ErrInvalidChecksum)
	}
	payload := data[:len(data)-4]
	expected := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(payload) != expected {
		return errors.WithStack(ErrInvalidChecksum)
	}

	var cl map[*[]byte][]byte
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&cl); err != nil {
		return errors.WithStack(err)
	}

	s.tree.Clear()
	for k, v := range cl {
		if k == nil {
			continue
		}
		s.tree.Put(*k, v)
	}

	return nil
}

func (s *rbMemoryStore) Expire(ctx context.Context, key []byte, ttl int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.ttl.Put(key, ttl)
	s.log.InfoContext(ctx, "Expire",
		slog.String("key", string(key)),
		slog.Int64("ttl", ttl),
	)

	return nil
}

func (s *rbMemoryStore) PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.tree.Put(key, value)
	s.ttl.Put(key, ttl)
	s.log.InfoContext(ctx, "Put",
		slog.String("key", string(key)),
		slog.String("value", string(value)),
	)

	return nil
}

func (s *rbMemoryStore) cleanExpired() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	now := time.Now().Unix()
	s.ttl.Each(func(key interface{}, value interface{}) {
		k, ok := key.([]byte)
		if !ok {
			return
		}
		v, ok := value.(int64)
		if !ok {
			return
		}

		if v > now {
			return
		}

		s.tree.Remove(k)
		s.ttl.Remove(k)
	})
}

type rbMemoryStoreTxn struct {
	mu *sync.RWMutex
	// Memory Structure during Transaction
	tree *treemap.Map
	// Time series operations during a transaction
	ops []rbMemOp
	s   *rbMemoryStore
}

func (s *rbMemoryStore) NewTxn() *rbMemoryStoreTxn {
	return &rbMemoryStoreTxn{
		mu:   &sync.RWMutex{},
		tree: treemap.NewWith(byteSliceComparator),
		ops:  []rbMemOp{},
		s:    s,
	}
}

func (s *rbMemoryStore) NewTTLTxn() *rbMemoryStoreTxn {
	return &rbMemoryStoreTxn{
		mu:   &sync.RWMutex{},
		tree: treemap.NewWith(byteSliceComparator),
		ops:  []rbMemOp{},
		s:    s,
	}
}

type rbMemOp struct {
	opType OpType
	key    []byte
	v      []byte
	ttl    int64
}

func (t *rbMemoryStoreTxn) Get(_ context.Context, key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	v, ok := t.tree.Get(key)
	if !ok {
		v, ok = t.s.tree.Get(key)
		if !ok {
			return nil, ErrKeyNotFound
		}
	}

	vv, ok := v.([]byte)
	if !ok {
		return nil, ErrKeyNotFound
	}

	return vv, nil
}

func (t *rbMemoryStoreTxn) Scan(_ context.Context, start []byte, end []byte, limit int) ([]*KVPair, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []*KVPair

	t.tree.Each(func(key interface{}, value interface{}) {
		k, ok := key.([]byte)
		if !ok {
			return
		}
		v, ok := value.([]byte)
		if !ok {
			return
		}

		if bytes.Compare(k, start) < 0 {
			return
		}

		if bytes.Compare(k, end) > 0 {
			return
		}

		if len(result) >= limit {
			return
		}

		result = append(result, &KVPair{
			Key:   k,
			Value: v,
		})
	})
	return result, nil
}

func (t *rbMemoryStoreTxn) Put(_ context.Context, key []byte, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tree.Put(key, value)
	t.ops = append(t.ops, rbMemOp{
		key:    key,
		opType: OpTypePut,
		v:      value,
	})
	return nil
}

func (t *rbMemoryStoreTxn) Delete(_ context.Context, key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tree.Remove(key)
	t.ops = append(t.ops, rbMemOp{
		key:    key,
		opType: OpTypeDelete,
	})

	return nil
}

func (t *rbMemoryStoreTxn) Exists(_ context.Context, key []byte) (bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.tree.Get(key)
	return ok, nil
}

func (t *rbMemoryStoreTxn) Expire(_ context.Context, key []byte, ttl int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, o := range t.ops {
		if !bytes.Equal(o.key, key) {
			continue
		}
		t.ops[i].ttl = ttl
		return nil
	}

	return errors.WithStack(ErrKeyNotFound)
}

func (t *rbMemoryStoreTxn) PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tree.Put(key, value)
	t.ops = append(t.ops, rbMemOp{
		key:    key,
		opType: OpTypePut,
		v:      value,
		ttl:    ttl,
	})

	return nil
}

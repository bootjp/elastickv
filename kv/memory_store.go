package kv

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/spaolacci/murmur3"
)

var ErrNotFound = errors.New("not found")

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Close() error
	Name() string
	Snapshot() (io.ReadWriter, error)
	Restore(buf io.Reader) error
	Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error

	hash([]byte) (uint64, error)
}

type Txn interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

type memoryStore struct {
	mtx sync.RWMutex
	m   map[uint64][]byte
	log *slog.Logger
}

func NewMemoryStore() Store {
	return &memoryStore{
		mtx: sync.RWMutex{},
		m:   map[uint64][]byte{},
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
}

var _ Store = &memoryStore{}

func (s *memoryStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	h, err := s.hash(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	slog.InfoContext(ctx, "Get",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
	)

	v, ok := s.m[h]
	if !ok {
		return nil, ErrNotFound
	}

	return v, nil
}

func (s *memoryStore) Put(ctx context.Context, key []byte, value []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	s.m[h] = value
	s.log.InfoContext(ctx, "Put",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
		slog.String("value", string(value)),
	)

	return nil
}

func (s *memoryStore) Delete(ctx context.Context, key []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h, err := s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	delete(s.m, h)
	s.log.InfoContext(ctx, "Delete",
		slog.String("key", string(key)),
		slog.Uint64("hash", h),
	)

	return nil
}

type OpType uint8

const (
	OpTypePut OpType = iota
	OpTypeDelete
)

type Op struct {
	op OpType
	h  uint64
	v  []byte
}

type memoryStoreTxn struct {
	mu *sync.RWMutex
	// トランザクション中のメモリデータ
	m map[uint64][]byte
	// トランザクション中の時系列操作
	ops []Op
	s   *memoryStore
}

func (s *memoryStore) NewTxn() *memoryStoreTxn {
	return &memoryStoreTxn{
		mu:  &sync.RWMutex{},
		m:   map[uint64][]byte{},
		ops: []Op{},
		s:   s,
	}
}

func (t *memoryStoreTxn) Get(_ context.Context, key []byte) ([]byte, error) {
	h, err := t.s.hash(key)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	v, ok := t.m[h]
	if ok {
		return v, nil
	}

	// トランザクション中に削除されている場合はErrNotFoundを返す
	for _, op := range t.ops {
		if op.h != h {
			continue
		}
		if op.op == OpTypeDelete {
			return nil, ErrNotFound
		}
	}

	v, ok = t.s.m[h]
	if !ok {
		return nil, ErrNotFound
	}

	return v, nil
}

func (t *memoryStoreTxn) Put(_ context.Context, key []byte, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	t.m[h] = value
	t.ops = append(t.ops, Op{
		h:  h,
		op: OpTypePut,
		v:  value,
	})
	return nil
}

func (s *memoryStore) Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	txn := s.NewTxn()

	err := f(ctx, txn)
	if err != nil {
		return errors.WithStack(err)
	}

	for _, op := range txn.ops {
		switch op.op {
		case OpTypePut:
			s.m[op.h] = op.v
		case OpTypeDelete:
			delete(s.m, op.h)
		default:
			return errors.WithStack(ErrUnknownRequestType)
		}
	}

	return nil
}

func (t *memoryStoreTxn) Delete(_ context.Context, key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, err := t.s.hash(key)
	if err != nil {
		return errors.WithStack(err)
	}

	delete(t.m, h)
	t.ops = append(t.ops, Op{
		h:  h,
		op: OpTypeDelete,
	})

	return nil
}

func (s *memoryStore) hash(key []byte) (uint64, error) {
	h := murmur3.New64()
	if _, err := h.Write(key); err != nil {
		return 0, errors.WithStack(err)
	}
	return h.Sum64(), nil
}

func (s *memoryStore) Close() error {
	return nil
}

func (s *memoryStore) Name() string {
	return "memory"
}

func (s *memoryStore) Snapshot() (io.ReadWriter, error) {
	s.mtx.RLock()
	cl := make(map[uint64][]byte, len(s.m))
	for k, v := range s.m {
		cl[k] = v
	}
	s.mtx.RUnlock()

	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(cl)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return buf, nil
}
func (s *memoryStore) Restore(buf io.Reader) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.m = make(map[uint64][]byte)
	err := gob.NewDecoder(buf).Decode(&s.m)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

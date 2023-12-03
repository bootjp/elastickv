package kv

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/spaolacci/murmur3"
)

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
}

type store struct {
	mtx sync.RWMutex
	m   map[uint64][]byte
	log *slog.Logger
}

func NewStore() Store {
	return &store{
		mtx: sync.RWMutex{},
		m:   map[uint64][]byte{},
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})),
	}
}

var _ Store = &store{}

func (s *store) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	h := murmur3.New64()
	if _, err := h.Write(key); err != nil {
		return nil, errors.WithStack(err)
	}
	hash := h.Sum64()

	slog.InfoContext(ctx, "Get",
		slog.String("key", string(key)),
		slog.Uint64("hash", hash),
	)

	v, ok := s.m[hash]
	if !ok {
		return nil, nil
	}

	return v, nil

}

func (s *store) Put(ctx context.Context, key []byte, value []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	h := murmur3.New64()
	if _, err := h.Write(key); err != nil {
		return errors.WithStack(err)
	}
	hash := h.Sum64()

	s.m[hash] = value

	s.log.InfoContext(ctx, "Put",
		slog.String("key", string(key)),
		slog.Uint64("hash", hash),
		slog.String("value", string(value)),
	)

	return nil
}

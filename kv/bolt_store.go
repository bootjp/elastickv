package kv

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")

type boltStore struct {
	mtx   sync.RWMutex
	log   *slog.Logger
	bbolt *bbolt.DB
}

const mode = 0666

func NewBoltStore(path string) (Store, error) {
	db, err := bbolt.Open(path, mode, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &boltStore{
		mtx:   sync.RWMutex{},
		bbolt: db,
		log:   slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{})),
	}, nil
}

var _ Store = &boltStore{}

func (s *boltStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	slog.InfoContext(ctx, "Get",
		slog.String("key", string(key)),
	)

	var v []byte

	err := s.bbolt.View(func(tx *bbolt.Tx) error {
		v = tx.Bucket(defaultBucket).Get(key)
		return nil
	})

	return v, errors.WithStack(err)
}

func (s *boltStore) Put(ctx context.Context, key []byte, value []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.log.InfoContext(ctx, "Put",
		slog.String("key", string(key)),
		slog.String("value", string(value)),
	)

	err := s.bbolt.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(defaultBucket)
		if err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(b.Put(key, value))
	})

	return errors.WithStack(err)
}

func (s *boltStore) Delete(ctx context.Context, key []byte) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.log.InfoContext(ctx, "Delete",
		slog.String("key", string(key)),
	)

	err := s.bbolt.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return nil
		}

		return errors.WithStack(b.Delete(key))
	})

	return errors.WithStack(err)
}

func (s *boltStore) hash(_ []byte) (uint64, error) {
	return 0, ErrNotImplemented
}

func (s *boltStore) Name() string {
	return "bolt"
}

func (s *boltStore) Close() error {
	return errors.WithStack(s.bbolt.Close())
}

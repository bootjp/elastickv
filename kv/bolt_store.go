package kv

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"go.etcd.io/bbolt"
)

var defaultBucket = []byte("default")

type boltStore struct {
	log   *slog.Logger
	bbolt *bbolt.DB
}

const mode = 0666

func NewBoltStore(path string) (Store, error) {
	db, err := bbolt.Open(path, mode, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	_, err = tx.CreateBucketIfNotExists(defaultBucket)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &boltStore{
		bbolt: db,
		log: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}, nil
}

var _ Store = &boltStore{}

func (s *boltStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.log.InfoContext(ctx, "Get",
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
	s.log.InfoContext(ctx, "put",
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

type boltStoreTxn struct {
	bucket *bbolt.Bucket
}

func (s *boltStore) NewBoltStoreTxn(tx *bbolt.Tx) Txn {
	return &boltStoreTxn{
		bucket: tx.Bucket(defaultBucket),
	}
}

func (t *boltStoreTxn) Get(_ context.Context, key []byte) ([]byte, error) {
	return t.bucket.Get(key), nil
}

func (t *boltStoreTxn) Put(_ context.Context, key []byte, value []byte) error {
	return errors.WithStack(t.bucket.Put(key, value))
}

func (t *boltStoreTxn) Delete(_ context.Context, key []byte) error {
	return errors.WithStack(t.bucket.Delete(key))
}

func (s *boltStore) Txn(ctx context.Context, fn func(ctx context.Context, txn Txn) error) error {
	btxn, err := s.bbolt.Begin(true)
	if err != nil {
		return errors.WithStack(err)
	}

	txn := s.NewBoltStoreTxn(btxn)
	err = fn(ctx, txn)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(btxn.Commit())
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

func (s *boltStore) Snapshot() (io.ReadWriter, error) {
	return nil, ErrNotImplemented
}

func (s *boltStore) Restore(buf io.Reader) error {
	return ErrNotImplemented
}

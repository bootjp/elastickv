package kv

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
)

var ErrKeyNotFound = errors.New("not found")

type KVPair struct {
	Key   []byte
	Value []byte
}

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
	Snapshot() (io.ReadWriter, error)
	Restore(buf io.Reader) error
	Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error
	Close() error
}

type ScanStore interface {
	Store
	Scan(ctx context.Context, start []byte, end []byte, limit int) ([]*KVPair, error)
}

type TTLStore interface {
	Store
	Expire(ctx context.Context, key []byte, ttl int64) error
	PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error
	TxnWithTTL(ctx context.Context, f func(ctx context.Context, txn TTLTxn) error) error
}

type Txn interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
}

type TTLTxn interface {
	Txn
	Expire(ctx context.Context, key []byte, ttl int64) error
	PutWithTTL(ctx context.Context, key []byte, value []byte, ttl int64) error
}

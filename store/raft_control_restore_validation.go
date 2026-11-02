package store

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/emirpasic/gods/maps/treemap"
)

// RaftControlSnapshotValidator validates decoded metadata before replacing the
// live database. The read function is valid only for the duration of validate.
type RaftControlSnapshotValidator interface {
	RestoreRaftControlSnapshot(context.Context, io.Reader, func(read func([]byte) ([]byte, error)) error) error
}

func (s *mvccStore) RestoreRaftControlSnapshot(ctx context.Context, r io.Reader, validate func(func([]byte) ([]byte, error)) error) error {
	return s.restoreSnapshot(&externalSnapshotReader{ctx: ctx, r: r}, func(_ *treemap.Map, meta mvccSnapshotMigrationMetadata) error {
		return validate(func(key []byte) ([]byte, error) {
			value, ok := meta.raftControl[string(key)]
			if !ok {
				return nil, ErrKeyNotFound
			}
			return append([]byte(nil), value...), nil
		})
	})
}

func (s *pebbleStore) RestoreRaftControlSnapshot(ctx context.Context, r io.Reader, validate func(func([]byte) ([]byte, error)) error) error {
	return s.restoreSnapshot(&externalSnapshotReader{ctx: ctx, r: r}, func(dir string) (retErr error) {
		opts, cache := defaultPebbleOptionsWithCache(false)
		db, err := pebble.Open(dir, opts)
		if err != nil {
			cache.Unref()
			return errors.WithStack(err)
		}
		defer cache.Unref()
		defer func() { retErr = errors.Join(retErr, db.Close(), ctx.Err()) }()
		return validate(func(key []byte) ([]byte, error) {
			physical, err := raftControlKey(key)
			if err != nil {
				return nil, err
			}
			value, closer, err := db.Get(physical)
			if errors.Is(err, pebble.ErrNotFound) {
				return nil, ErrKeyNotFound
			}
			if err != nil {
				return nil, errors.WithStack(err)
			}
			decoded, err := s.decodeRaftControlValue(physical, value)
			return decoded, errors.Join(err, closer.Close())
		})
	})
}

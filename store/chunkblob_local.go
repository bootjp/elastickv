package store

import (
	"bytes"
	"context"
	"math"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

// ChunkBlobLocalStore is the node-local side of both chunkblob reclaimers.
type ChunkBlobLocalStore interface {
	s3keys.ChunkBlobLocalStore
	s3keys.ChunkBlobOrphanLocalStore
}

// NewChunkBlobLocalStore selects the local implementation behind an MVCCStore.
func NewChunkBlobLocalStore(st MVCCStore) (ChunkBlobLocalStore, error) {
	local, ok := st.(ChunkBlobLocalStore)
	if !ok {
		return nil, errors.New("MVCC store does not support local chunkblob reclamation")
	}
	return local, nil
}

var _ ChunkBlobLocalStore = (*pebbleStore)(nil)
var _ ChunkBlobLocalStore = (*mvccStore)(nil)

func (s *mvccStore) ChunkBlobWrittenAt(ctx context.Context, sha [32]byte) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, errors.WithStack(err)
	}
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	ver, ok := s.latestVersionLocked(s3keys.ChunkBlobKey(sha))
	if !ok || ver.Tombstone || ver.ExpireAt != 0 {
		return 0, false, nil
	}
	return ver.TS, true, nil
}

func (s *mvccStore) ListLocalChunkBlobs(ctx context.Context) ([]s3keys.LocalChunkBlob, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	var blobs []s3keys.LocalChunkBlob
	it := s.entryTreeIterator()
	for it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}
		key, _ := it.Key().([]byte)
		if !bytes.HasPrefix(key, []byte(s3keys.ChunkBlobPrefix)) {
			continue
		}
		sha, valid := s3keys.ParseChunkBlobKey(key)
		if !valid || !bytes.Equal(key, s3keys.ChunkBlobKey(sha)) {
			continue
		}
		versions, _ := it.Value().([]VersionedValue)
		if len(versions) == 0 {
			continue
		}
		ver := versions[len(versions)-1]
		if !ver.Tombstone && ver.ExpireAt == 0 {
			blobs = append(blobs, s3keys.LocalChunkBlob{ContentSHA256: sha, WrittenAtTS: ver.TS})
		}
	}
	return blobs, nil
}

func (s *mvccStore) DeleteChunkBlobIfUnchanged(ctx context.Context, sha [32]byte, writtenAtTS uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, errors.WithStack(err)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.mtx.Lock()
	defer s.mtx.Unlock()
	key := s3keys.ChunkBlobKey(sha)
	ver, ok := s.latestVersionLocked(key)
	if !ok || ver.Tombstone || ver.ExpireAt != 0 || ver.TS != writtenAtTS {
		return false, nil
	}
	s.entryTreeRemove(key)
	return true, nil
}

func (s *pebbleStore) ChunkBlobWrittenAt(ctx context.Context, sha [32]byte) (uint64, bool, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.chunkBlobWrittenAtLocked(ctx, s3keys.ChunkBlobKey(sha))
}

func (s *pebbleStore) chunkBlobWrittenAtLocked(ctx context.Context, key []byte) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, errors.WithStack(err)
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: encodeKey(key, math.MaxUint64), UpperBound: keyUpperBound(key)})
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		if err := ctx.Err(); err != nil {
			return 0, false, errors.WithStack(err)
		}
		userKey, ts := decodeKeyView(it.Key())
		if !bytes.Equal(userKey, key) {
			continue
		}
		_, err := s.readVisibleVersion(it, key, math.MaxUint64)
		if errors.Is(err, ErrKeyNotFound) {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		return ts, true, nil
	}
	return 0, false, errors.WithStack(it.Error())
}

func (s *pebbleStore) ListLocalChunkBlobs(ctx context.Context) ([]s3keys.LocalChunkBlob, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	snap := s.db.NewSnapshot()
	defer snap.Close()
	prefix := []byte(s3keys.ChunkBlobPrefix)
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: keyUpperBound(prefix)})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer it.Close()
	var blobs []s3keys.LocalChunkBlob
	var previous []byte
	for ok := it.First(); ok; ok = it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}
		key, ts := decodeKeyView(it.Key())
		if bytes.Equal(key, previous) {
			continue
		}
		previous = bytes.Clone(key)
		sha, valid := s3keys.ParseChunkBlobKey(key)
		if !valid || !bytes.Equal(key, s3keys.ChunkBlobKey(sha)) {
			continue
		}
		value, err := decodeValue(it.Value())
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if !value.Tombstone && value.ExpireAt == 0 {
			blobs = append(blobs, s3keys.LocalChunkBlob{ContentSHA256: sha, WrittenAtTS: ts})
		}
	}
	return blobs, errors.WithStack(it.Error())
}

func (s *pebbleStore) DeleteChunkBlobIfUnchanged(ctx context.Context, sha [32]byte, writtenAtTS uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, errors.WithStack(err)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	key := s3keys.ChunkBlobKey(sha)
	currentTS, found, err := s.chunkBlobWrittenAtLocked(ctx, key)
	if err != nil || !found || currentTS != writtenAtTS {
		return false, err
	}
	if err := s.deleteChunkBlobVersions(ctx, key); err != nil {
		return false, err
	}
	return true, nil
}

func (s *pebbleStore) deleteChunkBlobVersions(ctx context.Context, key []byte) error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: encodeKey(key, math.MaxUint64), UpperBound: keyUpperBound(key)})
	if err != nil {
		return errors.WithStack(err)
	}
	defer it.Close()
	batch := s.db.NewBatch()
	defer batch.Close()
	for ok := it.First(); ok; ok = it.Next() {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		userKey, _ := decodeKeyView(it.Key())
		if bytes.Equal(userKey, key) {
			if err := batch.Delete(it.Key(), nil); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	if err := it.Error(); err != nil {
		return errors.WithStack(err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

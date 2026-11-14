package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type s3ChunkRefChange struct {
	key    []byte
	before *s3keys.ChunkRefValue
	after  *s3keys.ChunkRefValue
}

const s3ChunkRefMaxRelatedKeys = 2

// addS3ChunkRefAccounting adds RC and GC queue mutations to the same
// transaction as explicit chunkref key changes. It uses the transaction's
// read timestamp and adds every observed key to OCC validation.
func (s *S3Server) addS3ChunkRefAccounting(ctx context.Context, req *kv.OperationGroup[kv.OP]) error {
	if req == nil {
		return errors.New("s3: missing chunkref transaction")
	}
	changes, err := collectS3ChunkRefChanges(req.Elems)
	if err != nil || len(changes) == 0 {
		return err
	}
	if err := validateS3ChunkRefTxn(req); err != nil {
		return err
	}
	readKeys, err := s.loadS3ChunkRefChangesAt(ctx, changes, req.StartTS)
	if err != nil {
		return err
	}
	deltas, removedExisting := s3ChunkRefDeltas(changes)
	current, rcReadKeys, err := s.loadS3ChunkRefRCsAt(ctx, deltas, req.StartTS)
	if err != nil {
		return err
	}
	if err := validateS3ChunkRefCounts(removedExisting, current); err != nil {
		return err
	}
	mutations, err := s3keys.PlanChunkRefRCMutations(deltas, current, req.CommitTS)
	if err != nil {
		return errors.WithStack(err)
	}
	req.ReadKeys = append(req.ReadKeys, readKeys...)
	req.ReadKeys = append(req.ReadKeys, rcReadKeys...)
	appendS3ChunkRefRCMutations(req, mutations)
	return nil
}

func validateS3ChunkRefTxn(req *kv.OperationGroup[kv.OP]) error {
	if !req.IsTxn || req.StartTS == 0 || req.CommitTS == 0 {
		return errors.New("s3: chunkref accounting requires a pinned transaction")
	}
	return nil
}

func appendS3ChunkRefRCMutations(req *kv.OperationGroup[kv.OP], mutations []s3keys.ChunkRefRCMutation) {
	for _, mutation := range mutations {
		op := kv.Put
		if mutation.Delete {
			op = kv.Del
		}
		req.Elems = append(req.Elems, &kv.Elem[kv.OP]{Op: op, Key: mutation.Key, Value: mutation.Value})
	}
}

func collectS3ChunkRefChanges(elems []*kv.Elem[kv.OP]) ([]*s3ChunkRefChange, error) {
	changes := make(map[string]*s3ChunkRefChange)
	order := make([]*s3ChunkRefChange, 0)
	for _, elem := range elems {
		if elem == nil || !bytes.HasPrefix(elem.Key, []byte(s3keys.ChunkRefPrefix)) {
			continue
		}
		if _, _, _, _, _, _, ok := s3keys.ParseChunkRefKey(elem.Key); !ok {
			return nil, errors.New("s3: malformed chunkref key")
		}
		key := string(elem.Key)
		change, seen := changes[key]
		if !seen {
			change = &s3ChunkRefChange{key: append([]byte(nil), elem.Key...)}
			changes[key] = change
			order = append(order, change)
		}
		switch elem.Op {
		case kv.Put:
			ref, ok := s3keys.DecodeChunkRefValue(elem.Value)
			if !ok {
				return nil, errors.New("s3: malformed chunkref value")
			}
			change.after = &ref
		case kv.Del:
			change.after = nil
		case kv.DelPrefix:
			return nil, errors.New("s3: prefix deletion requires explicit chunkref accounting")
		}
	}
	return order, nil
}

func (s *S3Server) readS3ChunkRefAt(ctx context.Context, key []byte, readTS uint64) (*s3keys.ChunkRefValue, error) {
	raw, err := requestGetAt(ctx, s.store, key, readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ref, ok := s3keys.DecodeChunkRefValue(raw)
	if !ok {
		return nil, errors.New("s3: stored chunkref value is malformed")
	}
	return &ref, nil
}

func (s *S3Server) loadS3ChunkRefChangesAt(ctx context.Context, changes []*s3ChunkRefChange, readTS uint64) ([][]byte, error) {
	readKeys := make([][]byte, 0, len(changes))
	for _, change := range changes {
		ref, err := s.readS3ChunkRefAt(ctx, change.key, readTS)
		if err != nil {
			return nil, err
		}
		change.before = ref
		readKeys = append(readKeys, change.key)
	}
	return readKeys, nil
}

func s3ChunkRefDeltas(changes []*s3ChunkRefChange) ([]s3keys.ChunkRefDelta, map[[32]byte]uint64) {
	deltas := make([]s3keys.ChunkRefDelta, 0, len(changes)*s3ChunkRefMaxRelatedKeys)
	removedExisting := make(map[[32]byte]uint64)
	for _, change := range changes {
		if change.before != nil {
			sha := change.before.ContentSHA256
			deltas = append(deltas, s3keys.ChunkRefDelta{ContentSHA256: sha, Removed: 1})
			removedExisting[sha]++
		}
		if change.after != nil {
			deltas = append(deltas, s3keys.ChunkRefDelta{ContentSHA256: change.after.ContentSHA256, Added: 1})
		}
	}
	return deltas, removedExisting
}

func (s *S3Server) readS3ChunkRefRCAt(ctx context.Context, key []byte, readTS uint64) (s3keys.ChunkRefRC, error) {
	raw, err := requestGetAt(ctx, s.store, key, readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return s3keys.ChunkRefRC{}, nil
	}
	if err != nil {
		return s3keys.ChunkRefRC{}, errors.WithStack(err)
	}
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	if !ok {
		return s3keys.ChunkRefRC{}, errors.New("s3: stored chunkref reference count is malformed")
	}
	return rc, nil
}

func (s *S3Server) loadS3ChunkRefRCsAt(ctx context.Context, deltas []s3keys.ChunkRefDelta, readTS uint64) (map[[32]byte]s3keys.ChunkRefRC, [][]byte, error) {
	current := make(map[[32]byte]s3keys.ChunkRefRC)
	readKeys := make([][]byte, 0, len(deltas))
	for _, delta := range deltas {
		sha := delta.ContentSHA256
		if _, seen := current[sha]; seen {
			continue
		}
		key := s3keys.ChunkRefRCKey(sha)
		rc, err := s.readS3ChunkRefRCAt(ctx, key, readTS)
		if err != nil {
			return nil, nil, err
		}
		current[sha] = rc
		readKeys = append(readKeys, key)
	}
	return current, readKeys, nil
}

func validateS3ChunkRefCounts(removedExisting map[[32]byte]uint64, current map[[32]byte]s3keys.ChunkRefRC) error {
	for sha, removed := range removedExisting {
		if current[sha].Count < removed {
			return errors.Wrapf(s3keys.ErrChunkRefRCUnderflow,
				"sha=%x count=%d existing_refs=%d", sha[:4], current[sha].Count, removed)
		}
	}
	return nil
}

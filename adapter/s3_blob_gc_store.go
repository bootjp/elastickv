package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var _ s3keys.ChunkBlobSweepStore = (*S3Server)(nil)
var _ s3keys.ChunkBlobOrphanStore = (*S3Server)(nil)

func (s *S3Server) ScanGCQueue(ctx context.Context, startKey, endKey []byte, limit int) ([]s3keys.ChunkBlobGCQueueEntry, []byte, error) {
	if limit <= 0 {
		return nil, nil, errors.New("s3 gc: queue scan limit must be positive")
	}
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, "s3 gc: scan queue")
	if err != nil {
		return nil, nil, err
	}
	readPin := s.pinReadTS(readTimestamp.Timestamp())
	defer readPin.Release()
	rows, err := requestScanAt(ctx, s.store, startKey, endKey, limit+1, readTimestamp.Timestamp())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	var next []byte
	if len(rows) > limit {
		next = append([]byte(nil), rows[limit].Key...)
		rows = rows[:limit]
	}
	entries := make([]s3keys.ChunkBlobGCQueueEntry, 0, len(rows))
	for _, row := range rows {
		commitTS, sha, ok := s3keys.ParseChunkBlobGCQueueKey(row.Key)
		if !ok || len(row.Value) != 0 {
			return nil, nil, errors.New("s3 gc: malformed queue entry")
		}
		entries = append(entries, s3keys.ChunkBlobGCQueueEntry{CommitTS: commitTS, ContentSHA256: sha})
	}
	return entries, next, nil
}

func (s *S3Server) ReadChunkRefRC(ctx context.Context, sha [32]byte) ([]byte, bool, error) {
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, "s3 gc: read reference count")
	if err != nil {
		return nil, false, err
	}
	readPin := s.pinReadTS(readTimestamp.Timestamp())
	defer readPin.Release()
	raw, err := requestGetAt(ctx, s.store, s3keys.ChunkRefRCKey(sha), readTimestamp.Timestamp())
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	return raw, err == nil, errors.WithStack(err)
}

func (s *S3Server) GCQueueEntryExists(ctx context.Context, sha [32]byte) (bool, error) {
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, "s3 gc: check queue entry")
	if err != nil {
		return false, err
	}
	readPin := s.pinReadTS(readTimestamp.Timestamp())
	defer readPin.Release()
	raw, err := requestGetAt(ctx, s.store, s3keys.ChunkRefRCKey(sha), readTimestamp.Timestamp())
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	if !ok {
		return false, errors.New("s3 gc: malformed reference count")
	}
	if rc.QueuedAtTS == 0 {
		return false, nil
	}
	_, err = requestGetAt(ctx, s.store, s3keys.ChunkBlobGCQueueKey(rc.QueuedAtTS, sha), readTimestamp.Timestamp())
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, errors.WithStack(err)
}

type s3GCQueueMutation uint8

const (
	s3GCDeleteIfUnreferenced s3GCQueueMutation = iota
	s3GCDeleteStaleEntry
	s3GCClearStaleMetadata
)

func (s *S3Server) DeleteGCQueueEntryIfUnreferenced(ctx context.Context, entry s3keys.ChunkBlobGCQueueEntry) error {
	return s.mutateGCQueueEntry(ctx, entry, s3GCDeleteIfUnreferenced)
}

func (s *S3Server) DeleteGCQueueEntry(ctx context.Context, entry s3keys.ChunkBlobGCQueueEntry) error {
	return s.mutateGCQueueEntry(ctx, entry, s3GCDeleteStaleEntry)
}

func (s *S3Server) ClearGCQueueMetadata(ctx context.Context, entry s3keys.ChunkBlobGCQueueEntry) error {
	return s.mutateGCQueueEntry(ctx, entry, s3GCClearStaleMetadata)
}

func (s *S3Server) mutateGCQueueEntry(ctx context.Context, entry s3keys.ChunkBlobGCQueueEntry, mode s3GCQueueMutation) error {
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, "s3 gc: mutate queue")
	if err != nil {
		return err
	}
	readPin := s.pinReadTS(readTimestamp.Timestamp())
	defer readPin.Release()
	queueKey := s3keys.ChunkBlobGCQueueKey(entry.CommitTS, entry.ContentSHA256)
	rcKey := s3keys.ChunkRefRCKey(entry.ContentSHA256)
	rc, rcFound, err := s.readGCQueueStateAt(ctx, queueKey, rcKey, readTimestamp.Timestamp())
	if err != nil {
		return err
	}
	elems, err := planGCQueueMutation(entry, mode, queueKey, rcKey, rc, rcFound)
	if err != nil {
		return err
	}
	commitTS, err := s.nextTxnCommitTS(ctx, readTimestamp.Timestamp())
	if err != nil {
		return err
	}
	req := &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: readTimestamp.Timestamp(), CommitTS: commitTS,
		ReadKeys: [][]byte{queueKey, rcKey}, Elems: elems,
	}
	err = s.dispatchS3MetadataMutation(readTimestamp.WithDispatchVoucher(ctx), req)
	if errors.Is(err, store.ErrWriteConflict) {
		return errors.WithStack(s3keys.ErrQueueEntryChanged)
	}
	return errors.WithStack(err)
}

func (s *S3Server) readGCQueueStateAt(ctx context.Context, queueKey, rcKey []byte, readTS uint64) (s3keys.ChunkRefRC, bool, error) {
	value, err := requestGetAt(ctx, s.store, queueKey, readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return s3keys.ChunkRefRC{}, false, s3keys.ErrQueueEntryChanged
	}
	if err != nil {
		return s3keys.ChunkRefRC{}, false, errors.WithStack(err)
	}
	if len(value) != 0 {
		return s3keys.ChunkRefRC{}, false, errors.New("s3 gc: malformed queue entry")
	}
	raw, err := requestGetAt(ctx, s.store, rcKey, readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return s3keys.ChunkRefRC{}, false, nil
	}
	if err != nil {
		return s3keys.ChunkRefRC{}, false, errors.WithStack(err)
	}
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	if !ok {
		return s3keys.ChunkRefRC{}, false, errors.New("s3 gc: malformed reference count")
	}
	return rc, true, nil
}

func planGCQueueMutation(entry s3keys.ChunkBlobGCQueueEntry, mode s3GCQueueMutation, queueKey, rcKey []byte, rc s3keys.ChunkRefRC, rcFound bool) ([]*kv.Elem[kv.OP], error) {
	elems := []*kv.Elem[kv.OP]{{Op: kv.Del, Key: queueKey}}
	switch mode {
	case s3GCDeleteIfUnreferenced:
		return planUnreferencedGCMutation(entry, elems, rcKey, rc, rcFound)
	case s3GCDeleteStaleEntry:
		return elems, nil
	case s3GCClearStaleMetadata:
		return planClearStaleGCMetadata(entry, elems, rcKey, rc, rcFound)
	default:
		return nil, errors.New("s3 gc: unknown queue mutation")
	}
}

func planUnreferencedGCMutation(entry s3keys.ChunkBlobGCQueueEntry, elems []*kv.Elem[kv.OP], rcKey []byte, rc s3keys.ChunkRefRC, rcFound bool) ([]*kv.Elem[kv.OP], error) {
	if rcFound && (rc.Count != 0 || rc.QueuedAtTS != entry.CommitTS) {
		return nil, s3keys.ErrQueueEntryChanged
	}
	if rcFound {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: rcKey})
	}
	return elems, nil
}

func planClearStaleGCMetadata(entry s3keys.ChunkBlobGCQueueEntry, elems []*kv.Elem[kv.OP], rcKey []byte, rc s3keys.ChunkRefRC, rcFound bool) ([]*kv.Elem[kv.OP], error) {
	if !rcFound || rc.Count == 0 || rc.QueuedAtTS != entry.CommitTS {
		return nil, s3keys.ErrQueueEntryChanged
	}
	rc.QueuedAtTS = 0
	return append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: rcKey, Value: s3keys.EncodeChunkRefRC(rc)}), nil
}

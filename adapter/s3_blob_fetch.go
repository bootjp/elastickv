package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"math"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	s3ChunkBlobSHA256Bytes = 32
	s3BlobFetchFrameBytes  = 256 * 1024

	s3BlobFetchRegistrationRetryInitial = 10 * time.Millisecond
	s3BlobFetchRegistrationRetryMax     = 250 * time.Millisecond
	s3BlobFetchRegistrationRetryFactor  = 2
)

// S3BlobFetchServer serves local content-addressed S3 chunk blobs. It is the
// internal peer-to-peer RPC substrate for the chunkref/chunkblob rollout; it
// deliberately does not enable the public S3 PUT/GET offload path by itself.
type S3BlobFetchServer struct {
	store       store.MVCCStore
	observer    S3BlobOffloadObserver
	clock       *kv.HLC
	pushBlocked func() bool

	pb.UnimplementedS3BlobFetchServer
}

type S3BlobFetchServerOption func(*S3BlobFetchServer)

type s3ChunkBlobMutationStore interface {
	ApplyMutationsPreservingLastCommitTS(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error
}

func WithS3BlobFetchClock(clock *kv.HLC) S3BlobFetchServerOption {
	return func(s *S3BlobFetchServer) {
		s.clock = clock
	}
}

func WithS3BlobFetchPushBlocked(blocked func() bool) S3BlobFetchServerOption {
	return func(s *S3BlobFetchServer) {
		s.pushBlocked = blocked
	}
}

func NewS3BlobFetchServer(st store.MVCCStore, observer S3BlobOffloadObserver, opts ...S3BlobFetchServerOption) *S3BlobFetchServer {
	server := &S3BlobFetchServer{
		store:    st,
		observer: observer,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(server)
		}
	}
	return server
}

func (s *S3BlobFetchServer) FetchChunkBlob(req *pb.FetchChunkBlobRequest, stream pb.S3BlobFetch_FetchChunkBlobServer) error {
	if s == nil || s.store == nil {
		return s3BlobFetchStatus(codes.FailedPrecondition, "s3 blob fetch store is not configured")
	}
	digest, err := s3ChunkBlobDigest(req.GetContentSha256())
	if err != nil {
		return err
	}
	payload, err := s.fetchChunkBlobPayload(stream.Context(), digest)
	if err != nil {
		return err
	}
	if err := s.verifyChunkBlobDigest(digest, payload, codes.InvalidArgument); err != nil {
		return err
	}
	return sendChunkBlobPayload(stream, payload)
}

func (s *S3BlobFetchServer) fetchChunkBlobPayload(ctx context.Context, digest [s3ChunkBlobSHA256Bytes]byte) ([]byte, error) {
	key := s3keys.ChunkBlobKey(digest)
	payload, exists, err := s.currentChunkBlobPayload(ctx, key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, s3BlobFetchStatus(codes.NotFound, "s3 chunkblob not found")
	}
	return payload, nil
}

func sendChunkBlobPayload(stream pb.S3BlobFetch_FetchChunkBlobServer, payload []byte) error {
	if len(payload) == 0 {
		return errors.WithStack(stream.Send(&pb.FetchChunkBlobResponse{Eof: true}))
	}
	for offset := 0; offset < len(payload); {
		end := offset + s3BlobFetchFrameBytes
		if end > len(payload) {
			end = len(payload)
		}
		if err := stream.Send(&pb.FetchChunkBlobResponse{
			Payload: payload[offset:end],
			Eof:     end == len(payload),
		}); err != nil {
			return errors.WithStack(err)
		}
		offset = end
	}
	return nil
}

func (s *S3BlobFetchServer) PushChunkBlob(stream pb.S3BlobFetch_PushChunkBlobServer) error {
	if s == nil || s.store == nil {
		return s3BlobFetchStatus(codes.FailedPrecondition, "s3 blob fetch server is not configured")
	}
	if err := s.ensurePushAllowed(); err != nil {
		return err
	}
	digest, payload, commitTS, err := s.recvChunkBlob(stream)
	if err != nil {
		return err
	}
	if err := s.verifyChunkBlobDigest(digest, payload, codes.InvalidArgument); err != nil {
		return err
	}
	if err := s.ensurePushAllowed(); err != nil {
		return err
	}
	if err := s.storeChunkBlob(stream.Context(), digest, payload, commitTS); err != nil {
		return err
	}
	return sendChunkBlobPushAck(stream)
}

func (s *S3BlobFetchServer) storeChunkBlob(
	ctx context.Context,
	digest [s3ChunkBlobSHA256Bytes]byte,
	payload []byte,
	commitTS uint64,
) error {
	key := s3keys.ChunkBlobKey(digest)
	for {
		if err := s.ensurePushAllowed(); err != nil {
			return err
		}
		startTS, err := s.chunkBlobWriteStartTS(ctx, key, digest, payload, commitTS)
		if err != nil {
			return err
		}
		if startTS == commitTS {
			s.observeCommitTS(commitTS)
			return nil
		}
		if err := s.applyChunkBlobUntilRegistered(ctx, key, payload, startTS, commitTS); err != nil {
			done, retry, retryErr := s.retryAfterChunkBlobWriteConflict(ctx, err, key, digest, payload, commitTS)
			if retryErr != nil {
				return retryErr
			}
			if done {
				s.observeCommitTS(commitTS)
				return nil
			}
			if retry {
				continue
			}
			if code := status.Code(err); code != codes.Unknown {
				return err
			}
			return s3BlobFetchStatusf(codes.Internal, "write s3 chunkblob: %v", err)
		}
		s.observeCommitTS(commitTS)
		return nil
	}
}

func (s *S3BlobFetchServer) retryAfterChunkBlobWriteConflict(
	ctx context.Context,
	err error,
	key []byte,
	digest [s3ChunkBlobSHA256Bytes]byte,
	payload []byte,
	commitTS uint64,
) (bool, bool, error) {
	if !errors.Is(err, store.ErrWriteConflict) {
		return false, false, nil
	}
	latestTS, exists, latestErr := s.latestChunkBlobTS(ctx, key)
	if latestErr != nil {
		return false, false, latestErr
	}
	stored, storedErr := s.chunkBlobAlreadyStored(ctx, key, digest, payload)
	if storedErr != nil {
		return false, false, storedErr
	}
	if stored {
		return exists && latestTS >= commitTS, exists && latestTS < commitTS, nil
	}
	if exists && latestTS < commitTS {
		return false, true, nil
	}
	return false, false, s3BlobFetchStatusf(
		codes.FailedPrecondition,
		"s3 chunkblob commit timestamp %d is not after latest version %d",
		commitTS,
		latestTS,
	)
}

func sendChunkBlobPushAck(stream pb.S3BlobFetch_PushChunkBlobServer) error {
	return errors.WithStack(stream.SendAndClose(&pb.PushChunkBlobResponse{Durable: true}))
}

func (s *S3BlobFetchServer) currentChunkBlobPayload(ctx context.Context, key []byte) ([]byte, bool, error) {
	payload, err := s.store.GetAt(ctx, key, math.MaxUint64)
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, s3BlobFetchStatusf(codes.Internal, "read s3 chunkblob: %v", err)
	}
	return payload, true, nil
}

func (s *S3BlobFetchServer) chunkBlobAlreadyStored(
	ctx context.Context,
	key []byte,
	digest [s3ChunkBlobSHA256Bytes]byte,
	payload []byte,
) (bool, error) {
	existing, exists, err := s.currentChunkBlobPayload(ctx, key)
	if err != nil || !exists {
		return false, err
	}
	if bytes.Equal(existing, payload) {
		return true, nil
	}
	if err := s.verifyChunkBlobDigest(digest, existing, codes.InvalidArgument); err != nil {
		return false, err
	}
	return false, s3BlobFetchStatus(codes.InvalidArgument, "s3 chunkblob already exists with different payload")
}

func (s *S3BlobFetchServer) chunkBlobWriteStartTS(
	ctx context.Context,
	key []byte,
	digest [s3ChunkBlobSHA256Bytes]byte,
	payload []byte,
	commitTS uint64,
) (uint64, error) {
	if commitTS == 0 {
		return 0, s3BlobFetchStatus(codes.InvalidArgument, "missing s3 chunkblob commit timestamp")
	}
	latestTS, exists, err := s.latestChunkBlobTS(ctx, key)
	if err != nil {
		return 0, err
	}
	if exists && commitTS <= latestTS {
		stored, storedErr := s.chunkBlobAlreadyStored(ctx, key, digest, payload)
		if storedErr != nil {
			return 0, storedErr
		}
		if stored {
			return commitTS, nil
		}
		return 0, s3BlobFetchStatusf(
			codes.FailedPrecondition,
			"s3 chunkblob commit timestamp %d is not after latest version %d",
			commitTS,
			latestTS,
		)
	}
	return latestTS, nil
}

func (s *S3BlobFetchServer) latestChunkBlobTS(ctx context.Context, key []byte) (uint64, bool, error) {
	latestTS, exists, err := s.store.LatestCommitTS(ctx, key)
	if err != nil {
		return 0, false, s3BlobFetchStatusf(codes.Internal, "read s3 chunkblob latest timestamp: %v", err)
	}
	return latestTS, exists, nil
}

func (s *S3BlobFetchServer) applyChunkBlob(ctx context.Context, key, payload []byte, startTS, commitTS uint64) error {
	chunkStore, ok := s.store.(s3ChunkBlobMutationStore)
	if !ok {
		return s3BlobFetchStatus(codes.FailedPrecondition, "s3 chunkblob store cannot preserve mvcc watermark")
	}
	if err := chunkStore.ApplyMutationsPreservingLastCommitTS(ctx, []*store.KVPairMutation{{
		Op:    store.OpTypePut,
		Key:   key,
		Value: payload,
	}}, nil, startTS, commitTS); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *S3BlobFetchServer) applyChunkBlobUntilRegistered(ctx context.Context, key, payload []byte, startTS, commitTS uint64) error {
	backoff := s3BlobFetchRegistrationRetryInitial
	for {
		if err := s.ensurePushAllowed(); err != nil {
			return err
		}
		err := s.applyChunkBlob(ctx, key, payload, startTS, commitTS)
		if err == nil || !errors.Is(err, store.ErrWriterNotRegistered) {
			return err
		}
		select {
		case <-ctx.Done():
			return s3BlobFetchStatusf(codes.Unavailable, "s3 chunkblob writer registration: %v", ctx.Err())
		case <-time.After(backoff):
			backoff *= s3BlobFetchRegistrationRetryFactor
			if backoff > s3BlobFetchRegistrationRetryMax {
				backoff = s3BlobFetchRegistrationRetryMax
			}
		}
	}
}

func (s *S3BlobFetchServer) recvChunkBlob(stream pb.S3BlobFetch_PushChunkBlobServer) ([s3ChunkBlobSHA256Bytes]byte, []byte, uint64, error) {
	var state s3ChunkBlobReceiveState
	for {
		if err := s.ensurePushAllowed(); err != nil {
			return state.digest, nil, 0, err
		}
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return state.finish()
		}
		if err != nil {
			return state.digest, nil, 0, errors.WithStack(err)
		}
		if err := state.apply(req); err != nil {
			return state.digest, nil, 0, err
		}
	}
}

func (s *S3BlobFetchServer) ensurePushAllowed() error {
	if s != nil && s.pushBlocked != nil && s.pushBlocked() {
		return s3BlobFetchStatus(codes.Unavailable, "startup rotation has not completed")
	}
	return nil
}

func (s *S3BlobFetchServer) observeCommitTS(commitTS uint64) {
	if s != nil && s.clock != nil {
		s.clock.Observe(commitTS)
	}
}

type s3ChunkBlobReceiveState struct {
	digest       [s3ChunkBlobSHA256Bytes]byte
	commitTS     uint64
	haveDigest   bool
	haveCommitTS bool
	seenEOF      bool
	payload      bytes.Buffer
}

func (s *s3ChunkBlobReceiveState) finish() ([s3ChunkBlobSHA256Bytes]byte, []byte, uint64, error) {
	if !s.haveDigest {
		return s.digest, nil, 0, s3BlobFetchStatus(codes.InvalidArgument, "missing s3 chunkblob sha256")
	}
	if !s.haveCommitTS {
		return s.digest, nil, 0, s3BlobFetchStatus(codes.InvalidArgument, "missing s3 chunkblob commit timestamp")
	}
	if !s.seenEOF {
		return s.digest, nil, 0, s3BlobFetchStatus(codes.InvalidArgument, "missing s3 chunkblob eof")
	}
	return s.digest, s.payload.Bytes(), s.commitTS, nil
}

func (s *s3ChunkBlobReceiveState) apply(req *pb.PushChunkBlobRequest) error {
	if req == nil {
		return s3BlobFetchStatus(codes.InvalidArgument, "nil s3 chunkblob request")
	}
	if s.seenEOF {
		return s3BlobFetchStatus(codes.InvalidArgument, "s3 chunkblob frame after eof")
	}
	if err := s.applyDigest(req.GetContentSha256()); err != nil {
		return err
	}
	if !s.haveDigest {
		return s3BlobFetchStatus(codes.InvalidArgument, "first s3 chunkblob frame must include sha256")
	}
	if err := s.applyCommitTS(req.GetCommitTs()); err != nil {
		return err
	}
	if !s.haveCommitTS {
		return s3BlobFetchStatus(codes.InvalidArgument, "first s3 chunkblob frame must include commit timestamp")
	}
	if s.payload.Len()+len(req.GetPayload()) > s3ChunkSize {
		return s3BlobFetchStatus(codes.ResourceExhausted, "s3 chunkblob payload exceeds chunk size")
	}
	if _, err := s.payload.Write(req.GetPayload()); err != nil {
		return s3BlobFetchStatusf(codes.Internal, "buffer s3 chunkblob: %v", err)
	}
	if req.GetEof() {
		s.seenEOF = true
	}
	return nil
}

func (s *s3ChunkBlobReceiveState) applyCommitTS(commitTS uint64) error {
	if commitTS == 0 {
		return nil
	}
	if s.haveCommitTS && commitTS != s.commitTS {
		return s3BlobFetchStatus(codes.InvalidArgument, "s3 chunkblob commit timestamp changed mid-stream")
	}
	s.commitTS = commitTS
	s.haveCommitTS = true
	return nil
}

func (s *s3ChunkBlobReceiveState) applyDigest(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	digest, err := s3ChunkBlobDigest(raw)
	if err != nil {
		return err
	}
	if s.haveDigest && digest != s.digest {
		return s3BlobFetchStatus(codes.InvalidArgument, "s3 chunkblob sha256 changed mid-stream")
	}
	s.digest = digest
	s.haveDigest = true
	return nil
}

func (s *S3BlobFetchServer) verifyChunkBlobDigest(expected [s3ChunkBlobSHA256Bytes]byte, payload []byte, code codes.Code) error {
	actual := sha256.Sum256(payload)
	if actual == expected {
		return nil
	}
	s.observeSHAMismatch()
	return s3BlobFetchStatus(code, "s3 chunkblob sha256 mismatch")
}

func s3ChunkBlobDigest(raw []byte) ([s3ChunkBlobSHA256Bytes]byte, error) {
	var digest [s3ChunkBlobSHA256Bytes]byte
	if len(raw) != s3ChunkBlobSHA256Bytes {
		return digest, s3BlobFetchStatusf(codes.InvalidArgument, "s3 chunkblob sha256 must be %d bytes", s3ChunkBlobSHA256Bytes)
	}
	copy(digest[:], raw)
	return digest, nil
}

func (s *S3BlobFetchServer) observeSHAMismatch() {
	if s != nil && s.observer != nil {
		s.observer.ObserveS3ChunkBlobSHAMismatch()
	}
}

func s3BlobFetchStatus(code codes.Code, msg string) error {
	return errors.WithStack(status.Error(code, msg))
}

func s3BlobFetchStatusf(code codes.Code, format string, args ...any) error {
	return errors.WithStack(status.Errorf(code, format, args...))
}

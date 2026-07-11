package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestS3BlobFetchPushStoresAndFetchStreamsPayload(t *testing.T) {
	t.Parallel()

	st := &recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	observer := &recordingS3BlobOffloadObserver{}
	clock := &s3BlobFetchTestClock{next: 99}
	server := NewS3BlobFetchServer(st, clock, observer)
	payload := bytes.Repeat([]byte("payload-"), (s3BlobFetchFrameBytes/len("payload-"))+2)
	digest := sha256.Sum256(payload)

	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], Payload: payload[:17]},
			{Payload: payload[17:], Eof: true},
		},
	}
	require.NoError(t, server.PushChunkBlob(push))
	require.True(t, push.response.GetDurable())
	require.Equal(t, 1, st.applyCalls)
	require.Zero(t, st.putCalls)
	require.Equal(t, []uint64{100}, st.applyCommitTS)
	require.Equal(t, 1, clock.calls)

	key := s3keys.ChunkBlobKey(digest)
	readTS, exists, err := st.LatestCommitTS(context.Background(), key)
	require.NoError(t, err)
	require.True(t, exists)
	stored, err := st.GetAt(context.Background(), key, readTS)
	require.NoError(t, err)
	require.Equal(t, payload, stored)

	fetch := &recordingS3BlobFetchStream{ctx: context.Background()}
	require.NoError(t, server.FetchChunkBlob(&pb.FetchChunkBlobRequest{ContentSha256: digest[:]}, fetch))
	require.Greater(t, len(fetch.responses), 1)
	require.True(t, fetch.responses[len(fetch.responses)-1].GetEof())
	for _, resp := range fetch.responses[:len(fetch.responses)-1] {
		require.False(t, resp.GetEof())
	}
	var fetched []byte
	for _, resp := range fetch.responses {
		fetched = append(fetched, resp.GetPayload()...)
	}
	require.Equal(t, payload, fetched)
	require.Zero(t, observer.shaMismatch)
}

func TestS3BlobFetchRejectsMissingBlob(t *testing.T) {
	t.Parallel()

	server := NewS3BlobFetchServer(store.NewMVCCStore(), &s3BlobFetchTestClock{}, nil)
	digest := sha256.Sum256([]byte("missing"))
	fetch := &recordingS3BlobFetchStream{ctx: context.Background()}

	err := server.FetchChunkBlob(&pb.FetchChunkBlobRequest{ContentSha256: digest[:]}, fetch)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Empty(t, fetch.responses)
}

func TestS3BlobFetchRejectsPushDigestMismatch(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	observer := &recordingS3BlobOffloadObserver{}
	server := NewS3BlobFetchServer(st, &s3BlobFetchTestClock{}, observer)
	wrongDigest := sha256.Sum256([]byte("expected"))
	payload := []byte("actual")
	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: wrongDigest[:], Payload: payload, Eof: true},
		},
	}

	err := server.PushChunkBlob(push)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Nil(t, push.response)
	_, exists, latestErr := st.LatestCommitTS(context.Background(), s3keys.ChunkBlobKey(wrongDigest))
	require.NoError(t, latestErr)
	require.False(t, exists)
	require.Equal(t, 1, observer.shaMismatch)
}

func TestS3BlobFetchRejectsOversizedPush(t *testing.T) {
	t.Parallel()

	server := NewS3BlobFetchServer(store.NewMVCCStore(), &s3BlobFetchTestClock{}, nil)
	payload := bytes.Repeat([]byte{0xab}, s3ChunkSize+1)
	digest := sha256.Sum256(payload)
	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], Payload: payload, Eof: true},
		},
	}

	err := server.PushChunkBlob(push)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Nil(t, push.response)
}

func TestS3BlobFetchRejectsInvalidDigestLength(t *testing.T) {
	t.Parallel()

	server := NewS3BlobFetchServer(store.NewMVCCStore(), &s3BlobFetchTestClock{}, nil)
	fetch := &recordingS3BlobFetchStream{ctx: context.Background()}
	err := server.FetchChunkBlob(&pb.FetchChunkBlobRequest{ContentSha256: []byte("short")}, fetch)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: []byte("short"), Eof: true},
		},
	}
	err = server.PushChunkBlob(push)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Nil(t, push.response)
}

func TestS3BlobFetchRejectsFrameAfterEOF(t *testing.T) {
	t.Parallel()

	server := NewS3BlobFetchServer(store.NewMVCCStore(), &s3BlobFetchTestClock{}, nil)
	digest := sha256.Sum256([]byte("payloadextra"))
	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], Payload: []byte("payload"), Eof: true},
			{Payload: []byte("extra")},
		},
	}

	err := server.PushChunkBlob(push)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Nil(t, push.response)
}

func TestS3BlobFetchPushIsIdempotent(t *testing.T) {
	t.Parallel()

	st := &recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	clock := &s3BlobFetchTestClock{next: 41}
	server := NewS3BlobFetchServer(st, clock, nil)
	payload := []byte("idempotent payload")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStream(digest, payload)))
	require.NoError(t, server.PushChunkBlob(newS3BlobPushStream(digest, payload)))

	require.Equal(t, 1, st.applyCalls)
	require.Zero(t, st.putCalls)
	require.Equal(t, []uint64{42}, st.applyCommitTS)
	require.Equal(t, 1, clock.calls)
}

func TestS3BlobFetchPushAcknowledgesConcurrentMatchingWrite(t *testing.T) {
	t.Parallel()

	st := &conflictOnceS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	clock := &s3BlobFetchTestClock{next: 50}
	server := NewS3BlobFetchServer(st, clock, nil)
	payload := []byte("concurrent payload")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStream(digest, payload)))
	require.Equal(t, 1, st.applyCalls)
	require.Equal(t, 1, clock.calls)
}

func TestS3BlobFetchPushFailsWhenFenceExpired(t *testing.T) {
	t.Parallel()

	st := &recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	server := NewS3BlobFetchServer(st, &s3BlobFetchTestClock{err: kv.ErrCeilingExpired}, nil)
	payload := []byte("fenced payload")
	digest := sha256.Sum256(payload)

	err := server.PushChunkBlob(newS3BlobPushStream(digest, payload))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, st.applyCalls)
	require.Zero(t, st.putCalls)
	_, exists, latestErr := st.LatestCommitTS(context.Background(), s3keys.ChunkBlobKey(digest))
	require.NoError(t, latestErr)
	require.False(t, exists)
}

func TestS3BlobFetchFetchReadsInMemoryAfterCompaction(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	requireFetchAfterCompaction(t, st)
}

func TestS3BlobFetchFetchReadsPebbleAfterCompaction(t *testing.T) {
	t.Parallel()

	st, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	requireFetchAfterCompaction(t, st)
}

func requireFetchAfterCompaction(t *testing.T, st store.MVCCStore) {
	t.Helper()

	payload := []byte("compacted payload")
	digest := sha256.Sum256(payload)
	key := s3keys.ChunkBlobKey(digest)
	require.NoError(t, st.PutAt(context.Background(), key, payload, 10, 0))
	require.NoError(t, st.Compact(context.Background(), 20))
	_, err := st.GetAt(context.Background(), key, 10)
	require.ErrorIs(t, err, store.ErrReadTSCompacted)

	server := NewS3BlobFetchServer(st, nil, nil)
	fetch := &recordingS3BlobFetchStream{ctx: context.Background()}
	require.NoError(t, server.FetchChunkBlob(&pb.FetchChunkBlobRequest{ContentSha256: digest[:]}, fetch))
	require.Len(t, fetch.responses, 1)
	require.Equal(t, payload, fetch.responses[0].GetPayload())
	require.True(t, fetch.responses[0].GetEof())
}

func TestS3BlobFetchFetchDigestMismatchReturnsInvalidArgument(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	observer := &recordingS3BlobOffloadObserver{}
	expected := sha256.Sum256([]byte("expected payload"))
	require.NoError(t, st.PutAt(context.Background(), s3keys.ChunkBlobKey(expected), []byte("corrupt payload"), 10, 0))

	server := NewS3BlobFetchServer(st, nil, observer)
	fetch := &recordingS3BlobFetchStream{ctx: context.Background()}
	err := server.FetchChunkBlob(&pb.FetchChunkBlobRequest{ContentSha256: expected[:]}, fetch)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Empty(t, fetch.responses)
	require.Equal(t, 1, observer.shaMismatch)
}

func newS3BlobPushStream(digest [s3ChunkBlobSHA256Bytes]byte, payload []byte) *recordingS3BlobPushStream {
	return &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], Payload: payload, Eof: true},
		},
	}
}

type recordingS3BlobFetchStream struct {
	ctx       context.Context
	responses []*pb.FetchChunkBlobResponse
}

func (s *recordingS3BlobFetchStream) Send(resp *pb.FetchChunkBlobResponse) error {
	s.responses = append(s.responses, &pb.FetchChunkBlobResponse{
		Payload: bytes.Clone(resp.GetPayload()),
		Eof:     resp.GetEof(),
	})
	return nil
}

func (*recordingS3BlobFetchStream) SetHeader(metadata.MD) error { return nil }

func (*recordingS3BlobFetchStream) SendHeader(metadata.MD) error { return nil }

func (*recordingS3BlobFetchStream) SetTrailer(metadata.MD) {}

func (s *recordingS3BlobFetchStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (*recordingS3BlobFetchStream) SendMsg(any) error { return nil }

func (*recordingS3BlobFetchStream) RecvMsg(any) error { return nil }

type recordingS3BlobPushStream struct {
	ctx      context.Context
	reqs     []*pb.PushChunkBlobRequest
	next     int
	response *pb.PushChunkBlobResponse
}

func (s *recordingS3BlobPushStream) Recv() (*pb.PushChunkBlobRequest, error) {
	if s.next >= len(s.reqs) {
		return nil, io.EOF
	}
	req := s.reqs[s.next]
	s.next++
	return req, nil
}

func (s *recordingS3BlobPushStream) SendAndClose(resp *pb.PushChunkBlobResponse) error {
	s.response = resp
	return nil
}

func (*recordingS3BlobPushStream) SetHeader(metadata.MD) error { return nil }

func (*recordingS3BlobPushStream) SendHeader(metadata.MD) error { return nil }

func (*recordingS3BlobPushStream) SetTrailer(metadata.MD) {}

func (s *recordingS3BlobPushStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (*recordingS3BlobPushStream) SendMsg(any) error { return nil }

func (*recordingS3BlobPushStream) RecvMsg(any) error { return nil }

type s3BlobFetchTestClock struct {
	next  uint64
	calls int
	err   error
}

func (c *s3BlobFetchTestClock) NextFenced() (uint64, error) {
	c.calls++
	if c.err != nil {
		return 0, c.err
	}
	c.next++
	return c.next, nil
}

type recordingS3BlobFetchStore struct {
	store.MVCCStore
	applyCalls    int
	putCalls      int
	applyCommitTS []uint64
}

func (s *recordingS3BlobFetchStore) PutAt(context.Context, []byte, []byte, uint64, uint64) error {
	s.putCalls++
	return errors.New("unexpected PutAt call for S3 chunkblob durability")
}

func (s *recordingS3BlobFetchStore) ApplyMutations(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	s.applyCommitTS = append(s.applyCommitTS, commitTS)
	return s.MVCCStore.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS)
}

type conflictOnceS3BlobFetchStore struct {
	store.MVCCStore
	applyCalls int
}

func (s *conflictOnceS3BlobFetchStore) ApplyMutations(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	if s.applyCalls == 1 {
		if err := s.MVCCStore.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS); err != nil {
			return err
		}
		return store.ErrWriteConflict
	}
	return s.MVCCStore.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS)
}

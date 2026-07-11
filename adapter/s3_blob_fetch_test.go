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

type s3BlobFetchPreservingStore interface {
	ApplyMutationsPreservingLastCommitTS(context.Context, []*store.KVPairMutation, [][]byte, uint64, uint64) error
}

type s3BlobFetchLastCommitTSStore interface {
	LastCommitTS() uint64
}

func requireS3BlobFetchLastCommitTS(t *testing.T, st store.MVCCStore, want uint64) {
	t.Helper()
	reader, ok := st.(s3BlobFetchLastCommitTSStore)
	require.True(t, ok)
	require.Equal(t, want, reader.LastCommitTS())
}

func applyS3BlobFetchMutationsPreservingLastCommitTS(
	st store.MVCCStore,
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	preserving, ok := st.(s3BlobFetchPreservingStore)
	if !ok {
		return errors.New("s3 blob fetch test store cannot preserve last commit timestamp")
	}
	return preserving.ApplyMutationsPreservingLastCommitTS(ctx, mutations, readKeys, startTS, commitTS)
}

func TestS3BlobFetchPushStoresAndFetchStreamsPayload(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	st := &recordingS3BlobFetchStore{MVCCStore: base}
	observer := &recordingS3BlobOffloadObserver{}
	server := NewS3BlobFetchServer(st, observer)
	payload := bytes.Repeat([]byte("payload-"), (s3BlobFetchFrameBytes/len("payload-"))+2)
	digest := sha256.Sum256(payload)

	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], CommitTs: 100, Payload: payload[:17]},
			{Payload: payload[17:], Eof: true},
		},
	}
	require.NoError(t, server.PushChunkBlob(push))
	require.True(t, push.response.GetDurable())
	require.Equal(t, 1, st.applyCalls)
	require.Zero(t, st.putCalls)
	require.Equal(t, []uint64{100}, st.applyCommitTS)
	requireS3BlobFetchLastCommitTS(t, base, 0)

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

	server := NewS3BlobFetchServer(store.NewMVCCStore(), nil)
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
	server := NewS3BlobFetchServer(st, observer)
	wrongDigest := sha256.Sum256([]byte("expected"))
	payload := []byte("actual")
	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: wrongDigest[:], CommitTs: 1, Payload: payload, Eof: true},
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

	server := NewS3BlobFetchServer(store.NewMVCCStore(), nil)
	payload := bytes.Repeat([]byte{0xab}, s3ChunkSize+1)
	digest := sha256.Sum256(payload)
	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], CommitTs: 1, Payload: payload, Eof: true},
		},
	}

	err := server.PushChunkBlob(push)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Nil(t, push.response)
}

func TestS3BlobFetchRejectsInvalidDigestLength(t *testing.T) {
	t.Parallel()

	server := NewS3BlobFetchServer(store.NewMVCCStore(), nil)
	fetch := &recordingS3BlobFetchStream{ctx: context.Background()}
	err := server.FetchChunkBlob(&pb.FetchChunkBlobRequest{ContentSha256: []byte("short")}, fetch)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: []byte("short"), CommitTs: 1, Eof: true},
		},
	}
	err = server.PushChunkBlob(push)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Nil(t, push.response)
}

func TestS3BlobFetchRejectsFrameAfterEOF(t *testing.T) {
	t.Parallel()

	server := NewS3BlobFetchServer(store.NewMVCCStore(), nil)
	digest := sha256.Sum256([]byte("payloadextra"))
	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], CommitTs: 1, Payload: []byte("payload"), Eof: true},
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
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("idempotent payload")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 42)))
	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 43)))

	require.Equal(t, 2, st.applyCalls)
	require.Zero(t, st.putCalls)
	require.Equal(t, []uint64{42, 43}, st.applyCommitTS)

	key := s3keys.ChunkBlobKey(digest)
	latestTS, exists, err := st.LatestCommitTS(context.Background(), key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(43), latestTS)
}

func TestS3BlobFetchPushRestampsExistingPayloadAtLeaderCommitTS(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	st := &recordingS3BlobFetchStore{MVCCStore: base}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("already present payload")
	digest := sha256.Sum256(payload)
	key := s3keys.ChunkBlobKey(digest)
	require.NoError(t, base.PutAt(context.Background(), key, payload, 10, 0))

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 30)))

	require.Equal(t, []uint64{30}, st.applyCommitTS)
	requireS3BlobFetchLastCommitTS(t, base, 10)
	latestTS, exists, err := st.LatestCommitTS(context.Background(), key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(30), latestTS)
}

func TestS3BlobFetchPushDoesNotAdvanceMVCCWatermark(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	st := &recordingS3BlobFetchStore{MVCCStore: base}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("remote leader timestamp must not become local watermark")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 500)))

	requireS3BlobFetchLastCommitTS(t, base, 0)
	key := s3keys.ChunkBlobKey(digest)
	latestTS, exists, err := st.LatestCommitTS(context.Background(), key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(500), latestTS)
	got, err := st.GetAt(context.Background(), key, 500)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestS3BlobFetchPushDoesNotAdvancePebbleMVCCWatermark(t *testing.T) {
	t.Parallel()

	base, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	st := &recordingS3BlobFetchStore{MVCCStore: base}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("remote leader timestamp must not become pebble watermark")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 700)))

	require.Equal(t, uint64(0), base.LastCommitTS())
	key := s3keys.ChunkBlobKey(digest)
	latestTS, exists, latestErr := st.LatestCommitTS(context.Background(), key)
	require.NoError(t, latestErr)
	require.True(t, exists)
	require.Equal(t, uint64(700), latestTS)
	got, getErr := st.GetAt(context.Background(), key, 700)
	require.NoError(t, getErr)
	require.Equal(t, payload, got)
}

func TestS3BlobFetchPushAcknowledgesConcurrentMatchingWrite(t *testing.T) {
	t.Parallel()

	st := &conflictOnceS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("concurrent payload")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 51)))
	require.Equal(t, 1, st.applyCalls)
}

func TestS3BlobFetchPushRetriesAfterConcurrentTombstone(t *testing.T) {
	t.Parallel()

	st := &tombstoneConflictS3BlobFetchStore{
		recordingS3BlobFetchStore: recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()},
		tombstoneTS:               20,
	}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("retry after tombstone")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 30)))
	require.Equal(t, 2, st.applyCalls)
	require.Equal(t, []uint64{0, 20}, st.applyStartTS)
	require.Equal(t, []uint64{30, 30}, st.applyCommitTS)
}

func TestS3BlobFetchPushObservesLeaderCommitTS(t *testing.T) {
	t.Parallel()

	clock := kv.NewHLC()
	server := NewS3BlobFetchServer(
		store.NewMVCCStore(),
		nil,
		WithS3BlobFetchClock(clock),
	)
	payload := []byte("observe commit timestamp")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 100)))
	require.Equal(t, uint64(100), clock.Current())
}

func TestS3BlobFetchPushRechecksGateBetweenFrames(t *testing.T) {
	t.Parallel()

	st := &recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	blocked := false
	server := NewS3BlobFetchServer(
		st,
		nil,
		WithS3BlobFetchPushBlocked(func() bool { return blocked }),
	)
	payload := []byte("gate closes while streaming")
	digest := sha256.Sum256(payload)
	push := &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], CommitTs: 10, Payload: payload[:4]},
			{Payload: payload[4:], Eof: true},
		},
		afterRecv: func(n int) {
			if n == 1 {
				blocked = true
			}
		},
	}

	err := server.PushChunkBlob(push)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, st.applyCalls)
}

func TestS3BlobFetchRejectsPushMissingCommitTS(t *testing.T) {
	t.Parallel()

	st := &recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("missing commit timestamp")
	digest := sha256.Sum256(payload)

	err := server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 0))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, st.applyCalls)
	require.Zero(t, st.putCalls)
	_, exists, latestErr := st.LatestCommitTS(context.Background(), s3keys.ChunkBlobKey(digest))
	require.NoError(t, latestErr)
	require.False(t, exists)
}

func TestS3BlobFetchPushRecreatesAfterTombstone(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	st := &recordingS3BlobFetchStore{MVCCStore: base}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("recreated payload")
	digest := sha256.Sum256(payload)
	key := s3keys.ChunkBlobKey(digest)
	require.NoError(t, st.DeleteAt(context.Background(), key, 100))

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 101)))
	require.Equal(t, []uint64{101}, st.applyCommitTS)
	requireS3BlobFetchLastCommitTS(t, base, 100)

	stored, err := st.GetAt(context.Background(), key, 101)
	require.NoError(t, err)
	require.Equal(t, payload, stored)
}

func TestS3BlobFetchPushRejectsStaleCommitTSAfterTombstone(t *testing.T) {
	t.Parallel()

	st := &recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("stale payload")
	digest := sha256.Sum256(payload)
	key := s3keys.ChunkBlobKey(digest)
	require.NoError(t, st.DeleteAt(context.Background(), key, 100))

	err := server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 100))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, st.applyCalls)
}

func TestS3BlobFetchPushRetriesUntilWriterRegistered(t *testing.T) {
	t.Parallel()

	st := &registrationOnceS3BlobFetchStore{recordingS3BlobFetchStore: recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}}
	server := NewS3BlobFetchServer(st, nil)
	payload := []byte("registration retry payload")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 12)))
	require.Equal(t, 2, st.applyCalls)
}

func TestS3BlobFetchPushRechecksGateDuringRegistrationRetry(t *testing.T) {
	t.Parallel()

	blocked := false
	st := &registrationOnceS3BlobFetchStore{
		recordingS3BlobFetchStore: recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()},
		afterFirstErr:             func() { blocked = true },
	}
	server := NewS3BlobFetchServer(
		st,
		nil,
		WithS3BlobFetchPushBlocked(func() bool { return blocked }),
	)
	payload := []byte("registration retry gate closes")
	digest := sha256.Sum256(payload)

	err := server.PushChunkBlob(newS3BlobPushStreamAt(digest, payload, 12))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 1, st.applyCalls)
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

	server := NewS3BlobFetchServer(st, nil)
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

	server := NewS3BlobFetchServer(st, observer)
	fetch := &recordingS3BlobFetchStream{ctx: context.Background()}
	err := server.FetchChunkBlob(&pb.FetchChunkBlobRequest{ContentSha256: expected[:]}, fetch)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Empty(t, fetch.responses)
	require.Equal(t, 1, observer.shaMismatch)
}

func newS3BlobPushStreamAt(digest [s3ChunkBlobSHA256Bytes]byte, payload []byte, commitTS uint64) *recordingS3BlobPushStream {
	return &recordingS3BlobPushStream{
		ctx: context.Background(),
		reqs: []*pb.PushChunkBlobRequest{
			{ContentSha256: digest[:], CommitTs: commitTS, Payload: payload, Eof: true},
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
	ctx       context.Context
	reqs      []*pb.PushChunkBlobRequest
	next      int
	afterRecv func(int)
	response  *pb.PushChunkBlobResponse
}

func (s *recordingS3BlobPushStream) Recv() (*pb.PushChunkBlobRequest, error) {
	if s.next >= len(s.reqs) {
		return nil, io.EOF
	}
	req := s.reqs[s.next]
	s.next++
	if s.afterRecv != nil {
		s.afterRecv(s.next)
	}
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

type recordingS3BlobFetchStore struct {
	store.MVCCStore
	applyCalls    int
	putCalls      int
	applyStartTS  []uint64
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
	s.applyStartTS = append(s.applyStartTS, startTS)
	s.applyCommitTS = append(s.applyCommitTS, commitTS)
	return s.MVCCStore.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS)
}

func (s *recordingS3BlobFetchStore) ApplyMutationsPreservingLastCommitTS(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	s.applyStartTS = append(s.applyStartTS, startTS)
	s.applyCommitTS = append(s.applyCommitTS, commitTS)
	return applyS3BlobFetchMutationsPreservingLastCommitTS(s.MVCCStore, ctx, mutations, readKeys, startTS, commitTS)
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

func (s *conflictOnceS3BlobFetchStore) ApplyMutationsPreservingLastCommitTS(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	if s.applyCalls == 1 {
		if err := applyS3BlobFetchMutationsPreservingLastCommitTS(s.MVCCStore, ctx, mutations, readKeys, startTS, commitTS); err != nil {
			return err
		}
		return store.ErrWriteConflict
	}
	return applyS3BlobFetchMutationsPreservingLastCommitTS(s.MVCCStore, ctx, mutations, readKeys, startTS, commitTS)
}

type registrationOnceS3BlobFetchStore struct {
	recordingS3BlobFetchStore
	afterFirstErr func()
}

func (s *registrationOnceS3BlobFetchStore) ApplyMutations(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	s.applyStartTS = append(s.applyStartTS, startTS)
	if s.applyCalls == 1 {
		return store.ErrWriterNotRegistered
	}
	s.applyCommitTS = append(s.applyCommitTS, commitTS)
	return s.MVCCStore.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS)
}

func (s *registrationOnceS3BlobFetchStore) ApplyMutationsPreservingLastCommitTS(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	s.applyStartTS = append(s.applyStartTS, startTS)
	if s.applyCalls == 1 {
		if s.afterFirstErr != nil {
			s.afterFirstErr()
		}
		return store.ErrWriterNotRegistered
	}
	s.applyCommitTS = append(s.applyCommitTS, commitTS)
	return applyS3BlobFetchMutationsPreservingLastCommitTS(s.MVCCStore, ctx, mutations, readKeys, startTS, commitTS)
}

type tombstoneConflictS3BlobFetchStore struct {
	recordingS3BlobFetchStore
	tombstoneTS uint64
}

func (s *tombstoneConflictS3BlobFetchStore) ApplyMutations(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	s.applyStartTS = append(s.applyStartTS, startTS)
	s.applyCommitTS = append(s.applyCommitTS, commitTS)
	if s.applyCalls == 1 {
		if len(mutations) == 0 {
			return store.ErrWriteConflict
		}
		if err := s.DeleteAt(ctx, mutations[0].Key, s.tombstoneTS); err != nil {
			return err
		}
		return store.ErrWriteConflict
	}
	return s.MVCCStore.ApplyMutations(ctx, mutations, readKeys, startTS, commitTS)
}

func (s *tombstoneConflictS3BlobFetchStore) ApplyMutationsPreservingLastCommitTS(
	ctx context.Context,
	mutations []*store.KVPairMutation,
	readKeys [][]byte,
	startTS uint64,
	commitTS uint64,
) error {
	s.applyCalls++
	s.applyStartTS = append(s.applyStartTS, startTS)
	s.applyCommitTS = append(s.applyCommitTS, commitTS)
	if s.applyCalls == 1 {
		if len(mutations) == 0 {
			return store.ErrWriteConflict
		}
		if err := s.DeleteAt(ctx, mutations[0].Key, s.tombstoneTS); err != nil {
			return err
		}
		return store.ErrWriteConflict
	}
	return applyS3BlobFetchMutationsPreservingLastCommitTS(s.MVCCStore, ctx, mutations, readKeys, startTS, commitTS)
}

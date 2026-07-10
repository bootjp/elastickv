package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
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

	st := store.NewMVCCStore()
	observer := &recordingS3BlobOffloadObserver{}
	server := NewS3BlobFetchServer(st, kv.NewHLC(), observer)
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

	server := NewS3BlobFetchServer(store.NewMVCCStore(), kv.NewHLC(), nil)
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
	server := NewS3BlobFetchServer(st, kv.NewHLC(), observer)
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

	server := NewS3BlobFetchServer(store.NewMVCCStore(), kv.NewHLC(), nil)
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

	server := NewS3BlobFetchServer(store.NewMVCCStore(), kv.NewHLC(), nil)
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

	server := NewS3BlobFetchServer(store.NewMVCCStore(), kv.NewHLC(), nil)
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

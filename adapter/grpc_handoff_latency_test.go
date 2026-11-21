package adapter

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRawHandoffReturnsProofBeforeCallerDeadline(t *testing.T) {
	proof := handoffNonadmissionProof(t)
	server := NewGRPCServer(&handoffTestStore{MVCCStore: store.NewMVCCStore()}, &handoffTestCoordinator{dispatch: func(context.Context) (*kv.CoordinateResponse, error) { return nil, proof }})
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := server.RawPut(ctx, &pb.RawPutRequest{Key: []byte("n"), Value: []byte("v")})
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "the caller needs time to receive the proven rejection instead of timing out")
	deadline, _ := ctx.Deadline()
	require.Greater(t, time.Until(deadline), 10*time.Millisecond)
}

type handoffReadStore struct {
	store.MVCCStore
	attempts   int
	persistent bool
	failure    error
	requests   []*kv.ReadRequest
}

func (s *handoffReadStore) GetAtWithReadFence(ctx context.Context, _ []byte, _ uint64, _ uint64, _ uint64) ([]byte, error) {
	s.attempts++
	request, _ := kv.ReadRequestFromContext(ctx)
	s.requests = append(s.requests, request)
	if s.failure != nil {
		return nil, s.failure
	}
	if s.persistent || s.attempts == 1 {
		return nil, kv.ErrRouteCutoverPending
	}
	return []byte("v"), nil
}
func TestRawGetRetriesCutoverWithFreshReadRequest(t *testing.T) {
	st := &handoffReadStore{MVCCStore: store.NewMVCCStore()}
	server := NewGRPCServer(st, &stubAdapterCoordinator{})
	response, err := server.RawGet(t.Context(), &pb.RawGetRequest{Key: []byte("n"), Ts: 1})
	require.NoError(t, err)
	require.Equal(t, []byte("v"), response.GetValue())
	require.Equal(t, 2, st.attempts)
	require.NotNil(t, st.requests[0])
	require.NotSame(t, st.requests[0], st.requests[1])
}

func TestRawGetCutoverRetryIsBounded(t *testing.T) {
	st := &handoffReadStore{MVCCStore: store.NewMVCCStore(), persistent: true}
	server := NewGRPCServer(st, &stubAdapterCoordinator{})
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := server.RawGet(ctx, &pb.RawGetRequest{Key: []byte("n"), Ts: 1})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NoError(t, ctx.Err())
	require.Greater(t, st.attempts, 1)
}
func TestRawGetDoesNotRetryPinnedOrMixedCutover(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint64
		failure error
	}{
		{"pinned", 9, kv.ErrRouteCutoverPending},
		{"mixed", 0, errors.Join(kv.ErrRouteCutoverPending, io.EOF)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &handoffReadStore{MVCCStore: store.NewMVCCStore(), failure: tc.failure}
			server := NewGRPCServer(st, &stubAdapterCoordinator{})
			_, err := server.RawGet(t.Context(), &pb.RawGetRequest{Key: []byte("n"), Ts: 1, ReadRouteVersion: tc.version})
			require.Error(t, err)
			require.Equal(t, 1, st.attempts)
		})
	}
}

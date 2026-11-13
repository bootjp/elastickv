package adapter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeS3MetadataDispatchError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		err         error
		wantUnknown bool
	}{
		{"nil", nil, false},
		{"local conflict", store.ErrWriteConflict, false},
		{"local circuit open before dispatch", kv.ErrLeaderProxyCircuitOpen, false},
		{"already unknown", kv.ErrTransactionOutcomeUnknown, true},
		{"unmarked unavailable", status.Error(codes.Unavailable, "private-key"), true},
		{"unmarked aborted", status.Error(codes.Aborted, "private-key"), true},
		{"lost response", io.EOF, true},
		{"deadline", context.DeadlineExceeded, true},
		{"leadership lost", raftengine.ErrLeadershipLost, true},
		{"transport loss with conflict", errors.Join(io.EOF, store.ErrWriteConflict), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeS3MetadataDispatchError(tc.err)
			if tc.wantUnknown {
				require.ErrorIs(t, got, kv.ErrTransactionOutcomeUnknown)
				require.False(t, isRetryableS3MutationErr(got))
				return
			}
			require.Equal(t, tc.err, got)
		})
	}
}

func TestS3UnmarkedMetadataDispatchProjectsUnknownOutcome(t *testing.T) {
	t.Parallel()
	err := normalizeS3MetadataDispatchError(status.Error(codes.Unavailable, "private-key: leader not found"))
	rec := httptest.NewRecorder()
	writeS3MutationError(rec, err, "bucket", "object")
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Contains(t, rec.Body.String(), "<Code>OperationOutcomeUnknown</Code>")
	require.Contains(t, rec.Body.String(), "<Message>Operation outcome unknown</Message>")
	require.NotContains(t, rec.Body.String(), "private-key")
	require.NotContains(t, rec.Body.String(), "bucket")
}

func TestS3MutationRetryPreservesUnknownOutcome(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{store.ErrWriteConflict, kv.ErrTxnLocked, kv.ErrRouteWriteFenced} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			unknown := errors.Join(kv.ErrTransactionOutcomeUnknown, cause)
			attempts := 0
			err := (&S3Server{}).retryS3Mutation(context.Background(), func() error {
				attempts++
				if attempts == 1 {
					return unknown
				}
				return nil
			})
			require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
			require.Equal(t, 1, attempts, "an uncertain write must not be replayed as a fresh mutation")
			require.False(t, isRetryableS3MutationErr(unknown))
		})
	}
}

func TestS3MutationRetryRebuildsDefinitiveConflict(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := (&S3Server{}).retryS3Mutation(context.Background(), func() error {
		attempts++
		if attempts == 1 {
			return store.ErrWriteConflict
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

type s3LostPartReplyCoordinator struct {
	*s3OCCReproCoordinator
	partKey []byte
	err     error
	lost    atomic.Bool
}

func (c *s3LostPartReplyCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	response, err := c.s3OCCReproCoordinator.Dispatch(ctx, req)
	if err != nil || req == nil || !req.IsTxn {
		return response, errors.WithStack(err)
	}
	for _, elem := range req.Elems {
		if elem.Op == kv.Put && string(elem.Key) == string(c.partKey) && c.lost.CompareAndSwap(false, true) {
			return nil, errors.WithStack(c.err)
		}
	}
	return response, nil
}

func TestS3UploadPartLostCommitReplyPreservesReferencedChunks(t *testing.T) {
	t.Parallel()
	for _, offloaded := range []bool{false, true} {
		name := "inline"
		if offloaded {
			name = "offloaded"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testS3LostPartReplyOutcomes(t, offloaded)
		})
	}
}

func testS3LostPartReplyOutcomes(t *testing.T, offloaded bool) {
	t.Helper()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unknown", kv.ErrTransactionOutcomeUnknown},
		{"unknown joined conflict", errors.Join(kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)},
		{"unknown joined lock", errors.Join(kv.ErrTransactionOutcomeUnknown, kv.ErrTxnLocked)},
		{"unknown joined fence", errors.Join(kv.ErrTransactionOutcomeUnknown, kv.ErrRouteWriteFenced)},
		{"deadline after commit", context.DeadlineExceeded},
		{"unmarked unavailable after commit", status.Error(codes.Unavailable, "private-key: not leader")},
		{"lost transport reply after commit", io.EOF},
		{"leadership lost after commit", raftengine.ErrLeadershipLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproModeHarness(t, false, offloaded)
			u := h.startUpload("unknown-part")
			require.Equal(t, http.StatusOK, h.uploadPart1(u, "original").Code)
			h.waitCleanups()
			previous, exists := h.part1Descriptor(u)
			require.True(t, exists)
			coord := &s3LostPartReplyCoordinator{s3OCCReproCoordinator: h.coord, partKey: u.partKey, err: tc.err}
			h.server.coordinator = coord

			response := h.uploadPart1(u, "replacement")
			require.Equal(t, http.StatusConflict, response.Code, "the success reply was lost")
			require.Contains(t, response.Body.String(), "<Code>OperationOutcomeUnknown</Code>")
			require.NotContains(t, response.Body.String(), "private-key")
			require.True(t, coord.lost.Load(), "the failure must follow a real descriptor commit")
			h.waitCleanups()
			committed, exists := h.part1Descriptor(u)
			require.True(t, exists)
			require.NotEqual(t, previous.PartVersion, committed.PartVersion)
			_, err := h.st.GetAt(t.Context(), u.chunkKey(0, s3ObjectPart(*committed)), ^uint64(0))
			require.NoError(t, err, "an uncertain response cannot authorize deleting committed chunks")
			_, err = h.st.GetAt(t.Context(), u.chunkKey(0, s3ObjectPart(*previous)), ^uint64(0))
			require.NoError(t, err, "superseded chunks are retired only after confirmed success")
		})
	}
}

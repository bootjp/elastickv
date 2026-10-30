package adapter

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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
			require.NotEqual(t, http.StatusOK, response.Code, "the success reply was lost")
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

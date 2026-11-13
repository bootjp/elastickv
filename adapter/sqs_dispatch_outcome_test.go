package adapter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeSQSDispatchError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		err         error
		wantUnknown bool
	}{
		{"nil", nil, false},
		{"local conflict", store.ErrWriteConflict, false},
		{"local circuit open", kv.ErrLeaderProxyCircuitOpen, false},
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
			got := normalizeSQSDispatchError(tc.err)
			if tc.wantUnknown {
				require.ErrorIs(t, got, kv.ErrTransactionOutcomeUnknown)
				require.False(t, isRetryableTransactWriteError(got))
				require.False(t, isIgnorableSQSReceiveRace(got))
				return
			}
			require.Equal(t, tc.err, got)
		})
	}
}

func TestSQSUnmarkedDispatchProjectsUnknownOutcome(t *testing.T) {
	t.Parallel()
	server := &SQSServer{coordinator: &availabilityErrorCoordinator{
		err: status.Error(codes.Unavailable, "private-key: not leader"),
	}}
	err := server.dispatchSQSMutation(context.Background(), &kv.OperationGroup[kv.OP]{})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)

	jsonResponse := httptest.NewRecorder()
	writeSQSErrorFromErr(jsonResponse, err)
	require.Equal(t, http.StatusConflict, jsonResponse.Code)
	require.Contains(t, jsonResponse.Body.String(), sqsErrOperationOutcomeUnknown)
	require.NotContains(t, jsonResponse.Body.String(), "private-key")

	queryResponse := httptest.NewRecorder()
	writeSQSQueryError(queryResponse, err)
	require.Equal(t, http.StatusConflict, queryResponse.Code)
	require.Contains(t, queryResponse.Body.String(), "<Code>"+sqsErrOperationOutcomeUnknown+"</Code>")
	require.NotContains(t, queryResponse.Body.String(), "private-key")
}

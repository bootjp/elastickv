package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mutationEvidence struct {
	Uncertain, Conflict, Locked, Fenced, Composed, CircuitOpen bool
}

type mutationProofCase struct {
	Evidence                       mutationEvidence
	RetryRedis, RetryAWS, SkipRace bool
	GRPCReason, GRPCMixedReason    string
}

func (e mutationEvidence) causes(uncertainty, composed error) []error {
	var causes []error
	for _, flag := range []struct {
		set bool
		err error
	}{
		{e.Uncertain, uncertainty}, {e.Conflict, store.ErrWriteConflict},
		{e.Locked, kv.ErrTxnLocked}, {e.Fenced, kv.ErrRouteWriteFenced},
		{e.Composed, composed}, {e.CircuitOpen, kv.ErrLeaderProxyCircuitOpen},
	} {
		if flag.set {
			causes = append(causes, flag.err)
		}
	}
	return causes
}

// make lean-check supplies every input and its expected decisions from the
// same Lean definitions used in no_dispatch_after_uncertainty.
func TestAdapterMutationLeanConformance(t *testing.T) {
	path := os.Getenv("ADAPTER_MUTATION_LEAN_CASES")
	if path == "" {
		t.Skip("run make lean-check to supply the Lean decision table")
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var cases []mutationProofCase
	require.NoError(t, json.Unmarshal(data, &cases))
	seen := make(map[mutationEvidence]bool)
	for i, c := range cases {
		require.False(t, seen[c.Evidence], "duplicate input %v", c.Evidence)
		seen[c.Evidence] = true
		t.Run(fmt.Sprintf("%02d", i), func(t *testing.T) {
			uncertainties := []error{nil}
			if c.Evidence.Uncertain {
				uncertainties = []error{
					kv.ErrTransactionOutcomeUnknown, raftengine.ErrLeadershipLost, context.Canceled,
					context.DeadlineExceeded, io.EOF, io.ErrUnexpectedEOF, &net.DNSError{Err: "lost reply"},
					status.Error(codes.Aborted, "wire conflict"), status.Error(codes.Unavailable, "lost reply"),
				}
			}
			for _, uncertainty := range uncertainties {
				for _, route := range []error{kv.ErrComposed1Violation, kv.ErrComposed1VersionGCd} {
					causes := c.Evidence.causes(uncertainty, route)
					if len(causes) == 1 {
						c.check(t, errors.Wrap(causes[0], "dispatch"), c.GRPCReason)
					}
					mixed := append([]error{errors.New("unclassified dispatch failure")}, causes...)
					c.check(t, errors.Wrap(errors.Join(mixed...), "dispatch"), c.GRPCMixedReason)
				}
			}
		})
	}
	require.Len(t, seen, 64)
}

func (c mutationProofCase) check(t *testing.T, err error, grpcReason string) {
	t.Helper()
	require.Equal(t, c.RetryRedis, isRetryableRedisTxnErr(err), "%+v", err)
	require.Equal(t, c.RetryAWS, isRetryableTransactWriteError(err))
	require.Equal(t, c.RetryAWS, isRetryableS3MutationErr(err))
	require.Equal(t, c.SkipRace, isIgnorableSQSReceiveRace(err))
	st := status.Convert(projectPublicGRPCMutationError(err))
	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, grpcReason, info.GetReason())
	for _, normalize := range []func(error) error{
		normalizeDynamoWriteDispatchError, normalizeS3MetadataDispatchError, normalizeSQSDispatchError,
	} {
		require.Equal(t, c.Evidence.Uncertain, errors.Is(normalize(err), kv.ErrTransactionOutcomeUnknown))
	}
	if !c.Evidence.Uncertain {
		return
	}
	require.Equal(t, codes.Unknown, st.Code())
	require.False(t, shouldPreserveRedisTxnAttempt(err))
	require.False(t, isAmbiguousRedisExecDispatchErr(err))
	require.ErrorIs(t, normalizeRetryableRedisTxnErr(err), kv.ErrTransactionOutcomeUnknown)
	conn := &captureConn{}
	writeRedisError(conn, err)
	require.Equal(t, redisOutcomeUnknownReply, conn.lastErr)
	dynamo, s3, sqs, query := httptest.NewRecorder(), httptest.NewRecorder(), httptest.NewRecorder(), httptest.NewRecorder()
	writeDynamoErrorFromErr(dynamo, normalizeDynamoWriteDispatchError(err))
	writeS3MutationError(s3, normalizeS3MetadataDispatchError(err), "bucket", "key")
	writeSQSErrorFromErr(sqs, normalizeSQSDispatchError(err))
	writeSQSQueryError(query, normalizeSQSDispatchError(err))
	for _, rec := range []*httptest.ResponseRecorder{dynamo, s3, sqs, query} {
		require.Equal(t, http.StatusConflict, rec.Code)
	}
	require.Equal(t, "OperationOutcomeUnknown", dynamo.Header().Get("x-amzn-ErrorType"))
	require.Equal(t, "OperationOutcomeUnknown", sqs.Header().Get("x-amzn-ErrorType"))
	require.Contains(t, s3.Body.String(), "<Code>OperationOutcomeUnknown</Code>")
	require.Contains(t, query.Body.String(), "<Code>OperationOutcomeUnknown</Code>")
}

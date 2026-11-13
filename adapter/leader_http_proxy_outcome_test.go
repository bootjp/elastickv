package adapter

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPLeaderProxyFailedReplyProjectsMutationOutcome(t *testing.T) {
	t.Parallel()
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("response writer does not support hijacking")
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer leader.Close()
	leaderAddress := strings.TrimPrefix(leader.URL, "http://")
	coordinator := &stubAdapterCoordinator{leaderSet: true, raftLeader: "leader"}

	for _, tc := range []struct {
		name       string
		writer     httpLeaderErrorWriter
		method     string
		target     string
		content    string
		url        string
		wantStatus int
		wantCode   string
	}{
		{"sqs json mutation", sqsLeaderProxyErrorWriter, http.MethodPost, sqsSendMessageTarget, sqsContentTypeJSON, "/", http.StatusConflict, sqsErrOperationOutcomeUnknown},
		{"sqs json read", sqsLeaderProxyErrorWriter, http.MethodPost, sqsListQueuesTarget, sqsContentTypeJSON, "/", http.StatusServiceUnavailable, sqsErrServiceUnavailable},
		{"sqs query mutation", sqsLeaderProxyErrorWriter, http.MethodPost, "", sqsContentTypeQueryURLEncoded, "/", http.StatusConflict, sqsErrOperationOutcomeUnknown},
		{"sqs query get mutation", sqsLeaderProxyErrorWriter, http.MethodGet, "", "", "/?Action=DeleteMessage", http.StatusConflict, sqsErrOperationOutcomeUnknown},
		{"sqs query get read", sqsLeaderProxyErrorWriter, http.MethodGet, "", "", "/?Action=ListQueues", http.StatusServiceUnavailable, sqsErrServiceUnavailable},
		{"dynamo mutation", dynamoLeaderProxyErrorWriter, http.MethodPost, putItemTarget, "application/x-amz-json-1.0", "/", http.StatusConflict, dynamoErrOperationOutcomeUnknown},
		{"dynamo read", dynamoLeaderProxyErrorWriter, http.MethodPost, getItemTarget, "application/x-amz-json-1.0", "/", http.StatusServiceUnavailable, dynamoErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.url, strings.NewReader("Action=SendMessage"))
			if tc.target != "" {
				req.Header.Set("X-Amz-Target", tc.target)
			}
			if tc.content != "" {
				req.Header.Set("Content-Type", tc.content)
			}
			rec := httptest.NewRecorder()
			require.True(t, proxyHTTPRequestToLeader(coordinator, map[string]string{"leader": leaderAddress}, tc.writer, rec, req))
			require.Equal(t, tc.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tc.wantCode)
			require.NotContains(t, rec.Body.String(), leaderAddress)
			require.NotContains(t, rec.Body.String(), "leader proxy error")
		})
	}
}

func TestHTTPLeaderProxyPreForwardFailureRemainsRetryable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		writer   httpLeaderErrorWriter
		target   string
		wantCode string
	}{
		{"sqs", sqsLeaderProxyErrorWriter, sqsSendMessageTarget, sqsErrServiceUnavailable},
		{"dynamo", dynamoLeaderProxyErrorWriter, putItemTarget, dynamoErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coordinator := &stubAdapterCoordinator{leaderSet: true}
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			require.True(t, proxyHTTPRequestToLeader(coordinator, map[string]string{"leader": "127.0.0.1:1"}, tc.writer, rec, req))
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.Contains(t, rec.Body.String(), tc.wantCode)
		})
	}
}

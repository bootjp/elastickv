package adapter

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestWithSQSPublicEndpoint(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, endpoint, want string
		invalid              bool
	}{
		{name: "request default", want: "http://internal:9324/orders"},
		{name: "https proxy", endpoint: "https://sqs.example.test", want: "https://sqs.example.test/orders"},
		{name: "trailing slash", endpoint: "https://sqs.example.test/", want: "https://sqs.example.test/orders"},
		{name: "http port", endpoint: "http://sqs.example.test:9324", want: "http://sqs.example.test:9324/orders"},
		{name: "IPv6", endpoint: "https://[::1]:9443", want: "https://[::1]:9443/orders"},
		{name: "scheme", endpoint: "ftp://sqs.example.test", invalid: true},
		{name: "relative", endpoint: "/queues", invalid: true},
		{name: "no host", endpoint: "https://", invalid: true},
		{name: "credentials", endpoint: "https://user:password@sqs.example.test", invalid: true},
		{name: "path", endpoint: "https://sqs.example.test/prefix", invalid: true},
		{name: "query", endpoint: "https://sqs.example.test?q=1", invalid: true},
		{name: "empty query", endpoint: "https://sqs.example.test?", invalid: true},
		{name: "empty fragment", endpoint: "https://sqs.example.test#", invalid: true},
		{name: "fragment", endpoint: "https://sqs.example.test#part", invalid: true},
		{name: "invalid port", endpoint: "https://sqs.example.test:bad", invalid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			option, err := WithSQSPublicEndpoint(tc.endpoint)
			if tc.invalid {
				require.ErrorIs(t, err, errInvalidSQSPublicEndpoint)
				return
			}
			require.NoError(t, err)
			server := &SQSServer{}
			option(server)
			req := httptest.NewRequest(http.MethodPost, "http://internal:9324/", nil)
			req.Header.Set("X-Forwarded-Proto", "https")
			req.Header.Set("X-Forwarded-Host", "untrusted.example.test")
			require.Equal(t, tc.want, server.queueURL(req, "orders"))
		})
	}
}

func TestSQSQueueURLPreservesDirectTLSDefault(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "http://internal:9324/", nil)
	req.TLS = &tls.ConnectionState{}
	require.Equal(t, "https://internal:9324/orders", (&SQSServer{}).queueURL(req, "orders"))
}

func TestSQSPublicEndpointCatalogResponses(t *testing.T) {
	t.Parallel()
	server := newDelayTestServer(t, store.NewMVCCStore())
	option, err := WithSQSPublicEndpoint("https://sqs.example.test")
	require.NoError(t, err)
	option(server)
	cases := []struct {
		target, body, field string
		list                bool
	}{
		{target: sqsCreateQueueTarget, body: `{"QueueName":"orders"}`, field: "QueueUrl"},
		{target: sqsGetQueueUrlTarget, body: `{"QueueName":"orders"}`, field: "QueueUrl"},
		{target: sqsListQueuesTarget, body: `{}`, field: "QueueUrls", list: true},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, "http://internal:9324/", strings.NewReader(tc.body))
		req.Header.Set("X-Amz-Target", tc.target)
		req.Header.Set("Content-Type", sqsContentTypeJSON)
		req.Header.Set("X-Forwarded-Proto", "http")
		response := httptest.NewRecorder()
		server.handle(response, req)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var result map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		if tc.list {
			require.Equal(t, []any{"https://sqs.example.test/orders"}, result[tc.field])
		} else {
			require.Equal(t, "https://sqs.example.test/orders", result[tc.field])
		}
	}
}

func TestSQSPublicEndpointQueryResponse(t *testing.T) {
	t.Parallel()
	server := newDelayTestServer(t, store.NewMVCCStore())
	option, err := WithSQSPublicEndpoint("https://sqs.example.test")
	require.NoError(t, err)
	option(server)
	request := httptest.NewRequest(http.MethodPost, "http://internal:9324/", strings.NewReader("Action=CreateQueue&Version=2012-11-05&QueueName=orders"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	server.handle(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "<QueueUrl>https://sqs.example.test/orders</QueueUrl>")
}

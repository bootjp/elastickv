package monitoring

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricsAddressRequiresToken(t *testing.T) {
	require.False(t, MetricsAddressRequiresToken("localhost:9090"))
	require.False(t, MetricsAddressRequiresToken("127.0.0.1:9090"))
	require.False(t, MetricsAddressRequiresToken("[::1]:9090"))
	require.True(t, MetricsAddressRequiresToken(":9090"))
	require.True(t, MetricsAddressRequiresToken("0.0.0.0:9090"))
	require.True(t, MetricsAddressRequiresToken("10.0.0.1:9090"))
}

func TestProtectHandlerRequiresBearerToken(t *testing.T) {
	protected := ProtectHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), "secret-token")

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "bearer secret-token")
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
}

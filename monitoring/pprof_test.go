package monitoring

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPprofHandlerServesIndexRoute(t *testing.T) {
	h := NewPprofHandler()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestNewPprofHandlerServesProfileRoute(t *testing.T) {
	h := NewPprofHandler()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/debug/pprof/profile?seconds=1", nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()
	<-done
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestNewPprofServerRequiresBearerToken(t *testing.T) {
	server := NewPprofServer("pprof-token")
	require.NotNil(t, server)
	require.NotNil(t, server.Handler)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/debug/pprof/", nil)
	req.Header.Set("Authorization", "Bearer pprof-token")
	rec = httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestNewPprofServerNoTokenAllowsAllRequests(t *testing.T) {
	server := NewPprofServer("")
	require.NotNil(t, server)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

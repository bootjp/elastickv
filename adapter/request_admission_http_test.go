package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var errRequestRevoked = errors.New("request revoked")

type testRequestOwnership struct {
	ctx              context.Context
	revoked          bool
	joined, finished bool
}

func (o *testRequestOwnership) Context() context.Context { return o.ctx }
func (o *testRequestOwnership) Validate() error {
	if o.revoked {
		return errRequestRevoked
	}
	return nil
}
func (o *testRequestOwnership) Finish(err error) error {
	o.finished = true
	return errors.Join(err, o.Validate())
}
func (o *testRequestOwnership) CancelAndJoin() { o.joined = true }

func TestHTTPRequestAdmissionRejectsBeforeHandler(t *testing.T) {
	called := false
	handler := withHTTPRequestAdmission(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }),
		func(context.Context) (RequestOwnership, error) { return nil, errRequestRevoked })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.False(t, called)
}

func TestHTTPRequestAdmissionRetainsRequestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	owner := &testRequestOwnership{ctx: ctx}
	handler := withHTTPRequestAdmission(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		require.Same(t, ctx, request.Context())
		_, err := w.Write([]byte("current"))
		require.NoError(t, err)
	}), func(context.Context) (RequestOwnership, error) { return owner, nil })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, "current", response.Body.String())
	require.True(t, owner.finished)
	require.True(t, owner.joined)
}

func TestHTTPRequestAdmissionDiscardsStaleDirectRead(t *testing.T) {
	owner := &testRequestOwnership{ctx: t.Context()}
	handler := withHTTPRequestAdmission(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		owner.revoked = true
		_, _ = w.Write([]byte("stale value"))
	}), func(context.Context) (RequestOwnership, error) { return owner, nil })
	response := httptest.NewRecorder()
	require.PanicsWithValue(t, http.ErrAbortHandler, func() {
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	})
	require.Empty(t, response.Body.String(), "post-work revocation must not masquerade as pre-admission 503")
	require.NotContains(t, response.Body.String(), "stale value")
	require.True(t, owner.joined)
}

func TestHTTPRequestAdmissionAbortsStalePartialStream(t *testing.T) {
	owner := &testRequestOwnership{ctx: t.Context()}
	handler := withHTTPRequestAdmission(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte("current"))
		require.NoError(t, err)
		owner.revoked = true
		_, _ = w.Write([]byte("stale"))
	}), func(context.Context) (RequestOwnership, error) { return owner, nil })
	response := httptest.NewRecorder()
	require.PanicsWithValue(t, http.ErrAbortHandler, func() {
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	})
	require.Equal(t, "current", response.Body.String())
	require.True(t, owner.joined)
}

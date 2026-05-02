package adapter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// TestServeSQSHealthz_LegacyTextPath pins the byte-identical legacy
// behaviour: a GET / HEAD with no Accept header (or a bare "*/*"
// wildcard) returns "ok\n" with text/plain content-type. The
// existing k8s liveness probe and curl-style integrations rely on
// this body, so the JSON capability extension MUST NOT alter it.
func TestServeSQSHealthz_LegacyTextPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		accept string
	}{
		{"no Accept header", ""},
		{"wildcard Accept", "*/*"},
		{"text/plain Accept", "text/plain"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, sqsHealthPath, nil)
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			rec := httptest.NewRecorder()
			serveSQSHealthz(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
			require.Equal(t, "ok\n", rec.Body.String())
		})
	}
}

// TestServeSQSHealthz_JSONShape pins the JSON capability response
// shape returned when the caller signals Accept: application/json.
// The CreateQueue capability gate (Phase 3.D PR 5) decodes this
// body on every peer, so a regression that drops the field or
// changes the field name would silently break the rolling-upgrade
// guard.
func TestServeSQSHealthz_JSONShape(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		accept string
	}{
		{"plain JSON", "application/json"},
		{"JSON with q-factor", "application/json;q=1.0"},
		{"comma-separated list with JSON", "text/plain, application/json;q=0.9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, sqsHealthPath, nil)
			req.Header.Set("Accept", tc.accept)
			rec := httptest.NewRecorder()
			serveSQSHealthz(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))

			var got sqsHealthBody
			require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(rec.Body.String())), &got))
			require.Equal(t, "ok", got.Status)
			require.Equal(t, sqsAdvertisedCapabilities(), got.Capabilities)
		})
	}
}

// TestServeSQSHealthz_HEAD_JSONOmitsBody pins that a HEAD request on
// the JSON path emits the JSON content-type but no body bytes —
// matching the existing legacy-path HEAD behaviour. Liveness probes
// often use HEAD to avoid log spam; the JSON path MUST behave the
// same way.
func TestServeSQSHealthz_HEAD_JSONOmitsBody(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodHead, sqsHealthPath, nil)
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	serveSQSHealthz(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	require.Empty(t, rec.Body.String())
}

// TestServeSQSHealthz_RejectsNonGETHEAD pins that POST / PUT / DELETE
// are still rejected with 405 in both the legacy and JSON modes —
// the JSON extension MUST NOT widen the method surface of the
// endpoint.
func TestServeSQSHealthz_RejectsNonGETHEAD(t *testing.T) {
	t.Parallel()
	for _, accept := range []string{"", "application/json"} {
		t.Run("Accept="+accept, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, sqsHealthPath, nil)
			if accept != "" {
				req.Header.Set("Accept", accept)
			}
			rec := httptest.NewRecorder()
			serveSQSHealthz(rec, req)
			require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			require.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
		})
	}
}

// TestSQSAdvertisedCapabilities_TracksFlag pins the relationship
// between htfifoCapabilityAdvertised and the emitted list. If a
// future change flips the constant, the JSON response must reflect
// it without further wiring — the constant is the single source of
// truth.
func TestSQSAdvertisedCapabilities_TracksFlag(t *testing.T) {
	t.Parallel()
	caps := sqsAdvertisedCapabilities()
	if htfifoCapabilityAdvertised {
		require.Contains(t, caps, sqsCapabilityHTFIFO,
			"htfifo must be in the list when the flag is true")
	} else {
		require.NotContains(t, caps, sqsCapabilityHTFIFO,
			"htfifo must NOT be in the list when the flag is false; "+
				"a partial deploy that advertised the capability without "+
				"the routing + leadership-refusal pair would create new "+
				"partitioned queues that this binary cannot safely host")
	}
}

// TestClientAcceptsSQSHealthJSON_Boundaries pins the substring
// matcher's edge cases — these are the inputs the catalog-polling
// caller in Phase 3.D PR 5 will produce, and a regression that
// silently flips one of these to a wrong answer would either return
// the legacy body to a JSON peer (PR 5 decode error) or the JSON
// body to a curl client (UI noise).
func TestClientAcceptsSQSHealthJSON_Boundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		accept []string
		want   bool
	}{
		{"nil request", nil, false},
		{"empty header value", []string{""}, false},
		{"bare wildcard", []string{"*/*"}, false},
		{"text/plain only", []string{"text/plain"}, false},
		{"plain JSON", []string{"application/json"}, true},
		{"JSON with parameters", []string{"application/json; charset=utf-8"}, true},
		{"multi-value with JSON last", []string{"text/plain", "application/json"}, true},
		{"multi-value with JSON first", []string{"application/json", "text/plain"}, true},
		{"comma-list with JSON", []string{"text/plain, application/json;q=0.9"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var req *http.Request
			if tc.accept != nil {
				req = httptest.NewRequest(http.MethodGet, sqsHealthPath, nil)
				req.Header["Accept"] = tc.accept
			}
			require.Equal(t, tc.want, clientAcceptsSQSHealthJSON(req))
		})
	}
}

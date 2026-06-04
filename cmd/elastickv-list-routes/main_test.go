package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

// TestEmit_Empty checks the JSON shape when no routes exist.
// catalog_version is still emitted, routes is an empty array
// (not nil) so Clojure callers don't have to special-case
// nil-vs-empty.
func TestEmit_Empty(t *testing.T) {
	tmp := tmpFile(t)
	defer os.Remove(tmp.Name())
	require.NoError(t, emit(&pb.ListRoutesResponse{CatalogVersion: 7}, tmp))
	require.NoError(t, tmp.Close())

	raw, err := os.ReadFile(tmp.Name())
	require.NoError(t, err)
	var out struct {
		CatalogVersion uint64 `json:"catalog_version"`
		Routes         []any  `json:"routes"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Equal(t, uint64(7), out.CatalogVersion)
	require.NotNil(t, out.Routes)
	require.Empty(t, out.Routes)
}

// TestEmit_RoundTripsRouteBytes pins the on-the-wire shape against
// the JSON Clojure will parse.  Start/End are base64-encoded so any
// byte sequence survives — verified by round-tripping a routing-key
// shape that contains '|' (ASCII 124, outside the base64 alphabet).
func TestEmit_RoundTripsRouteBytes(t *testing.T) {
	tmp := tmpFile(t)
	defer os.Remove(tmp.Name())

	startBytes := []byte("!ddb|route|table|amVwc2VuX2FwcGVuZF90MQ")
	endBytes := []byte("!ddb|route|table|amVwc2VuX2FwcGVuZF90Mw")
	require.NoError(t, emit(&pb.ListRoutesResponse{
		CatalogVersion: 3,
		Routes: []*pb.RouteDescriptor{
			{
				RouteId:     100,
				RaftGroupId: 1,
				Start:       startBytes,
				End:         endBytes,
				State:       pb.RouteState_ROUTE_STATE_ACTIVE,
			},
		},
	}, tmp))
	require.NoError(t, tmp.Close())

	raw, err := os.ReadFile(tmp.Name())
	require.NoError(t, err)
	var out responseJSON
	require.NoError(t, json.Unmarshal(raw, &out))

	require.Equal(t, uint64(3), out.CatalogVersion)
	require.Len(t, out.Routes, 1)
	require.Equal(t, uint64(100), out.Routes[0].RouteID)
	require.Equal(t, uint64(1), out.Routes[0].RaftGroupID)

	// Round-trip the base64-encoded bytes — the load-bearing claim is
	// that the Clojure caller decodes back to the exact bytes the
	// server holds.
	decodedStart, err := base64.StdEncoding.DecodeString(out.Routes[0].Start)
	require.NoError(t, err)
	require.Equal(t, startBytes, decodedStart)
	decodedEnd, err := base64.StdEncoding.DecodeString(out.Routes[0].End)
	require.NoError(t, err)
	require.Equal(t, endBytes, decodedEnd)
}

// TestEmit_EmptyEndDistinguishable verifies that an unset End
// (the +infinity sentinel) round-trips as an empty string rather
// than as a missing field — the Clojure setup-hook relies on this
// to detect the rightmost route in the catalog.
func TestEmit_EmptyEndDistinguishable(t *testing.T) {
	tmp := tmpFile(t)
	defer os.Remove(tmp.Name())
	require.NoError(t, emit(&pb.ListRoutesResponse{
		Routes: []*pb.RouteDescriptor{
			{RouteId: 1, RaftGroupId: 2, Start: []byte("x"), End: nil},
		},
	}, tmp))
	require.NoError(t, tmp.Close())

	raw, err := os.ReadFile(tmp.Name())
	require.NoError(t, err)
	require.Contains(t, string(raw), `"end": ""`,
		"empty End must serialise as an explicit empty string so Clojure can detect the +infinity boundary")
}

func tmpFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "lr-*.json")
	require.NoError(t, err)
	return f
}

// silence unused-imports under partial-failure scenarios; bytes is
// pulled in for future test growth (e.g. raw-bytes diffing if Clojure
// callers ever consume the binary shape directly).
var _ = bytes.NewReader

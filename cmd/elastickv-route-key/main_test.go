package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRouteKey_PinsAdapterEncoding pins the routeKey output
// against the adapter's actual encoding so any future drift in
// either side surfaces here.  The expected values were computed
// by `base64.RawURLEncoding.EncodeToString([]byte(name))` — the
// same call adapter/dynamodb.go:8441's encodeDynamoSegment makes.
//
// The four M5 table names are the load-bearing case (the launch
// script's --shardRanges boundary keys come from t2 / t3).  A
// single-character name is included to catch a class of off-by-one
// in the encoding (zero-pad behaviour differs between Padded and
// RawURL variants).
func TestRouteKey_PinsAdapterEncoding(t *testing.T) {
	cases := []struct {
		tableName string
		want      string
	}{
		{"jepsen_append_t1", "!ddb|route|table|amVwc2VuX2FwcGVuZF90MQ"},
		{"jepsen_append_t2", "!ddb|route|table|amVwc2VuX2FwcGVuZF90Mg"},
		{"jepsen_append_t3", "!ddb|route|table|amVwc2VuX2FwcGVuZF90Mw"},
		{"jepsen_append_t4", "!ddb|route|table|amVwc2VuX2FwcGVuZF90NA"},
		{"x", "!ddb|route|table|eA"}, // single byte → 2-char base64 RawURL (no padding)
	}
	for _, tc := range cases {
		t.Run(tc.tableName, func(t *testing.T) {
			require.Equal(t, tc.want, routeKey(tc.tableName))
		})
	}
}

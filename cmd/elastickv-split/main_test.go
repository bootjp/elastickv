package main

import (
	"flag"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestValidateFlags_RejectsMissingRequired pins the smoke-test
// behaviour the M5 design doc (§3.1) names as the only unit-level
// coverage for cmd/elastickv-split: missing required flags surface
// a clear error so the Jepsen nemesis fails fast.  The real
// coverage is the Jepsen run itself (M5b).
func TestValidateFlags_RejectsMissingRequired(t *testing.T) {
	cases := []struct {
		name            string
		routeID         uint64
		splitKey        string
		expectedVersion uint64
		wantErrSubstr   string
	}{
		{"missing route-id", 0, "k", 1, "--route-id is required"},
		{"missing split-key", 1, "", 1, "--split-key is required"},
		{"missing expected-version", 1, "k", 0, "--expected-version is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// validateFlags reads the package-level flag pointers, so
			// reset them around each subtest to avoid cross-test leakage.
			resetFlags(t)
			*routeID = tc.routeID
			*splitKey = tc.splitKey
			*expectedVersion = tc.expectedVersion
			err := validateFlags()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErrSubstr)
		})
	}
}

func TestValidateFlags_AcceptsAllPresent(t *testing.T) {
	resetFlags(t)
	*routeID = 42
	*splitKey = "!ddb|route|table|am-test"
	*expectedVersion = 7
	require.NoError(t, validateFlags())
}

// resetFlags reinitialises the package-level flag pointers around
// each test so subtests don't leak state through them.  We rebind
// the globals directly rather than re-parsing argv because the
// flag package's parse state is global to the process.
func resetFlags(t *testing.T) {
	t.Helper()
	// Sanity-check that the pointers are set (declared at file
	// init).  errors.New rather than t.Fatalf so a future
	// refactor that drops the package-level flags surfaces here
	// rather than silently no-oping the resets.
	if routeID == nil || splitKey == nil || expectedVersion == nil || address == nil {
		t.Fatal(errors.New("package-level flag pointers were not initialised"))
	}
	*routeID = 0
	*splitKey = ""
	*expectedVersion = 0
	*address = "127.0.0.1:50051"
	_ = flag.CommandLine // touch to avoid unused import on future trims
}

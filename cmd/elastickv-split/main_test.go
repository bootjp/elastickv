package main

import (
	"context"
	"flag"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

func TestValidateFlags_AcceptsAbandonOnly(t *testing.T) {
	resetFlags(t)
	*abandonJobID = 42
	require.NoError(t, validateFlags())
}

func TestValidateFlags_RejectsAbandonWithSplitFlags(t *testing.T) {
	resetFlags(t)
	*abandonJobID = 42
	*routeID = 7
	err := validateFlags()
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestValidateFlags_AcceptsGetOnly(t *testing.T) {
	resetFlags(t)
	*getJobID = 42
	require.NoError(t, validateFlags())
}

func TestValidateFlags_RejectsGetWithMutationFlags(t *testing.T) {
	resetFlags(t)
	*getJobID = 42
	*abandonJobID = 42
	err := validateFlags()
	require.ErrorContains(t, err, "mutually exclusive")
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
	if routeID == nil || splitKey == nil || expectedVersion == nil || targetGroupID == nil || abandonJobID == nil || getJobID == nil || address == nil {
		t.Fatal(errors.New("package-level flag pointers were not initialised"))
	}
	*routeID = 0
	*splitKey = ""
	*expectedVersion = 0
	*targetGroupID = 0
	*abandonJobID = 0
	*getJobID = 0
	*address = "127.0.0.1:50051"
	*tokenFile = ""
	_ = flag.CommandLine // touch to avoid unused import on future trims
}

type splitAuthTestServer struct {
	pb.UnimplementedDistributionServer
	jobs chan uint64
}

func (s *splitAuthTestServer) AbandonSplitJob(_ context.Context, req *pb.AbandonSplitJobRequest) (*pb.AbandonSplitJobResponse, error) {
	s.jobs <- req.GetJobId()
	return &pb.AbandonSplitJobResponse{}, nil
}

func TestRunSendsMigrationAdminToken(t *testing.T) {
	resetFlags(t)
	t.Cleanup(func() { resetFlags(t) })
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	unary, _ := adapter.SplitMigrationTokenAuth("test-admin-token", false)
	server := grpc.NewServer(grpc.UnaryInterceptor(unary))
	svc := &splitAuthTestServer{jobs: make(chan uint64, 1)}
	pb.RegisterDistributionServer(server, svc)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveResult)
	})
	*address = listener.Addr().String()
	*abandonJobID = 7
	err = run()
	require.ErrorContains(t, err, "Unauthenticated")
	require.Empty(t, svc.jobs)
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("test-admin-token\n"), 0o600))
	*tokenFile = path
	require.NoError(t, run())
	require.Equal(t, uint64(7), <-svc.jobs)
}

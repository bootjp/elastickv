package main

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEncryptionMain_RejectsUnknownSubcommand(t *testing.T) {
	t.Parallel()
	err := encryptionMain([]string{"bogus"})
	if err == nil {
		t.Fatalf("encryptionMain(bogus) returned nil, want error")
	}
	if !strings.Contains(err.Error(), "unknown subcommand") {
		t.Errorf("err=%v, want substring 'unknown subcommand'", err)
	}
}

func TestEncryptionMain_RequiresSubcommand(t *testing.T) {
	t.Parallel()
	err := encryptionMain(nil)
	if err == nil {
		t.Fatalf("encryptionMain(nil) returned nil, want usage error")
	}
}

func TestRunEncryptionStatus_Bootstrapped(t *testing.T) {
	t.Parallel()
	path := writeStatusFixture(t, &encryption.Sidecar{
		RaftAppliedIndex:         9,
		StorageEnvelopeActive:    true,
		RaftEnvelopeCutoverIndex: 25,
		Active:                   encryption.ActiveKeys{Storage: 11, Raft: 22},
		Keys: map[string]encryption.SidecarKey{
			"11": {Purpose: "storage", Wrapped: []byte("ws")},
			"22": {Purpose: "raft", Wrapped: []byte("wr")},
		},
	})
	addr := startEncryptionAdminTestServer(t,
		adapter.WithEncryptionAdminSidecarPath(path),
		adapter.WithEncryptionAdminFullNodeID(7),
		adapter.WithEncryptionAdminBuildSHA("deadbeef"),
	)

	var buf bytes.Buffer
	if err := runEncryptionStatus([]string{"--endpoint", addr, "--timeout", "3s"}, &buf); err != nil {
		t.Fatalf("runEncryptionStatus: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"encryption_capable: true",
		"sidecar_present:    true",
		"full_node_id:       7",
		"local_epoch:        0",
		"build_sha:          deadbeef",
		"active_storage_id:           11",
		"active_raft_id:              22",
		"storage_envelope_active:     true",
		"raft_envelope_cutover_index: 25",
		"latest_applied_index:        9",
		"wrapped_dek_ids:             [11 22]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestRunEncryptionStatus_NoSidecar(t *testing.T) {
	t.Parallel()
	addr := startEncryptionAdminTestServer(t,
		adapter.WithEncryptionAdminBuildSHA("dev"),
	)

	var buf bytes.Buffer
	if err := runEncryptionStatus([]string{"--endpoint", addr, "--timeout", "3s"}, &buf); err != nil {
		t.Fatalf("runEncryptionStatus: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "encryption_capable: false") {
		t.Errorf("expected encryption_capable=false on unconfigured node, got:\n%s", out)
	}
	if !strings.Contains(out, "sidecar_state: <unavailable") {
		t.Errorf("expected sidecar_state unavailable line, got:\n%s", out)
	}
}

// TestRunEncryptionStatus_PropagatesNonPreconditionError pins the
// PR754 codex P2 fix: only FailedPrecondition on GetSidecarState
// is treated as the "node not configured" soft fallback. Any
// other code (Internal, Unavailable, Unimplemented, …) must
// surface as a non-nil error so the CLI exits non-zero.
func TestRunEncryptionStatus_PropagatesNonPreconditionError(t *testing.T) {
	t.Parallel()
	addr := startCustomEncryptionAdminTestServer(t, &stubSidecarErrorServer{
		statusErr: status.Error(codes.Internal, "synthetic sidecar read failure"),
	})

	var buf bytes.Buffer
	err := runEncryptionStatus([]string{"--endpoint", addr, "--timeout", "3s"}, &buf)
	if err == nil {
		t.Fatalf("runEncryptionStatus returned nil, want non-FailedPrecondition error")
	}
	if status.Code(err) == codes.FailedPrecondition {
		t.Errorf("error code=FailedPrecondition, want propagated upstream code (got err=%v)", err)
	}
	if !strings.Contains(err.Error(), "GetSidecarState") {
		t.Errorf("error %q does not wrap the GetSidecarState callsite", err)
	}
}

// stubSidecarErrorServer answers GetCapability normally so the CLI
// reaches GetSidecarState, then returns a configured status.Error
// for GetSidecarState. Mutating RPCs and ResyncSidecar inherit the
// Unimplemented defaults — they are unreachable from
// runEncryptionStatus today.
type stubSidecarErrorServer struct {
	pb.UnimplementedEncryptionAdminServer
	statusErr error
}

func (s *stubSidecarErrorServer) GetCapability(context.Context, *pb.Empty) (*pb.CapabilityReport, error) {
	return &pb.CapabilityReport{
		EncryptionCapable: true,
		SidecarPresent:    true,
	}, nil
}

func (s *stubSidecarErrorServer) GetSidecarState(context.Context, *pb.Empty) (*pb.SidecarStateReport, error) {
	return nil, s.statusErr
}

func writeStatusFixture(t *testing.T, sc *encryption.Sidecar) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

// startEncryptionAdminTestServer brings up a real gRPC server on a
// loopback listener so the CLI dial path (grpc.NewClient against a
// host:port string) exercises the same code as production. The
// listener is closed at test cleanup; the server's GracefulStop is
// best-effort with a short deadline so a misbehaving client cannot
// hang the test process.
func startEncryptionAdminTestServer(t *testing.T, opts ...adapter.EncryptionAdminServerOption) string {
	t.Helper()
	return startCustomEncryptionAdminTestServer(t, adapter.NewEncryptionAdminServer(opts...))
}

// startCustomEncryptionAdminTestServer is the variant for tests
// that need a hand-rolled EncryptionAdminServer implementation
// (e.g., one that returns a specific status.Code from
// GetSidecarState). The cleanup contract matches
// startEncryptionAdminTestServer.
func startCustomEncryptionAdminTestServer(t *testing.T, impl pb.EncryptionAdminServer) string {
	t.Helper()
	lc := &net.ListenConfig{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lis, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterEncryptionAdminServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { srv.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			srv.Stop()
		}
	})
	return lis.Addr().String()
}

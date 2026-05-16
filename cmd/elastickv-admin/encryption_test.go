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

// TestEncryptionMain_HelpFlagExitsZero pins the CLI help-flag
// contract: `-h`, `--help`, and `help` must not fall through to
// the unknown-subcommand error path so shell scripts using $? to
// detect success do not trip on a help request.
func TestEncryptionMain_HelpFlagExitsZero(t *testing.T) {
	t.Parallel()
	for _, sub := range []string{"-h", "--help", "help"} {
		if err := encryptionMain([]string{sub}); err != nil {
			t.Errorf("encryptionMain(%q) returned %v, want nil", sub, err)
		}
	}
}

// TestRunEncryptionStatus_HelpFlagExitsZero pins the same property
// on the subcommand's own flag parser: flag.ContinueOnError surfaces
// `-h` via flag.ErrHelp; that sentinel must not bubble out as an
// error.
func TestRunEncryptionStatus_HelpFlagExitsZero(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := runEncryptionStatus([]string{"-h"}, &buf); err != nil {
		t.Errorf("runEncryptionStatus(-h) returned %v, want nil", err)
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
// error-narrowing contract: only FailedPrecondition on GetSidecarState
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

// stubMutatorServer captures the RotateDEK / RegisterEncryptionWriter
// requests so the CLI tests can verify both the wire-level args
// and the returned applied_index round-trip. Read-only RPCs
// inherit Unimplemented defaults.
type stubMutatorServer struct {
	pb.UnimplementedEncryptionAdminServer
	rotateCalls    []*pb.RotateDEKRequest
	registerCalls  []*pb.RegisterEncryptionWriterRequest
	bootstrapCalls []*pb.BootstrapEncryptionRequest
	appliedIndex   uint64
	returnErr      error
}

func (s *stubMutatorServer) RotateDEK(_ context.Context, req *pb.RotateDEKRequest) (*pb.RotateDEKResponse, error) {
	s.rotateCalls = append(s.rotateCalls, req)
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &pb.RotateDEKResponse{AppliedIndex: s.appliedIndex}, nil
}

func (s *stubMutatorServer) RegisterEncryptionWriter(_ context.Context, req *pb.RegisterEncryptionWriterRequest) (*pb.RegisterEncryptionWriterResponse, error) {
	s.registerCalls = append(s.registerCalls, req)
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &pb.RegisterEncryptionWriterResponse{AppliedIndex: s.appliedIndex}, nil
}

func (s *stubMutatorServer) BootstrapEncryption(_ context.Context, req *pb.BootstrapEncryptionRequest) (*pb.BootstrapEncryptionResponse, error) {
	s.bootstrapCalls = append(s.bootstrapCalls, req)
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	return &pb.BootstrapEncryptionResponse{AppliedIndex: s.appliedIndex}, nil
}

func TestRunEncryptionBootstrap_HappyPath(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{appliedIndex: 117}
	addr := startCustomEncryptionAdminTestServer(t, stub)

	var buf bytes.Buffer
	err := runEncryptionBootstrap([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--storage-dek-id", "1",
		"--raft-dek-id", "2",
		// "wrapped-storage" base64-encoded
		"--wrapped-storage-dek", "d3JhcHBlZC1zdG9yYWdl",
		// "wrapped-raft" base64-encoded
		"--wrapped-raft-dek", "d3JhcHBlZC1yYWZ0",
		"--writer", "11:0",
		"--writer", "22:5",
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionBootstrap: %v", err)
	}
	if !strings.Contains(buf.String(), "applied_index=117") {
		t.Errorf("output missing applied_index=117, got:\n%s", buf.String())
	}
	if len(stub.bootstrapCalls) != 1 {
		t.Fatalf("BootstrapEncryption calls=%d, want 1", len(stub.bootstrapCalls))
	}
	assertBootstrapCallMatches(t, stub.bootstrapCalls[0])
}

func assertBootstrapCallMatches(t *testing.T, call *pb.BootstrapEncryptionRequest) {
	t.Helper()
	if call.StorageDekId != 1 || call.RaftDekId != 2 {
		t.Errorf("dek ids=(s=%d,r=%d), want (1,2)", call.StorageDekId, call.RaftDekId)
	}
	if string(call.WrappedStorageDek) != "wrapped-storage" {
		t.Errorf("WrappedStorageDek=%q, want wrapped-storage", call.WrappedStorageDek)
	}
	if string(call.WrappedRaftDek) != "wrapped-raft" {
		t.Errorf("WrappedRaftDek=%q, want wrapped-raft", call.WrappedRaftDek)
	}
	if len(call.WriterBatch) != 2 {
		t.Fatalf("WriterBatch len=%d, want 2", len(call.WriterBatch))
	}
	if call.WriterBatch[0].FullNodeId != 11 || call.WriterBatch[0].LocalEpoch != 0 ||
		call.WriterBatch[1].FullNodeId != 22 || call.WriterBatch[1].LocalEpoch != 5 {
		t.Errorf("WriterBatch=%v, want [(11,0),(22,5)]", call.WriterBatch)
	}
}

// TestRunEncryptionBootstrap_RejectsEmptyWriterBatch pins the CLI
// parse-layer guard for the missing-entirely case (no --writer
// flags at all). The server-side "empty writer_batch" path is
// covered by adapter tests, but the CLI's own early-out in
// parseBootstrapArgs is otherwise unexercised.
func TestRunEncryptionBootstrap_RejectsEmptyWriterBatch(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionBootstrap([]string{
		"--endpoint", "127.0.0.1:1",
		"--storage-dek-id", "1",
		"--raft-dek-id", "2",
		"--wrapped-storage-dek", "d3JhcHBlZC1zdG9yYWdl",
		"--wrapped-raft-dek", "d3JhcHBlZC1yYWZ0",
	}, &buf)
	if err == nil {
		t.Fatalf("runEncryptionBootstrap returned nil, want error on missing --writer flags")
	}
	if !strings.Contains(err.Error(), "writer") {
		t.Errorf("error %q does not surface the missing --writer condition", err)
	}
}

func TestRunEncryptionBootstrap_RejectsBadWriter(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	for _, c := range []struct {
		name  string
		entry string
	}{
		{"missing colon", "12345"},
		{"non-numeric node_id", "abc:0"},
		{"non-numeric epoch", "1:xyz"},
		{"zero node_id", "0:0"},
		{"epoch above 0xFFFF", "1:70000"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := runEncryptionBootstrap([]string{
				"--endpoint", "127.0.0.1:1",
				"--storage-dek-id", "1",
				"--raft-dek-id", "2",
				"--wrapped-storage-dek", "d3JhcHBlZC1zdG9yYWdl",
				"--wrapped-raft-dek", "d3JhcHBlZC1yYWZ0",
				"--writer", c.entry,
			}, &buf)
			if err == nil {
				t.Errorf("%s: runEncryptionBootstrap returned nil, want error for --writer=%q", c.name, c.entry)
			}
		})
	}
}

func TestRunEncryptionRotateDEK_HappyPath(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{appliedIndex: 73}
	addr := startCustomEncryptionAdminTestServer(t, stub)

	var buf bytes.Buffer
	err := runEncryptionRotateDEK([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--purpose", "storage",
		"--new-dek-id", "9",
		// "raw-wrapped" base64-encoded
		"--wrapped-new-dek", "cmF3LXdyYXBwZWQ=",
		"--proposer-node-id", "111",
		"--proposer-local-epoch", "42",
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionRotateDEK: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "applied_index=73") {
		t.Errorf("output missing applied_index=73, got:\n%s", out)
	}
	if len(stub.rotateCalls) != 1 {
		t.Fatalf("RotateDEK calls=%d, want 1", len(stub.rotateCalls))
	}
	call := stub.rotateCalls[0]
	if call.Purpose != pb.RotateDEKRequest_PURPOSE_STORAGE ||
		call.NewDekId != 9 ||
		string(call.WrappedNewDek) != "raw-wrapped" ||
		call.ProposerNodeId != 111 ||
		call.ProposerLocalEpoch != 42 {
		t.Errorf("RotateDEK call=%+v does not match flag inputs", call)
	}
}

func TestRunEncryptionRotateDEK_RejectsBadPurpose(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionRotateDEK([]string{
		"--endpoint", "127.0.0.1:1",
		"--purpose", "junk",
		"--new-dek-id", "9",
		"--wrapped-new-dek", "cmF3LXdyYXBwZWQ=",
		"--proposer-node-id", "1",
		"--proposer-local-epoch", "0",
	}, &buf)
	if err == nil {
		t.Fatalf("runEncryptionRotateDEK returned nil, want error on --purpose=junk")
	}
}

func TestRunEncryptionRotateDEK_RejectsBadEpoch(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionRotateDEK([]string{
		"--endpoint", "127.0.0.1:1",
		"--purpose", "storage",
		"--new-dek-id", "9",
		"--wrapped-new-dek", "cmF3LXdyYXBwZWQ=",
		"--proposer-node-id", "1",
		"--proposer-local-epoch", "70000", // > 0xFFFF
	}, &buf)
	if err == nil || !strings.Contains(err.Error(), "16-bit bound") {
		t.Fatalf("runEncryptionRotateDEK error=%v, want bound-violation", err)
	}
}

func TestRunEncryptionRegisterWriter_HappyPath(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{appliedIndex: 88}
	addr := startCustomEncryptionAdminTestServer(t, stub)

	var buf bytes.Buffer
	err := runEncryptionRegisterWriter([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--dek-id", "3",
		"--full-node-id", "555",
		"--local-epoch", "21",
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionRegisterWriter: %v", err)
	}
	if !strings.Contains(buf.String(), "applied_index=88") {
		t.Errorf("missing applied_index=88, got:\n%s", buf.String())
	}
	if len(stub.registerCalls) != 1 {
		t.Fatalf("RegisterEncryptionWriter calls=%d, want 1", len(stub.registerCalls))
	}
	call := stub.registerCalls[0]
	if call.DekId != 3 ||
		len(call.Writers) != 1 ||
		call.Writers[0].FullNodeId != 555 ||
		call.Writers[0].LocalEpoch != 21 {
		t.Errorf("RegisterEncryptionWriter call=%+v does not match flag inputs", call)
	}
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
	go func() {
		// Surface unexpected Serve failures so a listener bind
		// or accept error before the test calls GracefulStop is
		// visible. GracefulStop returns nil from Serve, so the
		// happy-path exit prints nothing.
		if err := srv.Serve(lis); err != nil {
			t.Logf("encryption-admin test server Serve: %v", err)
		}
	}()
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

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
	rotateCalls             []*pb.RotateDEKRequest
	registerCalls           []*pb.RegisterEncryptionWriterRequest
	bootstrapCalls          []*pb.BootstrapEncryptionRequest
	capability              *pb.CapabilityReport
	capabilityErr           error
	enableEnvelopeCalls     []*pb.EnableStorageEnvelopeRequest
	enableEnvelopeResp      *pb.EnableStorageEnvelopeResponse
	enableRaftEnvelopeCalls []*pb.EnableRaftEnvelopeRequest
	enableRaftEnvelopeResp  *pb.EnableRaftEnvelopeResponse
	appliedIndex            uint64
	returnErr               error
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

func (s *stubMutatorServer) GetCapability(context.Context, *pb.Empty) (*pb.CapabilityReport, error) {
	if s.capabilityErr != nil {
		return nil, s.capabilityErr
	}
	if s.capability != nil {
		return s.capability, nil
	}
	return &pb.CapabilityReport{
		EncryptionCapable: true,
		FullNodeId:        1,
	}, nil
}

func (s *stubMutatorServer) EnableStorageEnvelope(_ context.Context, req *pb.EnableStorageEnvelopeRequest) (*pb.EnableStorageEnvelopeResponse, error) {
	s.enableEnvelopeCalls = append(s.enableEnvelopeCalls, req)
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	// enableEnvelopeResp lets tests pin the exact response shape
	// (fresh-success vs idempotent-retry vs defensive
	// cutover_index_unknown). When nil the stub defaults to the
	// fresh-success shape with appliedIndex; this keeps the
	// existing rotate / register / bootstrap fixtures unchanged
	// while the cutover tests opt in to the richer shape.
	if s.enableEnvelopeResp != nil {
		return s.enableEnvelopeResp, nil
	}
	return &pb.EnableStorageEnvelopeResponse{AppliedIndex: s.appliedIndex}, nil
}

func (s *stubMutatorServer) EnableRaftEnvelope(_ context.Context, req *pb.EnableRaftEnvelopeRequest) (*pb.EnableRaftEnvelopeResponse, error) {
	s.enableRaftEnvelopeCalls = append(s.enableRaftEnvelopeCalls, req)
	if s.returnErr != nil {
		return nil, s.returnErr
	}
	if s.enableRaftEnvelopeResp != nil {
		return s.enableRaftEnvelopeResp, nil
	}
	return &pb.EnableRaftEnvelopeResponse{AppliedIndex: s.appliedIndex}, nil
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

func TestRunEncryptionBootstrap_DiscoverFromHappyPath(t *testing.T) {
	t.Parallel()
	leader := &stubMutatorServer{
		appliedIndex: 117,
		capability: &pb.CapabilityReport{
			EncryptionCapable: true,
			FullNodeId:        11,
			LocalEpoch:        0,
		},
	}
	leaderAddr := startCustomEncryptionAdminTestServer(t, leader)
	follower := &stubMutatorServer{
		capability: &pb.CapabilityReport{
			EncryptionCapable: true,
			FullNodeId:        22,
			LocalEpoch:        5,
		},
	}
	followerAddr := startCustomEncryptionAdminTestServer(t, follower)

	var buf bytes.Buffer
	err := runEncryptionBootstrap([]string{
		"--endpoint", leaderAddr,
		"--timeout", "3s",
		"--storage-dek-id", "1",
		"--raft-dek-id", "2",
		"--wrapped-storage-dek", "d3JhcHBlZC1zdG9yYWdl",
		"--wrapped-raft-dek", "d3JhcHBlZC1yYWZ0",
		"--discover-from", leaderAddr,
		"--discover-from", followerAddr,
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionBootstrap: %v", err)
	}
	if !strings.Contains(buf.String(), "writers=2") {
		t.Errorf("output missing writers=2, got:\n%s", buf.String())
	}
	if len(leader.bootstrapCalls) != 1 {
		t.Fatalf("BootstrapEncryption calls=%d, want 1", len(leader.bootstrapCalls))
	}
	assertBootstrapCallMatches(t, leader.bootstrapCalls[0])
}

func TestRunEncryptionBootstrap_RejectsWriterAndDiscoverFrom(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionBootstrap([]string{
		"--endpoint", "127.0.0.1:1",
		"--storage-dek-id", "1",
		"--raft-dek-id", "2",
		"--wrapped-storage-dek", "d3JhcHBlZC1zdG9yYWdl",
		"--wrapped-raft-dek", "d3JhcHBlZC1yYWZ0",
		"--writer", "11:0",
		"--discover-from", "127.0.0.1:50051",
	}, &buf)
	if err == nil {
		t.Fatal("runEncryptionBootstrap returned nil, want mutually-exclusive writer source error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error %q does not mention mutually exclusive writer sources", err)
	}
}

func TestRunEncryptionBootstrap_DiscoverFromRejectsBadCapability(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		capability *pb.CapabilityReport
		want       string
	}{
		{
			name: "not capable",
			capability: &pb.CapabilityReport{
				EncryptionCapable: false,
				FullNodeId:        11,
			},
			want: "not encryption_capable",
		},
		{
			name: "zero full node id",
			capability: &pb.CapabilityReport{
				EncryptionCapable: true,
				FullNodeId:        0,
			},
			want: "full_node_id=0",
		},
		{
			name: "local epoch above uint16",
			capability: &pb.CapabilityReport{
				EncryptionCapable: true,
				FullNodeId:        11,
				LocalEpoch:        70000,
			},
			want: "exceeds 0xFFFF",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			leader := &stubMutatorServer{appliedIndex: 117}
			leaderAddr := startCustomEncryptionAdminTestServer(t, leader)
			peer := &stubMutatorServer{capability: tc.capability}
			peerAddr := startCustomEncryptionAdminTestServer(t, peer)

			var buf bytes.Buffer
			err := runEncryptionBootstrap([]string{
				"--endpoint", leaderAddr,
				"--timeout", "3s",
				"--storage-dek-id", "1",
				"--raft-dek-id", "2",
				"--wrapped-storage-dek", "d3JhcHBlZC1zdG9yYWdl",
				"--wrapped-raft-dek", "d3JhcHBlZC1yYWZ0",
				"--discover-from", peerAddr,
			}, &buf)
			if err == nil {
				t.Fatalf("runEncryptionBootstrap returned nil, want %q error", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
			if len(leader.bootstrapCalls) != 0 {
				t.Errorf("BootstrapEncryption calls=%d, want 0 after discovery rejection", len(leader.bootstrapCalls))
			}
		})
	}
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

// TestRunEncryptionProbeNodeID_Hex pins the §5.1 collision-
// mitigation helper from the 6D design doc: a hex full_node_id
// is parsed and narrowed via uint16(full_node_id & 0xFFFF) — the
// same mask that applier.go (writer-registry keying) and §4.1
// (GCM nonce prefix) already use. The CLI prints both the
// full_node_id (canonical 16-hex-digit form) and the derived
// node_id so the operator can verify the value is free in the
// cluster's allocated node_id space before joining.
func TestRunEncryptionProbeNodeID_Hex(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionProbeNodeID([]string{"--full-node-id=0xDEADBEEFCAFE1234"}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionProbeNodeID: %v", err)
	}
	out := buf.String()
	for _, needle := range []string{
		"full_node_id: 0xdeadbeefcafe1234",
		"node_id:      0x1234",
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("output missing %q:\n%s", needle, out)
		}
	}
}

// TestRunEncryptionProbeNodeID_Decimal pins the dual-radix
// parser: decimal input works alongside the 0x-prefixed hex case.
func TestRunEncryptionProbeNodeID_Decimal(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionProbeNodeID([]string{"--full-node-id=305419896"}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionProbeNodeID: %v", err)
	}
	// 305419896 = 0x12345678; low 16 bits = 0x5678
	out := buf.String()
	for _, needle := range []string{
		"full_node_id: 0x0000000012345678",
		"node_id:      0x5678",
	} {
		if !strings.Contains(out, needle) {
			t.Errorf("output missing %q:\n%s", needle, out)
		}
	}
}

// TestRunEncryptionProbeNodeID_RequiresFlag pins the
// required-flag contract: omitting --full-node-id returns an
// explicit error rather than producing a misleading "probe of
// zero" output.
func TestRunEncryptionProbeNodeID_RequiresFlag(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionProbeNodeID([]string{}, &buf)
	if err == nil {
		t.Fatal("missing --full-node-id: want error, got nil")
	}
	if !strings.Contains(err.Error(), "--full-node-id is required") {
		t.Errorf("error message: want mention of required flag, got %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("must not print output on error: got %q", buf.String())
	}
}

// TestRunEncryptionProbeNodeID_RejectsBadInput pins the
// invalid-input case: non-numeric, non-hex input returns an
// error rather than silently truncating.
func TestRunEncryptionProbeNodeID_RejectsBadInput(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionProbeNodeID([]string{"--full-node-id=notanumber"}, &buf)
	if err == nil {
		t.Fatal("invalid input: want error, got nil")
	}
}

// TestRunEncryptionProbeNodeID_HelpFlagExitsZero pins the
// flag.ErrHelp special-case: every other encryption subcommand
// (status, rotate-dek, register-writer, bootstrap) returns nil
// on `-h`/`--help` so shell scripts that test $? on help
// invocations don't trip. probe-node-id must match the
// convention. Codex r2 flagged the absent ErrHelp branch.
func TestRunEncryptionProbeNodeID_HelpFlagExitsZero(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionProbeNodeID([]string{"-h"}, &buf)
	if err != nil {
		t.Fatalf("-h: want nil, got %v", err)
	}
}

// TestRunEncryptionProbeNodeID_RejectsPartialInput pins the
// strconv.ParseUint vs fmt.Sscanf distinction: parseUint64WithRadix
// MUST reject inputs where only a prefix is parseable, because
// "0x1234ZZZZ" silently parsing as 0x1234 would mislead the
// operator into joining a node under a different node_id than
// the one they meant to probe. Sscanf accepts partial input;
// ParseUint rejects it.
func TestRunEncryptionProbeNodeID_RejectsPartialInput(t *testing.T) {
	t.Parallel()
	cases := []string{
		"--full-node-id=0x1234ZZZZ", // hex prefix with non-hex tail
		"--full-node-id=1234abc",    // decimal prefix with letter tail
		"--full-node-id=0xDEAD GHI", // hex with embedded space
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		err := runEncryptionProbeNodeID([]string{tc}, &buf)
		if err == nil {
			t.Errorf("partial-input %q: want error, got nil (output=%q)", tc, buf.String())
		}
	}
}

// TestEncryptionMain_ProbeNodeIDSubcommand pins the dispatch in
// encryptionMain: the new probe-node-id case must route to the
// runner. Without this, a typo in encryptionMain's switch
// statement would route the subcommand to the default branch
// and fail with "unknown subcommand", and the dispatch test
// would not catch it.
func TestEncryptionMain_ProbeNodeIDSubcommand(t *testing.T) {
	t.Parallel()
	// Help-flag path: probe-node-id without flags errors with
	// "required" rather than "unknown subcommand". A successful
	// dispatch route reaches runEncryptionProbeNodeID's
	// missing-flag branch.
	err := encryptionMain([]string{"probe-node-id"})
	if err == nil {
		t.Fatal("probe-node-id with no flags: want error, got nil")
	}
	if !strings.Contains(err.Error(), "--full-node-id is required") {
		t.Errorf("dispatch reached wrong handler: got %v", err)
	}
}

// TestRunEncryptionEnableStorageEnvelope_HappyPath pins the
// §3.1 fresh-success rendering: enabled prefix + applied_index
// + the per-member capability summary. A regression that
// dropped the summary or printed the wrong applied_index would
// trip the substring assertions; the wire-level proto assertion
// pins the request shape so the CLI cannot silently mis-marshal
// the proposer identity fields.
func TestRunEncryptionEnableStorageEnvelope_HappyPath(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{
		appliedIndex: 1234,
		enableEnvelopeResp: &pb.EnableStorageEnvelopeResponse{
			AppliedIndex:     1234,
			WasAlreadyActive: false,
			CapabilitySummary: []*pb.CapabilityVerdict{
				{FullNodeId: 11, EncryptionCapable: true, BuildSha: "build-n1", SidecarPresent: true},
				{FullNodeId: 22, EncryptionCapable: true, BuildSha: "build-n2", SidecarPresent: true},
			},
		},
	}
	addr := startCustomEncryptionAdminTestServer(t, stub)
	var buf bytes.Buffer
	err := runEncryptionEnableStorageEnvelope([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--proposer-node-id", "11",
		"--proposer-local-epoch", "7",
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionEnableStorageEnvelope: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "enabled applied_index=1234") {
		t.Errorf("output prefix missing fresh-success shape, got:\n%s", out)
	}
	if !strings.Contains(out, "full_node_id=11") || !strings.Contains(out, "build_sha=build-n1") {
		t.Errorf("output missing first verdict, got:\n%s", out)
	}
	if !strings.Contains(out, "full_node_id=22") || !strings.Contains(out, "build_sha=build-n2") {
		t.Errorf("output missing second verdict, got:\n%s", out)
	}
	if len(stub.enableEnvelopeCalls) != 1 {
		t.Fatalf("EnableStorageEnvelope calls=%d, want 1", len(stub.enableEnvelopeCalls))
	}
	call := stub.enableEnvelopeCalls[0]
	if call.ProposerNodeId != 11 || call.ProposerLocalEpoch != 7 {
		t.Errorf("EnableStorageEnvelope call=%+v does not match flag inputs", call)
	}
}

// TestRunEncryptionEnableStorageEnvelope_IdempotentRetry pins
// the §6.4 was_already_active=true rendering: the output uses a
// distinct prefix ("already-active") so a shell script can
// switch on column 1 of the result line to discriminate
// fresh-success from a no-op retry without parsing the full
// message. The applied_index reports the ORIGINAL cutover
// index from sidecar.StorageEnvelopeCutoverIndex (not this
// call's Raft index).
func TestRunEncryptionEnableStorageEnvelope_IdempotentRetry(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{
		enableEnvelopeResp: &pb.EnableStorageEnvelopeResponse{
			AppliedIndex:        555,
			WasAlreadyActive:    true,
			CutoverIndexUnknown: false,
			CapabilitySummary:   nil, // §3.1: empty on retries
		},
	}
	addr := startCustomEncryptionAdminTestServer(t, stub)
	var buf bytes.Buffer
	err := runEncryptionEnableStorageEnvelope([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--proposer-node-id", "11",
		"--proposer-local-epoch", "7",
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionEnableStorageEnvelope: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "already-active applied_index=555") {
		t.Errorf("output prefix missing already-active shape, got:\n%s", out)
	}
	if strings.Contains(out, "capability summary") {
		t.Errorf("idempotent retry must NOT print the capability summary header, got:\n%s", out)
	}
	if strings.Contains(out, "warning:") {
		t.Errorf("idempotent retry without cutover_index_unknown must NOT emit a warning, got:\n%s", out)
	}
}

// TestRunEncryptionEnableStorageEnvelope_DefensiveCutoverIndexUnknown
// pins the §6.4 defensive-fallback warning: a sidecar reporting
// StorageEnvelopeActive=true with StorageEnvelopeCutoverIndex=0
// (operationally impossible under normal apply but hedged
// against schema rollback / hand-edited sidecars) triggers a
// "warning: cutover_index_unknown=true" line BEFORE the
// already-active result. Operators can grep on the warning
// substring to flag clusters that need investigation.
func TestRunEncryptionEnableStorageEnvelope_DefensiveCutoverIndexUnknown(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{
		enableEnvelopeResp: &pb.EnableStorageEnvelopeResponse{
			AppliedIndex:        900,
			WasAlreadyActive:    true,
			CutoverIndexUnknown: true,
		},
	}
	addr := startCustomEncryptionAdminTestServer(t, stub)
	var buf bytes.Buffer
	if err := runEncryptionEnableStorageEnvelope([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--proposer-node-id", "11",
		"--proposer-local-epoch", "7",
	}, &buf); err != nil {
		t.Fatalf("runEncryptionEnableStorageEnvelope: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "warning: cutover_index_unknown=true") {
		t.Errorf("output missing defensive-fallback warning, got:\n%s", out)
	}
	if !strings.Contains(out, "already-active applied_index=900") {
		t.Errorf("output missing already-active shape, got:\n%s", out)
	}
}

// TestRunEncryptionEnableStorageEnvelope_RejectsZeroProposerNodeID
// pins the §6.1 sentinel rejection on the CLI side: passing
// --proposer-node-id=0 (or omitting the flag) MUST refuse
// before the RPC round-trip so an operator with a misconfigured
// shell variable fails fast. The server re-validates the same
// sentinel as the source of truth.
func TestRunEncryptionEnableStorageEnvelope_RejectsZeroProposerNodeID(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionEnableStorageEnvelope([]string{
		"--endpoint", "127.0.0.1:1",
		"--proposer-node-id", "0",
		"--proposer-local-epoch", "7",
	}, &buf)
	if err == nil {
		t.Fatal("runEncryptionEnableStorageEnvelope returned nil, want error on --proposer-node-id=0")
	}
	if !strings.Contains(err.Error(), "proposer-node-id") {
		t.Errorf("error %q does not hint at the rejected flag", err)
	}
}

// TestRunEncryptionEnableStorageEnvelope_RejectsBadEpoch pins
// the §4.1 16-bit bound on the CLI side. Same source-of-truth
// posture as RotateDEK / BootstrapEncryption.
func TestRunEncryptionEnableStorageEnvelope_RejectsBadEpoch(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionEnableStorageEnvelope([]string{
		"--endpoint", "127.0.0.1:1",
		"--proposer-node-id", "11",
		"--proposer-local-epoch", "70000", // > 0xFFFF
	}, &buf)
	if err == nil || !strings.Contains(err.Error(), "16-bit bound") {
		t.Fatalf("runEncryptionEnableStorageEnvelope error=%v, want bound-violation", err)
	}
}

// TestEncryptionMain_EnableStorageEnvelopeSubcommand pins the
// dispatch entry in encryptionMain. A typo in the switch
// statement would route enable-storage-envelope to the default
// branch and the user would see "unknown subcommand" rather
// than the runner's "--proposer-node-id is required" error.
func TestEncryptionMain_EnableStorageEnvelopeSubcommand(t *testing.T) {
	t.Parallel()
	err := encryptionMain([]string{"enable-storage-envelope"})
	if err == nil {
		t.Fatal("enable-storage-envelope with no flags: want error, got nil")
	}
	if !strings.Contains(err.Error(), "proposer-node-id") {
		t.Errorf("dispatch reached wrong handler: got %v", err)
	}
}

// TestRunEncryptionEnableRaftEnvelope_HappyPath pins the §3.1
// fresh-success rendering for the raft variant. Structurally
// identical to the storage HappyPath test except the response
// shape lacks the cutover_index_unknown field (raft variant uses
// RaftEnvelopeCutoverIndex != 0 as the sole active sentinel).
func TestRunEncryptionEnableRaftEnvelope_HappyPath(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{
		appliedIndex: 4242,
		enableRaftEnvelopeResp: &pb.EnableRaftEnvelopeResponse{
			AppliedIndex:     4242,
			WasAlreadyActive: false,
			CapabilitySummary: []*pb.CapabilityVerdict{
				{FullNodeId: 11, EncryptionCapable: true, BuildSha: "build-n1", SidecarPresent: true},
				{FullNodeId: 22, EncryptionCapable: true, BuildSha: "build-n2", SidecarPresent: true},
			},
		},
	}
	addr := startCustomEncryptionAdminTestServer(t, stub)
	var buf bytes.Buffer
	err := runEncryptionEnableRaftEnvelope([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--proposer-node-id", "11",
		"--proposer-local-epoch", "7",
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionEnableRaftEnvelope: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "enabled applied_index=4242") {
		t.Errorf("output prefix missing fresh-success shape, got:\n%s", out)
	}
	if !strings.Contains(out, "full_node_id=11") || !strings.Contains(out, "build_sha=build-n1") {
		t.Errorf("output missing first verdict, got:\n%s", out)
	}
	if len(stub.enableRaftEnvelopeCalls) != 1 {
		t.Fatalf("EnableRaftEnvelope calls=%d, want 1", len(stub.enableRaftEnvelopeCalls))
	}
	call := stub.enableRaftEnvelopeCalls[0]
	if call.ProposerNodeId != 11 || call.ProposerLocalEpoch != 7 {
		t.Errorf("EnableRaftEnvelope call=%+v does not match flag inputs", call)
	}
}

// TestRunEncryptionEnableRaftEnvelope_IdempotentRetry pins the
// was_already_active=true rendering. applied_index reports the
// ORIGINAL cutover index from sidecar.RaftEnvelopeCutoverIndex.
// No warning line — the raft variant has no defensive
// cutover_index_unknown field.
func TestRunEncryptionEnableRaftEnvelope_IdempotentRetry(t *testing.T) {
	t.Parallel()
	stub := &stubMutatorServer{
		enableRaftEnvelopeResp: &pb.EnableRaftEnvelopeResponse{
			AppliedIndex:      777,
			WasAlreadyActive:  true,
			CapabilitySummary: nil,
		},
	}
	addr := startCustomEncryptionAdminTestServer(t, stub)
	var buf bytes.Buffer
	err := runEncryptionEnableRaftEnvelope([]string{
		"--endpoint", addr,
		"--timeout", "3s",
		"--proposer-node-id", "11",
		"--proposer-local-epoch", "7",
	}, &buf)
	if err != nil {
		t.Fatalf("runEncryptionEnableRaftEnvelope: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "already-active applied_index=777") {
		t.Errorf("output prefix missing already-active shape, got:\n%s", out)
	}
	if strings.Contains(out, "capability summary") {
		t.Errorf("idempotent retry must NOT print the capability summary header, got:\n%s", out)
	}
	if strings.Contains(out, "warning:") {
		t.Errorf("raft variant must NOT emit a warning line (no cutover_index_unknown field), got:\n%s", out)
	}
}

// TestRunEncryptionEnableRaftEnvelope_RejectsZeroProposerNodeID
// pins the §6.1 sentinel rejection on the CLI side. Same
// posture as the storage variant.
func TestRunEncryptionEnableRaftEnvelope_RejectsZeroProposerNodeID(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionEnableRaftEnvelope([]string{
		"--endpoint", "127.0.0.1:1",
		"--proposer-node-id", "0",
		"--proposer-local-epoch", "7",
	}, &buf)
	if err == nil {
		t.Fatal("runEncryptionEnableRaftEnvelope returned nil, want error on --proposer-node-id=0")
	}
	if !strings.Contains(err.Error(), "proposer-node-id") {
		t.Errorf("error %q does not hint at the rejected flag", err)
	}
}

// TestRunEncryptionEnableRaftEnvelope_RejectsBadEpoch pins the
// §4.1 16-bit bound on the CLI side.
func TestRunEncryptionEnableRaftEnvelope_RejectsBadEpoch(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := runEncryptionEnableRaftEnvelope([]string{
		"--endpoint", "127.0.0.1:1",
		"--proposer-node-id", "11",
		"--proposer-local-epoch", "70000",
	}, &buf)
	if err == nil || !strings.Contains(err.Error(), "16-bit bound") {
		t.Fatalf("runEncryptionEnableRaftEnvelope error=%v, want bound-violation", err)
	}
}

// TestEncryptionMain_EnableRaftEnvelopeSubcommand pins the
// dispatch entry in encryptionMain. Mirror of the storage variant
// subcommand test.
func TestEncryptionMain_EnableRaftEnvelopeSubcommand(t *testing.T) {
	t.Parallel()
	err := encryptionMain([]string{"enable-raft-envelope"})
	if err == nil {
		t.Fatal("enable-raft-envelope with no flags: want error, got nil")
	}
	if !strings.Contains(err.Error(), "proposer-node-id") {
		t.Errorf("dispatch reached wrong handler: got %v", err)
	}
}

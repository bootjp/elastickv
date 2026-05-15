package adapter

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEncryptionAdmin_GetCapability_NoSidecarPath(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(WithEncryptionAdminBuildSHA("test-sha"))
	got, err := srv.GetCapability(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("GetCapability: %v", err)
	}
	if got.EncryptionCapable {
		t.Errorf("EncryptionCapable=true, want false on a node with no sidecar path")
	}
	if got.SidecarPresent {
		t.Errorf("SidecarPresent=true, want false on a node with no sidecar path")
	}
	if got.BuildSha != "test-sha" {
		t.Errorf("BuildSha=%q, want %q", got.BuildSha, "test-sha")
	}
}

func TestEncryptionAdmin_GetCapability_SidecarMissing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminSidecarPath(filepath.Join(dir, "keys.json")),
		WithEncryptionAdminBuildSHA("test-sha"),
	)
	got, err := srv.GetCapability(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("GetCapability: %v", err)
	}
	// §7.1 Phase 0: a node restarted with --encryption-enabled
	// (proxied here by WithEncryptionAdminSidecarPath) is "capable"
	// even before bootstrap. The cutover command needs that signal
	// before the bootstrap entry has had a chance to write the
	// sidecar, otherwise the bootstrap pre-check is a chicken-and-egg
	// loop.
	if !got.EncryptionCapable {
		t.Errorf("EncryptionCapable=false, want true on a Phase-0 node (path configured, sidecar not yet created)")
	}
	if got.SidecarPresent {
		t.Errorf("SidecarPresent=true, want false when sidecar file is missing")
	}
}

func TestEncryptionAdmin_GetCapability_NotBootstrapped(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		// Active.Storage == 0 means "not bootstrapped" per §5.1.
		Active: encryption.ActiveKeys{},
		Keys:   map[string]encryption.SidecarKey{},
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminFullNodeID(0xDEADBEEF_DEADBEEF),
	)
	got, err := srv.GetCapability(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("GetCapability: %v", err)
	}
	// §7.1 Phase 0 + §6.1: encryption_capable is gated on "node has
	// been restarted with --encryption-enabled", NOT on
	// Active.Storage != 0. A pre-bootstrap capable node reports true
	// so the bootstrap pre-check fan-out can proceed at all.
	if !got.EncryptionCapable {
		t.Errorf("EncryptionCapable=false, want true on a configured pre-bootstrap node (regression for PR754 P1)")
	}
	if !got.SidecarPresent {
		t.Errorf("SidecarPresent=false, want true when sidecar exists")
	}
	if got.FullNodeId != 0xDEADBEEF_DEADBEEF {
		t.Errorf("FullNodeId=%x, want %x", got.FullNodeId, uint64(0xDEADBEEF_DEADBEEF))
	}
}

func TestEncryptionAdmin_GetCapability_Bootstrapped(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active: encryption.ActiveKeys{Storage: 1, Raft: 2},
		Keys: map[string]encryption.SidecarKey{
			"1": {Purpose: "storage", Wrapped: []byte("wrapped-1")},
			"2": {Purpose: "raft", Wrapped: []byte("wrapped-2")},
		},
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminFullNodeID(7),
	)
	got, err := srv.GetCapability(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("GetCapability: %v", err)
	}
	if !got.EncryptionCapable {
		t.Errorf("EncryptionCapable=false, want true after bootstrap")
	}
	if !got.SidecarPresent {
		t.Errorf("SidecarPresent=false, want true")
	}
	if got.LocalEpoch != 0 {
		// PR-A always reports local_epoch=0; Stage 7 will wire the
		// writer-registry counter. The cutover pre-check uses 0
		// because no DEK exists yet at bootstrap time.
		t.Errorf("LocalEpoch=%d, want 0 in PR-A", got.LocalEpoch)
	}
}

func TestEncryptionAdmin_GetSidecarState_NoSidecarPath(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer()
	_, err := srv.GetSidecarState(context.Background(), &pb.Empty{})
	if got, want := status.Code(err), codes.FailedPrecondition; got != want {
		t.Errorf("status=%v, want %v (err=%v)", got, want, err)
	}
}

func TestEncryptionAdmin_GetSidecarState_ShipsWrappedDEKs(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		RaftAppliedIndex:         42,
		StorageEnvelopeActive:    true,
		RaftEnvelopeCutoverIndex: 100,
		Active:                   encryption.ActiveKeys{Storage: 1, Raft: 2},
		Keys: map[string]encryption.SidecarKey{
			"1": {Purpose: "storage", Wrapped: []byte("wrapped-1")},
			"2": {Purpose: "raft", Wrapped: []byte("wrapped-2")},
		},
	})
	srv := NewEncryptionAdminServer(WithEncryptionAdminSidecarPath(path))
	got, err := srv.GetSidecarState(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("GetSidecarState: %v", err)
	}
	assertSidecarHeader(t, got)
	assertSidecarWrappedDEKs(t, got)
}

func assertSidecarHeader(t *testing.T, got *pb.SidecarStateReport) {
	t.Helper()
	if got.ActiveStorageId != 1 || got.ActiveRaftId != 2 {
		t.Errorf("active=(s=%d,r=%d), want (1,2)", got.ActiveStorageId, got.ActiveRaftId)
	}
	if !got.StorageEnvelopeActive {
		t.Errorf("StorageEnvelopeActive=false, want true")
	}
	if got.RaftEnvelopeCutoverIndex != 100 {
		t.Errorf("RaftEnvelopeCutoverIndex=%d, want 100", got.RaftEnvelopeCutoverIndex)
	}
	if got.LatestAppliedIndex != 42 {
		t.Errorf("LatestAppliedIndex=%d, want 42 (sidecar fallback)", got.LatestAppliedIndex)
	}
}

func assertSidecarWrappedDEKs(t *testing.T, got *pb.SidecarStateReport) {
	t.Helper()
	if string(got.WrappedDeksById[1]) != "wrapped-1" {
		t.Errorf("wrapped[1]=%q, want %q", got.WrappedDeksById[1], "wrapped-1")
	}
	if string(got.WrappedDeksById[2]) != "wrapped-2" {
		t.Errorf("wrapped[2]=%q, want %q", got.WrappedDeksById[2], "wrapped-2")
	}
	if got.WriterRegistryForCaller == nil {
		t.Errorf("WriterRegistryForCaller=nil, want empty non-nil map (PR-A contract)")
	}
	if len(got.WriterRegistryForCaller) != 0 {
		t.Errorf("WriterRegistryForCaller=%v, want empty in PR-A", got.WriterRegistryForCaller)
	}
}

func TestEncryptionAdmin_GetSidecarState_CallbackOverridesSidecarIndex(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		RaftAppliedIndex: 10,
		Active:           encryption.ActiveKeys{Storage: 1, Raft: 2},
		Keys: map[string]encryption.SidecarKey{
			"1": {Purpose: "storage", Wrapped: []byte("w")},
			"2": {Purpose: "raft", Wrapped: []byte("w")},
		},
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminLatestAppliedIndex(func() uint64 { return 9999 }),
	)
	got, err := srv.GetSidecarState(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("GetSidecarState: %v", err)
	}
	if got.LatestAppliedIndex != 9999 {
		t.Errorf("LatestAppliedIndex=%d, want callback value 9999", got.LatestAppliedIndex)
	}
}

func TestEncryptionAdmin_ResyncSidecar_ShipsWrappedDEKs(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		RaftAppliedIndex: 17,
		Active:           encryption.ActiveKeys{Storage: 3, Raft: 4},
		Keys: map[string]encryption.SidecarKey{
			"3": {Purpose: "storage", Wrapped: []byte("ws")},
			"4": {Purpose: "raft", Wrapped: []byte("wr")},
		},
	})
	srv := NewEncryptionAdminServer(WithEncryptionAdminSidecarPath(path))
	got, err := srv.ResyncSidecar(context.Background(), &pb.ResyncSidecarRequest{CallerFullNodeId: 5})
	if err != nil {
		t.Fatalf("ResyncSidecar: %v", err)
	}
	if got.ActiveStorageId != 3 || got.ActiveRaftId != 4 {
		t.Errorf("active=(s=%d,r=%d), want (3,4)", got.ActiveStorageId, got.ActiveRaftId)
	}
	if got.LeaderLatestAppliedIndex != 17 {
		t.Errorf("LeaderLatestAppliedIndex=%d, want 17", got.LeaderLatestAppliedIndex)
	}
	if string(got.WrappedDeksById[3]) != "ws" || string(got.WrappedDeksById[4]) != "wr" {
		t.Errorf("wrapped=%v, want id3=ws id4=wr", got.WrappedDeksById)
	}
}

func TestEncryptionAdmin_MutatingRPCs_AreUnimplemented(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer()
	ctx := context.Background()
	if _, err := srv.BootstrapEncryption(ctx, &pb.BootstrapEncryptionRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("BootstrapEncryption status=%v, want Unimplemented", status.Code(err))
	}
	if _, err := srv.RotateDEK(ctx, &pb.RotateDEKRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("RotateDEK status=%v, want Unimplemented", status.Code(err))
	}
	if _, err := srv.RegisterEncryptionWriter(ctx, &pb.RegisterEncryptionWriterRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("RegisterEncryptionWriter status=%v, want Unimplemented", status.Code(err))
	}
}

// writeSidecarFixture builds a valid keys.json fixture in a tmpdir and
// returns the path. The caller's *Sidecar is shallow-copied before
// WriteSidecar so the fixture's Version is set automatically.
func writeSidecarFixture(t *testing.T, sc *encryption.Sidecar) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	if err := encryption.WriteSidecar(path, sc); err != nil {
		t.Fatalf("WriteSidecar: %v", err)
	}
	return path
}

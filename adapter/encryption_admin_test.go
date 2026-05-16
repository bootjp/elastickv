package adapter

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
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
		t.Errorf("EncryptionCapable=false, want true on a configured pre-bootstrap node")
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
		// Stage 5 reports local_epoch=0 unconditionally; Stage 7
		// wires the writer-registry counter so the cutover
		// pre-check captures the per-node value. The §5.6 step
		// 1a flow tolerates 0 here because no DEK exists yet at
		// bootstrap time.
		t.Errorf("LocalEpoch=%d, want 0 pre-Stage-7", got.LocalEpoch)
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
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminSidecarPath(path),
		// Wire a leader-stable LeaderView so the happy path
		// exercises State()==StateLeader && VerifyLeader()==nil
		// end-to-end rather than short-circuiting through the
		// nil-leaderView test escape hatch.
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
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
	// Mirror the GetSidecarState contract: non-nil empty map until
	// Stage 7 wires the writer registry. Locks in the §5.5 promise
	// so a future change to the field cannot silently degrade to nil.
	if got.WriterRegistryForCaller == nil {
		t.Errorf("WriterRegistryForCaller=nil, want empty non-nil map (PR-A contract)")
	}
	if len(got.WriterRegistryForCaller) != 0 {
		t.Errorf("WriterRegistryForCaller=%v, want empty in PR-A", got.WriterRegistryForCaller)
	}
}

// TestEncryptionAdmin_MutatingRPCs_RejectWithoutProposer pins the
// production-inert guarantee: an EncryptionAdminServer built
// without WithEncryptionAdminProposer must reject every mutating
// RPC. The future cluster flag is what flips this on by wiring a
// real proposer.
func TestEncryptionAdmin_MutatingRPCs_RejectWithoutProposer(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer()
	ctx := context.Background()
	if _, err := srv.BootstrapEncryption(ctx, validBootstrapEncryptionRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("BootstrapEncryption status=%v, want FailedPrecondition (no proposer wired)", status.Code(err))
	}
	if _, err := srv.RotateDEK(ctx, validRotateDEKRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RotateDEK status=%v, want FailedPrecondition (no proposer wired)", status.Code(err))
	}
	if _, err := srv.RegisterEncryptionWriter(ctx, validRegisterEncryptionWriterRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RegisterEncryptionWriter status=%v, want FailedPrecondition (no proposer wired)", status.Code(err))
	}
}

func validBootstrapEncryptionRequest() *pb.BootstrapEncryptionRequest {
	return &pb.BootstrapEncryptionRequest{
		WrappedStorageDek: []byte("wrapped-storage-bytes"),
		WrappedRaftDek:    []byte("wrapped-raft-bytes"),
		StorageDekId:      1,
		RaftDekId:         2,
		WriterBatch: []*pb.WriterRegistryEntry{
			{FullNodeId: 11, LocalEpoch: 0},
			{FullNodeId: 22, LocalEpoch: 0},
		},
	}
}

func validRotateDEKRequest() *pb.RotateDEKRequest {
	return &pb.RotateDEKRequest{
		Purpose:            pb.RotateDEKRequest_PURPOSE_STORAGE,
		NewDekId:           5,
		WrappedNewDek:      []byte("wrapped-bytes"),
		ProposerNodeId:     11,
		ProposerLocalEpoch: 7,
	}
}

func validRegisterEncryptionWriterRequest() *pb.RegisterEncryptionWriterRequest {
	return &pb.RegisterEncryptionWriterRequest{
		DekId: 5,
		Writers: []*pb.WriterRegistryEntry{
			{FullNodeId: 11, LocalEpoch: 7},
		},
	}
}

// TestEncryptionAdmin_Validate_RejectsProposerWithoutLeaderView
// pins the production-wiring guard: a server constructed with a
// proposer but no LeaderView is rejected at Validate() time so a
// follower cannot silently propose state-changing entries.
func TestEncryptionAdmin_Validate_RejectsProposerWithoutLeaderView(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
	)
	if err := srv.Validate(); err == nil {
		t.Fatalf("Validate returned nil, want error for proposer-without-leaderView")
	}
}

// TestEncryptionAdmin_Validate_OKWithoutProposer keeps the
// read-only-server affordance: a server with neither proposer
// nor leaderView is valid (it just rejects every mutator with
// "proposer not configured"). Tests in this package rely on
// this path.
func TestEncryptionAdmin_Validate_OKWithoutProposer(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer()
	if err := srv.Validate(); err != nil {
		t.Errorf("Validate returned %v, want nil for read-only server", err)
	}
}

// TestEncryptionAdmin_Validate_OKWithBothWired covers the
// production happy path.
func TestEncryptionAdmin_Validate_OKWithBothWired(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	if err := srv.Validate(); err != nil {
		t.Errorf("Validate returned %v, want nil", err)
	}
}

// TestEncryptionAdmin_BootstrapEncryption_HappyPath verifies the
// byte layout of the proposed §5.6 0x04 Raft entry by
// round-tripping through fsmwire.DecodeBootstrap. Each writer in
// the request expands into TWO registry rows (one per active
// dek_id) so the §4.1 nonce-uniqueness invariant covers both
// envelope purposes from the first post-bootstrap entry.
func TestEncryptionAdmin_BootstrapEncryption_HappyPath(t *testing.T) {
	t.Parallel()
	proposer := &recordingProposer{commitIndex: 5}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validBootstrapEncryptionRequest()
	got, err := srv.BootstrapEncryption(context.Background(), req)
	if err != nil {
		t.Fatalf("BootstrapEncryption: %v", err)
	}
	if got.AppliedIndex != 5 {
		t.Errorf("AppliedIndex=%d, want 5", got.AppliedIndex)
	}
	assertSingleProposalOpcode(t, proposer.calls, fsmwire.OpBootstrap)
	decoded, err := fsmwire.DecodeBootstrap(proposer.calls[0][1:])
	if err != nil {
		t.Fatalf("DecodeBootstrap: %v", err)
	}
	assertBootstrapDecodes(t, decoded, req)
}

func assertBootstrapDecodes(t *testing.T, got fsmwire.BootstrapPayload, req *pb.BootstrapEncryptionRequest) {
	t.Helper()
	assertBootstrapHeader(t, got, req)
	wantRows := 2 * len(req.WriterBatch)
	if len(got.BatchRegistry) != wantRows {
		t.Fatalf("BatchRegistry len=%d, want %d (2 rows per writer)",
			len(got.BatchRegistry), wantRows)
	}
	for i, w := range req.WriterBatch {
		assertBootstrapWriterRows(t, i, got.BatchRegistry, w, req.StorageDekId, req.RaftDekId)
	}
}

func assertBootstrapHeader(t *testing.T, got fsmwire.BootstrapPayload, req *pb.BootstrapEncryptionRequest) {
	t.Helper()
	if got.StorageDEKID != req.StorageDekId || got.RaftDEKID != req.RaftDekId {
		t.Errorf("dek_ids=(s=%d, r=%d), want (%d, %d)",
			got.StorageDEKID, got.RaftDEKID, req.StorageDekId, req.RaftDekId)
	}
	if string(got.WrappedStorage) != string(req.WrappedStorageDek) {
		t.Errorf("WrappedStorage=%q, want %q", got.WrappedStorage, req.WrappedStorageDek)
	}
	if string(got.WrappedRaft) != string(req.WrappedRaftDek) {
		t.Errorf("WrappedRaft=%q, want %q", got.WrappedRaft, req.WrappedRaftDek)
	}
}

func assertBootstrapWriterRows(t *testing.T, i int, rows []fsmwire.RegistrationPayload, w *pb.WriterRegistryEntry, storageDEKID, raftDEKID uint32) {
	t.Helper()
	storage := rows[2*i]
	raft := rows[2*i+1]
	if storage.DEKID != storageDEKID || storage.FullNodeID != w.FullNodeId ||
		raft.DEKID != raftDEKID || raft.FullNodeID != w.FullNodeId ||
		uint32(storage.LocalEpoch) != w.LocalEpoch || uint32(raft.LocalEpoch) != w.LocalEpoch {
		t.Errorf("writer[%d]: storage=%+v raft=%+v, want both anchored to full_node_id=%d epoch=%d",
			i, storage, raft, w.FullNodeId, w.LocalEpoch)
	}
}

func TestEncryptionAdmin_BootstrapEncryption_RejectsBadInputs(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	type tc struct {
		name string
		mut  func(r *pb.BootstrapEncryptionRequest)
	}
	for _, c := range []tc{
		{"zero storage_dek_id", func(r *pb.BootstrapEncryptionRequest) { r.StorageDekId = 0 }},
		{"zero raft_dek_id", func(r *pb.BootstrapEncryptionRequest) { r.RaftDekId = 0 }},
		{"storage_dek_id == raft_dek_id", func(r *pb.BootstrapEncryptionRequest) {
			r.StorageDekId = 7
			r.RaftDekId = 7
		}},
		{"empty wrapped_storage_dek", func(r *pb.BootstrapEncryptionRequest) { r.WrappedStorageDek = nil }},
		{"empty wrapped_raft_dek", func(r *pb.BootstrapEncryptionRequest) { r.WrappedRaftDek = nil }},
		{"empty writer_batch", func(r *pb.BootstrapEncryptionRequest) { r.WriterBatch = nil }},
		{"writer with zero full_node_id", func(r *pb.BootstrapEncryptionRequest) {
			r.WriterBatch = []*pb.WriterRegistryEntry{{FullNodeId: 0, LocalEpoch: 0}}
		}},
		{"writer with local_epoch above 0xFFFF", func(r *pb.BootstrapEncryptionRequest) {
			r.WriterBatch = []*pb.WriterRegistryEntry{{FullNodeId: 1, LocalEpoch: 0x10000}}
		}},
		{"nil writer in batch", func(r *pb.BootstrapEncryptionRequest) {
			r.WriterBatch = []*pb.WriterRegistryEntry{nil}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			req := validBootstrapEncryptionRequest()
			c.mut(req)
			_, err := srv.BootstrapEncryption(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("%s: status=%v, want InvalidArgument", c.name, status.Code(err))
			}
		})
	}
}

// TestEncryptionAdmin_BootstrapEncryption_RejectsDuplicateFullNodeID
// pins the cluster-safety invariant: a writer batch that names
// the same full_node_id twice would, after FSM apply, attempt to
// re-register the same registry row with possibly-different
// local_epochs — which the §4.1 case-3 rollback guard would
// reject. FSM apply errors are fatal under the HaltApply seam,
// so the cluster halts. Reject at the gRPC boundary instead.
func TestEncryptionAdmin_BootstrapEncryption_RejectsDuplicateFullNodeID(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validBootstrapEncryptionRequest()
	req.WriterBatch = []*pb.WriterRegistryEntry{
		{FullNodeId: 7, LocalEpoch: 0},
		{FullNodeId: 7, LocalEpoch: 0},
	}
	_, err := srv.BootstrapEncryption(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("status=%v, want InvalidArgument on duplicate full_node_id", status.Code(err))
	}
}

// TestEncryptionAdmin_BootstrapEncryption_RejectsNodeIDCollision
// pins the §4.1 case-4 invariant: two distinct full_node_ids that
// collide on the uint16 narrowing share the same registry row
// key (!encryption|writers|<dek_id>|<uint16(node_id)>). The FSM
// halts on ErrNodeIDCollision so the boundary must reject.
func TestEncryptionAdmin_BootstrapEncryption_RejectsNodeIDCollision(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validBootstrapEncryptionRequest()
	req.WriterBatch = []*pb.WriterRegistryEntry{
		{FullNodeId: 0x0001_0000_0000_0007, LocalEpoch: 0}, // uint16=0x0007
		{FullNodeId: 0x0002_0000_0000_0007, LocalEpoch: 0}, // same uint16, different upper bits
	}
	_, err := srv.BootstrapEncryption(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("status=%v, want InvalidArgument on uint16(node_id) collision", status.Code(err))
	}
}

// TestEncryptionAdmin_BootstrapEncryption_RejectsOversizeWrappedDEK
// pins the DoS-defense bound: wrapped DEK payloads above the
// per-field cap are rejected at the gRPC boundary so a crafted
// request cannot push fsmwire.EncodeBootstrap toward its safeU32
// guard.
func TestEncryptionAdmin_BootstrapEncryption_RejectsOversizeWrappedDEK(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	huge := make([]byte, maxWrappedDEKSize+1)
	for _, mut := range []func(r *pb.BootstrapEncryptionRequest){
		func(r *pb.BootstrapEncryptionRequest) { r.WrappedStorageDek = huge },
		func(r *pb.BootstrapEncryptionRequest) { r.WrappedRaftDek = huge },
	} {
		req := validBootstrapEncryptionRequest()
		mut(req)
		_, err := srv.BootstrapEncryption(context.Background(), req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("status=%v, want InvalidArgument on oversize wrapped DEK", status.Code(err))
		}
	}
}

func TestEncryptionAdmin_BootstrapEncryption_RejectsOversizeBatch(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validBootstrapEncryptionRequest()
	// bootstrapBatchRowCap is 16384 ≈ 2 * 8192 writers. Stage 1a
	// expands each writer into two rows; we ship just above the
	// cap so the boundary check fires.
	writers := make([]*pb.WriterRegistryEntry, bootstrapBatchRowCap/2+1)
	for i := range writers {
		// i ranges 0..~8192; bound-checked to stay inside uint64.
		writers[i] = &pb.WriterRegistryEntry{FullNodeId: uint64(i) + 1} //nolint:gosec // i is bounded by len(writers) ≪ math.MaxInt64
	}
	req.WriterBatch = writers
	_, err := srv.BootstrapEncryption(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("BootstrapEncryption status=%v, want InvalidArgument on oversize batch", status.Code(err))
	}
}

// TestEncryptionAdmin_RotateDEK_HappyPath verifies the byte layout
// of the proposed Raft entry: leading opcode tag 0x05 followed by
// the fsmwire-encoded body produced by fsmwire.EncodeRotation.
// Pins the wire so a future refactor of either the server or
// fsmwire cannot silently desync.
func TestEncryptionAdmin_RotateDEK_HappyPath(t *testing.T) {
	t.Parallel()
	proposer := &recordingProposer{commitIndex: 99}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validRotateDEKRequest()
	got, err := srv.RotateDEK(context.Background(), req)
	if err != nil {
		t.Fatalf("RotateDEK: %v", err)
	}
	if got.AppliedIndex != 99 {
		t.Errorf("AppliedIndex=%d, want 99", got.AppliedIndex)
	}
	assertSingleProposalOpcode(t, proposer.calls, fsmwire.OpRotation)
	body := proposer.calls[0][1:]
	decoded, err := fsmwire.DecodeRotation(body)
	if err != nil {
		t.Fatalf("DecodeRotation: %v", err)
	}
	assertRotationDecodes(t, decoded, req)
}

func assertSingleProposalOpcode(t *testing.T, calls [][]byte, want byte) {
	t.Helper()
	if len(calls) != 1 {
		t.Fatalf("propose calls=%d, want 1", len(calls))
	}
	if len(calls[0]) == 0 || calls[0][0] != want {
		t.Fatalf("entry[0]=0x%02x, want 0x%02x", calls[0][0], want)
	}
}

func assertRotationDecodes(t *testing.T, got fsmwire.RotationPayload, req *pb.RotateDEKRequest) {
	t.Helper()
	if got.SubTag != fsmwire.RotateSubRotateDEK {
		t.Errorf("SubTag=0x%02x, want RotateSubRotateDEK", got.SubTag)
	}
	if got.DEKID != req.NewDekId {
		t.Errorf("DEKID=%d, want %d", got.DEKID, req.NewDekId)
	}
	if got.Purpose != fsmwire.PurposeStorage {
		t.Errorf("Purpose=%v, want PurposeStorage", got.Purpose)
	}
	if string(got.Wrapped) != string(req.WrappedNewDek) {
		t.Errorf("Wrapped=%q, want %q", got.Wrapped, req.WrappedNewDek)
	}
	if got.ProposerRegistration.DEKID != req.NewDekId ||
		got.ProposerRegistration.FullNodeID != req.ProposerNodeId ||
		uint32(got.ProposerRegistration.LocalEpoch) != req.ProposerLocalEpoch {
		t.Errorf("ProposerRegistration=%+v does not match request (dek=%d node=%d epoch=%d)",
			got.ProposerRegistration, req.NewDekId, req.ProposerNodeId, req.ProposerLocalEpoch)
	}
}

func TestEncryptionAdmin_RotateDEK_RejectsOnFollower(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{
			state:  raftengine.StateFollower,
			leader: raftengine.LeaderInfo{ID: "n2", Address: "127.0.0.1:50052"},
		}),
	)
	_, err := srv.RotateDEK(context.Background(), validRotateDEKRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RotateDEK status=%v, want FailedPrecondition", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "n2") || !strings.Contains(err.Error(), "127.0.0.1:50052") {
		t.Errorf("error %q does not embed the leader hint (id=n2 addr=127.0.0.1:50052)", err)
	}
}

// TestEncryptionAdmin_RotateDEK_RejectsOversizeWrappedDEK is the
// RotateDEK twin of the Bootstrap size-cap defence: a wrapped DEK
// above maxWrappedDEKSize at the gRPC boundary cannot push
// fsmwire.EncodeRotation toward its safeU32 length-prefix guard.
func TestEncryptionAdmin_RotateDEK_RejectsOversizeWrappedDEK(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validRotateDEKRequest()
	req.WrappedNewDek = make([]byte, maxWrappedDEKSize+1)
	_, err := srv.RotateDEK(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("RotateDEK status=%v, want InvalidArgument on oversize wrapped_new_dek", status.Code(err))
	}
}

func TestEncryptionAdmin_RotateDEK_RejectsBadInputs(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	type tc struct {
		name string
		mut  func(r *pb.RotateDEKRequest)
	}
	for _, c := range []tc{
		{"zero new_dek_id", func(r *pb.RotateDEKRequest) { r.NewDekId = 0 }},
		{"empty wrapped", func(r *pb.RotateDEKRequest) { r.WrappedNewDek = nil }},
		{"unspecified purpose", func(r *pb.RotateDEKRequest) { r.Purpose = pb.RotateDEKRequest_PURPOSE_UNSPECIFIED }},
		{"local_epoch above 0xFFFF", func(r *pb.RotateDEKRequest) { r.ProposerLocalEpoch = 0x10000 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			req := validRotateDEKRequest()
			c.mut(req)
			_, err := srv.RotateDEK(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("%s: status=%v, want InvalidArgument", c.name, status.Code(err))
			}
		})
	}
}

func TestEncryptionAdmin_RegisterEncryptionWriter_HappyPath(t *testing.T) {
	t.Parallel()
	proposer := &recordingProposer{commitIndex: 42}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validRegisterEncryptionWriterRequest()
	got, err := srv.RegisterEncryptionWriter(context.Background(), req)
	if err != nil {
		t.Fatalf("RegisterEncryptionWriter: %v", err)
	}
	if got.AppliedIndex != 42 {
		t.Errorf("AppliedIndex=%d, want 42", got.AppliedIndex)
	}
	if len(proposer.calls) != 1 {
		t.Fatalf("propose calls=%d, want 1", len(proposer.calls))
	}
	if proposer.calls[0][0] != fsmwire.OpRegistration {
		t.Fatalf("entry[0]=0x%02x, want OpRegistration 0x03", proposer.calls[0][0])
	}
	got2, err := fsmwire.DecodeRegistration(proposer.calls[0][1:])
	if err != nil {
		t.Fatalf("DecodeRegistration: %v", err)
	}
	if got2.DEKID != req.DekId ||
		got2.FullNodeID != req.Writers[0].FullNodeId ||
		uint32(got2.LocalEpoch) != req.Writers[0].LocalEpoch {
		t.Errorf("decoded registration %+v does not match request %+v", got2, req)
	}
}

// TestEncryptionAdmin_RotateDEK_RejectsZeroProposerNodeID pins the
// §6.1 invariant that full_node_id=0 is the "not encryption-capable"
// sentinel and must not be persisted into a writer-registry row.
// Reject at the gRPC boundary with InvalidArgument.
func TestEncryptionAdmin_RotateDEK_RejectsZeroProposerNodeID(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := validRotateDEKRequest()
	req.ProposerNodeId = 0
	_, err := srv.RotateDEK(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("RotateDEK status=%v, want InvalidArgument (zero proposer_node_id is reserved)", status.Code(err))
	}
}

// TestEncryptionAdmin_RegisterEncryptionWriter_RejectsZeroFullNodeID
// is the twin regression test for the registration path.
func TestEncryptionAdmin_RegisterEncryptionWriter_RejectsZeroFullNodeID(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := &pb.RegisterEncryptionWriterRequest{
		DekId: 5,
		Writers: []*pb.WriterRegistryEntry{
			{FullNodeId: 0, LocalEpoch: 1},
		},
	}
	_, err := srv.RegisterEncryptionWriter(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("RegisterEncryptionWriter status=%v, want InvalidArgument (zero full_node_id is reserved)", status.Code(err))
	}
}

// TestEncryptionAdmin_RotateDEK_RejectsStaleLeader pins the
// stale-leader invariant: a partitioned former leader whose
// local State() still reports StateLeader but whose VerifyLeader
// fails (no quorum) must reject mutating RPCs with
// FailedPrecondition. Without this guard, a stranded leader
// would accept proposals that may never replicate.
func TestEncryptionAdmin_RotateDEK_RejectsStaleLeader(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{
			state:     raftengine.StateLeader,
			verifyErr: errors.New("synthetic quorum loss"),
		}),
	)
	_, err := srv.RotateDEK(context.Background(), validRotateDEKRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RotateDEK status=%v, want FailedPrecondition on stale-leader (VerifyLeader failure)", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "VerifyLeader") {
		t.Errorf("error %q does not surface the VerifyLeader failure path", err)
	}
}

// TestEncryptionAdmin_ResyncSidecar_RejectsStaleLeader is the
// read-only path twin of the above. ResyncSidecar has no
// Propose() to catch leadership loss, so VerifyLeader is the
// only defence between a follower's recovery flow and a stale
// leader's outdated sidecar contents.
func TestEncryptionAdmin_ResyncSidecar_RejectsStaleLeader(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active: encryption.ActiveKeys{Storage: 1, Raft: 2},
		Keys: map[string]encryption.SidecarKey{
			"1": {Purpose: "storage", Wrapped: []byte("w")},
			"2": {Purpose: "raft", Wrapped: []byte("w")},
		},
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminLeaderView(stubLeaderView{
			state:     raftengine.StateLeader,
			verifyErr: errors.New("synthetic quorum loss"),
		}),
	)
	_, err := srv.ResyncSidecar(context.Background(), &pb.ResyncSidecarRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResyncSidecar status=%v, want FailedPrecondition on stale-leader", status.Code(err))
	}
}

// TestEncryptionAdmin_RotateDEK_VerifyLeader_PreservesContextCodes
// pins the context-code mapping: when VerifyLeader returns
// context.Canceled / context.DeadlineExceeded (the caller's ctx
// was canceled or the deadline elapsed during the ReadIndex
// round-trip), requireLeader MUST surface the matching
// codes.Canceled / codes.DeadlineExceeded — NOT a flat
// FailedPrecondition that would make a transport timeout look
// like a leadership rejection.
func TestEncryptionAdmin_RotateDEK_VerifyLeader_PreservesContextCodes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"context.Canceled", context.Canceled, codes.Canceled},
		{"context.DeadlineExceeded", context.DeadlineExceeded, codes.DeadlineExceeded},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := NewEncryptionAdminServer(
				WithEncryptionAdminProposer(&recordingProposer{}),
				WithEncryptionAdminLeaderView(stubLeaderView{
					state:     raftengine.StateLeader,
					verifyErr: c.err,
				}),
			)
			_, err := srv.RotateDEK(context.Background(), validRotateDEKRequest())
			if got := status.Code(err); got != c.want {
				t.Errorf("RotateDEK status=%v, want %v for VerifyLeader=%v", got, c.want, c.err)
			}
		})
	}
}

// TestEncryptionAdmin_RotateDEK_MapsProposeLeaderErrorToFailedPrecondition
// pins the Propose-side leadership-error mapping: Propose() returning
// ErrNotLeader / ErrLeadershipLost / ErrLeadershipTransferInProgress
// must surface as FailedPrecondition with the engine error in the
// status detail so clients can retry against the right node, NOT
// as the default codes.Unknown a generic wrap would produce.
func TestEncryptionAdmin_RotateDEK_MapsProposeLeaderErrorToFailedPrecondition(t *testing.T) {
	t.Parallel()
	for _, sentinel := range []error{
		raftengine.ErrNotLeader,
		raftengine.ErrLeadershipLost,
		raftengine.ErrLeadershipTransferInProgress,
	} {
		t.Run(sentinel.Error(), func(t *testing.T) {
			proposer := &recordingProposer{err: sentinel}
			srv := NewEncryptionAdminServer(
				WithEncryptionAdminProposer(proposer),
				WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
			)
			_, err := srv.RotateDEK(context.Background(), validRotateDEKRequest())
			if status.Code(err) != codes.FailedPrecondition {
				t.Errorf("RotateDEK status=%v, want FailedPrecondition for %v", status.Code(err), sentinel)
			}
		})
	}
}

// TestEncryptionAdmin_RotateDEK_MapsProposeOtherErrorToUnavailable
// pins the non-leadership failure-mode mapping. A propose failure
// that is NOT a known leadership sentinel surfaces as Unavailable
// (retryable transient failure)
// instead of Unknown.
func TestEncryptionAdmin_RotateDEK_MapsProposeOtherErrorToUnavailable(t *testing.T) {
	t.Parallel()
	proposer := &recordingProposer{err: errors.New("synthetic engine failure")}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	_, err := srv.RotateDEK(context.Background(), validRotateDEKRequest())
	if status.Code(err) != codes.Unavailable {
		t.Errorf("RotateDEK status=%v, want Unavailable for non-leadership propose error", status.Code(err))
	}
}

// TestEncryptionAdmin_RegisterEncryptionWriter_RejectsBadInputs is
// the table-driven twin of TestEncryptionAdmin_RotateDEK_RejectsBadInputs
// so the registration path's boundary validation has the same
// pin-down as rotation's. Each entry mutates a single field on a
// known-good request to isolate the failing branch.
func TestEncryptionAdmin_RegisterEncryptionWriter_RejectsBadInputs(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	type tc struct {
		name string
		mut  func(r *pb.RegisterEncryptionWriterRequest)
	}
	for _, c := range []tc{
		{"zero dek_id", func(r *pb.RegisterEncryptionWriterRequest) { r.DekId = 0 }},
		{"local_epoch above 0xFFFF", func(r *pb.RegisterEncryptionWriterRequest) { r.Writers[0].LocalEpoch = 0x10000 }},
		{"zero full_node_id", func(r *pb.RegisterEncryptionWriterRequest) { r.Writers[0].FullNodeId = 0 }},
		{"empty writers", func(r *pb.RegisterEncryptionWriterRequest) { r.Writers = nil }},
	} {
		t.Run(c.name, func(t *testing.T) {
			req := validRegisterEncryptionWriterRequest()
			c.mut(req)
			_, err := srv.RegisterEncryptionWriter(context.Background(), req)
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("%s: status=%v, want InvalidArgument", c.name, status.Code(err))
			}
		})
	}
}

// TestEncryptionAdmin_RegisterEncryptionWriter_EmptyWritersMessage
// pins the message routing for the zero-length writers case: a
// zero-length writers slice returns a "got 0" message, not the
// misleading "use BootstrapEncryption for multi-writer batches"
// text aimed at the >1 case.
func TestEncryptionAdmin_RegisterEncryptionWriter_EmptyWritersMessage(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	_, err := srv.RegisterEncryptionWriter(context.Background(), &pb.RegisterEncryptionWriterRequest{
		DekId:   5,
		Writers: nil,
	})
	if err == nil {
		t.Fatalf("RegisterEncryptionWriter returned nil, want InvalidArgument")
	}
	if !strings.Contains(err.Error(), "got 0") {
		t.Errorf("error %q does not surface the 0-writer case", err)
	}
	if strings.Contains(err.Error(), "BootstrapEncryption") {
		t.Errorf("error %q misroutes the operator to BootstrapEncryption for the 0-writer case", err)
	}
}

func TestEncryptionAdmin_RegisterEncryptionWriter_RejectsBatch(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	req := &pb.RegisterEncryptionWriterRequest{
		DekId: 5,
		Writers: []*pb.WriterRegistryEntry{
			{FullNodeId: 1, LocalEpoch: 1},
			{FullNodeId: 2, LocalEpoch: 2},
		},
	}
	_, err := srv.RegisterEncryptionWriter(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("status=%v, want InvalidArgument", status.Code(err))
	}
}

func TestEncryptionAdmin_ResyncSidecar_RejectsOnFollower(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active: encryption.ActiveKeys{Storage: 1, Raft: 2},
		Keys: map[string]encryption.SidecarKey{
			"1": {Purpose: "storage", Wrapped: []byte("w")},
			"2": {Purpose: "raft", Wrapped: []byte("w")},
		},
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminLeaderView(stubLeaderView{
			state:  raftengine.StateFollower,
			leader: raftengine.LeaderInfo{ID: "n2", Address: "127.0.0.1:50052"},
		}),
	)
	_, err := srv.ResyncSidecar(context.Background(), &pb.ResyncSidecarRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResyncSidecar status=%v, want FailedPrecondition on a follower", status.Code(err))
	}
}

// recordingProposer captures every Propose call so tests can
// inspect the proposed entry bytes. It is unsafe for parallel use
// across goroutines but every test that uses it does so
// single-threaded.
type recordingProposer struct {
	calls       [][]byte
	commitIndex uint64
	err         error
}

func (p *recordingProposer) Propose(_ context.Context, data []byte) (*raftengine.ProposalResult, error) {
	dup := make([]byte, len(data))
	copy(dup, data)
	p.calls = append(p.calls, dup)
	if p.err != nil {
		return nil, p.err
	}
	return &raftengine.ProposalResult{CommitIndex: p.commitIndex}, nil
}

// stubLeaderView reports a fixed leadership state. verifyErr lets
// a test simulate a partitioned former leader: State() still
// reports StateLeader but VerifyLeader returns an error because
// the local node cannot confirm leadership via a quorum
// ReadIndex round-trip.
type stubLeaderView struct {
	state     raftengine.State
	leader    raftengine.LeaderInfo
	verifyErr error
}

func (s stubLeaderView) State() raftengine.State       { return s.state }
func (s stubLeaderView) Leader() raftengine.LeaderInfo { return s.leader }
func (s stubLeaderView) VerifyLeader(context.Context) error {
	return s.verifyErr
}
func (s stubLeaderView) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
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

package adapter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/admin"
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
	if _, err := srv.EnableStorageEnvelope(ctx, validEnableStorageEnvelopeRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition (no proposer wired)", status.Code(err))
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

func validEnableStorageEnvelopeRequest() *pb.EnableStorageEnvelopeRequest {
	return &pb.EnableStorageEnvelopeRequest{
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

func TestEncryptionAdmin_NonCutoverAdminEntriesUsePostCutoverProposer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		call       func(context.Context, *EncryptionAdminServer) (uint64, error)
		wantOpcode byte
	}{
		{
			name: "RotateDEK",
			call: func(ctx context.Context, srv *EncryptionAdminServer) (uint64, error) {
				resp, err := srv.RotateDEK(ctx, validRotateDEKRequest())
				if resp == nil {
					return 0, err
				}
				return resp.AppliedIndex, err
			},
			wantOpcode: fsmwire.OpRotation,
		},
		{
			name: "RegisterEncryptionWriter",
			call: func(ctx context.Context, srv *EncryptionAdminServer) (uint64, error) {
				resp, err := srv.RegisterEncryptionWriter(ctx, validRegisterEncryptionWriterRequest())
				if resp == nil {
					return 0, err
				}
				return resp.AppliedIndex, err
			},
			wantOpcode: fsmwire.OpRegistration,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := &recordingProposer{commitIndex: 17}
			postCutover := &recordingProposer{commitIndex: 99}
			srv := NewEncryptionAdminServer(
				WithEncryptionAdminProposer(raw),
				WithEncryptionAdminPostCutoverProposer(postCutover),
				WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
			)
			gotIdx, err := tc.call(context.Background(), srv)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if gotIdx != 99 {
				t.Errorf("AppliedIndex=%d, want 99 from post-cutover proposer", gotIdx)
			}
			if len(raw.calls) != 0 {
				t.Fatalf("raw proposer calls=%d, want 0 for non-cutover admin entry", len(raw.calls))
			}
			assertSingleProposalOpcode(t, postCutover.calls, tc.wantOpcode)
		})
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

// ProposeAdmin records the call into the same slice as Propose so
// existing assertions on p.calls continue to work after the
// adapter/encryption_admin.go production callers (which now route
// every control-plane entry through ProposeAdmin) switched paths.
// In the current build both methods are operationally identical;
// the bookkeeping is unified intentionally.
func (p *recordingProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
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

// fixedCapabilityFanout returns a closure that yields the supplied
// result regardless of context — lets tests drive the §4 fan-out
// branches deterministically without spinning real clients. A
// non-nil err exercises the "fan-out helper itself failed" path
// (§3.2 step 7 wraps as FailedPrecondition).
func fixedCapabilityFanout(result admin.CapabilityFanoutResult, err error) CapabilityFanoutFn {
	return func(context.Context) (admin.CapabilityFanoutResult, error) {
		return result, err
	}
}

// failOnCallCapabilityFanout returns a closure that fails the test
// if it is invoked. Used by the §6.4 idempotent-retry tests to
// pin the "already-active short-circuit must NOT run fan-out"
// invariant: a regression that re-ordered the cutoverPrecheck so
// the fan-out fired before the short-circuit would trip this
// fixture and the test would fail at the wire-level rather than
// silently passing on a successful fan-out. PR812 coderabbit
// quick-win round-2.
func failOnCallCapabilityFanout(t *testing.T) CapabilityFanoutFn {
	t.Helper()
	return func(context.Context) (admin.CapabilityFanoutResult, error) {
		t.Errorf("capability fan-out invoked on idempotent-retry path; the §6.4 short-circuit MUST skip fan-out")
		return admin.CapabilityFanoutResult{}, errors.New("fan-out invoked on idempotent path")
	}
}

// applyingProposer is a recordingProposer that also simulates the
// FSM apply by writing the supplied applyFn output into the
// sidecar before returning from Propose. The 6D-6 RPC re-reads
// the sidecar after Propose to discriminate fresh-success vs.
// stale-DEKID vs. concurrent-overlap; without simulating the
// apply, the RPC's post-read would always see the pre-cutover
// sidecar and incorrectly classify every propose as stale-DEKID.
type applyingProposer struct {
	recordingProposer
	sidecarPath string
	applyFn     func(*encryption.Sidecar, uint64)
}

func (p *applyingProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	res, err := p.recordingProposer.Propose(ctx, data)
	if err != nil || res == nil || p.applyFn == nil {
		return res, err
	}
	sc, rerr := encryption.ReadSidecar(p.sidecarPath)
	if rerr != nil {
		return nil, rerr
	}
	p.applyFn(sc, res.CommitIndex)
	if werr := encryption.WriteSidecar(p.sidecarPath, sc); werr != nil {
		return nil, werr
	}
	return res, nil
}

// ProposeAdmin redirects to applyingProposer.Propose (not the
// embedded recordingProposer.Propose) so the applyFn side-effect
// fires for admin-path proposals as well. Without this explicit
// override, Go method resolution would dispatch
// recordingProposer.ProposeAdmin -> recordingProposer.Propose and
// skip the applyingProposer-level Propose body — silently losing
// the apply emulation that EnableStorageEnvelope /
// EnableRaftEnvelope happy-path tests depend on.
func (p *applyingProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

// applyCutover is the §6.4 fresh-success apply effect: flip
// StorageEnvelopeActive to true and stamp the cutover index with
// the apply's Raft index. Used by the EnableStorageEnvelope happy-
// path test to drive the post-Propose sidecar re-read into the
// fresh-success branch.
func applyCutover(sc *encryption.Sidecar, raftIdx uint64) {
	sc.StorageEnvelopeActive = true
	sc.StorageEnvelopeCutoverIndex = raftIdx
	if raftIdx > sc.RaftAppliedIndex {
		sc.RaftAppliedIndex = raftIdx
	}
}

// applyStaleDEKIDRace simulates the §2.1 #3 / §6E-1a constraint
// #3 benign-no-op shape shared by the storage and raft variants:
// the applier consumed the entry without flipping the variant's
// "cutover active" sentinel (StorageEnvelopeActive for the storage
// path; the non-zero RaftEnvelopeCutoverIndex for the raft path)
// because a RotateDEK raced and advanced Active.Storage /
// Active.Raft respectively. Only RaftAppliedIndex advances. Used
// by both EnableStorageEnvelope and EnableRaftEnvelope tests to
// pin the no-cutover branch (claude r3 minor nit on PR933).
func applyStaleDEKIDRace(sc *encryption.Sidecar, raftIdx uint64) {
	if raftIdx > sc.RaftAppliedIndex {
		sc.RaftAppliedIndex = raftIdx
	}
}

// allOKFanoutResult is the deterministic "fan-out approved" fixture
// the happy-path test feeds the cutover RPC. The build SHA is
// trivially distinct so the projection-to-proto check can assert
// the field actually flows through (a regression that dropped the
// SHA would otherwise pass on an empty-string comparison).
func allOKFanoutResult() admin.CapabilityFanoutResult {
	return admin.CapabilityFanoutResult{
		Verdicts: []admin.CapabilityVerdict{
			{FullNodeID: 11, EncryptionCapable: true, BuildSHA: "build-n1", SidecarPresent: true, Reachable: true},
			{FullNodeID: 22, EncryptionCapable: true, BuildSHA: "build-n2", SidecarPresent: true, Reachable: true},
		},
		OK: true,
	}
}

// cutoverReadySidecarFixture writes a sidecar past Bootstrap but
// pre-cutover (Active.Storage != 0 AND StorageEnvelopeActive ==
// false). Returns the path so the test can hand it to the server
// + re-read it afterwards to verify the apply effect.
func cutoverReadySidecarFixture(t *testing.T) string {
	t.Helper()
	return writeSidecarFixture(t, &encryption.Sidecar{
		Active: encryption.ActiveKeys{Storage: 5, Raft: 6},
		Keys: map[string]encryption.SidecarKey{
			"5": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("ws"), Created: "x", LocalEpoch: 0},
			"6": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("wr"), Created: "x", LocalEpoch: 0},
		},
		RaftAppliedIndex: 100,
	})
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsOnFollower
// pins the §3.2 step 3 leader gate. The leader hint must be
// embedded in the FailedPrecondition status detail so the
// operator's CLI can retry against the right node.
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsOnFollower(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{
			state:  raftengine.StateFollower,
			leader: raftengine.LeaderInfo{ID: "n2", Address: "127.0.0.1:50052"},
		}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
	)
	_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "n2") || !strings.Contains(err.Error(), "127.0.0.1:50052") {
		t.Errorf("error %q does not embed the leader hint (id=n2 addr=127.0.0.1:50052)", err)
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsWithoutSidecarPath
// covers the §6.4 sidecar dependency: without a sidecar path
// wired, the RPC cannot read the pre-cutover state and refuses.
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsWithoutSidecarPath(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
	)
	_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsZeroProposerNodeID
// pins §3.1 / §6.1: the not-capable sentinel (full_node_id=0) is
// rejected at the gRPC boundary before any sidecar read.
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsZeroProposerNodeID(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
	)
	req := validEnableStorageEnvelopeRequest()
	req.ProposerNodeId = 0
	_, err := srv.EnableStorageEnvelope(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("EnableStorageEnvelope status=%v, want InvalidArgument", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsOversizedLocalEpoch
// pins §3.1 / §4.1: the proto3 uint32 wire field MUST be <= 0xFFFF.
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsOversizedLocalEpoch(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
	)
	req := validEnableStorageEnvelopeRequest()
	req.ProposerLocalEpoch = math.MaxUint16 + 1
	_, err := srv.EnableStorageEnvelope(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("EnableStorageEnvelope status=%v, want InvalidArgument", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsNotBootstrapped
// pins §3.2 step 4: a sidecar with Active.Storage == 0 means
// BootstrapEncryption has not committed yet, so the cutover must
// refuse with a clear hint.
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsNotBootstrapped(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active: encryption.ActiveKeys{Storage: 0, Raft: 0},
		Keys:   map[string]encryption.SidecarKey{},
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
	)
	_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "BootstrapEncryption") {
		t.Errorf("error %q does not hint at BootstrapEncryption", err)
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_IdempotentRetry pins
// §3.2 step 5 + §6.4: a duplicate call against an already-active
// sidecar returns OK with was_already_active=true and
// applied_index = sidecar.StorageEnvelopeCutoverIndex. No
// capability fan-out, no propose. The CapabilitySummary is
// intentionally empty so a caller cannot accidentally re-use
// the original cutover's membership view (which may no longer
// reflect current cluster shape).
func TestEncryptionAdmin_EnableStorageEnvelope_IdempotentRetry(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active:                      encryption.ActiveKeys{Storage: 5, Raft: 6},
		Keys:                        map[string]encryption.SidecarKey{"5": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("ws"), Created: "x", LocalEpoch: 0}, "6": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("wr"), Created: "x", LocalEpoch: 0}},
		StorageEnvelopeActive:       true,
		StorageEnvelopeCutoverIndex: 555,
		RaftAppliedIndex:            900,
	})
	proposer := &recordingProposer{}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		// Use failOnCallCapabilityFanout: the §6.4 short-circuit
		// MUST skip the fan-out; if a regression re-ordered the
		// checks the fan-out fixture trips the test.
		WithEncryptionAdminCapabilityFanout(failOnCallCapabilityFanout(t)),
	)
	got, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if err != nil {
		t.Fatalf("EnableStorageEnvelope: %v", err)
	}
	if !got.WasAlreadyActive {
		t.Error("WasAlreadyActive=false, want true (idempotent retry)")
	}
	if got.AppliedIndex != 555 {
		t.Errorf("AppliedIndex=%d, want 555 (original StorageEnvelopeCutoverIndex)", got.AppliedIndex)
	}
	if got.CutoverIndexUnknown {
		t.Error("CutoverIndexUnknown=true, want false (cutover index is set)")
	}
	if len(got.CapabilitySummary) != 0 {
		t.Errorf("CapabilitySummary len=%d, want 0 (empty on idempotent retries)", len(got.CapabilitySummary))
	}
	if len(proposer.calls) != 0 {
		t.Errorf("proposer.calls len=%d, want 0 (no propose on idempotent retry)", len(proposer.calls))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_DefensiveCutoverIndexUnknown
// pins the §6.4 defensive branch: a sidecar reporting
// StorageEnvelopeActive=true paired with
// StorageEnvelopeCutoverIndex=0 is operationally impossible under
// the 6D-4 apply path but hedged against schema rollback / hand-
// edited sidecars. The RPC falls back to RaftAppliedIndex with
// CutoverIndexUnknown=true so operators see the warning.
func TestEncryptionAdmin_EnableStorageEnvelope_DefensiveCutoverIndexUnknown(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active:                      encryption.ActiveKeys{Storage: 5, Raft: 6},
		Keys:                        map[string]encryption.SidecarKey{"5": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("ws"), Created: "x", LocalEpoch: 0}, "6": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("wr"), Created: "x", LocalEpoch: 0}},
		StorageEnvelopeActive:       true,
		StorageEnvelopeCutoverIndex: 0, // operationally impossible; hedge for hand-edits
		RaftAppliedIndex:            900,
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		// The §6.4 defensive branch is still on the
		// idempotent-retry path (sidecar reports active); fan-out
		// MUST be skipped exactly as in the standard retry case.
		WithEncryptionAdminCapabilityFanout(failOnCallCapabilityFanout(t)),
	)
	got, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if err != nil {
		t.Fatalf("EnableStorageEnvelope: %v", err)
	}
	if !got.WasAlreadyActive {
		t.Error("WasAlreadyActive=false, want true (sidecar reports active)")
	}
	if !got.CutoverIndexUnknown {
		t.Error("CutoverIndexUnknown=false, want true (defensive branch)")
	}
	if got.AppliedIndex != 900 {
		t.Errorf("AppliedIndex=%d, want 900 (RaftAppliedIndex fallback)", got.AppliedIndex)
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsWithoutCapabilityFanout
// pins the §4 pre-flight requirement: without the fan-out wired,
// the RPC refuses even on a fully-bootstrapped pre-cutover
// sidecar. The §6D-6 main.go wiring is what threads the fan-out
// in; tests that skip it deliberately must opt out via the
// idempotent-retry path (already-active sidecar).
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsWithoutCapabilityFanout(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
	)
	_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition (no fan-out wired)", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsOnCapabilityRefusal
// pins §3.2 step 7: any fan-out verdict with
// EncryptionCapable=false or Reachable=false fails the pre-flight
// and the RPC refuses with FailedPrecondition. The status detail
// names the specific node so the operator's CLI can diagnose
// without trawling leader logs.
//
// Table-driven across the two refusal shapes so both branches of
// capabilityRefusalSummary (Reachable=false first, then
// EncryptionCapable=false) are exercised — without the unreachable
// case the "unreachable member" status detail would lose its
// regression coverage. PR812 claude P2.
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsOnCapabilityRefusal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		refusal     admin.CapabilityFanoutResult
		wantNodeStr string
		wantReason  string
	}{
		{
			name: "not_capable",
			refusal: admin.CapabilityFanoutResult{
				Verdicts: []admin.CapabilityVerdict{
					{FullNodeID: 11, EncryptionCapable: true, BuildSHA: "build-n1", SidecarPresent: true, Reachable: true},
					{FullNodeID: 99, EncryptionCapable: false, BuildSHA: "build-n99", SidecarPresent: false, Reachable: true},
				},
				OK: false,
			},
			wantNodeStr: "99",
			wantReason:  "not-capable",
		},
		{
			name: "unreachable",
			refusal: admin.CapabilityFanoutResult{
				Verdicts: []admin.CapabilityVerdict{
					{FullNodeID: 11, EncryptionCapable: true, BuildSHA: "build-n1", SidecarPresent: true, Reachable: true},
					{FullNodeID: 88, EncryptionCapable: false, BuildSHA: "", SidecarPresent: false, Reachable: false},
				},
				OK: false,
			},
			wantNodeStr: "88",
			wantReason:  "unreachable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runCapabilityRefusalCase(t, tc.refusal, tc.wantNodeStr, tc.wantReason)
		})
	}
}

func runCapabilityRefusalCase(t *testing.T, refusal admin.CapabilityFanoutResult, wantNodeStr, wantReason string) {
	t.Helper()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(refusal, nil)),
	)
	_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), wantNodeStr) {
		t.Errorf("error %q does not name the refusing node (full_node_id=%s)", err, wantNodeStr)
	}
	if err == nil || !strings.Contains(err.Error(), wantReason) {
		t.Errorf("error %q does not name the refusal reason (%q)", err, wantReason)
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_RejectsOnFanoutError
// covers the §3.2 step 7 wrap-fallback: the fan-out helper itself
// erroring out (e.g., zero-member snapshot rejected by input
// validation) surfaces as FailedPrecondition rather than Internal,
// so the operator's CLI treats it as a configuration issue rather
// than a transient bug.
func TestEncryptionAdmin_EnableStorageEnvelope_RejectsOnFanoutError(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(admin.CapabilityFanoutResult{}, errors.New("fan-out: bad routes input"))),
	)
	_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_SerializesConcurrentCutovers
// pins the §2.1 #4 mutator-lock invariant: two concurrent cutover
// RPCs MUST NOT both produce a freshCutoverResponse — the second
// one must observe StorageEnvelopeActive=true at its precheck and
// fall through to the §6.4 idempotent-retry shape. Coderabbit
// Major on PR812.
//
// The applying proposer flips the sidecar on the FIRST propose
// call. The second call enters EnableStorageEnvelope, waits on
// cutoverMu, then runs its precheck. Without the lock, the
// second call's precheck could see the pre-flip sidecar and
// re-propose — producing a freshCutoverResponse with
// WasAlreadyActive=false but the FIRST cutover's
// StorageEnvelopeCutoverIndex (the §6.4 contract violation).
func TestEncryptionAdmin_EnableStorageEnvelope_SerializesConcurrentCutovers(t *testing.T) {
	t.Parallel()
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 7000},
		sidecarPath:       path,
		applyFn:           applyCutover,
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
	)
	const callers = 4
	results := make(chan *pb.EnableStorageEnvelopeResponse, callers)
	errs := make(chan error, callers)
	var ready sync.WaitGroup
	var release sync.WaitGroup
	ready.Add(callers)
	release.Add(1)
	for range callers {
		go func() {
			ready.Done()
			release.Wait()
			resp, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
			results <- resp
			errs <- err
		}()
	}
	ready.Wait()
	release.Done()
	freshCount := 0
	idempotentCount := 0
	for i := 0; i < callers; i++ {
		err := <-errs
		resp := <-results
		if err != nil {
			t.Fatalf("EnableStorageEnvelope call %d: %v", i, err)
		}
		if resp.WasAlreadyActive {
			idempotentCount++
		} else {
			freshCount++
		}
	}
	if freshCount != 1 {
		t.Errorf("freshCount=%d, want exactly 1 (only one caller should propose; the rest must hit the §6.4 idempotent-retry path)", freshCount)
	}
	if idempotentCount != callers-1 {
		t.Errorf("idempotentCount=%d, want %d", idempotentCount, callers-1)
	}
	if len(proposer.calls) != 1 {
		t.Errorf("proposer.calls=%d, want 1 (cutover proposed exactly once across concurrent callers)", len(proposer.calls))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_PreCanceledCtxDeterministicShortCircuit
// pins codex P1 round-4 on PR812: when ctx is already canceled
// at entry AND the cutoverSem has capacity (no in-flight cutover
// is holding it), Go's select would pick non-deterministically
// between the send case and the ctx.Done case. An explicit
// ctx.Err() check before the select turns the cancellation into
// a deterministic short-circuit so a caller who canceled before
// the RPC even started cannot accidentally drive precheck /
// fan-out / propose work.
//
// The test repeats the pre-canceled call N times against a fresh
// (capacity-free) semaphore: every iteration MUST surface the
// ctx error, never advance into the fan-out closure (which the
// test fixture would observe via a t.Errorf if hit).
func TestEncryptionAdmin_EnableStorageEnvelope_PreCanceledCtxDeterministicShortCircuit(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		ctxFn    func() (context.Context, context.CancelFunc)
		wantCode codes.Code
	}{
		{
			name: "canceled",
			ctxFn: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			wantCode: codes.Canceled,
		},
		{
			name: "deadline_exceeded",
			ctxFn: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				return ctx, cancel
			},
			wantCode: codes.DeadlineExceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// failOnCallCapabilityFanout fires t.Errorf if the
			// RPC ever advances into fan-out — that would mean
			// the semaphore acquire DID take the send case
			// despite the canceled ctx.
			srv := NewEncryptionAdminServer(
				WithEncryptionAdminProposer(&recordingProposer{}),
				WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
				WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
				WithEncryptionAdminCapabilityFanout(failOnCallCapabilityFanout(t)),
			)
			// 100 iterations against a freshly-created server
			// (semaphore is empty / capacity available). Without
			// the explicit ctx.Err() check, Go's pseudo-random
			// select choice would make some of these iterations
			// advance into precheck/fan-out, tripping the
			// failOnCallCapabilityFanout fixture.
			for range 100 {
				ctx, cancel := tc.ctxFn()
				_, err := srv.EnableStorageEnvelope(ctx, validEnableStorageEnvelopeRequest())
				cancel()
				if status.Code(err) != tc.wantCode {
					t.Fatalf("EnableStorageEnvelope status=%v, want %v (must short-circuit on pre-canceled ctx)",
						status.Code(err), tc.wantCode)
				}
			}
		})
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_HonorsCtxDeadlineWaitingOnMutator
// pins codex P2 round-3 on PR812: when one cutover RPC holds
// the mutator semaphore through a slow fan-out + propose, a
// concurrent caller with a short deadline MUST return
// DeadlineExceeded / Canceled at the gRPC boundary rather than
// blocking past its own ctx.
//
// The test drives the first call into a fan-out that blocks
// indefinitely on a never-firing channel; while it sits in
// fan-out (holding cutoverSem), the second call attempts the
// RPC with an already-expired ctx and must return immediately
// with the matching gRPC code.
func TestEncryptionAdmin_EnableStorageEnvelope_HonorsCtxDeadlineWaitingOnMutator(t *testing.T) {
	t.Parallel()
	path := cutoverReadySidecarFixture(t)
	blockFanout := make(chan struct{}) // never closed; first call blocks here forever
	t.Cleanup(func() { close(blockFanout) })
	blockingFanout := func(callCtx context.Context) (admin.CapabilityFanoutResult, error) {
		select {
		case <-blockFanout:
			return admin.CapabilityFanoutResult{}, errors.New("test teardown")
		case <-callCtx.Done():
			return admin.CapabilityFanoutResult{}, callCtx.Err()
		}
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(blockingFanout),
	)
	// First call: launch in a goroutine; it holds cutoverSem
	// while blocked in fan-out.
	firstStarted := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		close(firstStarted)
		_, _ = srv.EnableStorageEnvelope(ctx, validEnableStorageEnvelopeRequest())
	}()
	<-firstStarted
	// Yield so the first call reaches the fan-out wait — a
	// short sleep is the only reliable signal short of plumbing
	// a "fan-out entered" channel through the test fixture.
	time.Sleep(50 * time.Millisecond)
	// Second call with an already-expired ctx. It must NOT
	// block past its deadline waiting on the mutator semaphore.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	start := time.Now()
	_, err := srv.EnableStorageEnvelope(ctx, validEnableStorageEnvelopeRequest())
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("EnableStorageEnvelope blocked %v past ctx deadline; want immediate return", elapsed)
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("EnableStorageEnvelope status=%v, want DeadlineExceeded", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_PreservesContextCancellationOnFanoutNotOK
// pins codex P2 round-2 on PR812: the production fan-out helper
// can synthesize Reachable=false verdicts and return (result, nil)
// with OK=false when ctx expires mid-probe. In that case
// EnableStorageEnvelope MUST preserve the transport-layer
// cancellation/deadline shape rather than misclassifying the
// outcome as a configuration refusal.
func TestEncryptionAdmin_EnableStorageEnvelope_PreservesContextCancellationOnFanoutNotOK(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		ctxFn    func() (context.Context, context.CancelFunc)
		wantCode codes.Code
	}{
		{
			name: "canceled",
			ctxFn: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			wantCode: codes.Canceled,
		},
		{
			name: "deadline_exceeded",
			ctxFn: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				return ctx, cancel
			},
			wantCode: codes.DeadlineExceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Synthesize the production fan-out's behavior: it returns
			// a NOT-OK result without err when ctx expired mid-probe.
			notOK := admin.CapabilityFanoutResult{
				Verdicts: []admin.CapabilityVerdict{{FullNodeID: 11, Reachable: false}},
				OK:       false,
			}
			srv := NewEncryptionAdminServer(
				WithEncryptionAdminProposer(&recordingProposer{}),
				WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
				WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
				WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(notOK, nil)),
			)
			ctx, cancel := tc.ctxFn()
			defer cancel()
			_, err := srv.EnableStorageEnvelope(ctx, validEnableStorageEnvelopeRequest())
			if status.Code(err) != tc.wantCode {
				t.Errorf("EnableStorageEnvelope status=%v, want %v (ctx error should take precedence over FailedPrecondition)",
					status.Code(err), tc.wantCode)
			}
		})
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_PreservesContextCancellation
// pins the codex P2 finding on PR812: context.Canceled and
// context.DeadlineExceeded surfaced by the fan-out closure MUST
// keep their native gRPC code (Canceled / DeadlineExceeded)
// rather than being squashed into FailedPrecondition.
// FailedPrecondition is a configuration-failure signal; mapping
// a transport-layer cancellation to it breaks client retry
// logic that switches on the code.
func TestEncryptionAdmin_EnableStorageEnvelope_PreservesContextCancellation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "canceled", err: context.Canceled, wantCode: codes.Canceled},
		{name: "deadline_exceeded", err: context.DeadlineExceeded, wantCode: codes.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := NewEncryptionAdminServer(
				WithEncryptionAdminProposer(&recordingProposer{}),
				WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
				WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
				WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(admin.CapabilityFanoutResult{}, tc.err)),
			)
			_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
			if status.Code(err) != tc.wantCode {
				t.Errorf("EnableStorageEnvelope status=%v, want %v", status.Code(err), tc.wantCode)
			}
		})
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_HappyPath drives the
// full §3.2 happy path: leader gate passes, fan-out approves,
// propose lands, post-apply sidecar shows the cutover effect, the
// response carries WasAlreadyActive=false +
// CapabilitySummary populated + AppliedIndex matching the
// proposed Raft index. The applying proposer also simulates the
// §6.4 apply (StorageEnvelopeActive=true,
// StorageEnvelopeCutoverIndex=raftIdx) so the post-Propose
// sidecar read takes the fresh-success branch rather than the
// stale-DEKID fallback.
func TestEncryptionAdmin_EnableStorageEnvelope_HappyPath(t *testing.T) {
	t.Parallel()
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 1234},
		sidecarPath:       path,
		applyFn:           applyCutover,
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
	)
	got, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if err != nil {
		t.Fatalf("EnableStorageEnvelope: %v", err)
	}
	assertFreshCutoverResponse(t, got, 1234, 2)
	// Wire-level pin: verify the proposed Raft entry decodes as
	// SubTag=RotateSubEnableStorageEnvelope with the §2.1
	// constraints (Purpose=PurposeStorage, len(Wrapped)==0,
	// DEKID=sidecar.Active.Storage).
	assertSingleProposalOpcode(t, proposer.calls, fsmwire.OpRotation)
	decoded, err := fsmwire.DecodeRotation(proposer.calls[0][1:])
	if err != nil {
		t.Fatalf("DecodeRotation: %v", err)
	}
	assertCutoverProposalShape(t, decoded, 5, 11, 7)
}

// assertFreshCutoverResponse pins the §3.2 happy-path response
// shape: WasAlreadyActive=false, CutoverIndexUnknown=false,
// AppliedIndex matches the proposed Raft index, capability
// summary populated with the expected verdict count.
func assertFreshCutoverResponse(t *testing.T, resp *pb.EnableStorageEnvelopeResponse, wantIdx uint64, wantVerdicts int) {
	t.Helper()
	if resp.WasAlreadyActive {
		t.Error("WasAlreadyActive=true, want false (fresh cutover)")
	}
	if resp.CutoverIndexUnknown {
		t.Error("CutoverIndexUnknown=true, want false")
	}
	if resp.AppliedIndex != wantIdx {
		t.Errorf("AppliedIndex=%d, want %d (proposed Raft index)", resp.AppliedIndex, wantIdx)
	}
	if len(resp.CapabilitySummary) != wantVerdicts {
		t.Errorf("CapabilitySummary len=%d, want %d", len(resp.CapabilitySummary), wantVerdicts)
	}
}

// assertCutoverProposalShape pins the §2.1 wire layout of a
// decoded cutover rotation: SubTag=RotateSubEnableStorageEnvelope,
// empty Wrapped, Purpose=PurposeStorage, ProposerRegistration
// matching the supplied identifiers.
func assertCutoverProposalShape(t *testing.T, decoded fsmwire.RotationPayload, wantDEKID uint32, wantNodeID uint64, wantEpoch uint16) {
	t.Helper()
	if decoded.SubTag != fsmwire.RotateSubEnableStorageEnvelope {
		t.Errorf("SubTag=0x%02x, want RotateSubEnableStorageEnvelope (0x04)", decoded.SubTag)
	}
	if decoded.DEKID != wantDEKID {
		t.Errorf("DEKID=%d, want %d (sidecar.Active.Storage)", decoded.DEKID, wantDEKID)
	}
	if decoded.Purpose != fsmwire.PurposeStorage {
		t.Errorf("Purpose=%v, want PurposeStorage", decoded.Purpose)
	}
	if len(decoded.Wrapped) != 0 {
		t.Errorf("Wrapped len=%d, want 0 (§2.1 constraint #2)", len(decoded.Wrapped))
	}
	if decoded.ProposerRegistration.DEKID != wantDEKID ||
		decoded.ProposerRegistration.FullNodeID != wantNodeID ||
		decoded.ProposerRegistration.LocalEpoch != wantEpoch {
		t.Errorf("ProposerRegistration=%+v, want {DEKID:%d FullNodeID:%d LocalEpoch:%d}",
			decoded.ProposerRegistration, wantDEKID, wantNodeID, wantEpoch)
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_StaleDEKIDRace pins
// §2.1 #3: when a RotateDEK races between propose and apply, the
// 6D-4 applier consumes the cutover entry as a benign no-op
// (sidecar.StorageEnvelopeActive stays false), and the RPC's
// post-apply read takes the stale-DEKID branch. The RPC must
// surface FailedPrecondition with a retry hint rather than
// reporting fresh-success.
func TestEncryptionAdmin_EnableStorageEnvelope_StaleDEKIDRace(t *testing.T) {
	t.Parallel()
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 1234},
		sidecarPath:       path,
		applyFn:           applyStaleDEKIDRace, // does NOT flip StorageEnvelopeActive
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
	)
	_, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableStorageEnvelope status=%v, want FailedPrecondition (stale DEKID race)", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "RotateDEK") {
		t.Errorf("error %q does not hint at the RotateDEK race", err)
	}
}

// TestEncryptionAdmin_EnableStorageEnvelope_CapabilitySummaryProjection
// pins the wire-shape mapping from internal CapabilityVerdict to
// proto.CapabilityVerdict. A regression that re-orders or drops
// any field would show up here rather than at the user-facing
// RPC client.
func TestEncryptionAdmin_EnableStorageEnvelope_CapabilitySummaryProjection(t *testing.T) {
	t.Parallel()
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 2025},
		sidecarPath:       path,
		applyFn:           applyCutover,
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
	)
	got, err := srv.EnableStorageEnvelope(context.Background(), validEnableStorageEnvelopeRequest())
	if err != nil {
		t.Fatalf("EnableStorageEnvelope: %v", err)
	}
	if len(got.CapabilitySummary) != 2 {
		t.Fatalf("CapabilitySummary len=%d, want 2", len(got.CapabilitySummary))
	}
	assertProtoVerdict(t, got.CapabilitySummary[0], 11, "build-n1")
	assertProtoVerdict(t, got.CapabilitySummary[1], 22, "build-n2")
}

// assertProtoVerdict pins one row of the projected
// proto.CapabilityVerdict slice. All fan-out OK-path verdicts
// share the same shape (EncryptionCapable=true, SidecarPresent=true,
// Reachable not projected); the per-row assertion captures only
// the variable identity fields so the caller test stays terse.
func assertProtoVerdict(t *testing.T, v *pb.CapabilityVerdict, wantNodeID uint64, wantBuildSHA string) {
	t.Helper()
	if v.FullNodeId != wantNodeID || v.BuildSha != wantBuildSHA || !v.EncryptionCapable || !v.SidecarPresent {
		t.Errorf("verdict=%+v, want {FullNodeId:%d BuildSha:%s EncryptionCapable:true SidecarPresent:true}",
			v, wantNodeID, wantBuildSHA)
	}
}

// validEnableRaftEnvelopeRequest is the canonical valid request
// for the raft-variant tests. Same proposer identity shape as the
// storage variant — the two requests' Go types are independent
// but structurally identical (proposer_node_id, proposer_local_epoch).
func validEnableRaftEnvelopeRequest() *pb.EnableRaftEnvelopeRequest {
	return &pb.EnableRaftEnvelopeRequest{
		ProposerNodeId:     11,
		ProposerLocalEpoch: 7,
	}
}

// applyRaftCutover is the §6.4-equivalent fresh-success apply
// effect for the raft variant: stamp RaftEnvelopeCutoverIndex
// with the apply's Raft index. Used by the EnableRaftEnvelope
// happy-path test to drive the post-Propose sidecar re-read into
// the fresh-success branch.
func applyRaftCutover(sc *encryption.Sidecar, raftIdx uint64) {
	sc.RaftEnvelopeCutoverIndex = raftIdx
	if raftIdx > sc.RaftAppliedIndex {
		sc.RaftAppliedIndex = raftIdx
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_RejectsZeroProposerNodeID
// pins the §6.1 sentinel rejection at the gRPC boundary.
func TestEncryptionAdmin_EnableRaftEnvelope_RejectsZeroProposerNodeID(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
	)
	req := validEnableRaftEnvelopeRequest()
	req.ProposerNodeId = 0
	_, err := srv.EnableRaftEnvelope(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("EnableRaftEnvelope status=%v, want InvalidArgument", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_RejectsOversizedLocalEpoch
// pins the §4.1 16-bit bound at the gRPC boundary.
func TestEncryptionAdmin_EnableRaftEnvelope_RejectsOversizedLocalEpoch(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(cutoverReadySidecarFixture(t)),
	)
	req := validEnableRaftEnvelopeRequest()
	req.ProposerLocalEpoch = math.MaxUint16 + 1
	_, err := srv.EnableRaftEnvelope(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("EnableRaftEnvelope status=%v, want InvalidArgument", status.Code(err))
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_RejectsNotBootstrapped pins
// the raft-variant bootstrap gate: Active.Raft == 0 means
// BootstrapEncryption has not committed yet, so the cutover must
// refuse.
func TestEncryptionAdmin_EnableRaftEnvelope_RejectsNotBootstrapped(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active: encryption.ActiveKeys{Storage: 0, Raft: 0},
		Keys:   map[string]encryption.SidecarKey{},
	})
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(&recordingProposer{}),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableRaftEnvelope status=%v, want FailedPrecondition", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "BootstrapEncryption") {
		t.Errorf("error %q does not hint at BootstrapEncryption", err)
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_IdempotentRetry pins the
// retry path: a duplicate call against a sidecar with
// RaftEnvelopeCutoverIndex != 0 returns OK with
// was_already_active=true and applied_index = the original
// cutover index. No fan-out, no propose. The raft variant has no
// CutoverIndexUnknown field, so the response is intentionally
// narrower than the storage variant's idempotent shape.
func TestEncryptionAdmin_EnableRaftEnvelope_IdempotentRetry(t *testing.T) {
	t.Parallel()
	path := writeSidecarFixture(t, &encryption.Sidecar{
		Active:                   encryption.ActiveKeys{Storage: 5, Raft: 6},
		Keys:                     map[string]encryption.SidecarKey{"5": {Purpose: encryption.SidecarPurposeStorage, Wrapped: []byte("ws"), Created: "x", LocalEpoch: 0}, "6": {Purpose: encryption.SidecarPurposeRaft, Wrapped: []byte("wr"), Created: "x", LocalEpoch: 0}},
		RaftEnvelopeCutoverIndex: 777,
		RaftAppliedIndex:         900,
	})
	proposer := &recordingProposer{}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(failOnCallCapabilityFanout(t)),
	)
	got, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if err != nil {
		t.Fatalf("EnableRaftEnvelope: %v", err)
	}
	if !got.WasAlreadyActive {
		t.Error("WasAlreadyActive=false, want true (idempotent retry)")
	}
	if got.AppliedIndex != 777 {
		t.Errorf("AppliedIndex=%d, want 777 (original RaftEnvelopeCutoverIndex)", got.AppliedIndex)
	}
	if len(got.CapabilitySummary) != 0 {
		t.Errorf("CapabilitySummary len=%d, want 0 (empty on idempotent retries)", len(got.CapabilitySummary))
	}
	if len(proposer.calls) != 0 {
		t.Errorf("proposer.calls len=%d, want 0 (no propose on idempotent retry)", len(proposer.calls))
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_GatedUntil6E2 pins the
// fail-closed gate: while raftEnvelopeWrapEnabled is false (i.e.
// before Stage 6E-2 ships wrap-on-propose / unwrap-on-apply /
// §7.1 barrier), the RPC MUST refuse fresh cutover proposals
// with FailedPrecondition rather than recording
// RaftEnvelopeCutoverIndex=N. Recording N now would let cleartext
// entries land at indexes > N and the 6E-2 engine apply-hook
// would treat them as envelopes on upgrade, halting apply
// cluster-wide.
//
// The test wires an `applyingProposer` exactly as a future
// happy-path test would, then verifies the gate refuses BEFORE
// any proposal is composed (proposer.calls is empty) and BEFORE
// any sidecar mutation lands (RaftEnvelopeCutoverIndex stays 0).
// When 6E-2 lands and flips raftEnvelopeWrapEnabled to true, this
// test becomes the regression pin for the gate-flip and a
// HappyPath sibling is added.
func TestEncryptionAdmin_EnableRaftEnvelope_GatedUntil6E2(t *testing.T) {
	t.Parallel()
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		applyFn:           applyRaftCutover,
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableRaftEnvelope status=%v, want FailedPrecondition (gated until 6E-2)", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "6E-2") {
		t.Errorf("error %q does not hint at the 6E-2 gate", err)
	}
	if len(proposer.calls) != 0 {
		t.Errorf("proposer.calls len=%d, want 0 (gate must refuse before propose)", len(proposer.calls))
	}
	// Sidecar must remain untouched: RaftEnvelopeCutoverIndex
	// still 0 means the cluster has not entered Phase 2 and a
	// future 6E-2 upgrade is still safe.
	sc, readErr := encryption.ReadSidecar(path)
	if readErr != nil {
		t.Fatalf("ReadSidecar: %v", readErr)
	}
	if sc.RaftEnvelopeCutoverIndex != 0 {
		t.Errorf("RaftEnvelopeCutoverIndex=%d, want 0 (gate must refuse before sidecar mutation)", sc.RaftEnvelopeCutoverIndex)
	}
}

// recordingCutoverBarrier is a CutoverBarrierController stub that
// records the call order so the §7.1 6-step state-machine tests can
// assert Begin → WaitDrained → (propose) → InstallWrap → End. It
// also tracks call counts so tests can pin "End MUST be called once
// even on error" without flake risk.
type recordingCutoverBarrier struct {
	order []string // appends "Begin", "WaitDrained", "InstallWrap", "End"
	// waitErr is returned from WaitDrained when non-nil so the test
	// can exercise the step-2 timeout path. Otherwise WaitDrained
	// returns nil immediately.
	waitErr error
}

func (b *recordingCutoverBarrier) Begin() <-chan struct{} {
	b.order = append(b.order, "Begin")
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (b *recordingCutoverBarrier) WaitDrained(_ context.Context) error {
	b.order = append(b.order, "WaitDrained")
	return b.waitErr
}

func (b *recordingCutoverBarrier) InstallWrap() {
	b.order = append(b.order, "InstallWrap")
}

func (b *recordingCutoverBarrier) End() {
	b.order = append(b.order, "End")
}

// withWrapEnabledForTest flips raftEnvelopeWrapEnabled to true for
// the duration of t and restores the original value via t.Cleanup.
// Tests that exercise the §7.1 state machine MUST call this — the
// production value is still false in 6E-2d so the gated short-
// circuit would otherwise eat the test before the barrier path
// runs. Returns no value; t.Cleanup handles teardown so callers
// don't need to defer.
//
// Safe to call from t.Parallel() tests: raftEnvelopeWrapEnabled is
// an atomic.Bool, so concurrent reads from sibling parallel tests
// (e.g. GatedUntil6E2) observe a coherent value rather than
// tripping -race detection (claude finding 3 round-1).
func withWrapEnabledForTest(t *testing.T) {
	t.Helper()
	prev := raftEnvelopeWrapEnabled.Load()
	raftEnvelopeWrapEnabled.Store(true)
	t.Cleanup(func() { raftEnvelopeWrapEnabled.Store(prev) })
}

// TestEncryptionAdmin_EnableRaftEnvelope_RefusesWithoutBarrier
// pins the §7.1 wiring gate: even with raftEnvelopeWrapEnabled
// flipped true (the future 6E-2f production state), the handler
// MUST refuse with FailedPrecondition when the cutoverBarrier
// controller is not wired. Silently skipping the barrier would
// let a fresh USER proposal land at index > proposedIdx mid-
// cutover and halt apply cluster-wide on the §6.3 strict-`>` hook.
func TestEncryptionAdmin_EnableRaftEnvelope_RefusesWithoutBarrier(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		applyFn:           applyRaftCutover,
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		// Deliberately NO WithEncryptionAdminCutoverBarrier.
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableRaftEnvelope status=%v, want FailedPrecondition (no barrier wired)", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "cutover barrier") {
		t.Errorf("error %q does not hint at the missing barrier wiring", err)
	}
	if len(proposer.calls) != 0 {
		t.Errorf("proposer.calls len=%d, want 0 (no barrier MUST refuse before propose)", len(proposer.calls))
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_RefusesWithoutAppliedIndex
// pins the §7.1 step-4 wiring gate: without a wired
// latestAppliedIndex callback, awaitCutoverApply has no oracle for
// "the cutover entry has been applied locally". Skipping the wait
// would let InstallWrap (step 5) run before the cutover entry's
// FSM apply, potentially wrapping the cutover entry itself if a
// Propose racing the install observed the freshly-set wrap pointer.
// Refuse before any side effect.
func TestEncryptionAdmin_EnableRaftEnvelope_RefusesWithoutAppliedIndex(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		applyFn:           applyRaftCutover,
	}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(&recordingCutoverBarrier{}),
		// Deliberately NO WithEncryptionAdminLatestAppliedIndex.
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableRaftEnvelope status=%v, want FailedPrecondition (no applied-index wired)", status.Code(err))
	}
	if err == nil || !strings.Contains(err.Error(), "latest-applied-index") {
		t.Errorf("error %q does not hint at the missing applied-index wiring", err)
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_DrivesBarrierSequence
// pins the §7.1 6-step state-machine ordering: Begin → WaitDrained
// → (ProposeAdmin) → (await apply) → InstallWrap → End. A
// regression that reordered these (e.g. InstallWrap before
// awaitCutoverApply) would create the same race as skipping the
// apply wait — install the wrap closure while the engine has not
// yet seen the cutover entry, letting a Propose-side wrap apply to
// the cutover marker itself.
func TestEncryptionAdmin_EnableRaftEnvelope_DrivesBarrierSequence(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		applyFn:           applyRaftCutover,
	}
	postCutover := &recordingProposer{commitIndex: 9999}
	barrier := &recordingCutoverBarrier{}
	// latestAppliedIndex returns the proposer's commitIndex so the
	// step-4 wait completes immediately on the first poll. A
	// production wiring would have the FSM apply path update this.
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminPostCutoverProposer(postCutover),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(barrier),
		WithEncryptionAdminLatestAppliedIndex(func() uint64 { return 4242 }),
	)
	resp, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if err != nil {
		t.Fatalf("EnableRaftEnvelope: %v", err)
	}
	if resp == nil || resp.GetAppliedIndex() != 4242 {
		t.Fatalf("response applied_index=%v, want 4242", resp)
	}
	wantOrder := []string{"Begin", "WaitDrained", "InstallWrap", "End"}
	if !equalStrings(barrier.order, wantOrder) {
		t.Errorf("barrier call order = %v, want %v (Propose runs between WaitDrained and InstallWrap; the barrier doesn't observe it)", barrier.order, wantOrder)
	}
	if len(proposer.calls) != 1 {
		t.Errorf("proposer.calls len=%d, want 1 (one ProposeAdmin between WaitDrained and InstallWrap)", len(proposer.calls))
	}
	if len(postCutover.calls) != 0 {
		t.Errorf("post-cutover proposer calls=%d, want 0 for the cutover marker itself", len(postCutover.calls))
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_BarrierStaysOpenOnProposeError
// pins the codex P1 round-4 ambiguous-propose-outcome invariant on
// PR933: if proposeRaftCutoverEntry returns an error, the
// underlying etcd engine cannot retract a rawNode.Propose that has
// already been submitted to Raft. The handler therefore CANNOT
// distinguish "marker never reached Raft" from "marker submitted,
// outcome unknown" and MUST treat every propose-side error as the
// latter — leaving the barrier OPEN so a marker that subsequently
// commits cluster-wide cannot brick the §6.3 strict-`>` unwrap
// hook by letting cleartext user proposals land at
// `index > cutoverIdx`.
//
// Trade-off: a propose error that definitively was rejected before
// submission (ErrNotLeader transient) leaves this leader's barrier
// stuck until restart — recoverable by operator action — but the
// cluster-brick alternative is not.
func TestEncryptionAdmin_EnableRaftEnvelope_BarrierStaysOpenOnProposeError(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &recordingProposer{err: errors.New("engine refused")}
	barrier := &recordingCutoverBarrier{}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(barrier),
		WithEncryptionAdminLatestAppliedIndex(func() uint64 { return 0 }),
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if err == nil {
		t.Fatal("EnableRaftEnvelope: want error from propose-failure, got nil")
	}
	for _, op := range barrier.order {
		if op == "End" {
			t.Errorf("End was called after propose error; order = %v; this releases the barrier across an ambiguous commit window (codex P1 round-4)", barrier.order)
		}
		if op == "InstallWrap" {
			t.Errorf("InstallWrap MUST NOT run on propose error; order = %v", barrier.order)
		}
	}
	// Sequence reached Begin and WaitDrained, then propose
	// errored. InstallWrap and End must not run.
	wantPrefix := []string{"Begin", "WaitDrained"}
	if len(barrier.order) < len(wantPrefix) || !slices.Equal(barrier.order[:len(wantPrefix)], wantPrefix) {
		t.Errorf("barrier.order prefix = %v, want prefix %v", barrier.order, wantPrefix)
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_EndCalledOnDrainTimeout
// pins step-2's failure path: if WaitDrained returns an error
// (typically context cancellation or a misbehaving in-flight
// proposal), the handler MUST still call End() so the barrier
// closes. Symmetric with the propose-error case above.
func TestEncryptionAdmin_EnableRaftEnvelope_EndCalledOnDrainTimeout(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &recordingProposer{}
	barrier := &recordingCutoverBarrier{waitErr: errors.New("drain timeout")}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(barrier),
		WithEncryptionAdminLatestAppliedIndex(func() uint64 { return 0 }),
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if err == nil {
		t.Fatal("EnableRaftEnvelope: want error from drain timeout, got nil")
	}
	if status.Code(err) != codes.Unavailable {
		t.Errorf("EnableRaftEnvelope status=%v, want Unavailable for drain timeout", status.Code(err))
	}
	wantOrder := []string{"Begin", "WaitDrained", "End"}
	if !equalStrings(barrier.order, wantOrder) {
		t.Errorf("barrier call order = %v, want %v (End MUST run on drain timeout)", barrier.order, wantOrder)
	}
	if len(proposer.calls) != 0 {
		t.Errorf("proposer.calls len=%d, want 0 (drain timeout MUST refuse before propose)", len(proposer.calls))
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_BarrierStaysOpenOnApplyTimeout
// pins the codex P1 round-1 invariant: once
// proposeRaftCutoverEntry has succeeded, the cutover entry is
// committed in Raft and will apply on every replica. If the
// remainder of the sequence fails (apply-wait ctx fires before
// the local FSM catches up), the handler MUST NOT call End() —
// releasing the barrier without InstallWrap lets fresh USER
// proposals on this leader land cleartext at indexes >
// cutover_index, which the §6.3 strict-`>` apply hook would then
// halt on cluster-wide.
//
// The releaseSafe flag in runRaftEnvelopeCutoverBarrier gates the
// deferred End() so this test catches a regression that reordered
// the flag flip back inside the apply-wait error path.
func TestEncryptionAdmin_EnableRaftEnvelope_BarrierStaysOpenOnApplyTimeout(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		// Note: applyFn is nil so the sidecar's
		// RaftEnvelopeCutoverIndex stays 0, simulating local FSM
		// apply lag (the entry is committed in Raft but not yet
		// applied locally).
	}
	barrier := &recordingCutoverBarrier{}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(barrier),
		// latestAppliedIndex never catches up to 4242 — simulates
		// permanently-lagging local apply that exhausts the gRPC
		// deadline.
		WithEncryptionAdminLatestAppliedIndex(func() uint64 { return 0 }),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := srv.EnableRaftEnvelope(ctx, validEnableRaftEnvelopeRequest())
	if err == nil {
		t.Fatal("EnableRaftEnvelope: want apply-timeout error, got nil")
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("EnableRaftEnvelope status=%v, want DeadlineExceeded for apply-wait ctx fire", status.Code(err))
	}
	// Critical assertion: End was NOT called. The barrier stays
	// open so user proposals on this leader continue to fail with
	// ErrEnvelopeCutoverInProgress until operator intervention.
	for _, op := range barrier.order {
		if op == "End" {
			t.Errorf("barrier.End() WAS called after propose-success + apply-timeout; order = %v; this opens the cluster-brick window codex P1 round-1 flagged", barrier.order)
		}
	}
	// And the sequence reached propose + drain but not InstallWrap.
	wantPrefix := []string{"Begin", "WaitDrained"}
	if len(barrier.order) < len(wantPrefix) || !equalStrings(barrier.order[:len(wantPrefix)], wantPrefix) {
		t.Errorf("barrier.order prefix = %v, want prefix %v", barrier.order, wantPrefix)
	}
	for _, op := range barrier.order {
		if op == "InstallWrap" {
			t.Errorf("InstallWrap MUST NOT run when apply-wait fails; order = %v", barrier.order)
		}
	}
	if len(proposer.calls) != 1 {
		t.Errorf("proposer.calls len=%d, want 1 (cutover entry MUST have been proposed before the apply-wait timeout)", len(proposer.calls))
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_RefusesOnStaleDEKApplyRace
// pins the codex P1 round-2 invariant (codex P1 #2 on PR933): if a
// concurrent RotateDEK advances sidecar.Active.Raft between
// propose and apply, the applier consumes the cutover marker as a
// stale-DEK no-op (RaftAppliedIndex advances but
// RaftEnvelopeCutoverIndex stays 0 — §6E-1a constraint #3 benign
// no-op). The handler MUST detect this race by re-reading the
// sidecar after awaitCutoverApply and refuse to InstallWrap.
// Installing the wrap while the FSM sidecar still reads
// RaftEnvelopeCutoverIndex == 0 would brick the cluster: the §6.3
// strict-`>` hook would treat every post-install proposal at
// index > 0 as a wrapped envelope while pre-install admin entries
// (RotateDEK from the race, cleartext via ProposeAdmin) still sit
// in the apply queue at indexes > 0, halting on unwrap-failure.
//
// Expected outcome on the race:
//   - awaitCutoverApply returns success (RaftAppliedIndex advanced).
//   - Sidecar re-read shows RaftEnvelopeCutoverIndex == 0.
//   - InstallWrap is NOT called.
//   - releaseSafe stays true, deferred End() releases the barrier
//     (no cutover took effect; safe to resume user writes — they
//     remain in cleartext pre-cutover mode).
//   - Handler returns a FailedPrecondition error so the operator's
//     CLI can retry against the now-updated Active.Raft.
func TestEncryptionAdmin_EnableRaftEnvelope_RefusesOnStaleDEKApplyRace(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		// applyStaleDEKIDRace advances RaftAppliedIndex but
		// deliberately does NOT touch RaftEnvelopeCutoverIndex —
		// the §2.1 #3 benign-no-op shape the applier produces for
		// a stale-DEK race.
		applyFn: applyStaleDEKIDRace,
	}
	barrier := &recordingCutoverBarrier{}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(barrier),
		WithEncryptionAdminLatestAppliedIndex(func() uint64 { return 4242 }),
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if err == nil {
		t.Fatal("EnableRaftEnvelope: want stale-DEK-race error, got nil")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("EnableRaftEnvelope status=%v, want FailedPrecondition (cutover marker consumed as no-op)", status.Code(err))
	}
	for _, op := range barrier.order {
		if op == "InstallWrap" {
			t.Errorf("InstallWrap WAS called after stale-DEK race; order = %v; this is the codex P1 #2 cluster-brick window", barrier.order)
		}
	}
	// End MUST have run: releaseSafe stays true because no cutover
	// took effect, so unblocking user writes is safe (they stay
	// in pre-cutover cleartext mode, consistent with the FSM state).
	endCalled := false
	for _, op := range barrier.order {
		if op == "End" {
			endCalled = true
			break
		}
	}
	if !endCalled {
		t.Errorf("End MUST run on stale-DEK no-op (no committed cutover, safe to release); order = %v", barrier.order)
	}
	// Sidecar still at the pre-cutover state: RaftEnvelopeCutoverIndex == 0.
	sc, readErr := encryption.ReadSidecar(path)
	if readErr != nil {
		t.Fatalf("ReadSidecar: %v", readErr)
	}
	if sc.RaftEnvelopeCutoverIndex != 0 {
		t.Errorf("RaftEnvelopeCutoverIndex=%d after stale-DEK race, want 0", sc.RaftEnvelopeCutoverIndex)
	}
}

// TestEncryptionAdmin_EnableRaftEnvelope_BarrierStaysOpenOnSidecarReadError
// pins the new r2 sidecar-I/O-failure branch of
// runRaftEnvelopeCutoverBarrier (claude r3 finding 1 on PR933).
// After awaitCutoverApply succeeds, the handler re-reads the
// sidecar to distinguish fresh-success from stale-DEK no-op. If
// that re-read fails (corrupted sidecar, filesystem fault, ENOENT
// after the fact), the handler MUST NOT InstallWrap and MUST NOT
// release the barrier: the cutover entry IS committed in Raft and
// the operator cannot determine the outcome without sidecar
// access. End() stays unreached so user proposals on this leader
// remain rejected with ErrEnvelopeCutoverInProgress until
// supervised restart.
//
// Construction: applyingProposer writes a valid sidecar with
// RaftEnvelopeCutoverIndex=4242 (fresh success), then a
// post-apply hook truncates the sidecar to zero bytes before
// EnableRaftEnvelope can re-read it. ReadSidecar surfaces a
// decode error; the handler must propagate that as an error
// without InstallWrap or End.
func TestEncryptionAdmin_EnableRaftEnvelope_BarrierStaysOpenOnSidecarReadError(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		applyFn: func(sc *encryption.Sidecar, raftIdx uint64) {
			// Apply the cutover as fresh-success (mirrors what the
			// real applier does on the no-race path). The
			// post-write sidecar corruption is handled by the
			// postProposeCorruptingProposer wrapper below, not
			// here — applyFn only sees the sc value pre-write, so
			// we can't reach the on-disk file from this hook.
			applyRaftCutover(sc, raftIdx)
		},
	}
	// postProposeCorruptingProposer composes applyingProposer and
	// truncates the sidecar on disk after applyingProposer.Propose
	// returns, simulating an I/O fault (or filesystem corruption)
	// in the narrow window between the cutover apply and the
	// handler's post-apply sidecar re-read.
	corruptingProposer := &postProposeCorruptingProposer{
		inner:       proposer,
		sidecarPath: path,
	}
	barrier := &recordingCutoverBarrier{}
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(corruptingProposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(barrier),
		WithEncryptionAdminLatestAppliedIndex(func() uint64 { return 4242 }),
	)
	_, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if err == nil {
		t.Fatal("EnableRaftEnvelope: want sidecar-read error post-apply, got nil")
	}
	for _, op := range barrier.order {
		if op == "InstallWrap" {
			t.Errorf("InstallWrap MUST NOT run on sidecar-read failure (cutover outcome unknown); order = %v", barrier.order)
		}
		if op == "End" {
			t.Errorf("End MUST NOT run on sidecar-read failure (releaseSafe stays false; operator must intervene); order = %v", barrier.order)
		}
	}
	// Sequence reached propose + drain but not the post-apply
	// re-read's successful branch.
	wantPrefix := []string{"Begin", "WaitDrained"}
	if len(barrier.order) < len(wantPrefix) || !slices.Equal(barrier.order[:len(wantPrefix)], wantPrefix) {
		t.Errorf("barrier.order prefix = %v, want prefix %v", barrier.order, wantPrefix)
	}
}

// postProposeCorruptingProposer wraps applyingProposer and, after
// the apply callback writes the fresh-success sidecar, truncates
// the file to zero bytes so the handler's subsequent re-read
// surfaces a decode error. Used by the sidecar-read-failure test
// above. Only Propose / ProposeAdmin go through this wrapper;
// other methods (Stop, etc.) aren't needed on the test path.
type postProposeCorruptingProposer struct {
	inner       *applyingProposer
	sidecarPath string
}

func (p *postProposeCorruptingProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	res, err := p.inner.Propose(ctx, data)
	if err == nil {
		// Truncate the sidecar so the handler's re-read fails.
		// os.Truncate to 0 bytes is enough — ReadSidecar's JSON
		// decode of an empty buffer surfaces an error.
		if truncErr := truncateFileToZero(p.sidecarPath); truncErr != nil {
			return nil, truncErr
		}
	}
	return res, err
}

func (p *postProposeCorruptingProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

// truncateFileToZero rewrites the file at path to an empty byte
// sequence, preserving the path so ReadSidecar's open succeeds and
// the decode is what fails. Used only by the sidecar-read-failure
// test.
func truncateFileToZero(path string) error {
	if err := os.WriteFile(path, []byte{}, 0o600); err != nil {
		return fmt.Errorf("test: truncate sidecar: %w", err)
	}
	return nil
}

// TestEncryptionAdmin_EnableRaftEnvelope_PollsApplyIndex pins the
// awaitCutoverApply ticker-poll branch (claude finding 2 round-1):
// the happy-path test wires latestAppliedIndex to return the
// proposed index immediately, which short-circuits the poll loop
// before time.NewTicker fires. A regression that broke the ticker
// branch (wrong duration, ticker.Stop missed, etc.) would only
// surface under a lagging-apply scenario like this one.
//
// Strategy: have latestAppliedIndex return 0 for the first few
// calls and proposedIdx after that, forcing the loop to poll
// through the ticker at least once.
func TestEncryptionAdmin_EnableRaftEnvelope_PollsApplyIndex(t *testing.T) {
	withWrapEnabledForTest(t)
	path := cutoverReadySidecarFixture(t)
	proposer := &applyingProposer{
		recordingProposer: recordingProposer{commitIndex: 4242},
		sidecarPath:       path,
		applyFn:           applyRaftCutover,
	}
	barrier := &recordingCutoverBarrier{}
	var pollCount atomic.Int32
	srv := NewEncryptionAdminServer(
		WithEncryptionAdminProposer(proposer),
		WithEncryptionAdminLeaderView(stubLeaderView{state: raftengine.StateLeader}),
		WithEncryptionAdminSidecarPath(path),
		WithEncryptionAdminCapabilityFanout(fixedCapabilityFanout(allOKFanoutResult(), nil)),
		WithEncryptionAdminCutoverBarrier(barrier),
		WithEncryptionAdminLatestAppliedIndex(func() uint64 {
			if pollCount.Add(1) < 4 {
				return 0
			}
			return 4242
		}),
	)
	resp, err := srv.EnableRaftEnvelope(context.Background(), validEnableRaftEnvelopeRequest())
	if err != nil {
		t.Fatalf("EnableRaftEnvelope: %v", err)
	}
	if resp == nil || resp.GetAppliedIndex() != 4242 {
		t.Fatalf("response applied_index=%v, want 4242", resp)
	}
	if got := pollCount.Load(); got < 4 {
		t.Errorf("pollCount = %d, want >= 4 (ticker poll branch should have fired at least 3 times)", got)
	}
	// Full success path: barrier closes after InstallWrap.
	wantOrder := []string{"Begin", "WaitDrained", "InstallWrap", "End"}
	if !equalStrings(barrier.order, wantOrder) {
		t.Errorf("barrier.order = %v, want %v", barrier.order, wantOrder)
	}
}

// equalStrings is a thin alias over slices.Equal, kept as a
// named helper so the existing call sites (Begin / WaitDrained /
// InstallWrap / End order assertions) read naturally. Claude r2
// finding C: prefer slices.Equal from stdlib over a hand-rolled
// loop.
func equalStrings(a, b []string) bool {
	return slices.Equal(a, b)
}

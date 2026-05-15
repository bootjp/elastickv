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

// TestEncryptionAdmin_BootstrapEncryption_Unimplemented pins the
// PR-B contract that BootstrapEncryption is still un-wired and
// returns Unimplemented. PR-C will replace this with positive
// tests once the §5.6 step 1a capability fan-out lands.
func TestEncryptionAdmin_BootstrapEncryption_Unimplemented(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer()
	ctx := context.Background()
	if _, err := srv.BootstrapEncryption(ctx, &pb.BootstrapEncryptionRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("BootstrapEncryption status=%v, want Unimplemented", status.Code(err))
	}
}

// TestEncryptionAdmin_MutatingRPCs_RejectWithoutProposer pins the
// PR-A production-inert guarantee: an EncryptionAdminServer built
// without WithEncryptionAdminProposer must reject every mutating
// RPC. The §6.5 cluster flag (Stage 6) is what flips this on by
// wiring a real proposer.
func TestEncryptionAdmin_MutatingRPCs_RejectWithoutProposer(t *testing.T) {
	t.Parallel()
	srv := NewEncryptionAdminServer()
	ctx := context.Background()
	if _, err := srv.RotateDEK(ctx, validRotateDEKRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RotateDEK status=%v, want FailedPrecondition (no proposer wired)", status.Code(err))
	}
	if _, err := srv.RegisterEncryptionWriter(ctx, validRegisterEncryptionWriterRequest()); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RegisterEncryptionWriter status=%v, want FailedPrecondition (no proposer wired)", status.Code(err))
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
// PR756 codex P1 regression: full_node_id=0 is the §6.1 sentinel
// for "not encryption-capable" and must not be persisted into a
// writer-registry row. Reject at the gRPC boundary with
// InvalidArgument.
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
// PR756 codex P1 (round-2) regression: a partitioned former
// leader whose local State() still reports StateLeader but
// whose VerifyLeader fails (no quorum) must reject mutating
// RPCs with FailedPrecondition. Without this, a stranded leader
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

// TestEncryptionAdmin_RotateDEK_MapsProposeLeaderErrorToFailedPrecondition
// pins the PR756 codex P1 regression: Propose() returning
// ErrNotLeader / ErrLeadershipLost / ErrLeadershipTransferInProgress
// must surface as FailedPrecondition with the engine error in the
// status detail so clients can retry against the right node, NOT
// as the default codes.Unknown that pkgerrors.Wrap would produce.
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
// pins the PR756 codex P1 regression for the non-leadership
// failure mode. A propose failure that is NOT a known leadership
// sentinel surfaces as Unavailable (retryable transient failure)
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
// pins the PR756 claude[bot] P2 fix: a zero-length writers slice
// returns a "got 0" message, not the misleading "use
// BootstrapEncryption for multi-writer batches" text.
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

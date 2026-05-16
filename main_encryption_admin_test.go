package main

import (
	"context"
	"net"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// stubEncryptionAdminEngine satisfies the encryptionAdminEngine
// interface (Proposer + LeaderView) with do-nothing impls. The
// registration helper only stores the engine and validates the
// option pairing, so the methods are unreachable from the
// registration path itself.
type stubEncryptionAdminEngine struct{}

func (stubEncryptionAdminEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}
func (stubEncryptionAdminEngine) State() raftengine.State       { return raftengine.StateLeader }
func (stubEncryptionAdminEngine) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{} }
func (stubEncryptionAdminEngine) VerifyLeader(context.Context) error {
	return nil
}
func (stubEncryptionAdminEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

// TestEncryptionAdminFullNodeID_DistinctPerRaftId pins the
// PR760 r1 Codex P1 regression: full_node_id must be derived from
// --raftId (per-node-stable) and NOT from the Raft group id
// (shared across replicas). An earlier wiring passed rt.spec.id,
// which makes every replica return the same CapabilityReport
// value and breaks BootstrapEncryption's
// validateWriterBatchUniqueness check. The fix routes through
// etcd.DeriveNodeID(raftId) at the call site; this test confirms
// the canonical derivation is in fact distinct for the canonical
// 3-node config (n1 / n2 / n3) the demo + Jepsen runners use.
func TestEncryptionAdminFullNodeID_DistinctPerRaftId(t *testing.T) {
	seen := make(map[uint64]string)
	for _, id := range []string{"n1", "n2", "n3"} {
		got := etcdraftengine.DeriveNodeID(id)
		if got == 0 {
			t.Fatalf("DeriveNodeID(%q) returned 0; the §6.1 not-capable sentinel must never be used as a real node id", id)
		}
		if dup, exists := seen[got]; exists {
			t.Fatalf("DeriveNodeID(%q) = DeriveNodeID(%q) = %d; full_node_id must be distinct across raftIds — BootstrapEncryption uniqueness would reject", id, dup, got)
		}
		seen[got] = id
	}
}

// TestEncryptionAdmin_MutatingRPCRefusedWithoutSidecarPath pins
// the PR760 r2 Codex P1 regression: with --encryptionSidecarPath
// empty, registerEncryptionAdminServer must NOT wire the Proposer
// or LeaderView, so mutating RPCs (BootstrapEncryption /
// RotateDEK / RegisterEncryptionWriter) return FailedPrecondition
// at the RPC boundary instead of issuing a Raft proposal that
// would land on the still-unwired FSM applier (kvFSM.applyEncryption
// with nil applier) and halt the apply loop cluster-wide.
//
// The original wiring before the fix passed Proposer +
// LeaderView unconditionally, so a stray RotateDEK call against
// a sidecarless cluster would commit through Raft and halt every
// node on apply. This test would have failed under that wiring
// because the FailedPrecondition assertion would never fire —
// the call would have reached the stub Propose() and returned a
// fake success.
func TestEncryptionAdmin_MutatingRPCRefusedWithoutSidecarPath(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	registerEncryptionAdminServer(gs, stubEncryptionAdminEngine{}, 1, "")
	go func() { _ = gs.Serve(listener) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := pb.NewEncryptionAdminClient(conn)
	_, err = client.BootstrapEncryption(context.Background(), &pb.BootstrapEncryptionRequest{
		StorageDekId:      1,
		RaftDekId:         2,
		WrappedStorageDek: []byte("w"),
		WrappedRaftDek:    []byte("w"),
	})
	if err == nil {
		t.Fatalf("BootstrapEncryption succeeded with sidecarPath empty; mutating wiring must be gated to refuse the proposal before it reaches the FSM applier")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("BootstrapEncryption status=%v, want FailedPrecondition (sidecarPath empty → proposer not wired); err=%v", got, err)
	}
}

func TestRegisterEncryptionAdminServer_Registers(t *testing.T) {
	gs := grpc.NewServer()
	registerEncryptionAdminServer(gs, stubEncryptionAdminEngine{}, 1, "")
	// reflection-style check: the service descriptor must show up
	// in the registered service map. grpc.Server exposes this via
	// GetServiceInfo.
	info := gs.GetServiceInfo()
	// The .proto declares no package statement, so the service
	// name surfaces as the bare "EncryptionAdmin" in
	// grpc.Server.GetServiceInfo() (not "proto.EncryptionAdmin").
	if _, ok := info["EncryptionAdmin"]; !ok {
		var registered []string
		for name := range info {
			registered = append(registered, name)
		}
		t.Fatalf("EncryptionAdmin not registered; got services=%v", registered)
	}
}

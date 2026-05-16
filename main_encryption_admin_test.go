package main

import (
	"context"
	"net"
	"testing"

	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

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

// TestEncryptionAdmin_MutatingRPCRefusedUntilStage6 pins the
// PR760 r3 Codex P1 regression. registerEncryptionAdminServer
// must NOT wire the Proposer or LeaderView regardless of
// sidecarPath, because the §6.3 WithEncryption applier seam is
// still unwired on the FSM side pre-Stage-6 (FSM is constructed
// via kv.NewKvFSMWithHLC, no WithEncryption option). Gating the
// mutator wiring on sidecarPath alone (as PR760 r2 did) is not
// safe enough: an operator who sets --encryptionSidecarPath for
// read-only capability probing would still be able to trigger
// a cluster-halt by calling Bootstrap / RotateDEK /
// RegisterEncryptionWriter — the proposal would land on every
// replica, hit kvFSM.applyEncryption with nil applier, and stop
// the apply loop.
//
// Both sub-cases (sidecarPath empty AND sidecarPath set) MUST
// see the mutator refused at the gRPC boundary with
// FailedPrecondition. Stage 6 will reintroduce the mutator
// wiring in the same change that wires the applier.
func TestEncryptionAdmin_MutatingRPCRefusedUntilStage6(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sidecarPath string
	}{
		{name: "sidecar_empty", sidecarPath: ""},
		{name: "sidecar_set", sidecarPath: "/tmp/elastickv-stage5d-test-sidecar.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := bufconn.Listen(1024 * 1024)
			gs := grpc.NewServer()
			registerEncryptionAdminServer(gs, 1, tc.sidecarPath)
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
				t.Fatalf("BootstrapEncryption succeeded (sidecarPath=%q); mutating wiring must be refused at the RPC boundary until Stage 6 wires the applier", tc.sidecarPath)
			}
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("BootstrapEncryption status=%v, want FailedPrecondition (sidecarPath=%q → proposer not wired pre-Stage-6); err=%v", got, tc.sidecarPath, err)
			}
		})
	}
}

func TestRegisterEncryptionAdminServer_Registers(t *testing.T) {
	gs := grpc.NewServer()
	registerEncryptionAdminServer(gs, 1, "")
	info := gs.GetServiceInfo()
	if _, ok := info["EncryptionAdmin"]; !ok {
		var registered []string
		for name := range info {
			registered = append(registered, name)
		}
		t.Fatalf("EncryptionAdmin not registered; got services=%v", registered)
	}
}

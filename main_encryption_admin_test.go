package main

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"google.golang.org/grpc"
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

func TestRegisterEncryptionAdminServer_Registers(t *testing.T) {
	gs := grpc.NewServer()
	registerEncryptionAdminServer(gs, stubEncryptionAdminEngine{}, 1)
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

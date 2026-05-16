package main

import (
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// encryptionAdminEngine is the subset of raftengine.Engine the
// EncryptionAdminServer needs: a Proposer (for the mutating RPCs)
// and a LeaderView (for the requireLeader gate). Every shard's
// raftengine.Engine satisfies both interfaces; declaring a local
// intersection keeps the construction site decoupled from the
// concrete engine type and lets tests substitute a stub without
// pulling in the full engine surface.
type encryptionAdminEngine interface {
	raftengine.Proposer
	raftengine.LeaderView
}

// registerEncryptionAdminServer constructs and registers an
// EncryptionAdminServer on the supplied gRPC server. The function
// is intentionally per-shard: the §7.1 Phase-0 GetCapability
// fan-out polls every member, and mutator RPCs route through the
// shard's own raftengine.LeaderView so a follower returns
// FailedPrecondition with the current leader's hint.
//
// fullNodeID MUST be per-node-stable (every replica of the same
// group sees a distinct value), NOT the Raft group id (which is
// shared across replicas). Production callers derive it via
// etcd.DeriveNodeID(*raftId) at the call site so the value
// matches what raftengine itself uses for peer identification.
// A wrong-shape value here makes BootstrapEncryption fail with
// "duplicate full_node_id" because every node reports the same
// number — Codex r1 P1 on PR #760 caught the original wiring
// passing the shard id by mistake.
//
// Production-inert until Stage 6 — gating on --encryptionSidecarPath:
//   - sidecarPath is empty by default, so GetCapability returns
//     encryption_capable=false (the §7.1 cutover refuses with
//     ErrCapabilityCheckFailed).
//   - Proposer + LeaderView are wired ONLY when sidecarPath is
//     set. Without them, RotateDEK / BootstrapEncryption /
//     RegisterEncryptionWriter return FailedPrecondition
//     "proposer is not configured on this node" at the RPC
//     boundary — before any Raft proposal is issued. This is
//     the load-bearing safety boundary against Codex r2 P1: the
//     §6.3 WithEncryption applier seam is still unwired on the
//     FSM side pre-Stage-6, so a successfully-applied bootstrap
//     entry would hit the fail-closed halt path in
//     kvFSM.applyEncryption (nil applier) and stop the apply
//     loop cluster-wide. Refusing the proposal at the RPC layer
//     means an accidental client call cannot escalate from a
//     single RPC into a cluster-halt.
//
// Validate() panics on a misconfiguration that wires a proposer
// without a LeaderView. The wiring below pairs both options
// together (both wired iff sidecarPath set) so the invariant
// holds by construction — the panic is the load-bearing guard
// against a future refactor that splits the two options apart.
func registerEncryptionAdminServer(gs *grpc.Server, engine encryptionAdminEngine, fullNodeID uint64, sidecarPath string) {
	opts := []adapter.EncryptionAdminServerOption{
		adapter.WithEncryptionAdminFullNodeID(fullNodeID),
	}
	if sidecarPath != "" {
		opts = append(opts,
			adapter.WithEncryptionAdminSidecarPath(sidecarPath),
			adapter.WithEncryptionAdminProposer(engine),
			adapter.WithEncryptionAdminLeaderView(engine),
		)
	}
	srv := adapter.NewEncryptionAdminServer(opts...)
	if err := srv.Validate(); err != nil {
		panic(errors.Wrap(err, "encryption admin server validation"))
	}
	pb.RegisterEncryptionAdminServer(gs, srv)
}

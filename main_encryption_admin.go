package main

import (
	"github.com/bootjp/elastickv/adapter"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// registerEncryptionAdminServer constructs and registers an
// EncryptionAdminServer on the supplied gRPC server. The function
// is intentionally per-shard: the §7.1 Phase-0 GetCapability
// fan-out polls every member, and the §5.1 sidecar contents are
// per-node.
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
// Stage 5D is intentionally inert for mutating RPCs. This helper
// installs ONLY the sidecar-path + full-node-id options, never
// the Proposer or LeaderView. With both omitted,
// EncryptionAdminServer.BootstrapEncryption / RotateDEK /
// RegisterEncryptionWriter return FailedPrecondition at the gRPC
// boundary — the proposal never reaches Raft.
//
// The reason mutator wiring stays off ENTIRELY (not just
// sidecar-gated) is Codex r3 P1 on PR #760: the §6.3
// WithEncryption applier seam is plumbed by Stage 6. Today the
// FSM is constructed via kv.NewKvFSMWithHLC (no WithEncryption
// option). A successfully-applied bootstrap entry would land a
// 0x03/0x04/0x05 Raft entry on every replica, the FSM apply path
// would route through kvFSM.applyEncryption, the default branch
// would fire ErrEncryptionApply because the applier is nil, and
// the engine's HaltApply seam would stop apply on every node.
// Gating mutator wiring on --encryptionSidecarPath (as PR760 r2
// did) is not safe enough — an operator who sets sidecarPath for
// capability probing alone could still trigger a cluster-halt by
// calling a mutating RPC. The applier and the mutator wiring
// must land together in Stage 6.
//
// sidecarPath controls only the read-only capability surface:
// when empty, GetCapability reports encryption_capable=false
// (the §7.1 cutover refuses with ErrCapabilityCheckFailed);
// when set, capability probing reads the §5.1 keys.json and
// reports encryption_capable=true. ResyncSidecar's leader guard
// (requireLeader) returns nil when LeaderView is unset — the
// "test escape hatch" path documented in requireLeader's godoc —
// so ResyncSidecar serves the local sidecar on every node until
// Stage 6 wires the LeaderView. Pre-bootstrap the sidecar is
// either non-existent or empty so there is no §5.5 leak window.
//
// Validate() panics on a misconfiguration that wires a proposer
// without a LeaderView. The wiring below never wires either, so
// the invariant holds vacuously today; the panic remains the
// load-bearing guard against a future Stage 6 refactor that
// splits the two options apart.
func registerEncryptionAdminServer(gs *grpc.Server, fullNodeID uint64, sidecarPath string) {
	opts := []adapter.EncryptionAdminServerOption{
		adapter.WithEncryptionAdminFullNodeID(fullNodeID),
	}
	if sidecarPath != "" {
		opts = append(opts, adapter.WithEncryptionAdminSidecarPath(sidecarPath))
	}
	srv := adapter.NewEncryptionAdminServer(opts...)
	if err := srv.Validate(); err != nil {
		panic(errors.Wrap(err, "encryption admin server validation"))
	}
	pb.RegisterEncryptionAdminServer(gs, srv)
}

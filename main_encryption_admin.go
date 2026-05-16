package main

import (
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// encryptionMutatorsEnabled is the Stage 6B-2 double-gate
// readback. Returns true iff the operator has BOTH opted in
// (--encryption-enabled) AND supplied a KEK source (--kekFile).
// Either condition false → mutator wiring stays off.
//
// Kept in this file (not main.go) so the flag-driven gate logic
// is colocated with the registerEncryptionAdminServer helper
// that consumes it.
func encryptionMutatorsEnabled() bool {
	return *encryptionEnabled && *kekFile != ""
}

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
// Stage 6B-2: the mutator wiring (Proposer + LeaderView) is now
// gated on the supplied enableMutators boolean, which the caller
// in main.go computes as (--encryption-enabled AND --kekFile
// non-empty AND engine non-nil). When enableMutators is false,
// Proposer + LeaderView stay unwired and EncryptionAdminServer's
// BootstrapEncryption / RotateDEK / RegisterEncryptionWriter
// short-circuit at the gRPC boundary with FailedPrecondition —
// identical to the Stage 5D posture. When enableMutators is
// true, both options are wired and the mutators reach the §6.3
// applier through the supplied engine's Propose path.
//
// The double gate exists because both conditions are
// independently necessary:
//
//   - --encryption-enabled is the operator opt-in: an unset flag
//     means the cluster has explicitly chosen NOT to participate
//     in the §7.1 rollout, so mutator RPCs MUST refuse even on
//     a fully-keyed binary.
//   - --kekFile being non-empty means a KEK source is loaded;
//     without it, a mutator that committed would land in the
//     applier with no KEK and return ErrKEKNotConfigured from
//     the §6.3 HaltApply path — that is fail-closed but it
//     halts the whole cluster's apply loop. The RPC-layer
//     KEKConfigured() gate keeps the halt unreachable in
//     practice.
//
// Validate() panics on a misconfiguration that wires a proposer
// without a LeaderView. The wiring below pairs both options
// together (both wired iff enableMutators) so the invariant
// holds by construction.
//
// sidecarPath controls only the read-only capability surface:
// when empty, GetCapability reports encryption_capable=false
// (the §7.1 cutover refuses with ErrCapabilityCheckFailed);
// when set, capability probing reads the §5.1 keys.json and
// reports encryption_capable=true.
func registerEncryptionAdminServer(gs *grpc.Server, fullNodeID uint64, sidecarPath string, enableMutators bool, engine encryptionAdminEngine) {
	opts := []adapter.EncryptionAdminServerOption{
		adapter.WithEncryptionAdminFullNodeID(fullNodeID),
	}
	if sidecarPath != "" {
		opts = append(opts, adapter.WithEncryptionAdminSidecarPath(sidecarPath))
	}
	if enableMutators && engine != nil {
		opts = append(opts,
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

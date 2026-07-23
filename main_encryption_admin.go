package main

import (
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// encryptionMutatorsEnabled is the Stage 6B-2 triple-gate
// readback. Returns true iff THIS NODE has all three of:
//
//   - --encryption-enabled (explicit operator opt-in)
//   - --kekFile non-empty (KEK source loaded so ApplyBootstrap
//     / ApplyRotation can KEK-unwrap)
//   - --encryptionSidecarPath non-empty (so the applier can
//     crash-durably persist Active.{Storage,Raft} + keys[])
//
// Any one false → mutator wiring stays off. The sidecarPath
// condition catches the case where an operator with the other
// two flags set could trigger a HaltApply: without
// WithSidecarPath, the applier's bootstrapAndRotationConfigured()
// returns false and ApplyBootstrap halts apply with
// ErrKEKNotConfigured. The triple gate keeps that unreachable
// at the RPC boundary.
//
// Scope limitation — this is a NODE-LOCAL gate, not a
// cluster-wide readiness check. During a staged rolling
// upgrade where some peers still run the pre-6B-2 binary (or
// have the flags unset), a mutator proposal committed by a
// fully-configured leader would reach those replicas'
// appliers and HaltApply on the under-configured side. The
// design accepts this for Stage 6B-2 because:
//
//   - The §7.1 Phase-1 cutover RPC (Stage 6D's
//     enable-storage-envelope) is the load-bearing
//     cluster-wide gate; it runs a Voters ∪ Learners
//     capability fan-out via GetCapability and refuses if any
//     member is not encryption_capable. That is the right
//     layer for cluster-wide safety.
//   - Operators using Stage 6B-2 are explicitly opting in to
//     the mutator surface; the rolling-restart discipline
//     ("don't enable any mutator RPC until every member of
//     every Raft group reports encryption_capable") is the
//     same operator-discipline constraint documented in PR
//     #765 (Stage 6A) and in the Stage 6 plan in
//     docs/design/2026_04_29_partial_data_at_rest_encryption.md
//     (6A rationale, "Rolling 6A→6B upgrade caveat").
//   - Adding a cluster-wide readiness probe at this layer
//     would duplicate the §7.1 capability gate and ship
//     before the operator-facing GetCapability fan-out is
//     wired into the admin CLI.
//
// PR #776 round-2 codex P1 flagged this scope boundary; the
// resolution is to document the constraint explicitly here
// rather than ship a partial cluster-wide check that would
// be superseded by Stage 6D in any case.
//
// Kept in this file (not main.go) so the flag-driven gate logic
// is colocated with the registerEncryptionAdminServer helper
// that consumes it.
func encryptionMutatorsEnabled() bool {
	return *encryptionEnabled && *kekFile != "" && *encryptionSidecarPath != ""
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

func encryptionAdminWriterRegistry(sidecarPath string, st store.MVCCStore, groupID uint64) (encryption.WriterRegistryStore, error) {
	if sidecarPath == "" {
		return nil, nil
	}
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to construct encryption admin writer registry for group %d", groupID)
	}
	return reg, nil
}

func encryptionAdminDefaultWriterRegistry(sidecarPath string, shardGroups map[uint64]*kv.ShardGroup, defaultGroupID uint64) (encryption.WriterRegistryStore, error) {
	if sidecarPath == "" {
		return nil, nil
	}
	defaultGroup := shardGroups[defaultGroupID]
	if defaultGroup == nil || defaultGroup.Store == nil {
		return nil, errors.Errorf("failed to construct encryption admin writer registry: default group %d store is unavailable", defaultGroupID)
	}
	return encryptionAdminWriterRegistry(sidecarPath, defaultGroup.Store, defaultGroupID)
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
// applier through the supplied engine. Callers may append a
// wrap-aware post-cutover proposer option so non-cutover admin
// entries wrap after EnableRaftEnvelope while the cutover marker
// itself remains on the raw engine path.
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
func registerEncryptionAdminServer(gs *grpc.Server, fullNodeID uint64, sidecarPath string, enableMutators bool, engine encryptionAdminEngine, capabilityFanout adapter.CapabilityFanoutFn, writerRegistry encryption.WriterRegistryStore, extraOpts ...adapter.EncryptionAdminServerOption) {
	opts := []adapter.EncryptionAdminServerOption{
		adapter.WithEncryptionAdminFullNodeID(fullNodeID),
	}
	if sidecarPath != "" {
		opts = append(opts, adapter.WithEncryptionAdminSidecarPath(sidecarPath))
	}
	if writerRegistry != nil {
		opts = append(opts, adapter.WithEncryptionAdminWriterRegistry(writerRegistry))
	}
	if enableMutators && engine != nil {
		opts = append(opts,
			adapter.WithEncryptionAdminProposer(engine),
			adapter.WithEncryptionAdminLeaderView(engine),
		)
		// The §4 capability fan-out is only meaningful when the
		// cutover mutator is reachable (same enableMutators gate as
		// the Proposer/LeaderView). Without it the cutover RPC
		// refuses with the §4 "fan-out not wired" FailedPrecondition.
		if capabilityFanout != nil {
			opts = append(opts, adapter.WithEncryptionAdminCapabilityFanout(capabilityFanout))
		}
		opts = append(opts, extraOpts...)
	}
	srv := adapter.NewEncryptionAdminServer(opts...)
	if err := srv.Validate(); err != nil {
		panic(errors.Wrap(err, "encryption admin server validation"))
	}
	pb.RegisterEncryptionAdminServer(gs, srv)
}

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
// Production-inert until Stage 6:
//   - sidecarPath is empty by default, so GetCapability returns
//     encryption_capable=false (the §7.1 cutover refuses with
//     ErrCapabilityCheckFailed).
//   - The §6.3 WithEncryption applier seam is still unwired on the
//     FSM side, so a successfully-applied bootstrap entry is a
//     no-op storage-side.
//
// Validate() panics on a misconfiguration that wires a proposer
// without a LeaderView. The single callsite passes the same engine
// for both options, so the invariant holds by construction — the
// panic is the load-bearing guard against a future refactor that
// splits the two options apart.
func registerEncryptionAdminServer(gs *grpc.Server, engine encryptionAdminEngine, shardID uint64) {
	opts := []adapter.EncryptionAdminServerOption{
		adapter.WithEncryptionAdminProposer(engine),
		adapter.WithEncryptionAdminLeaderView(engine),
		adapter.WithEncryptionAdminFullNodeID(shardID),
	}
	if *encryptionSidecarPath != "" {
		opts = append(opts, adapter.WithEncryptionAdminSidecarPath(*encryptionSidecarPath))
	}
	srv := adapter.NewEncryptionAdminServer(opts...)
	if err := srv.Validate(); err != nil {
		panic(errors.Wrap(err, "encryption admin server validation"))
	}
	pb.RegisterEncryptionAdminServer(gs, srv)
}

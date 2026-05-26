package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// registrationRetryInitial / registrationRetryMax bound the backoff
	// of the Stage 7a process-start registration goroutine. The first
	// attempts often race leader election (no leader address yet), so we
	// start small and cap so a long leaderless window does not busy-loop.
	registrationRetryInitial = 100 * time.Millisecond
	registrationRetryMax     = 5 * time.Second
	// registrationBackoffFactor is the exponential multiplier applied
	// to the retry backoff between failed registration attempts.
	registrationBackoffFactor = 2
)

// installProcessStartRegistrationGate builds the Stage 7a registration
// gate and wires it onto the coordinator. A registry-read failure at
// this point is a startup fault; rather than add a branch to run() (it
// is already at the cyclop ceiling), the error is surfaced through the
// errgroup, whose cancellation tears the process down before it serves
// — the same lifecycle path every other startup goroutine uses.
func installProcessStartRegistrationGate(
	ctx context.Context,
	eg *errgroup.Group,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	raftID string,
) {
	gate, err := buildProcessStartRegistrationGate(ctx, eg, coordinate, defaultGroup, w, raftID)
	if err != nil {
		eg.Go(func() error {
			return errors.Wrap(err, "process-start writer registration intent")
		})
		return
	}
	coordinate.WithRegistrationGate(gate)
}

// buildProcessStartRegistrationGate implements the Stage 7a §4.1
// process-start registration intent + first-write barrier. It runs
// AFTER buildShardGroups (the default-group registry store is open) and
// AFTER chainEncryptionStartupGuard (so CheckLocalEpochRollback has
// passed — rollback and no-row-post-cutover are unreachable here).
//
// Returns nil (ungated) in every case except the "propose" branch:
//   - encryption off (no cipher wired),
//   - Phase 0 (cutover not yet fired — the node emits no encrypted
//     nonces yet; runtime-cutover registration is a follow-on, not the
//     process-start path),
//   - not bootstrapped (no active storage DEK),
//   - already registered at the current epoch (the bootstrap-cohort /
//     no-op-restart case — proposing would hit ErrLocalEpochRollback).
//
// In the propose branch it arms an open barrier, starts the async
// registration goroutine (leader-forwarding + bounded retry), and
// returns a gate whose predicates read the shared StateCache.
func buildProcessStartRegistrationGate(
	ctx context.Context,
	eg *errgroup.Group,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	raftID string,
) (*kv.RegistrationGate, error) {
	if w.cipher == nil || defaultGroup == nil {
		return nil, nil //nolint:nilnil // nil gate == ungated; not an error
	}
	if !w.cache.StorageEnvelopeActive() {
		return nil, nil //nolint:nilnil // Phase 0: no encrypted writes yet
	}
	activeDEK, ok := w.cache.ActiveStorageKeyID()
	if !ok {
		return nil, nil //nolint:nilnil // not bootstrapped
	}
	fullNodeID := etcdraftengine.DeriveNodeID(raftID)
	lastSeen, err := readWriterRegistryLastSeen(defaultGroup.Store, activeDEK, fullNodeID)
	if err != nil {
		return nil, err
	}
	// §4.1 case 2 monotonic advance: propose only when this load's epoch
	// is strictly ahead. Equal (already registered) or — unreachable
	// past the rollback guard — behind, skip.
	if w.epoch <= lastSeen {
		slog.Info("encryption: writer registration skipped (already current)",
			slog.Uint64("dek_id", uint64(activeDEK)),
			slog.Uint64("full_node_id", fullNodeID),
			slog.Uint64("local_epoch", uint64(w.epoch)))
		return nil, nil //nolint:nilnil // already registered at this epoch
	}

	barrier := make(chan struct{})
	entry := registrationEntry(activeDEK, fullNodeID, w.epoch)
	connCache := &kv.GRPCConnCache{}
	eg.Go(func() error {
		<-ctx.Done()
		if cerr := connCache.Close(); cerr != nil {
			return errors.Wrap(cerr, "close writer-registration gRPC connection cache")
		}
		return nil
	})
	eg.Go(func() error {
		runWriterRegistration(ctx, coordinate, defaultGroup.Engine, connCache, entry,
			registrationRequest(activeDEK, fullNodeID, w.epoch), barrier)
		return nil
	})
	return &kv.RegistrationGate{
		Barrier:               barrier,
		StorageEnvelopeActive: w.cache.StorageEnvelopeActive,
		ActiveStorageKeyID:    w.cache.ActiveStorageKeyID,
	}, nil
}

// readWriterRegistryLastSeen returns the registry's last_seen_local_epoch
// for (full_node_id, dek_id), or 0 when no row exists (treated as the
// "propose" trigger per the 7a design §3.1).
func readWriterRegistryLastSeen(st store.MVCCStore, dekID uint32, fullNodeID uint64) (uint16, error) {
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		return 0, errors.Wrap(err, "writer registration: registry handle")
	}
	raw, ok, err := reg.GetRegistryRow(encryption.RegistryKey(dekID, encryption.NodeID16(fullNodeID)))
	if err != nil {
		return 0, errors.Wrap(err, "writer registration: read registry row")
	}
	if !ok {
		return 0, nil
	}
	val, err := encryption.DecodeRegistryValue(raw)
	if err != nil {
		return 0, errors.Wrap(err, "writer registration: decode registry value")
	}
	return val.LastSeenLocalEpoch, nil
}

// registrationEntry composes the §11.3 0x03 OpRegistration Raft entry
// for the local-leader propose path.
func registrationEntry(dekID uint32, fullNodeID uint64, epoch uint16) []byte {
	body := fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{
		DEKID: dekID, FullNodeID: fullNodeID, LocalEpoch: epoch,
	})
	entry := make([]byte, 0, 1+len(body))
	entry = append(entry, fsmwire.OpRegistration)
	return append(entry, body...)
}

// registrationRequest composes the EncryptionAdmin RPC request for the
// follower-forward path.
func registrationRequest(dekID uint32, fullNodeID uint64, epoch uint16) *pb.RegisterEncryptionWriterRequest {
	return &pb.RegisterEncryptionWriterRequest{
		DekId:   dekID,
		Writers: []*pb.WriterRegistryEntry{{FullNodeId: fullNodeID, LocalEpoch: uint32(epoch)}},
	}
}

// runWriterRegistration drives the §4.1 registration to commit, then
// closes the barrier. It retries transient failures (no leader yet,
// proposal dropped, transport blip) with bounded backoff against ctx;
// on ctx cancellation it returns WITHOUT closing the barrier — never
// releasing writes on an uncommitted registration (7a §3.2).
func runWriterRegistration(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultEngine raftengine.Engine,
	connCache *kv.GRPCConnCache,
	entry []byte,
	req *pb.RegisterEncryptionWriterRequest,
	barrier chan struct{},
) {
	backoff := registrationRetryInitial
	for {
		if err := proposeWriterRegistration(ctx, coordinate, defaultEngine, connCache, entry, req); err == nil {
			close(barrier)
			slog.Info("encryption: writer registration committed; first-write barrier released",
				slog.Uint64("dek_id", uint64(req.GetDekId())))
			return
		} else {
			slog.Warn("encryption: writer registration attempt failed; retrying",
				slog.String("error", err.Error()), slog.Duration("backoff", backoff))
		}
		select {
		case <-ctx.Done():
			return // shutdown before commit: leave barrier open (fail-closed)
		case <-time.After(backoff):
			backoff = minDuration(backoff*registrationBackoffFactor, registrationRetryMax)
		}
	}
}

// proposeWriterRegistration submits the registration once: directly via
// the default-group engine when this node is the leader (DisableProposalForwarding
// means a follower's Propose is dropped), else by forwarding to the
// current leader's EncryptionAdmin endpoint.
func proposeWriterRegistration(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultEngine raftengine.Engine,
	connCache *kv.GRPCConnCache,
	entry []byte,
	req *pb.RegisterEncryptionWriterRequest,
) error {
	if coordinate.IsLeader() {
		if _, err := defaultEngine.Propose(ctx, entry); err != nil {
			return errors.Wrap(err, "writer registration: local propose")
		}
		return nil
	}
	addr := coordinate.RaftLeader()
	if addr == "" {
		return errors.New("writer registration: no default-group leader known yet")
	}
	conn, err := connCache.ConnFor(addr)
	if err != nil {
		return errors.Wrapf(err, "writer registration: dial leader %s", addr)
	}
	if _, err := pb.NewEncryptionAdminClient(conn).RegisterEncryptionWriter(ctx, req); err != nil {
		return errors.Wrap(err, "writer registration: forward to leader")
	}
	return nil
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

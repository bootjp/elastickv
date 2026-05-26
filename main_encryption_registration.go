package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/distribution"
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
	// registrationAttemptTimeout bounds a single propose/forward
	// attempt so it always returns to the retry loop (which re-resolves
	// the leader) rather than blocking indefinitely on a stale leader
	// address under GRPCConnCache's WaitForReady(true) dials.
	registrationAttemptTimeout = 5 * time.Second
	// registrationGateWarnAfter is how long the direct-path catalog
	// bootstrap Save may stay blocked on writer registration before a
	// diagnostic WARN fires (Stage 7a-2 §2.3). The retry continues past
	// this; the log line just makes a stuck node visible rather than
	// silently hung.
	registrationGateWarnAfter = 15 * time.Second
)

// retryUntilRegistered runs fn, retrying with bounded backoff while it
// returns store.ErrWriterNotRegistered — the Stage 7a-2 §4.1
// fail-closed signal that the §7.1 envelope is active but this load's
// writer registration has not yet committed. Any other error (or nil)
// returns immediately, so callers that never hit an encrypted direct
// write see the wrapped fn's result unchanged.
//
// The retry is bounded by ctx (the run context), so a shutdown during
// the empty-catalog + active-envelope startup edge cancels the loop and
// returns rather than hanging. There is deliberately no fail-OPEN
// fallback: letting the bootstrap Save proceed unregistered is the
// exact hazard 7a-2 closes (§2.3).
func retryUntilRegistered(ctx context.Context, what string, fn func() error) error {
	backoff := registrationRetryInitial
	start := time.Now()
	warned := false
	for {
		err := fn()
		if err == nil || !errors.Is(err, store.ErrWriterNotRegistered) {
			return err
		}
		if !warned && time.Since(start) >= registrationGateWarnAfter {
			slog.Warn("encryption: direct write blocked on writer registration; still retrying",
				slog.String("operation", what),
				slog.Duration("blocked_for", time.Since(start)))
			warned = true
		}
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "%s: cancelled while blocked on writer registration", what)
		case <-time.After(backoff):
			backoff = min(backoff*registrationBackoffFactor, registrationRetryMax)
		}
	}
}

// setupDistributionAndRegistration installs the Stage 7a process-start
// registration gate and then sets up the distribution catalog,
// returning the catalog store. Bundling the two lets run() handle both
// startup faults through a single error path (keeping run() within the
// cyclop budget) while preserving the fail-synchronously-before-serving
// guarantee for the registration gate.
//
// Ordering (Stage 7a-2 §2.3): the registration gate is armed FIRST,
// then EnsureCatalogSnapshot runs. The catalog bootstrap Save is a
// direct encrypted write that 7a-2 gates on Registered(); arming the
// gate (which starts the registration goroutine) before the bootstrap
// lets the empty-catalog + active-envelope edge clear once registration
// commits, via the bounded retryUntilRegistered wrapper inside
// setupDistributionCatalog. Registration has no dependency on the route
// catalog (the OpRegistration apply writes a writer-registry row, not a
// route), so arm-before-bootstrap cannot deadlock.
func setupDistributionAndRegistration(
	runCtx context.Context,
	eg *errgroup.Group,
	runtimes []*raftGroupRuntime,
	engine *distribution.Engine,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	raftID string,
) (*distribution.CatalogStore, error) {
	if err := installProcessStartRegistrationGate(runCtx, eg, coordinate, defaultGroup, w, raftID); err != nil {
		return nil, err
	}
	// Bootstrap + registration both run under runCtx so a shutdown
	// cancels the bounded retry rather than hanging.
	distCatalog, err := setupDistributionCatalog(runCtx, runtimes, engine)
	if err != nil {
		return nil, err
	}
	return distCatalog, nil
}

// installProcessStartRegistrationGate builds the Stage 7a registration
// gate and wires it onto the coordinator. A registry-read / behind-epoch
// failure here is a startup fault that MUST fail the process
// synchronously — before run() brings up the gRPC servers — so writes
// never serve with no gate installed (codex P1 on PR #839). The caller
// returns the error from run() before serving.
func installProcessStartRegistrationGate(
	ctx context.Context,
	eg *errgroup.Group,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	raftID string,
) error {
	gate, err := buildProcessStartRegistrationGate(ctx, eg, coordinate, defaultGroup, w, raftID)
	if err != nil {
		return errors.Wrap(err, "process-start writer registration intent")
	}
	coordinate.WithRegistrationGate(gate)
	return nil
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
	// An empty &RegistrationGate{} (nil Barrier) is the "ungated" state
	// — awaitRegistration short-circuits on a nil Barrier. Returning it
	// (rather than a nil gate) keeps the no-error/no-registration
	// branches free of //nolint:nilnil.
	if w.cipher == nil || defaultGroup == nil {
		return &kv.RegistrationGate{}, nil
	}
	if !w.cache.StorageEnvelopeActive() {
		return &kv.RegistrationGate{}, nil // Phase 0: no encrypted writes yet
	}
	activeDEK, ok := w.cache.ActiveStorageKeyID()
	if !ok {
		return &kv.RegistrationGate{}, nil // not bootstrapped
	}
	fullNodeID := etcdraftengine.DeriveNodeID(raftID)
	lastSeen, err := readWriterRegistryLastSeen(defaultGroup.Store, activeDEK, fullNodeID)
	if err != nil {
		return nil, err
	}
	// §4.1 case 2 monotonic advance:
	//   - epoch == last_seen → already registered (bootstrap cohort /
	//     no-op restart) → skip, ungated.
	//   - epoch  < last_seen → strictly behind. The §9.1
	//     ErrLocalEpochRollback startup guard should have refused boot;
	//     reaching here means a stale sidecar slipped past it. Fail
	//     closed (refuse startup) rather than serving with a behind
	//     epoch — proceeding ungated would let this load emit nonces
	//     under a recycled (node_id, local_epoch) (codex P1).
	//   - epoch  > last_seen → propose (the trigger).
	switch {
	case w.epoch == lastSeen:
		// Already registered (bootstrap cohort / no-op restart). This
		// branch returns ungated WITHOUT starting runWriterRegistration,
		// so it must seed the Stage 7a-2 direct-path gate itself —
		// otherwise Registered() stays false on every steady-state
		// restart and the direct bootstrap Save would wrongly fail-closed
		// with ErrWriterNotRegistered despite a current registry (codex
		// P1 on PR #843). MarkRegistered is a no-op when activeDEK == 0,
		// but we are past the not-bootstrapped guard so it is non-zero.
		w.cache.MarkRegistered(activeDEK)
		slog.Info("encryption: writer registration skipped (already current)",
			slog.Uint64("dek_id", uint64(activeDEK)),
			slog.Uint64("full_node_id", fullNodeID),
			slog.Uint64("local_epoch", uint64(w.epoch)))
		return &kv.RegistrationGate{}, nil
	case w.epoch < lastSeen:
		return nil, errors.Errorf(
			"encryption: writer registration: local_epoch %d is behind registry last_seen %d for dek_id %d node %#x (stale sidecar past the §9.1 rollback guard)",
			w.epoch, lastSeen, activeDEK, fullNodeID)
	}

	barrier := make(chan struct{})
	entry := registrationEntry(activeDEK, fullNodeID, w.epoch)
	req := registrationRequest(activeDEK, fullNodeID, w.epoch)
	// verifyRegistered re-reads the local registry to detect whether
	// our epoch has actually committed — covers a Propose attempt that
	// timed out (the waiter was removed) but whose entry Raft still
	// applied, or a registration that landed via another path. Without
	// it, consistently >registrationAttemptTimeout commit latency could
	// keep retrying-without-committing forever despite durable
	// registration (codex P2 #6).
	regStore := defaultGroup.Store
	verifyRegistered := func() (bool, error) {
		ls, err := readWriterRegistryLastSeen(regStore, activeDEK, fullNodeID)
		if err != nil {
			return false, err
		}
		return ls >= w.epoch, nil
	}
	eg.Go(func() error {
		runWriterRegistration(ctx, coordinate, defaultGroup.Engine, w.cache, activeDEK, entry, req, barrier, verifyRegistered)
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

// releaseBarrier is the single barrier-close site for the Stage 7a
// registration goroutine. It marks the Stage 7a-2 direct-path gate
// registered for dekID BEFORE closing the channel, so any write the
// barrier close releases (coordinator-gated client writes) observes
// Registered() == true rather than racing a still-false gate that would
// fail-close the first encrypted direct write. MarkRegistered is a
// no-op for dekID == 0 (the gate condition is then false anyway).
func releaseBarrier(barrier chan struct{}, cache *encryption.StateCache, dekID uint32) {
	cache.MarkRegistered(dekID)
	close(barrier)
}

// runWriterRegistration drives the §4.1 registration to commit, then
// closes the barrier. It retries transient failures (no leader yet,
// proposal dropped, transport blip) with bounded backoff against ctx;
// on ctx cancellation it returns WITHOUT closing the barrier — never
// releasing writes on an uncommitted registration (7a §3.2).
//
// cache + dekID feed releaseBarrier so both close sites (verify-before-
// propose and propose-success) seed the Stage 7a-2 direct-path gate.
func runWriterRegistration(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultEngine raftengine.Engine,
	cache *encryption.StateCache,
	dekID uint32,
	entry []byte,
	req *pb.RegisterEncryptionWriterRequest,
	barrier chan struct{},
	verifyRegistered func() (bool, error),
) {
	// The conn cache (forwarding to the leader's EncryptionAdmin) is
	// scoped to this one-shot goroutine: closed when registration
	// commits or ctx ends, so no process-lifetime cache + watcher
	// goroutine is needed.
	connCache := &kv.GRPCConnCache{}
	defer func() {
		if err := connCache.Close(); err != nil {
			slog.Warn("encryption: failed to close writer-registration gRPC connection cache",
				slog.String("error", err.Error()))
		}
	}()

	backoff := registrationRetryInitial
	for {
		// Verify-before-propose: a prior attempt may have timed out yet
		// still committed (Raft applies after the waiter is removed), or
		// another path registered us. Closing on durable registration
		// avoids retrying-without-committing forever under high commit
		// latency (codex P2 #6).
		if registered, verr := verifyRegistered(); verr == nil && registered {
			releaseBarrier(barrier, cache, dekID)
			slog.Info("encryption: writer registration confirmed committed; first-write barrier released",
				slog.Uint64("dek_id", uint64(req.GetDekId())))
			return
		}
		err := proposeWriterRegistration(ctx, coordinate, defaultEngine, connCache, entry, req)
		if err == nil {
			releaseBarrier(barrier, cache, dekID)
			slog.Info("encryption: writer registration committed; first-write barrier released",
				slog.Uint64("dek_id", uint64(req.GetDekId())))
			return
		}
		// On shutdown the failure is just the cancelled ctx — return
		// without a misleading "retrying" warning, leaving the barrier
		// open (fail-closed).
		if ctx.Err() != nil {
			return
		}
		slog.Warn("encryption: writer registration attempt failed; retrying",
			slog.String("error", err.Error()), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return // shutdown before commit: leave barrier open (fail-closed)
		case <-time.After(backoff):
			backoff = min(backoff*registrationBackoffFactor, registrationRetryMax)
		}
	}
}

// proposeWriterRegistration submits the registration once: directly via
// the default-group engine when this node is the leader (DisableProposalForwarding
// means a follower's Propose is dropped), else by forwarding to the
// current leader's EncryptionAdmin endpoint.
//
// Each attempt is bounded by registrationAttemptTimeout so it always
// returns to the retry loop (which re-resolves the leader). Without it,
// the forwarded RPC would inherit the deadline-less run-context and —
// because GRPCConnCache dials WaitForReady(true) — could block forever
// on a stale/unreachable leader address, leaving the barrier open and
// encrypted writes stuck even after leadership moves (codex P1 #5).
func proposeWriterRegistration(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultEngine raftengine.Engine,
	connCache *kv.GRPCConnCache,
	entry []byte,
	req *pb.RegisterEncryptionWriterRequest,
) error {
	attemptCtx, cancel := context.WithTimeout(ctx, registrationAttemptTimeout)
	defer cancel()
	if coordinate.IsLeader() {
		if _, err := defaultEngine.Propose(attemptCtx, entry); err != nil {
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
	if _, err := pb.NewEncryptionAdminClient(conn).RegisterEncryptionWriter(attemptCtx, req); err != nil {
		return errors.Wrap(err, "writer registration: forward to leader")
	}
	return nil
}

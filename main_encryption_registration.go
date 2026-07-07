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
	// runtimeRegistrationPollInterval is the cadence at which the Stage
	// 7b runtime watcher polls the shared StateCache for the cutover
	// trigger (envelope active && !Registered()). One second is short
	// enough that the gate-trapped window after a runtime
	// EnableStorageEnvelope is sub-second in practice, and long enough
	// that an idle node spends negligible CPU on it.
	runtimeRegistrationPollInterval = 1 * time.Second
	// runtimeRegistrationTickTimeout bounds a single tick's call to
	// runWriterRegistration. Without it, a prolonged leaderless /
	// partitioned window would block the watcher goroutine indefinitely
	// inside the inner retry loop — unable to re-poll, re-evaluate the
	// active DEK, or log a deferred-rotation skip (gemini HIGH on PR
	// #853). Choosing 30s gives the inner registrationAttemptTimeout
	// (5s) + backoff a few cycles before the outer timeout fires and
	// the loop reconsiders. Re-propose on the next tick is safe because
	// §4.1 case 2-idempotent makes a duplicate apply a no-op.
	runtimeRegistrationTickTimeout = 30 * time.Second
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
	var lastWarn time.Time
	for {
		err := fn()
		if err == nil || !errors.Is(err, store.ErrWriterNotRegistered) {
			return err
		}
		// Re-warn periodically (every registrationGateWarnAfter), not just
		// once: a node stuck for minutes after the first WARN would
		// otherwise go silent in log pipelines (claude review on PR #847).
		// lastWarn is the zero time on the first pass, so the first WARN
		// fires once registrationGateWarnAfter has elapsed since start.
		if blocked := time.Since(start); blocked >= registrationGateWarnAfter && time.Since(lastWarn) >= registrationGateWarnAfter {
			slog.Warn("encryption: direct write blocked on writer registration; still retrying",
				slog.String("operation", what),
				slog.Duration("blocked_for", blocked))
			lastWarn = time.Now()
		}
		select {
		case <-ctx.Done():
			// Clean-shutdown path: runCtx cancelled during the bootstrap
			// retry window (operator graceful restart during boot). Log at
			// INFO so this reads as shutdown, not failure (claude review on
			// PR #847). The returned error preserves the context.Canceled
			// chain — consistent with every other ctx-aware startup step
			// (EnsureCatalogSnapshot included), which run() surfaces the
			// same way.
			slog.Info("encryption: writer-registration bootstrap retry cancelled by shutdown",
				slog.String("operation", what))
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
	if err := installProcessStartRaftRegistration(runCtx, coordinate, defaultGroup, w, raftID); err != nil {
		return nil, err
	}
	// Stage 7b/7b': arm the runtime watcher. Handles all three runtime
	// registration triggers — cutover (Phase-0 → EnableStorageEnvelope),
	// pre-bootstrap (no DEK → runtime Bootstrap), and rotation (RotateDEK
	// from a non-proposer's perspective). All three collapse to the same
	// in-scope propose under the cache.Registered() gate in
	// runtimeRegistrationInScope. Must run AFTER
	// installProcessStartRegistrationGate so the gate is in place before
	// any runtime trigger can fire.
	installRuntimeRegistrationWatcher(runCtx, eg, coordinate, defaultGroup, w, raftID)
	// Bootstrap + registration both run under runCtx so a shutdown
	// cancels the bounded retry rather than hanging.
	distCatalog, err := setupDistributionCatalog(runCtx, runtimes, engine)
	if err != nil {
		return nil, err
	}
	return distCatalog, nil
}

func installProcessStartRaftRegistration(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	raftID string,
) error {
	if w.cipher == nil || w.raftEnvelope == nil || defaultGroup == nil || w.activeRaftDEKID == 0 {
		return nil
	}
	if w.raftEnvelope.engineCutoverIndex() == inertRaftEnvelopeCutoverIndex {
		return nil
	}
	fullNodeID := etcdraftengine.DeriveNodeID(raftID)
	reg, err := store.WriterRegistryFor(defaultGroup.Store)
	if err != nil {
		return errors.Wrap(err, "raft writer registration: registry handle")
	}
	lastSeen, err := registryLastSeen(reg, w.activeRaftDEKID, fullNodeID)
	if err != nil {
		return err
	}
	if err := validateRaftRegistrationEpoch(w, lastSeen, fullNodeID); err != nil {
		return err
	}
	if w.raftEpoch == lastSeen {
		slog.Info("encryption: raft writer registration skipped (already current)",
			slog.Uint64("dek_id", uint64(w.activeRaftDEKID)),
			slog.Uint64("full_node_id", fullNodeID),
			slog.Uint64("local_epoch", uint64(w.raftEpoch)))
		return nil
	}

	entry := registrationEntry(w.activeRaftDEKID, fullNodeID, w.raftEpoch)
	req := registrationRequest(w.activeRaftDEKID, fullNodeID, w.raftEpoch)
	verifyRegistered := func() (bool, error) {
		return registrationCommittedAtEpoch(reg, w.activeRaftDEKID, fullNodeID, w.raftEpoch)
	}
	return commitRaftWriterRegistration(ctx, coordinate, defaultGroup.Proposer(), entry, req, verifyRegistered)
}

func validateRaftRegistrationEpoch(w encryptionWriteWiring, lastSeen uint16, fullNodeID uint64) error {
	if w.raftEpoch >= lastSeen {
		return nil
	}
	return errors.Errorf(
		"encryption: raft writer registration: local_epoch %d is behind registry last_seen %d for dek_id %d node %#x",
		w.raftEpoch, lastSeen, w.activeRaftDEKID, fullNodeID)
}

func commitRaftWriterRegistration(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultProposer raftengine.Proposer,
	entry []byte,
	req *pb.RegisterEncryptionWriterRequest,
	verifyRegistered func() (bool, error),
) error {
	registerCtx, cancel := context.WithTimeout(ctx, runtimeRegistrationTickTimeout)
	defer cancel()
	barrier := make(chan struct{})
	done := make(chan struct{})
	go func() {
		runWriterRegistration(registerCtx, coordinate, defaultProposer, nil, 0, entry, req, barrier, verifyRegistered)
		close(done)
	}()
	select {
	case <-barrier:
		<-done
		return nil
	case <-done:
		return errors.New("encryption: raft writer registration exited before commit")
	case <-registerCtx.Done():
		<-done
		return errors.Wrap(registerCtx.Err(), "encryption: raft writer registration did not commit before startup deadline")
	}
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
	// Build the registry handle once and reuse it for the initial read
	// and the verifyRegistered retry loop, rather than re-asserting +
	// allocating a handle per call (claude review on PR #847).
	reg, err := store.WriterRegistryFor(defaultGroup.Store)
	if err != nil {
		return nil, errors.Wrap(err, "writer registration: registry handle")
	}
	lastSeen, err := registryLastSeen(reg, activeDEK, fullNodeID)
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
	//
	// Strict (full_node_id, epoch)-exact match, not lastSeen>=epoch: a
	// row at lastSeen > w.epoch is not proof this load registered, and
	// short-circuiting would MarkRegistered while the nonce factory
	// stays pinned to the lower epoch (codex P1 round-1 on PR #853). The
	// §9.1 startup guard at line 254 already rejects this state for the
	// 7a propose branch, so the strict check is defense-in-depth that
	// stays consistent with the 7b runtime watcher's verifyRegistered.
	verifyRegistered := func() (bool, error) {
		return registrationCommittedAtEpoch(reg, activeDEK, fullNodeID, w.epoch)
	}
	eg.Go(func() error {
		runWriterRegistration(ctx, coordinate, defaultGroup.Proposer(), w.cache, activeDEK, entry, req, barrier, verifyRegistered)
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
// "propose" trigger per the 7a design §3.1). It builds a fresh registry
// handle per call; hot/looping callers should build the handle once and
// use registryLastSeen instead (claude review on PR #847).
func readWriterRegistryLastSeen(st store.MVCCStore, dekID uint32, fullNodeID uint64) (uint16, error) {
	reg, err := store.WriterRegistryFor(st)
	if err != nil {
		return 0, errors.Wrap(err, "writer registration: registry handle")
	}
	return registryLastSeen(reg, dekID, fullNodeID)
}

// registryLastSeen reads last_seen_local_epoch from an already-built
// registry handle (no per-call WriterRegistryFor type-assert + alloc),
// so the verifyRegistered retry loop can reuse one handle.
func registryLastSeen(reg encryption.WriterRegistryStore, dekID uint32, fullNodeID uint64) (uint16, error) {
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

// registrationCommittedAtEpoch returns true only when an existing
// registry row matches (full_node_id, dek_id) AND its
// last_seen_local_epoch equals epoch exactly. Used by the Stage 7a/7b
// verifyRegistered closures as a strict "did THIS load register?" check.
//
// A row with last_seen > epoch is NOT proof this load registered: it
// could be a stale-sidecar load that slipped past the §9.1 startup
// guard, or a concurrent same-raftID instance that bumped to a newer
// epoch. Returning true on >epoch would MarkRegistered without
// proposing, opening the storage gate while this load's nonce factory
// remains pinned to the lower epoch — a previous load's (node, epoch)
// nonces could then collide with this load's emissions.
//
// A FullNodeID mismatch indicates the §6.1 uint16-collision case the
// applier rejects via case-4 halt apply; this helper returns false
// (rather than true) so runWriterRegistration's propose path drives the
// halt, instead of fail-OPENing the gate via the short-circuit branch.
func registrationCommittedAtEpoch(reg encryption.WriterRegistryStore, dekID uint32, fullNodeID uint64, epoch uint16) (bool, error) {
	raw, ok, err := reg.GetRegistryRow(encryption.RegistryKey(dekID, encryption.NodeID16(fullNodeID)))
	if err != nil {
		return false, errors.Wrap(err, "writer registration: read registry row")
	}
	if !ok {
		return false, nil
	}
	val, err := encryption.DecodeRegistryValue(raw)
	if err != nil {
		return false, errors.Wrap(err, "writer registration: decode registry value")
	}
	if val.FullNodeID != fullNodeID {
		return false, nil
	}
	return val.LastSeenLocalEpoch == epoch, nil
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
	defaultProposer raftengine.Proposer,
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
		err := proposeWriterRegistration(ctx, coordinate, defaultProposer, connCache, entry, req)
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
// the default-group proposer when this node is the leader (DisableProposalForwarding
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
	defaultProposer raftengine.Proposer,
	connCache *kv.GRPCConnCache,
	entry []byte,
	req *pb.RegisterEncryptionWriterRequest,
) error {
	attemptCtx, cancel := context.WithTimeout(ctx, registrationAttemptTimeout)
	defer cancel()
	if coordinate.IsLeader() {
		// Writer registrations are control-plane entries that must
		// remain admissible across the §7.1 quiescence barrier
		// (Stage 6E-2d). Without the admin path, a new member
		// joining mid-barrier — or a leader restart that triggers
		// buildProcessStartRegistrationGate in the middle of a
		// cutover window — could not register its writer entry;
		// the barrier would reject the registration with
		// ErrEnvelopeCutoverInProgress and the local epoch would
		// never publish.
		//
		// ProposeAdmin is barrier-exempt only; the proposer itself
		// remains the wrap-aware ShardGroup.Proposer() in production,
		// so post-cutover local registrations still carry the §4.2
		// raft envelope required by the strict-`>` apply hook.
		if _, err := defaultProposer.ProposeAdmin(attemptCtx, entry); err != nil {
			return errors.Wrap(err, "writer registration: local propose-admin")
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

// installRuntimeRegistrationWatcher arms the Stage 7b runtime watcher
// from setupDistributionAndRegistration (AFTER installProcessStartRegistrationGate).
// It captures the boot-time active storage DEK and starts the watcher
// goroutine under runCtx. A no-op when encryption is not wired.
func installRuntimeRegistrationWatcher(
	ctx context.Context,
	eg *errgroup.Group,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	raftID string,
) {
	// Encryption not wired (no cipher → no nonce factory → no direct-path
	// gate to clear). The 7a-2 storage gate is also un-armed in this
	// case, so there is nothing the runtime watcher could usefully do.
	if w.cipher == nil || defaultGroup == nil {
		return
	}
	// Capture the boot-time active DEK once at watcher construction
	// (per §2.1 of the design): the scope check
	// `bootDEKID != 0 && activeDEK != bootDEKID` reads this captured
	// value on every tick, NOT a re-read of the current DEK. The cache
	// reports (0, false) on pre-bootstrap; that 0 is the load-bearing
	// signal for the bootDEKID==0 branch.
	bootDEKID, _ := w.cache.ActiveStorageKeyID()
	fullNodeID := etcdraftengine.DeriveNodeID(raftID)
	eg.Go(func() error {
		runRuntimeRegistrationWatcher(ctx, coordinate, defaultGroup, w, bootDEKID, fullNodeID)
		return nil
	})
}

// runRuntimeRegistrationWatcher is the Stage 7b/7b' polling loop. It
// checks the §4.1 trigger condition every runtimeRegistrationPollInterval
// and proposes a registration synchronously for any in-scope case: the
// cutover (activeDEK == bootDEKID), pre-bootstrap (bootDEKID == 0), and
// rotation (activeDEK != bootDEKID) branches all collapse to the same
// in-scope propose under the cache.Registered() gate (7b' §3.2).
//
// The propose runs in this goroutine — at most one in-flight at a time,
// no goroutine accumulation, no duplicate Raft proposals from the
// watcher. registrationAttemptTimeout (inside runWriterRegistration)
// bounds individual sub-attempts. On runCtx cancellation the loop exits
// without closing any barrier — fail-closed posture preserved.
func runRuntimeRegistrationWatcher(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	bootDEKID uint32,
	fullNodeID uint64,
) {
	ticker := time.NewTicker(runtimeRegistrationPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		runtimeRegistrationTick(ctx, coordinate, defaultGroup, w, bootDEKID, fullNodeID)
	}
}

// runtimeRegistrationInScope evaluates the §1 scope check: returns the
// active storage DEK and true when the watcher should propose
// (cutover, pre-bootstrap, OR — after Stage 7b' — rotation).
//
// Check ordering (load-bearing):
//  1. `cache.Registered()` short-circuits — internally tests
//     `activeStorageDEKID == registeredStorageDEKID` so it auto-
//     scopes to the currently-active DEK and is the **single
//     source of truth** for "this load has registered for the
//     currently-active DEK". The §4.1 case-2 idempotent path
//     makes re-proposing safe; a single-DEK StateCache means a
//     B→C→B oscillation re-proposes for B and the case-2 apply
//     is a no-op (7b' §3.2.2).
//  2. Envelope-inactive / not-bootstrapped guards.
//  3. Scope branches: pre-bootstrap (bootDEKID==0), cutover
//     (activeDEK==bootDEKID), or rotation (activeDEK!=bootDEKID).
//     The 7b' implementation replaces 7b's log-once-skip deferral
//     for the rotation branch with an in-scope propose now that
//     applyRotateDEK records the per-node local_epoch
//     (writeRotationSidecar §3.1).
//
// Note: an earlier 7b' draft added a per-process-load
// `lastRegisteredDEK` counter to gate the rotation branch against
// re-proposing once we'd already registered for the current active
// DEK. That introduced a permanent fail-closed in the race where the
// 7a process-start goroutine's MarkRegistered(oldDEK) lands AFTER
// 7b's MarkRegistered(newDEK) — overwriting `registeredStorageDEKID`
// to a stale value, making `Registered()` false, but the new gate
// would still see `activeDEK == lastRegisteredDEK` and skip the
// recovery propose (gemini CRITICAL on PR #864). Letting
// `cache.Registered()` be the sole gate eliminates that hole at the
// cost of one extra Raft round-trip per tick whenever the cache is
// out of sync, which the §4.1 case-2 idempotent apply makes free of
// side effects.
func runtimeRegistrationInScope(cache *encryption.StateCache) (activeDEK uint32, inScope bool) {
	if cache.Registered() {
		return 0, false
	}
	if !cache.StorageEnvelopeActive() {
		return 0, false
	}
	activeDEK, ok := cache.ActiveStorageKeyID()
	if !ok {
		return 0, false
	}
	// All three 7b/7b' sub-cases collapse to the same propose:
	// - cutover (activeDEK == bootDEKID, Keys[activeDEK].LocalEpoch
	//   == w.epoch per 7b §2.2),
	// - pre-bootstrap (bootDEKID == 0, w.epoch == 0),
	// - rotation (activeDEK != bootDEKID, Keys[activeDEK].LocalEpoch
	//   == w.epoch via the 7b' applyRotateDEK per-node sidecar write —
	//   see internal/encryption/applier.go writeRotationSidecar).
	// The §4.1 case-2 idempotent apply makes a re-propose during
	// oscillation a no-op (§3.2.2). The Registered() short-circuit
	// above is the sole gate against busy re-proposing once a
	// registration has succeeded — earlier drafts added a
	// per-process-load lastRegisteredDEK gate here but that
	// permanently fails closed in the 7a-stale-MarkRegistered race
	// (gemini CRITICAL on PR #864).
	return activeDEK, true
}

// runtimeRegistrationTick is a single iteration of the watcher loop,
// extracted so the loop body stays under the cyclop budget and the
// scope-check + propose can be unit-tested independently of the ticker.
// Returns to the caller after either a no-op skip (when
// cache.Registered() is already true for the active DEK, or scope
// conditions reject the tick) or a complete synchronous registration
// attempt via runWriterRegistration. The Registered() short-circuit at
// runtimeRegistrationInScope step 1 is the sole gate against re-proposing
// once a registration has succeeded (gemini CRITICAL on PR #864).
func runtimeRegistrationTick(
	ctx context.Context,
	coordinate *kv.ShardedCoordinator,
	defaultGroup *kv.ShardGroup,
	w encryptionWriteWiring,
	bootDEKID uint32,
	fullNodeID uint64,
) {
	activeDEK, inScope := runtimeRegistrationInScope(w.cache)
	if !inScope {
		return
	}
	// In scope: cutover (activeDEK == bootDEKID), pre-bootstrap
	// (bootDEKID == 0), or 7b' rotation (activeDEK != bootDEKID).
	// All three satisfy sidecar.Keys[activeDEK].LocalEpoch == w.epoch
	// (cutover/pre-bootstrap per 7b §2.2; rotation per 7b' §3.1's
	// writeRotationSidecar local_epoch wiring). cache.Registered()
	// inside runtimeRegistrationInScope gates against re-proposing
	// after a successful registration.
	slog.Info("encryption: 7b runtime watcher proposing registration",
		slog.Uint64("active_dek_id", uint64(activeDEK)),
		slog.Uint64("full_node_id", fullNodeID),
		slog.Uint64("local_epoch", uint64(w.epoch)),
		slog.Uint64("boot_dek_id", uint64(bootDEKID)))
	reg, err := store.WriterRegistryFor(defaultGroup.Store)
	if err != nil {
		slog.Warn("encryption: 7b runtime watcher failed to obtain writer registry handle; will retry",
			slog.String("error", err.Error()))
		return
	}
	// Per design §5 item 5: explicit ok=true gating on GetRegistryRow,
	// NOT the existing registryLastSeen helper (which swallows ok=false
	// as 0 and would short-circuit registration when w.epoch==0 without
	// proposing a durable row, opening the gate fail-OPEN).
	//
	// Exact (full_node_id, epoch) match, not lastSeen>=epoch (codex P1
	// round-1 on PR #853): a row at lastSeen > w.epoch is not proof
	// THIS process load registered. Possible causes — a stale-sidecar
	// load that slipped past the §9.1 startup guard, or a concurrent
	// same-raftID instance that bumped to a newer epoch and registered
	// there. Treating that as "already registered" would MarkRegistered
	// without a propose, opening the storage gate while this load's
	// nonce factory still emits at the lower epoch — and the previous
	// load's (node, epoch=this.w.epoch, *) nonces would collide with
	// this load's emissions. The strict equality forces a propose on
	// >epoch, which Raft applies via §4.1 case 3 (rollback halt) —
	// loud, fail-closed, no nonce reuse.
	verifyRegistered := func() (bool, error) {
		return registrationCommittedAtEpoch(reg, activeDEK, fullNodeID, w.epoch)
	}
	// Synchronous propose in this goroutine. The barrier passed to
	// runWriterRegistration is solely to satisfy its signature; this
	// watcher does NOT select on it. Only the MarkRegistered side
	// effect (via releaseBarrier on success) matters here.
	//
	// Per-tick bounded timeout (gemini HIGH on PR #853): if the inner
	// retry loop is stuck (no leader, partition), this returns control
	// to the watcher's polling loop within runtimeRegistrationTickTimeout
	// so the next tick can re-poll, re-evaluate the active DEK, and log
	// a deferred-rotation skip if rotation happened mid-block. The
	// re-propose on the next tick is safe via §4.1 case-2-idempotent.
	tickCtx, cancel := context.WithTimeout(ctx, runtimeRegistrationTickTimeout)
	defer cancel()
	barrier := make(chan struct{})
	entry := registrationEntry(activeDEK, fullNodeID, w.epoch)
	req := registrationRequest(activeDEK, fullNodeID, w.epoch)
	runWriterRegistration(tickCtx, coordinate, defaultGroup.Proposer(), w.cache, activeDEK,
		entry, req, barrier, verifyRegistered)
	// No success-bookkeeping needed: cache.Registered() flips true via
	// runWriterRegistration's MarkRegistered side effect on the success
	// path, and the next tick's runtimeRegistrationInScope step (1)
	// short-circuit reads that state directly. Any race that leaves the
	// cache out of sync (e.g. the 7a process-start goroutine's
	// MarkRegistered landing after this watcher's MarkRegistered for a
	// newer DEK) is recovered by the next tick re-proposing, with the
	// §4.1 case-2 idempotent apply making the re-propose a no-op.
}

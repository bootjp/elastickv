package kv

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type ShardGroup struct {
	Engine raftengine.Engine
	Store  store.MVCCStore
	Txn    Transactional
	lease  leaseState
	// lp caches the Engine's optional LeaseProvider capability so the
	// groupLeaseRead / maybeRefresh hot paths test a single field for
	// nil instead of performing an interface type assertion per call.
	// NewShardedCoordinator resolves it once for every group it owns; it
	// is nil when the Engine does not implement raftengine.LeaseProvider.
	// Engine is not reassigned after the coordinator is constructed, so
	// the cached value stays valid.
	lp raftengine.LeaseProvider
	// raftPayloadWrap is the Stage 6E-2c hot-swap point for the raft
	// envelope wrap closure. A nil load means the wrap is inactive
	// and proposals pass through cleartext (the Stage 3 default).
	// A non-nil load applies the closure to every payload before it
	// reaches the engine — Propose and ProposeAdmin alike, by
	// design (see kv/raft_payload_wrapper.go for the rationale).
	//
	// The cell is read on every proposal via dynamicWrappedProposer,
	// installed in the TransactionManager's proposer chain AND on
	// the cached proposer below when the ShardGroup is constructed
	// via NewLeaderProxyForShardGroup. SetRaftPayloadWrap publishes
	// a fresh closure atomically so EnableRaftEnvelope (Stage
	// 6E-2d) can activate the wrap the instant the cutover entry
	// commits, without rebuilding the TransactionManager or
	// stalling in-flight proposals.
	raftPayloadWrap atomic.Pointer[RaftPayloadWrapper]
	// proposer is the wrap-aware proposer chain for this shard
	// group. Set by NewLeaderProxyForShardGroup so direct shard
	// proposals (HLC lease renewal — see RunHLCLeaseRenewal —
	// and any future direct-engine.Propose call site) route
	// through the same dynamicWrappedProposer the
	// TransactionManager uses. The Proposer() accessor falls back
	// to Engine when this is nil (the legacy / test-fixture path
	// that constructs ShardGroup via struct literal without
	// NewLeaderProxyForShardGroup), so the no-wrap default keeps
	// working.
	proposer raftengine.Proposer
}

// Proposer returns the wrap-aware proposer chain installed by
// NewLeaderProxyForShardGroup, or the raw Engine when the
// constructor was bypassed (legacy / test fixtures that build
// ShardGroup via struct literal). Direct shard proposals
// (HLC lease renewal, future cipher rotations, etc.) MUST go
// through this getter rather than g.Engine.Propose so the
// Stage 6E-2c dynamic wrap path applies — a direct g.Engine
// Propose call would bypass the wrap pointer and let post-cutover
// writes land cleartext above the raft-envelope cutover, halting
// the apply loop on §6.3 strict-> unwrap (codex P2 round-1).
func (g *ShardGroup) Proposer() raftengine.Proposer {
	if g.proposer != nil {
		return g.proposer
	}
	return g.Engine
}

// SetRaftPayloadWrap publishes wrap as the active raft envelope
// closure for this shard group. Passing nil clears the wrap (the
// proposer reverts to cleartext pass-through). Safe to call from
// any goroutine; the next Propose / ProposeAdmin observes the new
// state via the proposer's atomic.Pointer.Load.
//
// This is the sole supported way to install or rotate the wrap
// closure on a running coordinator. Stage 6E-2d's
// EnableRaftEnvelope handler will call this on every leader when
// the cutover entry commits.
func (g *ShardGroup) SetRaftPayloadWrap(wrap RaftPayloadWrapper) {
	if wrap == nil {
		g.raftPayloadWrap.Store(nil)
		return
	}
	g.raftPayloadWrap.Store(&wrap)
}

// RaftPayloadWrap returns the currently-installed wrap closure, or
// nil if the wrap is inactive. Primarily intended for tests and
// diagnostics; production proposers consult the underlying
// atomic.Pointer directly (see dynamicWrappedProposer).
func (g *ShardGroup) RaftPayloadWrap() RaftPayloadWrapper {
	if p := g.raftPayloadWrap.Load(); p != nil {
		return *p
	}
	return nil
}

// cutoverBarrierProposer is the optional interface a Proposer can
// satisfy to participate in the §7.1 quiescence barrier. Production
// proposers built via NewLeaderProxyForShardGroup (i.e.,
// *dynamicWrappedProposer) implement it; test fixtures that wire a
// bare engine into ShardGroup.Engine do not. The ShardGroup
// forwarders below type-assert on this interface so the barrier
// control degrades gracefully (immediate-success drain, no-op
// Begin/End) when the proposer can't participate.
type cutoverBarrierProposer interface {
	raftengine.Proposer
	BeginCutoverBarrier() <-chan struct{}
	WaitInflightDrained(ctx context.Context) error
	EndCutoverBarrier()
}

// BeginCutoverBarrier opens the §7.1 step-1 quiescence barrier on
// this shard group's proposer chain. Returns a channel that closes
// when all in-flight user Propose calls drain; the typical caller
// uses WaitInflightDrained which composes context cancellation.
//
// Forwards to *dynamicWrappedProposer when present. When the
// proposer is the bare engine (raw Engine fallback in test
// fixtures), returns a pre-closed channel so callers that don't
// distinguish barrier-capable from -incapable proposers can drive
// the same state-machine shape against either.
//
// 6E-2d wiring: every leader's EnableRaftEnvelope handler calls this
// on each ShardGroup that participates in the cutover before
// proposing the cutover entry. After return, the proposer's
// dynamicWrappedProposer.Propose rejects fresh user calls with
// raftengine.ErrEnvelopeCutoverInProgress.
func (g *ShardGroup) BeginCutoverBarrier() <-chan struct{} {
	if cbp, ok := g.Proposer().(cutoverBarrierProposer); ok {
		return cbp.BeginCutoverBarrier()
	}
	ch := make(chan struct{})
	close(ch)
	return ch
}

// WaitInflightDrained blocks until the in-flight Propose counter
// drops to 0 after BeginCutoverBarrier ran on this ShardGroup, or
// ctx fires. Returns nil on drain or when the proposer is barrier-
// incapable (degraded fast-path so test fixtures don't deadlock
// the handler). Wraps ctx.Err() on cancellation.
func (g *ShardGroup) WaitInflightDrained(ctx context.Context) error {
	if cbp, ok := g.Proposer().(cutoverBarrierProposer); ok {
		// dynamicWrappedProposer.WaitInflightDrained returns raw
		// ctx.Err() so this single wrap is the canonical operator-
		// log entry (claude r2 finding B: avoid the redundant
		// "kv: ... kv: ..." chain a doubled wrap would produce).
		if err := cbp.WaitInflightDrained(ctx); err != nil {
			return errors.Wrap(err, "kv: shard group wait inflight drained")
		}
		return nil
	}
	return nil
}

// EndCutoverBarrier closes the §7.1 step-6 barrier on this shard
// group's proposer chain. Idempotent against barrier-incapable
// proposers (no-op). Callers MUST pair each BeginCutoverBarrier
// with exactly one EndCutoverBarrier (the EnableRaftEnvelope
// handler uses defer).
func (g *ShardGroup) EndCutoverBarrier() {
	if cbp, ok := g.Proposer().(cutoverBarrierProposer); ok {
		cbp.EndCutoverBarrier()
	}
}

// NewLeaderProxyForShardGroup wires a LeaderProxy whose proposer
// consults g.raftPayloadWrap on every call, so SetRaftPayloadWrap
// becomes the hot-swap surface for the raft envelope cutover.
//
// Use this in preference to NewLeaderProxyWithEngine(g.Engine, ...)
// for any ShardGroup that participates in the encryption cutover
// pipeline — without the dynamic wrap, post-cutover writes would
// land cleartext at index > cutoverIndex and halt the apply loop on
// strict-> unwrap. The non-wrap-aware constructor remains as a
// convenience for shard groups that opt out of encryption (test
// fixtures, transient groups).
//
// Contract: g MUST be non-nil. The constructor takes the address
// of g.raftPayloadWrap so a nil receiver is an immediate nil deref
// — caller bug, not silently swallowed. Returning nil here would
// only defer the panic to the first sg.Txn.Commit call site (see
// main.go's buildShardGroups), with worse diagnostics; CLAUDE.md's
// "don't validate for scenarios that can't happen at internal
// boundaries" applies.
func NewLeaderProxyForShardGroup(g *ShardGroup, opts ...TransactionOption) *LeaderProxy {
	// Build a single dynamicWrappedProposer for this shard group and
	// cache it on g.proposer so direct shard proposals (HLC lease
	// renewal in RunHLCLeaseRenewal, future cipher rotations) and
	// the TransactionManager.Commit / Abort path share the same
	// wrap pointer and the same Propose / ProposeAdmin call shape.
	// A direct g.Engine.Propose call would bypass the wrap pointer
	// (codex P2 round-1); routing both paths through the same
	// proposer closes that hole.
	g.proposer = newDynamicWrappedProposer(g.Engine, &g.raftPayloadWrap)
	return &LeaderProxy{
		engine: g.Engine,
		tm:     NewTransactionWithProposer(g.proposer, opts...),
	}
}

// leaseRefreshingTxn wraps a Transactional so every Commit / Abort that
// produced a real Raft commit extends its shard's lease. Mirrors
// Coordinate.Dispatch's lease hook for the per-shard case.
//
// Both TransactionManager.Commit and .Abort can return success WITHOUT
// going through Raft -- Commit short-circuits on empty input, Abort
// short-circuits when every request's abortRequestFor is nil (nothing
// to release). Refreshing the lease in those cases would be unsound:
// no quorum confirmation happened. We gate the refresh on
// resp.CommitIndex > 0, which the underlying manager sets to the
// last applied index only when at least one proposal went through.
type leaseRefreshingTxn struct {
	inner Transactional
	g     *ShardGroup
	// coord back-references the owning coordinator so Commit can
	// consult the Stage 7a registration barrier. Every self-originated
	// write — client writes via router.Commit AND internal
	// lock-resolution via ShardStore's direct g.Txn.Commit — funnels
	// through this wrapper, so gating here (in addition to the
	// Dispatch-level gate for client groups) closes the lock-resolution
	// path codex P1 flagged. nil in tests that construct the wrapper
	// directly; awaitRegistrationBarrier is nil-receiver-safe.
	coord *ShardedCoordinator
}

func (t *leaseRefreshingTxn) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	// Stage 7a §4.1: every Commit is a self-originated write, so gate it
	// on this node's writer registration. This is the universal
	// chokepoint (router.Commit and ShardStore lock-resolution both land
	// here), closing the lock-resolution path the Dispatch-level gate
	// alone misses (codex P1 on PR #839).
	if err := t.coord.awaitRegistrationBarrier(ctx); err != nil {
		return nil, err
	}
	if err := t.stampRawTimestampsForLocalCommit(ctx, reqs); err != nil {
		return nil, err
	}
	start := monoclock.Now()
	expectedGen := t.g.lease.generation()
	resp, err := t.inner.Commit(ctx, reqs)
	if err != nil {
		// Only invalidate on errors that actually signal a leadership
		// change. Write-conflicts, validation errors, and deadline
		// exceeded on non-ReadIndex paths do NOT imply the leader is
		// gone; invalidating the lease for them forces every read
		// into the slow LinearizableRead path and defeats the whole
		// point of the lease. The engine's own leader-loss callback
		// already handles true leadership loss, plus
		// Coordinate.LeaseRead guards the fast path on
		// engine.State() == StateLeader.
		if isLeadershipLossError(err) {
			t.g.lease.invalidate()
		}
		return resp, errors.WithStack(err)
	}
	t.maybeRefresh(resp, start, expectedGen)
	return resp, nil
}

func (t *leaseRefreshingTxn) stampRawTimestampsForLocalCommit(ctx context.Context, reqs []*pb.Request) error {
	if t == nil || t.coord == nil || !hasUnstampedRawRequests(reqs) {
		return nil
	}
	if t.g != nil && t.g.Engine != nil {
		if !isLeaderEngine(t.g.Engine) {
			return nil
		}
		if !canStampRawTimestampsOnEngine(ctx, t.g.Engine) {
			return nil
		}
	}
	return t.coord.stampRawRequestTimestamps(ctx, reqs)
}

func canStampRawTimestampsOnEngine(ctx context.Context, engine raftengine.Engine) bool {
	return verifyLeaderEngineCtx(ctx, engine) == nil
}

func hasUnstampedRawRequests(reqs []*pb.Request) bool {
	for _, r := range reqs {
		if r != nil && !r.IsTxn && r.Ts == 0 {
			return true
		}
	}
	return false
}

func (t *leaseRefreshingTxn) Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	start := monoclock.Now()
	expectedGen := t.g.lease.generation()
	resp, err := t.inner.Abort(ctx, reqs)
	if err != nil {
		if isLeadershipLossError(err) {
			t.g.lease.invalidate()
		}
		return resp, errors.WithStack(err)
	}
	t.maybeRefresh(resp, start, expectedGen)
	return resp, nil
}

// maybeRefresh extends the per-shard lease only when the operation
// actually produced a Raft commit. expectedGen is sampled BEFORE the
// underlying Commit/Abort so an invalidation that fires during that
// call observes a generation mismatch inside extend and the refresh
// is rejected. See the struct doc comment for why.
func (t *leaseRefreshingTxn) maybeRefresh(resp *TransactionResponse, start monoclock.Instant, expectedGen uint64) {
	if resp == nil || resp.CommitIndex == 0 {
		return
	}
	if t.g.lp == nil {
		return
	}
	t.g.lease.extend(start.Add(t.g.lp.LeaseDuration()), expectedGen)
}

// Close forwards to the wrapped Transactional if it implements
// io.Closer. ShardStore.closeGroup relies on the type assertion
// `g.Txn.(io.Closer)` to release per-shard resources (e.g. the gRPC
// connection cached by LeaderProxy). Without this pass-through, the
// wrapping would silently swallow the Closer capability and leak
// connections / goroutines at shutdown.
func (t *leaseRefreshingTxn) Close() error {
	closer, ok := t.inner.(io.Closer)
	if !ok {
		return nil
	}
	if err := closer.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

const (
	txnPhaseCount = 2

	txnSecondaryCommitRetryAttempts = 3
	txnSecondaryCommitRetryBackoff  = 20 * time.Millisecond

	// composed1RetryAttempts bounds how many times Dispatch re-issues a
	// txn after the FSM's M3 verifyComposed1 gate returned
	// ErrComposed1Violation / ErrComposed1VersionGCd.  Per design doc
	// §M4: "retries it once" — so the total attempt count is 1 (initial)
	// + composed1RetryAttempts = 2.  Beyond one retry the sentinel
	// surfaces unchanged so the client (or a wrapping retry harness in
	// the adapter) sees the failure rather than the coordinator spinning
	// on a persistent route shift.
	composed1RetryAttempts = 1
)

// ShardedCoordinator routes operations to shard-specific raft groups.
// It issues timestamps via a shared HLC and uses ShardRouter to dispatch.
type ShardedCoordinator struct {
	engine       *distribution.Engine
	router       *ShardRouter
	groups       map[uint64]*ShardGroup
	defaultGroup uint64
	clock        *HLC
	// tsAllocator is an optional TSO-backed timestamp source used for every
	// coordinator-owned persistence timestamp. Nil preserves the legacy shared
	// HLC path.
	tsAllocator TimestampAllocator
	store       store.MVCCStore
	log         *slog.Logger
	// deregisterLeaseCbs removes the per-shard leader-loss callbacks
	// registered at construction. See Coordinate.Close for the
	// rationale.
	deregisterLeaseCbs []func()
	// leaseObserver records lease-read hit/miss for every shard the
	// coordinator owns. Nil-safe; see Coordinate.leaseObserver.
	leaseObserver LeaseReadObserver
	// sampler counts requests per RouteID for the key visualizer
	// heatmap. Nil-safe at the call site; the implementation
	// (keyviz.MemSampler) also tolerates a typed-nil receiver, so a
	// disabled keyviz wires through to a no-op without branching on
	// the hot path.
	sampler keyviz.Sampler
	// keyvizLabelsEnabled controls whether adapter-supplied labels are
	// allowed to reach the sampler. When false, observations keep the
	// legacy unlabeled route-only shape.
	keyvizLabelsEnabled bool
	// hlcRenewalBlocked lets startup code temporarily suppress background HLC
	// lease proposals while another startup-only Raft mutation must run first.
	hlcRenewalBlocked func() bool
	// hlcRenewalInFlight prevents a slow or quorum-stalled group from stacking
	// another background HLC lease proposal for the same group on the next tick.
	hlcRenewalMu       sync.Mutex
	hlcRenewalInFlight map[uint64]struct{}
	// registrationGate is the Stage 7a §4.1 first-write barrier: when
	// set, self-originated mutating writes that would land as §4.1
	// storage envelopes block until this node's writer registration
	// commits. nil when encryption is off / no registration is pending
	// (the common case — ungated). See RegistrationGate.
	registrationGate *RegistrationGate
}

// RegistrationGate carries the Stage 7a §4.1 registration-before-
// first-write barrier into the coordinator. main.go owns the
// encryption StateCache and the registration goroutine and supplies
// this; kv stays decoupled from internal/encryption by taking a plain
// channel + predicate closures rather than the StateCache type.
//
// Barrier three-state (per the 7a design §3.2):
//   - nil           → no registration pending (Phase 0 / skip / off) → ungated
//   - open (non-nil, not closed) → registration in flight → mutating
//     encrypted writes block on it
//   - closed        → registration committed → ungated (fast path)
type RegistrationGate struct {
	// Barrier is the open-or-closed channel described above. A nil
	// Barrier (the zero value, or an explicitly nil field) means
	// "never armed" → ungated; awaitRegistration checks for nil first
	// so a nil channel is never received on (which would block forever).
	Barrier <-chan struct{}
	// StorageEnvelopeActive and ActiveStorageKeyID read the process-wide
	// encryption StateCache. A write is gated only when the §7.1 cutover
	// has fired AND a storage DEK is active — i.e. the write would land
	// encrypted and thus emit a nonce under this node's identity.
	StorageEnvelopeActive func() bool
	ActiveStorageKeyID    func() (uint32, bool)
}

// WithRegistrationGate wires the Stage 7a first-write barrier onto the
// coordinator. Applied after construction for the same reason as the
// other With* options. A nil gate (or a gate with a nil Barrier)
// leaves every write ungated — the encryption-off / no-pending-
// registration posture.
func (c *ShardedCoordinator) WithRegistrationGate(g *RegistrationGate) *ShardedCoordinator {
	c.registrationGate = g
	return c
}

// WithTSOAllocator routes sharded-coordinator timestamp issuance through a
// TSO-compatible allocator. Existing deployments keep the legacy shared-HLC
// path unless this is wired explicitly.
func (c *ShardedCoordinator) WithTSOAllocator(alloc TimestampAllocator) *ShardedCoordinator {
	c.tsAllocator = alloc
	return c
}

// awaitRegistration implements the §4.1 first-write barrier (7a §3.2).
// It returns nil (ungated) unless a registration is genuinely pending
// AND this request is a mutating write that would land encrypted, in
// which case it blocks until the registration commits or ctx ends.
// Fail-closed: a write that cannot register within its ctx returns the
// ctx error rather than proceeding to emit an unregistered nonce.
func (c *ShardedCoordinator) awaitRegistration(ctx context.Context, reqs *OperationGroup[OP]) error {
	g := c.registrationGate
	if g == nil || g.Barrier == nil {
		return nil
	}
	select {
	case <-g.Barrier:
		return nil // registration committed — fast path
	default:
	}
	// Registration is in flight. Gate only mutating writes that would
	// land encrypted; reads never reach Dispatch, and cleartext writes
	// (pre-cutover / no active DEK) emit no nonce.
	if !c.writeWouldEncrypt(reqs) {
		return nil
	}
	select {
	case <-g.Barrier:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "encryption: write blocked on pending §4.1 writer registration")
	}
}

// writeWouldEncrypt reports whether reqs is a mutating write that would
// land as a §4.1 storage envelope under the current gate state — i.e.
// the cutover has fired AND a storage DEK is active. Only such writes
// are gated by awaitRegistration; reads and cleartext writes are not.
func (c *ShardedCoordinator) writeWouldEncrypt(reqs *OperationGroup[OP]) bool {
	if c.registrationGate == nil {
		return false
	}
	return hasMutatingElems(reqs) && c.encryptionWriteActive()
}

// awaitRegistrationBarrier is the write-layer (Transactional.Commit)
// half of the §4.1 first-write gate. Unlike awaitRegistration it takes
// no OperationGroup: every Commit is a write, so the gate fires
// whenever a registration is pending AND the write would land
// encrypted (cutover fired + active DEK). nil-receiver-safe (tests
// construct leaseRefreshingTxn without a coordinator).
func (c *ShardedCoordinator) awaitRegistrationBarrier(ctx context.Context) error {
	if c == nil {
		return nil
	}
	g := c.registrationGate
	if g == nil || g.Barrier == nil {
		return nil
	}
	select {
	case <-g.Barrier:
		return nil // committed — fast path
	default:
	}
	if !c.encryptionWriteActive() {
		return nil
	}
	select {
	case <-g.Barrier:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "encryption: commit blocked on pending §4.1 writer registration")
	}
}

// encryptionWriteActive reports whether a write would currently land as
// a §4.1 storage envelope: the cutover has fired AND a storage DEK is
// active. Shared by writeWouldEncrypt (Dispatch gate) and
// awaitRegistrationBarrier (Commit gate). Assumes c.registrationGate
// is non-nil (both callers check that first).
func (c *ShardedCoordinator) encryptionWriteActive() bool {
	g := c.registrationGate
	if g.StorageEnvelopeActive == nil || !g.StorageEnvelopeActive() {
		return false
	}
	if g.ActiveStorageKeyID == nil {
		return false
	}
	_, ok := g.ActiveStorageKeyID()
	return ok
}

// hasMutatingElems reports whether the group carries at least one
// mutating operation. Every OP in OperationGroup (Put / Del /
// DelPrefix) mutates — reads use separate Coordinator methods that
// never reach Dispatch — so a read-only transaction reaches here with
// only ReadKeys set and an empty Elems slice, and must stay ungated.
func hasMutatingElems(reqs *OperationGroup[OP]) bool {
	if reqs == nil {
		return false
	}
	// The per-Elem nil check is defensive: validateOperationGroup runs
	// before this and rejects groups with nil elements, so a non-empty
	// Elems is effectively all-non-nil in practice. The guard keeps
	// hasMutatingElems correct even if a future caller bypasses that
	// validation.
	for _, e := range reqs.Elems {
		if e != nil {
			return true
		}
	}
	return false
}

// WithLeaseReadObserver wires a LeaseReadObserver onto a
// ShardedCoordinator. Applied after construction because the
// NewShardedCoordinator signature is already heavily overloaded;
// see Coordinate.WithLeaseReadObserver for the equivalent option on
// the single-group coordinator, including the typed-nil guard
// rationale.
func (c *ShardedCoordinator) WithLeaseReadObserver(observer LeaseReadObserver) *ShardedCoordinator {
	c.leaseObserver = normalizeLeaseObserver(observer)
	return c
}

// SetHLCLeaseRenewalBlocker installs a predicate that suppresses background
// HLC lease-renewal proposals while it returns true.
func (c *ShardedCoordinator) SetHLCLeaseRenewalBlocker(blocked func() bool) {
	if c == nil {
		return
	}
	c.hlcRenewalBlocked = blocked
}

// WithSampler wires a keyviz.Sampler onto a ShardedCoordinator. The
// coordinator calls sampler.Observe at dispatch entry — once per
// resolved (RouteID, mutation key) pair — to feed the key visualizer
// heatmap (design doc §5.1). Applied after construction for the same
// reason as WithLeaseReadObserver: NewShardedCoordinator is already
// heavily overloaded.
//
// Passing a nil interface value is supported and disables sampling
// (the call site guards against it). Passing a typed-nil
// *keyviz.MemSampler also works because Observe is nil-safe by
// contract.
func (c *ShardedCoordinator) WithSampler(s keyviz.Sampler) *ShardedCoordinator {
	c.sampler = s
	return c
}

func (c *ShardedCoordinator) WithKeyVizLabelsEnabled(enabled bool) *ShardedCoordinator {
	c.keyvizLabelsEnabled = enabled
	return c
}

// WithPartitionResolver wires a PartitionResolver onto the
// coordinator's underlying ShardRouter. The resolver runs before
// the byte-range engine on every dispatch, so partition-keyspace
// schemes (e.g. SQS HT-FIFO) can override the default shard layout
// without breaking the engine's non-overlapping-cover invariant.
//
// Applied after construction for the same reason as the other
// With* options on this type — NewShardedCoordinator is already
// heavily overloaded. Passing a nil resolver clears any previously-
// installed resolver.
func (c *ShardedCoordinator) WithPartitionResolver(r PartitionResolver) *ShardedCoordinator {
	c.router.WithPartitionResolver(r)
	return c
}

// NewShardedCoordinator builds a coordinator for the provided shard groups.
// The defaultGroup is used for non-keyed leader checks.
func NewShardedCoordinator(engine *distribution.Engine, groups map[uint64]*ShardGroup, defaultGroup uint64, clock *HLC, st store.MVCCStore) *ShardedCoordinator {
	router := NewShardRouter(engine)
	// Construct the coordinator before wrapping the per-shard
	// Transactionals so each leaseRefreshingTxn can back-reference it
	// for the §4.1 registration barrier (the gate is installed later
	// via WithRegistrationGate and read at Commit time).
	c := &ShardedCoordinator{
		engine:       engine,
		router:       router,
		groups:       groups,
		defaultGroup: defaultGroup,
		clock:        clock,
		store:        st,
		log:          slog.Default(),
	}
	var deregisters []func()
	for gid, g := range groups {
		// Wrap Txn so every successful Commit/Abort refreshes the
		// per-shard lease. Leave nil transactions unchanged, and skip
		// if already wrapped so repeat calls don't stack wrappers.
		if g.Txn != nil {
			if existing, already := g.Txn.(*leaseRefreshingTxn); already {
				// Re-binding over the same ShardGroup (this function
				// supports repeat construction by not stacking
				// wrappers): point the existing wrapper at the NEW
				// coordinator so Commit consults the freshly-installed
				// registration gate, not a stale one (codex P2).
				existing.coord = c
			} else {
				g.Txn = &leaseRefreshingTxn{inner: g.Txn, g: g, coord: c}
			}
		}
		router.Register(gid, g.Txn, g.Store)
		// Resolve the optional LeaseProvider capability once so
		// groupLeaseRead / maybeRefresh test g.lp for nil instead of
		// re-asserting the interface per call.
		// Per-shard leader-loss hook: when this group's engine notices
		// a state transition out of leader, drop the lease so the next
		// LeaseReadForKey on that shard takes the slow path.
		if lp, ok := g.Engine.(raftengine.LeaseProvider); ok {
			g.lp = lp
			group := g
			deregisters = append(deregisters, lp.RegisterLeaderLossCallback(func() {
				group.lease.invalidate()
				c.invalidateTimestampWindow()
			}))
		}
	}
	c.deregisterLeaseCbs = deregisters
	return c
}

// Close releases per-shard engine-side registrations. Idempotent.
func (c *ShardedCoordinator) Close() error {
	if c == nil {
		return nil
	}
	cbs := c.deregisterLeaseCbs
	c.deregisterLeaseCbs = nil
	for _, fn := range cbs {
		fn()
	}
	return nil
}

func (c *ShardedCoordinator) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := validateOperationGroup(reqs); err != nil {
		return nil, err
	}

	// Stage 7a §4.1: block self-originated mutating encrypted writes
	// until this node's writer registration commits. Ungated fast path
	// (no gate / committed / cleartext) returns immediately.
	if err := c.awaitRegistration(ctx, reqs); err != nil {
		return nil, err
	}

	if resp, handled, err := c.dispatchBeforeShardRouting(ctx, reqs); handled {
		return resp, err
	}

	// Capture whether the caller supplied a non-zero StartTS BEFORE
	// the coordinator-allocates-on-zero branch below mutates the
	// field.  A caller-supplied StartTS names a specific snapshot
	// the caller already read against (e.g. adapter/s3.go:573
	// reads bucket metadata at readTS and dispatches with
	// StartTS=readTS; adapter/sqs_messages.go and adapter/dynamodb.go
	// follow the same readTS-then-mutate pattern across dozens of
	// sites).  The pre-Dispatch read is invisible to the
	// coordinator — it is NOT surfaced as a ReadKeys entry — so
	// the only signal that the snapshot is owned by the caller is
	// reqs.StartTS being non-zero on entry.  The M4 retry path
	// uses this to gate the StartTS bump: bumping a caller-owned
	// timestamp would let any commit landed in (originalStartTS,
	// newStartTS) bypass the FSM's latest>startTS write-conflict
	// check (codex P1 on 57da8886, PR #900).
	callerSuppliedStartTS := reqs.IsTxn && reqs.StartTS != 0

	if reqs.IsTxn && reqs.StartTS == 0 {
		startTS, err := c.nextStartTS(ctx, reqs.Elems)
		if err != nil {
			return nil, err
		}
		reqs.StartTS = startTS
		// When the coordinator assigns StartTS, also clear any caller-provided
		// CommitTS so dispatchTxn generates both timestamps consistently.
		// A caller-supplied CommitTS without a matching StartTS could produce
		// CommitTS <= StartTS (an invalid transaction).
		reqs.CommitTS = 0
	}

	if reqs.IsTxn {
		return c.dispatchTxnWithComposed1Retry(ctx, reqs, callerSuppliedStartTS)
	}

	return c.dispatchNonTxn(ctx, reqs)
}

func (c *ShardedCoordinator) dispatchBeforeShardRouting(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, bool, error) {
	// DEL_PREFIX cannot be routed to a single shard because the prefix may
	// span multiple shards (or be nil, meaning "all keys"). Broadcast the
	// operation to every shard group so each FSM scans locally.
	if hasDelPrefixElem(reqs.Elems) {
		resp, err := c.dispatchDelPrefixBroadcast(ctx, reqs.IsTxn, reqs.Elems)
		return resp, true, err
	}
	if err := c.rejectWriteFencedPointElems(reqs.Elems); err != nil {
		return nil, true, err
	}
	return nil, false, nil
}

// dispatchTxnWithComposed1Retry runs the M4 Composed-1 retry loop
// (design doc
// docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md
// §M4).  Pins reqs.ObservedRouteVersion to the engine's current
// catalog version on the FIRST attempt when the caller left it at the
// zero sentinel — every txn that flows through ShardedCoordinator
// gets gate-eligible by default so the M3 verifyComposed1 check has
// teeth without each adapter rediscovering the contract.  On
// ErrComposed1Violation or ErrComposed1VersionGCd, re-reads the
// catalog version, re-stamps observed+commit on the OperationGroup,
// and re-issues dispatchTxn ONCE.  Beyond one retry the sentinel
// surfaces unchanged.
//
// The retry is bounded at composed1RetryAttempts = 1 because both
// sentinels are "route shifted, retry on the fresh catalog" signals
// — a persistent failure across two attempts means the catalog is
// churning faster than this dispatch can keep up, and surfacing the
// error lets a wrapping adapter retry harness (or the client) decide
// whether to keep trying.
// maybeAutoPinObservedRouteVersion stamps reqs.ObservedRouteVersion
// with the engine's current catalog version at Dispatch entry, but
// ONLY when (a) the txn has no read keys AND (b) the caller did not
// supply StartTS AND (c) no element's key is claimed by a partition
// resolver.  Per design doc §4.1 the version must be pinned at
// BeginTxn (read-set capture time); the auto-pin is a best-effort
// backfill for callers that have not yet migrated to pin
// themselves.  Three cases skip the auto-pin:
//
//   - len(ReadKeys) != 0 — the caller already performed reads at
//     some earlier moment (Redis MULTI/EXEC's txnContext.commit
//     builds an OperationGroup with StartTS from MULTI and
//     ReadKeys from the GETs inside the txn body).  Stamping the
//     catalog version at this Dispatch call would associate
//     post-shift routes with pre-shift reads (codex P1 on
//     10123c5a, PR #900).
//
//   - callerSuppliedStartTS — the caller supplied StartTS,
//     meaning a read at the adapter layer happened against that
//     snapshot before Dispatch even though it is not surfaced as
//     a ReadKeys entry.  The 13+ S3/SQS/DynamoDB adapter sites
//     identified in the M4-StartTS-gate audit (adapter/s3.go:573
//     etc.) fall in this bucket.  Stamping the catalog version
//     at Dispatch time would mask any route shift between the
//     adapter-layer read and Dispatch entry — verifyComposed1
//     would find no mismatch at apply time and the shift becomes
//     INVISIBLE.  That is strictly worse than ObservedRouteVersion=0
//     (gate short-circuits, no false claim made): the spurious
//     auto-pin gives false confidence (codex P1 on 144ec0ca,
//     PR #900).
//
//   - any element's key is resolver-claimed — when a
//     PartitionResolver is wired (SQS HT-FIFO partition keys
//     etc.), ShardRouter routes via the resolver but
//     verifyComposed1 calls route-history OwnerOf on the raw
//     mutation key against the BYTE-RANGE engine snapshot.  The
//     engine has no knowledge of the resolver's mapping, so the
//     gate spuriously rejects resolver-routed commits even when
//     the resolver picked the correct gid.  Skip the auto-pin
//     for any resolver-recognised key; the request flows with
//     ObservedRouteVersion=0 and the M3 gate short-circuits —
//     restoring the pre-auto-pin behaviour for resolver-routed
//     txns.  Resolver-aware M3 is M5+ work (codex P1 on
//     6a458a28, PR #900).
//
// The non-auto-pin case (request flows with ObservedRouteVersion=0,
// M3 gate short-circuits) is the safe non-regressing posture for
// non-migrated callers — the gate cannot retroactively pin reads
// it was not present for.  Adapters that want M3 protection must
// migrate to pin at BeginTxn per §4.1.
//
// Extracted from dispatchTxnWithComposed1Retry to keep its
// cyclomatic complexity in the cyclop budget.
func (c *ShardedCoordinator) maybeAutoPinObservedRouteVersion(reqs *OperationGroup[OP], callerSuppliedStartTS bool) {
	if c.engine == nil || reqs.ObservedRouteVersion != 0 || len(reqs.ReadKeys) != 0 || callerSuppliedStartTS {
		return
	}
	if c.anyResolverClaimedKey(reqs.Elems) {
		return
	}
	reqs.ObservedRouteVersion = c.engine.Version()
}

// anyResolverClaimedKey reports whether any element's key is
// claimed by the partition resolver.  Returns false when the
// router has no resolver installed (the most common case) so the
// auto-pin path stays inexpensive in the byte-range-only deployment.
// Extracted from maybeAutoPinObservedRouteVersion to keep the
// auto-pin's cyclomatic complexity in the cyclop budget.
func (c *ShardedCoordinator) anyResolverClaimedKey(elems []*Elem[OP]) bool {
	if c.router == nil || c.router.partitionResolver == nil {
		return false
	}
	for _, e := range elems {
		if e == nil || len(e.Key) == 0 {
			continue
		}
		if c.router.partitionResolver.RecognisesPartitionedKey(e.Key) {
			return true
		}
	}
	return false
}

func (c *ShardedCoordinator) dispatchTxnWithComposed1Retry(ctx context.Context, reqs *OperationGroup[OP], callerSuppliedStartTS bool) (*CoordinateResponse, error) {
	c.maybeAutoPinObservedRouteVersion(reqs, callerSuppliedStartTS)

	for attempt := 0; attempt <= composed1RetryAttempts; attempt++ {
		resp, err := c.dispatchTxn(ctx, reqs.StartTS, reqs.CommitTS, reqs.PrevCommitTS, reqs.Elems, reqs.ReadKeys, reqs.ObservedRouteVersion, reqs.KeyVizLabel)
		if err == nil {
			return resp, nil
		}
		if !isComposed1RetryableError(err) {
			return resp, err
		}
		// Read-write txns (ReadKeys non-empty) MUST surface the
		// sentinel without retry.  The client already performed
		// the reads at the original StartTS; transparently bumping
		// StartTS on retry would let any concurrent write that
		// committed between the old and new timestamps go
		// undetected by the FSM's OCC check (which only rejects
		// versions with `latest > startTS`), letting the retry
		// commit decisions based on stale reads — a strict-
		// serialisability violation (gemini CRITICAL / codex P1
		// on PR #900).
		//
		// The caller / adapter is responsible for re-executing the
		// entire txn (including re-reading the keys at a fresh
		// timestamp) when this sentinel surfaces.  Write-only
		// txns (no reads to invalidate) remain safe to retry.
		if len(reqs.ReadKeys) > 0 {
			return resp, err
		}
		// Caller-supplied StartTS (ReadKeys-empty case) MUST also
		// surface — the caller's pre-Dispatch read at this snapshot
		// is invisible to the coordinator, so we cannot prove the
		// retry's bumped StartTS would still preserve OCC against
		// commits landed in (originalStartTS, newStartTS).  This
		// catches dozens of adapter sites that supply
		// StartTS=readTS without populating ReadKeys: S3
		// createBucket / admin object writes, SQS messages /
		// FIFO / purge / tags, DynamoDB metadata writes, etc.
		// Only coordinator-allocated StartTS (caller passed 0
		// at Dispatch entry, the coordinator allocated it from
		// nextStartTS, and no read has happened at it) is safe
		// to bump on retry (codex P1 on 57da8886, PR #900).
		if callerSuppliedStartTS {
			return resp, err
		}
		if attempt == composed1RetryAttempts {
			return resp, err
		}
		// Re-route by re-reading the engine's current catalog
		// version and stamping it on the txn.  groupMutations on
		// the next dispatchTxn pass re-runs router.ResolveGroup,
		// so a key whose owning group changed since the last
		// attempt naturally lands on the new group's FSM.
		if c.engine != nil {
			reqs.ObservedRouteVersion = c.engine.Version()
		}
		// Clear the timestamps so the next attempt allocates a
		// fresh pair against the post-shift HLC.  The OCC
		// invariants require commitTS > startTS computed under the
		// same HLC observation; reusing the stale pair would risk
		// a startTS that no longer dominates the new shard's
		// applied commitTS.
		//
		// Allocate StartTS unconditionally — nextStartTS already
		// handles the nil-clock case (falls back to
		// maxTS+1 internally).  Gating on c.clock!=nil would leave
		// reqs.StartTS at 0 in the nil-clock test fixture, and
		// dispatchTxn does NOT re-allocate when StartTS is zero,
		// so the retry would run with StartTS=0 — violating MVCC
		// invariants (gemini HIGH on PR #900).
		reqs.CommitTS = 0
		// IMPORTANT — do NOT clear reqs.PrevCommitTS here.  An earlier
		// round of this PR did (taking claude[bot]'s reasoning at face
		// value: "M3 fired at apply time, so the original attempt never
		// committed → probe is stale").  That reasoning conflates two
		// different "original attempts" and is wrong (codex P1 on
		// 43c55dfe, PR #900):
		//
		//   * Within M4: attempt 1's M3 rejection does prove attempt 1
		//     did not write anything (the FSM rejects before any state
		//     change).
		//   * Across adapter retries: PrevCommitTS names the
		//     ADAPTER'S prior attempt (e.g. adapter/redis.go:3209,
		//     :3615 send PrevCommitTS = pending.commitTS as the
		//     option-2 ambiguous-outcome probe).  That earlier
		//     attempt may have actually landed at the named commitTS
		//     before the route shift that triggered our M3 sentinel.
		//
		// Dropping the probe in the retry would let the FSM re-apply
		// the writes against the post-shift route, double-writing the
		// caller's at-most-once-payload if the named prior attempt did
		// land.  Preserving it has the cost that
		// dispatchMultiShardTxn:749 surfaces
		// ErrTxnDedupRequiresSingleShard when the post-shift route now
		// spans multiple groups — which IS the faithful signal: the
		// adapter's at-most-once retry contract cannot be transparently
		// honoured across a shard split, and the caller must reckon
		// with the ambiguity at a higher level (re-read, re-execute,
		// abort).  Surfacing that error is strictly safer than silently
		// losing dedup.
		startTS, allocErr := c.nextStartTS(ctx, reqs.Elems)
		if allocErr != nil {
			// resp here is the failed first attempt's response (always
			// nil for the Composed-1 sentinels) — return nil
			// explicitly so the contract "non-nil response on success
			// only" is unambiguous (claude[bot] nit on PR #900).
			return nil, allocErr
		}
		reqs.StartTS = startTS
	}
	// Unreachable — the loop body always returns on each iteration.
	// Kept here so the function has an unambiguous return shape.
	return nil, errors.WithStack(ErrInvalidRequest)
}

// isComposed1RetryableError reports whether err is one of the M3 gate
// sentinels that M4 retries.  Both surface as wrapped errors from the
// underlying transactional Commit; errors.Is unwraps them correctly.
func isComposed1RetryableError(err error) bool {
	return errors.Is(err, ErrComposed1Violation) || errors.Is(err, ErrComposed1VersionGCd)
}

// dispatchNonTxn routes a non-transactional operation group through the
// shard router. Extracted from Dispatch to keep that method's branch
// count within the cyclop budget after the 7a registration gate landed.
func (c *ShardedCoordinator) dispatchNonTxn(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	logs, err := c.requestLogs(ctx, reqs)
	if err != nil {
		return nil, err
	}
	r, err := c.router.Commit(ctx, logs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &CoordinateResponse{CommitIndex: r.CommitIndex}, nil
}

// hasDelPrefixElem returns true if any element is a DelPrefix operation.
func hasDelPrefixElem(elems []*Elem[OP]) bool {
	for _, e := range elems {
		if e != nil && e.Op == DelPrefix {
			return true
		}
	}
	return false
}

// validateDelPrefixOnly ensures all elements are DelPrefix operations.
// Mixing DEL_PREFIX with other operations (PUT, DEL) in a single dispatch is
// not allowed because the FSM handles DEL_PREFIX exclusively.
func validateDelPrefixOnly(elems []*Elem[OP]) error {
	for _, e := range elems {
		if e != nil && e.Op != DelPrefix {
			return errors.Wrap(ErrInvalidRequest, "DEL_PREFIX cannot be mixed with other operations")
		}
	}
	return nil
}

// dispatchDelPrefixBroadcast validates and broadcasts DEL_PREFIX operations
// to every shard group. Each element becomes a separate pb.Request (the FSM's
// extractDelPrefix processes only the first DEL_PREFIX mutation per request).
// All requests are batched into a single Commit call per shard group.
func (c *ShardedCoordinator) dispatchDelPrefixBroadcast(ctx context.Context, isTxn bool, elems []*Elem[OP]) (*CoordinateResponse, error) {
	if isTxn {
		return nil, errors.Wrap(ErrInvalidRequest, "DEL_PREFIX not supported in transactions")
	}
	if err := validateDelPrefixOnly(elems); err != nil {
		return nil, err
	}
	if err := c.rejectWriteFencedDelPrefixes(elems); err != nil {
		return nil, err
	}

	ts, err := c.allocateTimestamp(ctx, "allocate DEL_PREFIX broadcast ts")
	if err != nil {
		return nil, err
	}
	if err := c.rejectWriteTimestampFloorDelPrefixes(elems, ts); err != nil {
		return nil, err
	}
	requests := make([]*pb.Request, 0, len(elems))
	for _, elem := range elems {
		requests = append(requests, &pb.Request{
			IsTxn:     false,
			Phase:     pb.Phase_NONE,
			Ts:        ts,
			Mutations: []*pb.Mutation{elemToMutation(elem)},
		})
	}

	return c.broadcastToAllGroups(ctx, requests)
}

func (c *ShardedCoordinator) rejectWriteFencedPointElems(elems []*Elem[OP]) error {
	if c == nil || c.engine == nil {
		return nil
	}
	for _, elem := range elems {
		if elem == nil || len(elem.Key) == 0 {
			continue
		}
		if err := c.rejectWriteFencedPointKey(elem.Key); err != nil {
			return err
		}
	}
	return nil
}

func (c *ShardedCoordinator) rejectWriteFencedPointKey(key []byte) error {
	rkey := routeKey(key)
	if route, ok := c.engine.GetRoute(rkey); ok && route.State == distribution.RouteStateWriteFenced {
		return errors.Wrapf(ErrRouteWriteFenced, "key %q routeKey %q", key, rkey)
	}
	start, end, ok := s3BucketAuxiliaryRouteRange(key)
	if !ok {
		return nil
	}
	for _, route := range c.engine.GetIntersectingRoutes(start, end) {
		if route.State == distribution.RouteStateWriteFenced {
			return errors.Wrapf(ErrRouteWriteFenced, "key %q route range [%q,%q)", key, start, end)
		}
	}
	return nil
}

func (c *ShardedCoordinator) rejectWriteFencedDelPrefixes(elems []*Elem[OP]) error {
	if c == nil || c.engine == nil {
		return nil
	}
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		start, end := routePrefixRange(elem.Key)
		for _, route := range c.engine.GetIntersectingRoutes(start, end) {
			if route.State == distribution.RouteStateWriteFenced {
				return errors.Wrapf(ErrRouteWriteFenced, "prefix %q route range [%q,%q)", elem.Key, start, end)
			}
		}
	}
	return nil
}

func (c *ShardedCoordinator) rejectWriteTimestampFloorPointElems(elems []*Elem[OP], commitTS uint64) error {
	if c == nil || c.engine == nil || commitTS == 0 {
		return nil
	}
	for _, elem := range elems {
		if elem == nil || len(elem.Key) == 0 {
			continue
		}
		if err := c.rejectWriteTimestampFloorPointKey(elem.Key, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func (c *ShardedCoordinator) rejectWriteTimestampFloorPointKey(key []byte, commitTS uint64) error {
	rkey := routeKey(key)
	if route, ok := c.engine.GetRoute(rkey); ok && route.MinWriteTSExclusive != 0 && commitTS <= route.MinWriteTSExclusive {
		return errors.Wrapf(ErrRouteWriteTimestampTooLow, "key %q routeKey %q commit_ts=%d floor=%d", key, rkey, commitTS, route.MinWriteTSExclusive)
	}
	start, end, ok := s3BucketAuxiliaryRouteRange(key)
	if !ok {
		return nil
	}
	for _, route := range c.engine.GetIntersectingRoutes(start, end) {
		if route.MinWriteTSExclusive != 0 && commitTS <= route.MinWriteTSExclusive {
			return errors.Wrapf(ErrRouteWriteTimestampTooLow, "key %q route range [%q,%q) commit_ts=%d floor=%d", key, start, end, commitTS, route.MinWriteTSExclusive)
		}
	}
	return nil
}

func (c *ShardedCoordinator) rejectWriteTimestampFloorDelPrefixes(elems []*Elem[OP], commitTS uint64) error {
	if c == nil || c.engine == nil || commitTS == 0 {
		return nil
	}
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		start, end := routePrefixRange(elem.Key)
		for _, route := range c.engine.GetIntersectingRoutes(start, end) {
			if route.MinWriteTSExclusive != 0 && commitTS <= route.MinWriteTSExclusive {
				return errors.Wrapf(ErrRouteWriteTimestampTooLow, "prefix %q route range [%q,%q) commit_ts=%d floor=%d", elem.Key, start, end, commitTS, route.MinWriteTSExclusive)
			}
		}
	}
	return nil
}

// broadcastToAllGroups sends the same set of requests to every shard group in
// parallel and returns the maximum commit index.
func (c *ShardedCoordinator) broadcastToAllGroups(ctx context.Context, requests []*pb.Request) (*CoordinateResponse, error) {
	var (
		maxIndex atomic.Uint64
		firstErr error
		errMu    sync.Mutex
		wg       sync.WaitGroup
	)
	for _, g := range c.groups {
		wg.Add(1)
		go func(g *ShardGroup) {
			defer wg.Done()
			r, err := g.Txn.Commit(ctx, requests)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				return
			}
			if r != nil {
				for {
					cur := maxIndex.Load()
					if r.CommitIndex <= cur || maxIndex.CompareAndSwap(cur, r.CommitIndex) {
						break
					}
				}
			}
		}(g)
	}
	wg.Wait()

	if firstErr != nil {
		return nil, errors.WithStack(firstErr)
	}
	return &CoordinateResponse{CommitIndex: maxIndex.Load()}, nil
}

func (c *ShardedCoordinator) dispatchTxn(ctx context.Context, startTS uint64, commitTS uint64, prevCommitTS uint64, elems []*Elem[OP], readKeys [][]byte, observedRouteVersion uint64, label keyviz.Label) (*CoordinateResponse, error) {
	if len(readKeys) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	grouped, gids, err := c.groupMutations(elems, label)
	if err != nil {
		return nil, err
	}
	primaryKey := primaryKeyForElems(elems)
	if len(primaryKey) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}

	commitTS, err = c.resolveTxnCommitTS(ctx, startTS, commitTS)
	if err != nil {
		return nil, err
	}
	if err := c.rejectWriteTimestampFloorPointElems(elems, commitTS); err != nil {
		return nil, err
	}

	if len(gids) == 1 && c.allReadKeysInShard(readKeys, gids[0]) {
		// Fast path: all mutations and read keys are in a single shard.
		// Use the one-phase path without allocating a grouped-read-keys map.
		// If any read key belongs to a different shard the 2PC path is required
		// so that validateReadOnlyShards can issue a linearizable read barrier,
		// preserving SSI.
		return c.dispatchSingleShardTxn(ctx, startTS, commitTS, prevCommitTS, primaryKey, gids[0], elems, readKeys, observedRouteVersion)
	}
	return c.dispatchMultiShardTxn(ctx, startTS, commitTS, prevCommitTS, primaryKey, grouped, gids, readKeys, observedRouteVersion)
}

// dispatchMultiShardTxn runs the 2PC path. Extracted from dispatchTxn to keep
// that function under the cyclop budget after the prevCommitTS reject (codex
// P2 round-10) was added; the multi-shard branch already carries five linear
// error checks (groupReadKeys, prewrite, commitPrimary, abortCleanup,
// commitSecondaries) that pushed the parent over the 10-edge limit.
func (c *ShardedCoordinator) dispatchMultiShardTxn(ctx context.Context, startTS, commitTS, prevCommitTS uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64, readKeys [][]byte, observedRouteVersion uint64) (*CoordinateResponse, error) {
	// Fail-closed when a retry carries the option-2 dedup probe key but its
	// write set / read set spans shards (codex P2 round-10 "reject retries
	// that leave the one-phase path"). The 2PC log builders only encode
	// CommitTS and would silently drop PrevCommitTS — a landed ambiguous
	// attempt would then look like an ordinary write conflict, the adapter
	// would drop pending and recompute, and the duplicate this feature is
	// meant to prevent would reappear. Surface the constraint explicitly so
	// the caller (or a future multi-shard dedup design) knows the request
	// shape is unsupported.
	if prevCommitTS != 0 {
		return nil, errors.WithStack(ErrTxnDedupRequiresSingleShard)
	}

	// Group read keys by shard now. The result is passed directly to
	// prewriteTxn to avoid a second iteration inside that function. A
	// routing failure here aborts the transaction before any prewrite —
	// silently dropping unresolvable read keys would let OCC validation
	// run with an incomplete read set and break SSI.
	groupedReadKeys, err := c.groupReadKeysByShardID(readKeys)
	if err != nil {
		return nil, err
	}
	groupedReadKeys = c.groupedReadKeysWithStagedVisibilityMutationAliases(groupedReadKeys, grouped)
	if groupedReadKeyCount(groupedReadKeys) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	prepared, err := c.prewriteTxn(ctx, startTS, commitTS, primaryKey, grouped, gids, groupedReadKeys, observedRouteVersion)
	if err != nil {
		return nil, err
	}

	primaryGid, maxIndex, err := c.commitPrimaryTxn(ctx, startTS, primaryKey, grouped, commitTS, observedRouteVersion)
	if err != nil {
		// abortPreparedTxn must run even when ctx was the reason
		// commitPrimaryTxn failed — otherwise prewrite intents on
		// every prepared shard linger until LockResolver picks them
		// up at a future tick (lease window of expensive
		// keyspace-scan work). Detach cancellation but cap with
		// verifyLeaderTimeout so a hung Abort cannot leak.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), verifyLeaderTimeout)
		c.abortPreparedTxn(cleanupCtx, startTS, primaryKey, prepared, abortTSFrom(startTS, commitTS))
		cancel()
		return nil, errors.WithStack(err)
	}

	// commitSecondaryTxns propagates ObservedRouteVersion to per-
	// secondary commits so the M3 gate fires on route shifts between
	// primary-COMMIT and secondary-COMMIT.  Such a sentinel surfaces
	// as ErrTxnSecondaryRouteShiftedAfterPrimaryCommit (NOT
	// ErrComposed1Violation — the M4 retry must not loop here; the
	// primary is already durable and re-prewriting would conflict
	// with the existing locks).  d8487672 originally tried to avoid
	// the silent-partial-commit hazard by dropping the gate; codex
	// P1 (20:30:35) correctly showed that the dropped-gate posture
	// silently lands writes on stale owners that are no longer
	// reachable by readers on the new owner.  Both directions are
	// silent partial commits; surfacing the error is the only honest
	// posture (codex P1 on d8487672 + 6202b964, PR #900).
	maxIndex, err = c.commitSecondaryTxns(ctx, startTS, primaryGid, primaryKey, grouped, gids, commitTS, maxIndex, observedRouteVersion)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &CoordinateResponse{CommitIndex: maxIndex}, nil
}

func (c *ShardedCoordinator) resolveTxnCommitTS(ctx context.Context, startTS, commitTS uint64) (uint64, error) {
	if commitTS == 0 {
		next, err := c.nextTxnTSAfter(ctx, startTS)
		if err != nil {
			return 0, err
		}
		commitTS = next
	} else if c.clock != nil {
		// Observe caller-provided commitTS to keep the HLC monotonic; without
		// this the clock could later issue timestamps smaller than commitTS.
		c.clock.Observe(commitTS)
	}
	if commitTS == 0 || commitTS <= startTS {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return commitTS, nil
}

// allReadKeysInShard returns true when every key in readKeys belongs to gid.
// It performs a single O(n) pass without allocating a map, making it suitable
// for the single-shard fast path in dispatchTxn.
func (c *ShardedCoordinator) allReadKeysInShard(readKeys [][]byte, gid uint64) bool {
	for _, rk := range readKeys {
		if c.engineGroupIDForKey(rk) != gid {
			return false
		}
	}
	return true
}

func (c *ShardedCoordinator) dispatchSingleShardTxn(ctx context.Context, startTS, commitTS, prevCommitTS uint64, primaryKey []byte, gid uint64, elems []*Elem[OP], readKeys [][]byte, observedRouteVersion uint64) (*CoordinateResponse, error) {
	g, err := c.txnGroupForID(gid)
	if err != nil {
		return nil, err
	}
	readKeys = c.readKeysWithStagedVisibilityAliasesForGroup(gid, readKeys)
	readKeys = c.readKeysWithStagedVisibilityMutationAliasesForGroup(gid, readKeys, elems)
	if len(readKeys) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	// ReadKeys are included in the Raft log entry so the FSM validates
	// read-write conflicts atomically under applyMu. prevCommitTS, when set,
	// carries the one-phase dedup probe key for a retry that reuses a failed
	// attempt's write set.
	resp, err := g.Txn.Commit(ctx, []*pb.Request{
		onePhaseTxnRequestWithPrevCommit(startTS, commitTS, prevCommitTS, primaryKey, elems, readKeys, observedRouteVersion),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp == nil {
		return &CoordinateResponse{}, nil
	}
	return &CoordinateResponse{CommitIndex: resp.CommitIndex}, nil
}

func (c *ShardedCoordinator) readKeysWithStagedVisibilityAliasesForGroup(gid uint64, readKeys [][]byte) [][]byte {
	if len(readKeys) == 0 {
		return readKeys
	}
	var out [][]byte
	for _, key := range readKeys {
		alias, ok := c.stagedVisibilityReadKeyAlias(gid, key)
		if !ok {
			continue
		}
		if out == nil {
			out = append([][]byte(nil), readKeys...)
		}
		out = append(out, alias)
	}
	if out == nil {
		return readKeys
	}
	return out
}

func (c *ShardedCoordinator) readKeysWithStagedVisibilityMutationAliasesForGroup(gid uint64, readKeys [][]byte, elems []*Elem[OP]) [][]byte {
	var out [][]byte
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		alias, ok := c.stagedVisibilityReadKeyAlias(gid, elem.Key)
		if !ok {
			continue
		}
		if out == nil {
			out = append([][]byte(nil), readKeys...)
		}
		out = append(out, alias)
	}
	if out == nil {
		return readKeys
	}
	return out
}

type preparedGroup struct {
	gid  uint64
	keys []*pb.Mutation
}

func (c *ShardedCoordinator) prewriteTxn(ctx context.Context, startTS, commitTS uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64, groupedReadKeys map[uint64][][]byte, observedRouteVersion uint64) ([]preparedGroup, error) {
	prepareMeta := txnMetaMutation(primaryKey, defaultTxnLockTTLms, 0)
	prepared := make([]preparedGroup, 0, len(gids))

	for _, gid := range gids {
		g, err := c.txnGroupForID(gid)
		if err != nil {
			return nil, err
		}
		req := &pb.Request{
			IsTxn:                true,
			Phase:                pb.Phase_PREPARE,
			Ts:                   startTS,
			Mutations:            append([]*pb.Mutation{prepareMeta}, grouped[gid]...),
			ReadKeys:             groupedReadKeys[gid],
			ObservedRouteVersion: observedRouteVersion,
		}
		if _, err := g.Txn.Commit(ctx, []*pb.Request{req}); err != nil {
			// Same WithoutCancel pattern as dispatchTxn's
			// commitPrimaryTxn-failure cleanup: a cancelled ctx is
			// the most likely cause of Commit failing here, and the
			// abort MUST still go through to release the intents we
			// already wrote on prior shards. Otherwise LockResolver
			// holds the bag.
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), verifyLeaderTimeout)
			c.abortPreparedTxn(cleanupCtx, startTS, primaryKey, prepared, abortTSFrom(startTS, commitTS))
			cancel()
			return nil, errors.WithStack(err)
		}
		prepared = append(prepared, preparedGroup{gid: gid, keys: keyMutations(grouped[gid])})
	}

	// Validate read keys on read-only shards (shards that have read keys
	// but no mutations in this transaction). Without this, a concurrent
	// write to a read-only shard would go undetected.
	if err := c.validateReadOnlyShards(ctx, groupedReadKeys, gids, startTS); err != nil {
		// Same reasoning as the prepare-loop cleanup above: the
		// validate read fence may have failed because ctx
		// expired, so the abort needs detached cancellation to
		// avoid stranding intents.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), verifyLeaderTimeout)
		c.abortPreparedTxn(cleanupCtx, startTS, primaryKey, prepared, abortTSFrom(startTS, commitTS))
		cancel()
		return nil, err
	}

	return prepared, nil
}

func (c *ShardedCoordinator) commitPrimaryTxn(ctx context.Context, startTS uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, commitTS uint64, observedRouteVersion uint64) (uint64, uint64, error) {
	primaryGid := c.engineGroupIDForKey(primaryKey)
	if primaryGid == 0 {
		return 0, 0, errors.WithStack(ErrInvalidRequest)
	}

	g, err := c.txnGroupForID(primaryGid)
	if err != nil {
		return 0, 0, err
	}

	meta := txnMetaMutation(primaryKey, 0, commitTS)
	keys := keyMutations(grouped[primaryGid])
	req := &pb.Request{
		IsTxn:                true,
		Phase:                pb.Phase_COMMIT,
		Ts:                   startTS,
		Mutations:            append([]*pb.Mutation{meta}, keys...),
		ObservedRouteVersion: observedRouteVersion,
	}

	r, err := g.Txn.Commit(ctx, []*pb.Request{req})
	if err != nil {
		return primaryGid, 0, errors.WithStack(err)
	}
	if r == nil {
		return primaryGid, 0, nil
	}
	return primaryGid, r.CommitIndex, nil
}

func (c *ShardedCoordinator) commitSecondaryTxns(ctx context.Context, startTS uint64, primaryGid uint64, primaryKey []byte, grouped map[uint64][]*pb.Mutation, gids []uint64, commitTS uint64, maxIndex uint64, observedRouteVersion uint64) (uint64, error) {
	// Secondary commits are best-effort for non-Composed-1 errors:
	// if a shard is unavailable after the primary commits, read-time
	// lock resolution will commit the remaining secondaries based on
	// the primary commit record.  Retry a few times to absorb short
	// leader-election windows and reduce lock lag.
	//
	// Composed-1 errors are an exception (codex P1 on d8487672 +
	// 6202b964, PR #900).  When the M3 gate fires on a secondary
	// commit, the route catalog has moved between primary-COMMIT and
	// this secondary-COMMIT — the prepared lock lives at the old
	// gid, the new owner per the catalog has no commit record.  We
	// CANNOT silently land the write on the stale owner (codex 20:30:35
	// — that's invisible to readers on the new owner) and we CANNOT
	// silently swallow the error (codex 20:10:32 — uncommitted
	// secondary, silent partial commit on the missing direction).
	// The only honest posture is to surface
	// ErrTxnSecondaryRouteShiftedAfterPrimaryCommit so the caller
	// knows the txn state is uncertain and can do application-level
	// recovery.  This sentinel is distinct from ErrComposed1Violation
	// so the M4 retry path does not loop here (the primary is durable
	// and re-prewriting would conflict with the existing locks).
	meta := txnMetaMutation(primaryKey, 0, commitTS)
	for _, gid := range gids {
		if gid == primaryGid {
			continue
		}
		g, ok := c.groups[gid]
		if !ok || g == nil || g.Txn == nil {
			continue
		}
		req := &pb.Request{
			IsTxn:                true,
			Phase:                pb.Phase_COMMIT,
			Ts:                   startTS,
			Mutations:            append([]*pb.Mutation{meta}, keyMutations(grouped[gid])...),
			ObservedRouteVersion: observedRouteVersion,
		}
		r, err := commitSecondaryWithRetry(ctx, g, req)
		if err != nil {
			if isComposed1RetryableError(err) {
				// Fatal: surface the new sentinel so the caller is
				// informed that the secondary's write is missing and
				// the txn state is half-committed.  M4 retry won't
				// match this sentinel, so it doesn't loop.
				c.logger().Warn("txn secondary commit failed Composed-1 — surfacing as fatal",
					slog.Uint64("gid", gid),
					slog.String("primary_key", string(primaryKey)),
					slog.Uint64("start_ts", startTS),
					slog.Uint64("commit_ts", commitTS),
					slog.Any("err", err),
				)
				return maxIndex, errors.WithStack(ErrTxnSecondaryRouteShiftedAfterPrimaryCommit)
			}
			c.logger().Warn("txn secondary commit failed",
				slog.Uint64("gid", gid),
				slog.String("primary_key", string(primaryKey)),
				slog.Uint64("start_ts", startTS),
				slog.Uint64("commit_ts", commitTS),
				slog.Any("err", err),
			)
			continue
		}
		if r != nil && r.CommitIndex > maxIndex {
			maxIndex = r.CommitIndex
		}
	}
	return maxIndex, nil
}

func commitSecondaryWithRetry(ctx context.Context, g *ShardGroup, req *pb.Request) (*TransactionResponse, error) {
	if g == nil || g.Txn == nil || req == nil {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	var lastErr error
	for attempt := range txnSecondaryCommitRetryAttempts {
		resp, err := g.Txn.Commit(ctx, []*pb.Request{req})
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if attempt+1 < txnSecondaryCommitRetryAttempts {
			time.Sleep(txnSecondaryCommitRetryBackoff * time.Duration(attempt+1))
		}
	}
	return nil, errors.WithStack(lastErr)
}

func (c *ShardedCoordinator) logger() *slog.Logger {
	if c != nil && c.log != nil {
		return c.log
	}
	return slog.Default()
}

func (c *ShardedCoordinator) abortPreparedTxn(ctx context.Context, startTS uint64, primaryKey []byte, prepared []preparedGroup, abortTS uint64) {
	if len(prepared) == 0 {
		return
	}
	if abortTS == 0 || abortTS <= startTS {
		return
	}

	meta := txnMetaMutation(primaryKey, 0, abortTS)
	for _, pg := range prepared {
		g, ok := c.groups[pg.gid]
		if !ok || g == nil || g.Txn == nil {
			continue
		}
		req := &pb.Request{
			IsTxn:     true,
			Phase:     pb.Phase_ABORT,
			Ts:        startTS,
			Mutations: append([]*pb.Mutation{meta}, pg.keys...),
		}
		if _, err := g.Txn.Commit(ctx, []*pb.Request{req}); err != nil {
			if errors.Is(err, ErrTxnAlreadyCommitted) {
				continue
			}
			c.logger().Warn("txn abort failed; locks may remain until TTL expiry",
				slog.Uint64("gid", pg.gid),
				slog.String("primary_key", string(primaryKey)),
				slog.Uint64("start_ts", startTS),
				slog.Uint64("abort_ts", abortTS),
				slog.Any("err", err),
			)
		}
	}
}

func (c *ShardedCoordinator) txnGroupForID(gid uint64) (*ShardGroup, error) {
	g, ok := c.groups[gid]
	if !ok || g == nil || g.Txn == nil {
		return nil, errors.Wrapf(ErrInvalidRequest, "unknown group %d", gid)
	}
	return g, nil
}

func (c *ShardedCoordinator) allocateTimestamp(ctx context.Context, label string) (uint64, error) {
	return c.allocateTimestampAfter(ctx, label, 0)
}

func (c *ShardedCoordinator) allocateTimestampAfter(ctx context.Context, label string, min uint64) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.Wrap(ErrTxnCommitTSRequired, label)
	}
	if c.tsAllocator != nil {
		if min > 0 {
			return nextTimestampAfterFromAllocator(ctx, c.tsAllocator, min, label)
		}
		return nextTimestampFromAllocator(ctx, c.tsAllocator, label)
	}
	if c.clock == nil {
		return 0, errors.Wrap(ErrTSOClockNil, label)
	}
	if min > 0 {
		c.clock.Observe(min)
	}
	ts, err := c.clock.NextFenced()
	if err != nil {
		return 0, errors.Wrap(err, label)
	}
	return ts, nil
}

// Next makes ShardedCoordinator usable as a TimestampAllocator for adapter
// helpers that need to allocate persistence-grade timestamps outside Dispatch.
func (c *ShardedCoordinator) Next(ctx context.Context) (uint64, error) {
	return c.allocateTimestamp(ctx, "allocate sharded timestamp")
}

func (c *ShardedCoordinator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	return c.allocateTimestampAfter(ctx, "allocate sharded timestamp after observed ts", min)
}

func (c *ShardedCoordinator) nextTxnTSAfter(ctx context.Context, startTS uint64) (uint64, error) {
	if c.clock == nil && c.tsAllocator == nil {
		nextTS := startTS + 1
		if nextTS == 0 {
			return 0, nil
		}
		return nextTS, nil
	}
	ts, err := c.allocateTimestampAfter(ctx, "allocate txn commit ts", startTS)
	if err != nil {
		return 0, err
	}
	if ts <= startTS {
		if c.clock != nil {
			c.clock.Observe(startTS)
		}
		ts, err = c.allocateTimestampAfter(ctx, "re-allocate txn commit ts after Observe", startTS)
		if err != nil {
			return 0, err
		}
	}
	if ts <= startTS {
		return 0, nil
	}
	return ts, nil
}

func abortTSFrom(startTS, commitTS uint64) uint64 {
	const maxUint64 = ^uint64(0)

	// Prefer commitTS+1 when representable and strictly greater than startTS.
	if commitTS < maxUint64 {
		abortTS := commitTS + 1
		if abortTS > startTS {
			return abortTS
		}
	}

	// Fallback to startTS+1 when representable.
	if startTS < maxUint64 {
		return startTS + 1
	}

	// No representable timestamp exists that is strictly greater than startTS.
	return 0
}

func txnMetaMutation(primaryKey []byte, lockTTLms uint64, commitTS uint64) *pb.Mutation {
	return &pb.Mutation{
		Op:    pb.Op_PUT,
		Key:   []byte(txnMetaPrefix),
		Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: lockTTLms, CommitTS: commitTS}),
	}
}

func (c *ShardedCoordinator) nextStartTS(ctx context.Context, elems []*Elem[OP]) (uint64, error) {
	maxTS, err := c.maxLatestCommitTS(ctx, elems)
	if err != nil {
		return 0, err
	}
	if c.clock != nil && maxTS > 0 {
		c.clock.Observe(maxTS)
	}
	if c.clock == nil && c.tsAllocator == nil {
		return maxTS + 1, nil
	}
	ts, err := c.allocateTimestampAfter(ctx, "allocate sharded startTS", maxTS)
	if err != nil {
		return 0, err
	}
	return ts, nil
}

func (c *ShardedCoordinator) maxLatestCommitTS(ctx context.Context, elems []*Elem[OP]) (uint64, error) {
	if c.store == nil {
		return 0, nil
	}

	keys := make([][]byte, 0, len(elems))
	for _, e := range elems {
		if e == nil || len(e.Key) == 0 {
			continue
		}
		keys = append(keys, e.Key)
	}

	return MaxLatestCommitTS(ctx, c.store, keys)
}

func (c *ShardedCoordinator) IsLeader() bool {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return false
	}
	return isLeaderEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) IsTimestampLeader() bool {
	if c == nil {
		return false
	}
	for _, g := range c.groups {
		if isLeaderEngine(engineForGroup(g)) {
			return true
		}
	}
	return false
}

func (c *ShardedCoordinator) invalidateTimestampWindow() {
	if c == nil {
		return
	}
	invalidateTimestampWindow(c.tsAllocator)
}

func (c *ShardedCoordinator) VerifyLeader(ctx context.Context) error {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return verifyLeaderEngineCtx(ctx, engineForGroup(g))
}

func (c *ShardedCoordinator) RaftLeader() string {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return ""
	}
	return leaderAddrFromEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	return linearizableReadEngineCtx(ctx, engineForGroup(g))
}

func (c *ShardedCoordinator) IsLeaderForKey(key []byte) bool {
	g, ok := c.groupForKey(key)
	if !ok {
		return false
	}
	return isLeaderEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) VerifyLeaderForKey(ctx context.Context, key []byte) error {
	g, ok := c.groupForKey(key)
	if !ok {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return verifyLeaderEngineCtx(ctx, engineForGroup(g))
}

func (c *ShardedCoordinator) RaftLeaderForKey(key []byte) string {
	g, ok := c.groupForKey(key)
	if !ok {
		return ""
	}
	return leaderAddrFromEngine(engineForGroup(g))
}

func (c *ShardedCoordinator) LinearizableReadForKey(ctx context.Context, key []byte) (uint64, error) {
	routeID, g, ok := c.routeAndGroupForKey(key)
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	c.observeRead(ctx, routeID, key)
	return linearizableReadEngineCtx(ctx, engineForGroup(g))
}

// LeaseRead routes through the default group's lease. See Coordinate.LeaseRead
// for semantics.
func (c *ShardedCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	g, ok := c.groups[c.defaultGroup]
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	return groupLeaseRead(ctx, g, c.leaseObserver)
}

// LeaseReadForKey performs the lease check on the shard group that owns key.
// Each group maintains its own lease since each group has independent
// leadership and term.
func (c *ShardedCoordinator) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	routeID, g, ok := c.routeAndGroupForKey(key)
	if !ok {
		return 0, errors.WithStack(ErrLeaderNotFound)
	}
	c.observeRead(ctx, routeID, key)
	return groupLeaseRead(ctx, g, c.leaseObserver)
}

// LeaseReadAllGroups establishes the lease freshness bound on every shard
// group this coordinator owns. Multi-shard reads (Scan, GSI/whole-table
// Query) visit all intersecting routes across all groups (see
// ShardStore.ScanAt), so fencing only the default group would let those
// reads sample a snapshot on a non-default group without the freshness
// bound. It fails closed on the first group that cannot confirm its lease,
// since a partially-fenced read is exactly the stale read this guards
// against. Group iteration order is unspecified; correctness does not
// depend on it because every group must succeed.
func (c *ShardedCoordinator) LeaseReadAllGroups(ctx context.Context) error {
	if len(c.groups) == 0 {
		return errors.WithStack(ErrLeaderNotFound)
	}
	for _, g := range c.groups {
		if _, err := groupLeaseRead(ctx, g, c.leaseObserver); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// observeLeaseRead forwards a hit / miss signal to observer when it
// is non-nil. Kept as a package-level helper so both ShardedCoordinator
// and any future sharded caller share one nil-safe entrypoint.
func observeLeaseRead(observer LeaseReadObserver, hit bool) {
	if observer != nil {
		observer.ObserveLeaseRead(hit)
	}
}

func groupLeaseRead(ctx context.Context, g *ShardGroup, observer LeaseReadObserver) (uint64, error) {
	engine := engineForGroup(g)
	// g.lp caches the LeaseProvider assertion done once at construction
	// (NewShardedCoordinator); a nil group or an engine without the
	// capability falls through to the linearizable slow path. The nil-g
	// guard preserves engineForGroup's nil-safety since g.lp would panic
	// on a nil receiver.
	if g == nil || g.lp == nil {
		return linearizableReadEngineCtx(ctx, engine)
	}
	lp := g.lp
	leaseDur := lp.LeaseDuration()
	if leaseDur <= 0 {
		return linearizableReadEngineCtx(ctx, engine)
	}
	// Single monoclock.Now() sample so primary/secondary/extension
	// all see the same monotonic-raw instant. Clock-skew safety
	// delegated to engineLeaseAckValid (see Coordinate.LeaseRead).
	now := monoclock.Now()
	state := engine.State()
	if engineLeaseAckValid(state, lp.LastQuorumAck(), now, leaseDur) {
		observeLeaseRead(observer, true)
		return lp.AppliedIndex(), nil
	}
	expectedGen := g.lease.generation()
	if g.lease.valid(now) && state == raftengine.StateLeader {
		observeLeaseRead(observer, true)
		return lp.AppliedIndex(), nil
	}
	observeLeaseRead(observer, false)
	idx, err := linearizableReadEngineCtx(ctx, engine)
	if err != nil {
		if isLeadershipLossError(err) {
			g.lease.invalidate()
		}
		return 0, err
	}
	g.lease.extend(now.Add(leaseDur), expectedGen)
	return idx, nil
}

func (c *ShardedCoordinator) Clock() *HLC {
	return c.clock
}

func (c *ShardedCoordinator) groupForKey(key []byte) (*ShardGroup, bool) {
	gid, ok := c.router.ResolveGroup(key)
	if !ok {
		return nil, false
	}
	g, ok := c.groups[gid]
	return g, ok
}

// routeAndGroupForKey is groupForKey + the resolved RouteID. Read
// entry points that observe into keyviz call this so the GetRoute
// lookup runs once instead of twice (Gemini round-1 nit on PR #661).
// Leadership-only callers (IsLeaderForKey / VerifyLeaderForKey /
// RaftLeaderForKey) keep using groupForKey because they don't need
// the route ID.
//
// The gid comes from the partition-aware router (resolver-first,
// engine-fallback) so partitioned-FIFO traffic lands on the
// operator-chosen group. RouteID is read from the engine on the
// normalized key; the resolver does not have a notion of catalog
// RouteID, so partition-resolved keys observe under the engine's
// catalog RouteID for !sqs|route|global. Partition-aware keyviz
// is a Phase 3.D follow-up.
func (c *ShardedCoordinator) routeAndGroupForKey(key []byte) (uint64, *ShardGroup, bool) {
	gid, ok := c.router.ResolveGroup(key)
	if !ok {
		return 0, nil, false
	}
	g, ok := c.groups[gid]
	if !ok {
		return 0, nil, false
	}
	var routeID uint64
	if route, found := c.engine.GetRoute(routeKey(key)); found {
		routeID = route.RouteID
	}
	return routeID, g, true
}

func (c *ShardedCoordinator) engineGroupIDForKey(key []byte) uint64 {
	gid, ok := c.router.ResolveGroup(key)
	if !ok {
		return 0
	}
	return gid
}

// EngineGroupIDForKey reports the Raft group ID that owns key, or 0 when
// the key cannot be routed. Callers that batch lease checks across many
// keys use it to collapse keys sharing a group into a single lease read
// (see GroupRoutableCoordinator). It performs no I/O — only an in-memory
// router lookup — so it is safe on the read hot path.
func (c *ShardedCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	return c.engineGroupIDForKey(key)
}

// groupReadKeysByShardID groups txn read keys by their owning Raft
// group. Returns an error when ANY read key cannot be routed —
// silently skipping unresolvable keys would let a transaction
// commit with an incomplete OCC read-set, which breaks SSI under
// the §5 ShardRouter resolver-first dispatch (codex round-2 P1 on
// PR #715).
//
// The fail-closed semantic the resolver gained in PR 4-B-2 makes
// this path matter: a partitioned-shape read key whose queue is
// missing from --sqsFifoPartitionMap (drift / partial rollout)
// returns gid=0 from c.router.ResolveGroup. If we silently dropped
// those keys, the prewrite Raft entry would carry an empty
// ReadKeys slice for that key, the FSM's read-write conflict
// validation would never see it, and a concurrent write could
// commit alongside a stale read. Surface the routing failure as
// an error so the transaction aborts before any FSM apply.
func (c *ShardedCoordinator) groupReadKeysByShardID(readKeys [][]byte) (map[uint64][][]byte, error) {
	if len(readKeys) == 0 {
		return nil, nil
	}
	grouped := make(map[uint64][][]byte)
	for _, key := range readKeys {
		gid, ok := c.router.ResolveGroup(key)
		if !ok || gid == 0 {
			return nil, errors.Wrapf(ErrInvalidRequest,
				"no route for txn read key %q — recognised-but-"+
					"unresolved partition keys must fail closed to "+
					"preserve OCC read-set integrity", key)
		}
		grouped[gid] = append(grouped[gid], key)
		if alias, ok := c.stagedVisibilityReadKeyAlias(gid, key); ok {
			grouped[gid] = append(grouped[gid], alias)
		}
	}
	return grouped, nil
}

func (c *ShardedCoordinator) groupedReadKeysWithStagedVisibilityMutationAliases(groupedReadKeys map[uint64][][]byte, groupedMutations map[uint64][]*pb.Mutation) map[uint64][][]byte {
	out := groupedReadKeys
	for gid, muts := range groupedMutations {
		for _, mut := range muts {
			if mut == nil {
				continue
			}
			alias, ok := c.stagedVisibilityReadKeyAlias(gid, mut.Key)
			if !ok {
				continue
			}
			if out == nil {
				out = make(map[uint64][][]byte)
			}
			out[gid] = append(out[gid], alias)
		}
	}
	return out
}

func groupedReadKeyCount(grouped map[uint64][][]byte) int {
	var count int
	for _, keys := range grouped {
		count += len(keys)
	}
	return count
}

func (c *ShardedCoordinator) stagedVisibilityReadKeyAlias(gid uint64, key []byte) ([]byte, bool) {
	if c == nil || c.engine == nil || len(key) == 0 {
		return nil, false
	}
	if _, _, ok := distribution.MigrationStagedDataKeyParts(key); ok {
		return nil, false
	}
	route, ok := c.engine.GetRoute(routeKey(key))
	if !ok || route.GroupID != gid || !routeHasStagedVisibility(route) {
		return nil, false
	}
	return distribution.MigrationStagedDataKey(route.MigrationJobID, key), true
}

// validateReadOnlyShards checks read-write conflicts on shards that have
// read keys but no mutations in this transaction. writeGIDs is the set of
// shards that already received a PREPARE with their readKeys attached.
//
// Because these shards have no mutations, we cannot send a PREPARE request
// (the FSM rejects empty mutation lists). Instead we issue a linearizable
// read barrier on each read-only shard's Raft group (ensuring the local
// FSM has applied all committed log entries) and then check LatestCommitTS
// against the local store.
//
// NOTE: This check is performed outside the FSM's applyMu lock, so there
// is a small TOCTOU window between the linearizable read barrier and the
// LatestCommitTS check. A concurrent write that commits in this window may
// go undetected. Full SSI for read-only shards in multi-shard transactions
// would require a dedicated "read-validate" FSM request phase. For
// single-shard transactions and write-shard read keys, validation is fully
// atomic under applyMu.
func (c *ShardedCoordinator) validateReadOnlyShards(ctx context.Context, groupedReadKeys map[uint64][][]byte, writeGIDs []uint64, startTS uint64) error {
	if len(groupedReadKeys) == 0 {
		return nil
	}
	writeSet := make(map[uint64]struct{}, len(writeGIDs))
	for _, gid := range writeGIDs {
		writeSet[gid] = struct{}{}
	}
	for gid, keys := range groupedReadKeys {
		if _, isWrite := writeSet[gid]; isWrite {
			continue
		}
		if err := c.validateReadKeysOnShard(ctx, gid, keys, startTS); err != nil {
			return err
		}
	}
	return nil
}

func (c *ShardedCoordinator) validateReadKeysOnShard(ctx context.Context, gid uint64, keys [][]byte, startTS uint64) error {
	g, ok := c.groups[gid]
	if !ok {
		return nil
	}
	// Linearizable read barrier: wait until the shard's FSM has applied
	// all Raft-committed entries so LatestCommitTS reflects the latest
	// committed state. Without this, a concurrent write that is committed
	// in Raft but not yet applied locally would be invisible.
	if _, err := linearizableReadEngineCtx(ctx, engineForGroup(g)); err != nil {
		return errors.WithStack(err)
	}
	for _, key := range keys {
		ts, exists, err := c.latestCommitTSForReadKeyOnShard(ctx, gid, g, key)
		if err != nil {
			return errors.WithStack(err)
		}
		if exists && ts > startTS {
			return errors.WithStack(store.NewWriteConflictError(key))
		}
	}
	return nil
}

func (c *ShardedCoordinator) latestCommitTSForReadKeyOnShard(ctx context.Context, gid uint64, g *ShardGroup, key []byte) (uint64, bool, error) {
	liveTS, liveExists, err := g.Store.LatestCommitTS(ctx, key)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	route, ok := c.stagedVisibilityRouteForReadKey(gid, key)
	if !ok {
		return liveTS, liveExists, nil
	}
	stagedTS, stagedExists, err := g.Store.LatestCommitTS(ctx, distribution.MigrationStagedDataKey(route.MigrationJobID, key))
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	return maxStagedVisibilityLatestCommitTS(liveTS, liveExists, stagedTS, stagedExists), liveExists || stagedExists, nil
}

func (c *ShardedCoordinator) stagedVisibilityRouteForReadKey(gid uint64, key []byte) (distribution.Route, bool) {
	if c == nil || c.engine == nil {
		return distribution.Route{}, false
	}
	if _, _, ok := distribution.MigrationStagedDataKeyParts(key); ok {
		return distribution.Route{}, false
	}
	route, ok := c.engine.GetRoute(routeKey(key))
	return route, ok && route.GroupID == gid && routeHasStagedVisibility(route)
}

func maxStagedVisibilityLatestCommitTS(liveTS uint64, liveExists bool, stagedTS uint64, stagedExists bool) uint64 {
	if !liveExists {
		return stagedTS
	}
	if !stagedExists || liveTS > stagedTS {
		return liveTS
	}
	return stagedTS
}

var _ Coordinator = (*ShardedCoordinator)(nil)

func validateOperationGroup(reqs *OperationGroup[OP]) error {
	if reqs == nil || len(reqs.Elems) == 0 {
		return ErrInvalidRequest
	}
	for _, e := range reqs.Elems {
		if e == nil {
			return ErrInvalidRequest
		}
	}
	return nil
}

func (c *ShardedCoordinator) requestLogs(ctx context.Context, reqs *OperationGroup[OP]) ([]*pb.Request, error) {
	if reqs.IsTxn {
		return c.txnLogs(ctx, reqs)
	}
	return c.rawLogs(ctx, reqs)
}

func (c *ShardedCoordinator) rawLogs(ctx context.Context, reqs *OperationGroup[OP]) ([]*pb.Request, error) {
	grouped, gids, err := c.groupMutations(reqs.Elems, reqs.KeyVizLabel)
	if err != nil {
		return nil, err
	}

	logs := make([]*pb.Request, 0, len(gids))
	for _, gid := range gids {
		ts, err := c.rawLogTimestamp(ctx)
		if err != nil {
			return nil, err
		}
		if err := c.rejectWriteTimestampFloorMutations(grouped[gid], ts); err != nil {
			return nil, err
		}
		logs = append(logs, &pb.Request{
			IsTxn:     false,
			Phase:     pb.Phase_NONE,
			Ts:        ts,
			Mutations: grouped[gid],
		})
	}
	return logs, nil
}

func (c *ShardedCoordinator) rejectWriteTimestampFloorMutations(muts []*pb.Mutation, commitTS uint64) error {
	if c == nil || c.engine == nil || commitTS == 0 {
		return nil
	}
	for _, mut := range muts {
		if mut == nil || len(mut.Key) == 0 {
			continue
		}
		if err := c.rejectWriteTimestampFloorPointKey(mut.Key, commitTS); err != nil {
			return err
		}
	}
	return nil
}

func (c *ShardedCoordinator) rawLogTimestamp(ctx context.Context) (uint64, error) {
	if c.tsAllocator != nil {
		return 0, nil
	}
	return c.allocateTimestamp(ctx, "allocate sharded raw log ts")
}

func (c *ShardedCoordinator) stampRawRequestTimestamps(ctx context.Context, reqs []*pb.Request) error {
	for _, r := range reqs {
		if r == nil || r.IsTxn || r.Ts != 0 {
			continue
		}
		ts, err := c.allocateTimestamp(ctx, "stamp sharded raw log ts")
		if err != nil {
			return err
		}
		r.Ts = ts
		if err := c.rejectWriteTimestampFloorMutations(r.Mutations, r.Ts); err != nil {
			return err
		}
	}
	return nil
}

func (c *ShardedCoordinator) txnLogs(ctx context.Context, reqs *OperationGroup[OP]) ([]*pb.Request, error) {
	// NOTE: ShardedCoordinator implements distributed transactions directly in
	// Dispatch. txnLogs is retained for compatibility and single-shard helpers.
	grouped, gids, err := c.groupMutations(reqs.Elems, reqs.KeyVizLabel)
	if err != nil {
		return nil, err
	}
	if len(gids) != 1 {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	commitTS, err := c.resolveTxnCommitTS(ctx, reqs.StartTS, reqs.CommitTS)
	if err != nil {
		return nil, err
	}
	return buildTxnLogs(reqs.StartTS, commitTS, grouped, gids, reqs.ObservedRouteVersion)
}

// observeMutation: counted pre-commit, so a mutation that subsequently
// fails its Raft proposal is still recorded — the heatmap reflects
// offered load, not just committed writes (intentional for traffic
// visualisation). The early return keeps the disabled-keyviz hot
// path allocation-free. Reads have their own observeReadKey helper
// (LinearizableReadForKey / LeaseReadForKey).
func (c *ShardedCoordinator) observeMutation(routeID uint64, mut *pb.Mutation, label keyviz.Label) {
	if c.sampler == nil {
		return
	}
	c.sampler.Observe(routeID, mut.Key, keyviz.OpWrite, len(mut.Value), c.keyVizObserveLabel(label))
}

// observeRead records a single linearizable / lease read against the
// route. The full key is passed (not just its length) so the sampler
// can bucket the read into the key's sub-range for the hot-key heatmap;
// the read's valueLen is 0 — the consistency check at this layer
// doesn't fetch data; the actual GetAt on the store happens further
// down the stack and isn't observed yet.
//
// Callers MUST pass an already-resolved routeID (via
// routeAndGroupForKey) so the GetRoute lookup runs once across the
// dispatch path — repeating it here just to compute routeID would
// double the per-read overhead when sampling is enabled
// (Gemini round-1 nit on PR #661).
//
// Adapter-direct read paths (Redis / DynamoDB / S3 hitting
// MVCCStore.GetAt without going through the coordinator) still
// bypass keyviz; sampling those is task B in the design's Phase 2
// follow-up.
func (c *ShardedCoordinator) observeRead(ctx context.Context, routeID uint64, key []byte) {
	if c.sampler == nil {
		return
	}
	c.sampler.Observe(routeID, key, keyviz.OpRead, 0, c.keyVizObserveLabel(keyVizLabelFromContext(ctx)))
}

func (c *ShardedCoordinator) keyVizObserveLabel(label keyviz.Label) keyviz.Label {
	if !c.keyvizLabelsEnabled {
		return keyviz.LabelLegacy
	}
	return label
}

func (c *ShardedCoordinator) groupMutations(reqs []*Elem[OP], label keyviz.Label) (map[uint64][]*pb.Mutation, []uint64, error) {
	grouped := make(map[uint64][]*pb.Mutation)
	for _, req := range reqs {
		if req == nil {
			return nil, nil, ErrInvalidRequest
		}
		mut := elemToMutation(req)
		gid, ok := c.router.ResolveGroup(mut.Key)
		if !ok {
			return nil, nil, errors.Wrapf(ErrInvalidRequest, "no route for key %q", mut.Key)
		}
		// Engine RouteID for keyviz observation; partition-resolved
		// keys observe under the !sqs|route|global RouteID until
		// partition-aware keyviz lands.
		var routeID uint64
		if route, found := c.engine.GetRoute(routeKey(mut.Key)); found {
			routeID = route.RouteID
		}
		c.observeMutation(routeID, mut, label)
		grouped[gid] = append(grouped[gid], mut)
	}
	gids := make([]uint64, 0, len(grouped))
	for gid := range grouped {
		gids = append(gids, gid)
	}
	slices.Sort(gids)
	return grouped, gids, nil
}

func buildTxnLogs(startTS uint64, commitTS uint64, grouped map[uint64][]*pb.Mutation, gids []uint64, observedRouteVersion uint64) ([]*pb.Request, error) {
	logs := make([]*pb.Request, 0, len(gids)*txnPhaseCount)
	for _, gid := range gids {
		muts := grouped[gid]
		primaryKey, keys := primaryKeyAndKeyMutations(muts)
		if len(primaryKey) == 0 {
			return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
		}
		logs = append(logs,
			&pb.Request{
				IsTxn: true,
				Phase: pb.Phase_PREPARE,
				Ts:    startTS,
				Mutations: append([]*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms, CommitTS: 0})},
				}, muts...),
				ObservedRouteVersion: observedRouteVersion,
			},
			&pb.Request{
				IsTxn: true,
				Phase: pb.Phase_COMMIT,
				Ts:    startTS,
				Mutations: append([]*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: primaryKey, LockTTLms: 0, CommitTS: commitTS})},
				}, keys...),
				ObservedRouteVersion: observedRouteVersion,
			},
		)
	}
	return logs, nil
}

func primaryKeyAndKeyMutations(muts []*pb.Mutation) ([]byte, []*pb.Mutation) {
	seen := map[string]struct{}{}
	var primary []byte
	keys := make([]*pb.Mutation, 0, len(muts))
	for _, m := range muts {
		if m == nil || len(m.Key) == 0 {
			continue
		}
		k := string(m.Key)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		if primary == nil || bytes.Compare(m.Key, primary) < 0 {
			primary = m.Key
		}
		keys = append(keys, &pb.Mutation{Op: pb.Op_PUT, Key: m.Key})
	}
	return primary, keys
}

// RunHLCLeaseRenewal periodically proposes a new physical ceiling to every
// shard group's Raft cluster while this node is that group's leader. This
// mirrors the single-shard Coordinate.RunHLCLeaseRenewal behaviour while
// avoiding the multi-shard gap where a non-default group leader could issue
// timestamps without renewing the shared HLC's Raft-backed ceiling.
//
// RunHLCLeaseRenewal blocks until ctx is cancelled; call it in a goroutine.
func (c *ShardedCoordinator) RunHLCLeaseRenewal(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(c.groups) == 0 {
		c.logger().WarnContext(ctx, "hlc lease renewal: no shard groups configured")
		return
	}
	// Use a Timer rather than a Ticker so the next renewal is scheduled
	// relative to launching the previous round. Each per-group proposal has
	// its own timeout and goroutine, so a slow Raft group cannot delay
	// renewal for other groups.
	timer := time.NewTimer(hlcRenewalInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if c.hlcRenewalBlocked != nil && c.hlcRenewalBlocked() {
				timer.Reset(hlcRenewalInterval)
				continue
			}
			c.renewHLCLeases(ctx)
			timer.Reset(hlcRenewalInterval)
		case <-ctx.Done():
			return
		}
	}
}

// renewHLCLeases starts one renewal proposal for every shard group this node
// currently leads. It does not wait for those proposals before returning; the
// returned channel closes when the launched proposals finish and exists for
// tests/diagnostics only.
func (c *ShardedCoordinator) renewHLCLeases(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	for gid, group := range c.groups {
		if group == nil || group.Engine == nil {
			continue
		}
		if group.Engine.State() != raftengine.StateLeader {
			continue
		}
		if !c.startHLCLeaseRenewal(gid) {
			continue
		}
		wg.Add(1)
		go func(gid uint64, group *ShardGroup) {
			defer wg.Done()
			defer c.finishHLCLeaseRenewal(gid)
			pctx, cancel := context.WithTimeout(ctx, hlcRenewalInterval)
			defer cancel()
			c.renewHLCLease(pctx, gid, group)
		}(gid, group)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

func (c *ShardedCoordinator) startHLCLeaseRenewal(gid uint64) bool {
	c.hlcRenewalMu.Lock()
	defer c.hlcRenewalMu.Unlock()
	if c.hlcRenewalInFlight == nil {
		c.hlcRenewalInFlight = make(map[uint64]struct{})
	}
	if _, ok := c.hlcRenewalInFlight[gid]; ok {
		return false
	}
	c.hlcRenewalInFlight[gid] = struct{}{}
	return true
}

func (c *ShardedCoordinator) finishHLCLeaseRenewal(gid uint64) {
	c.hlcRenewalMu.Lock()
	defer c.hlcRenewalMu.Unlock()
	delete(c.hlcRenewalInFlight, gid)
}

// renewHLCLease proposes a fresh physical ceiling on one shard
// group and, on a successful (quorum-acked) propose, warms that group's
// read lease.
//
// The lease-extension base (start) and the invalidation generation are
// sampled BEFORE the propose so the warm-up mirrors leaseRefreshingTxn's
// success branch exactly: the window can only be SHORTER than the true
// safety window, and a leader-loss callback that fires during the
// propose advances the generation so extend refuses to resurrect a
// stale lease. The propose is the SAME quorum confirmation a client
// write goes through, so warming on its success cannot widen the
// lease-read freshness window beyond what a write on this group would.
// This is the background warm-up that flattens the read-only
// lease-expiry sawtooth for the default group on idle-write workloads;
// the lease window/duration semantics are unchanged.
//
// On a leadership-loss propose error the group lease is invalidated
// eagerly, mirroring leaseRefreshingTxn's error branch exactly: when
// Propose returns the loss before the async RegisterLeaderLossCallback
// fires, a stale-warm lease must not survive on a non-leader node for
// the callback latency window. Non-leadership errors (no quorum,
// validation) are NOT leadership signals and must not tear down a warm
// lease -- doing so would force every read onto the slow path.
func (c *ShardedCoordinator) renewHLCLease(ctx context.Context, gid uint64, group *ShardGroup) {
	ceilingMs := time.Now().UnixMilli() + hlcPhysicalWindowMs
	start := monoclock.Now()
	expectedGen := group.lease.generation()
	// Route through the ShardGroup's wrap-aware proposer chain — NOT a
	// direct group.Engine.Propose — so the Stage 6E-2c dynamic wrap
	// applies. Without this, an HLC lease-renewal entry committed
	// post-cutover at `index > raftEnvelopeCutoverIndex` would land
	// cleartext and trip the §6.3 strict-> unwrap on every replica's
	// apply loop (codex P2 round-1).
	if _, err := group.Proposer().Propose(ctx, marshalHLCLeaseRenew(ceilingMs)); err != nil {
		if isLeadershipLossError(err) {
			group.lease.invalidate()
		}
		c.logger().WarnContext(ctx, "hlc lease renewal failed",
			slog.Uint64("group_id", gid),
			slog.Int64("ceiling_ms", ceilingMs),
			slog.Any("err", err),
		)
		return
	}
	if lp, ok := group.Engine.(raftengine.LeaseProvider); ok {
		group.lease.extend(start.Add(lp.LeaseDuration()), expectedGen)
	}
}

func keyMutations(muts []*pb.Mutation) []*pb.Mutation {
	out := make([]*pb.Mutation, 0, len(muts))
	seen := map[string]struct{}{}
	for _, m := range muts {
		if m == nil || len(m.Key) == 0 {
			continue
		}
		k := string(m.Key)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, &pb.Mutation{Op: pb.Op_PUT, Key: m.Key})
	}
	return out
}

package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const redirectForwardTimeout = 5 * time.Second

// dispatchLeaderRetryBudget bounds how long Dispatch keeps absorbing
// transient leader-unavailable errors (no leader resolvable yet, local
// node just stepped down, forwarded RPC bounced off a stale leader).
// gRPC callers expect linearizable semantics — i.e. an operation either
// commits atomically or fails definitively — so the coordinator hides
// raft-internal leader churn behind a bounded retry instead of leaking
// "not leader" / "leader not found" errors out through the API.
//
// The budget is large enough to cover one or two complete re-elections
// even on a slow runner (etcd/raft randomised election timeout up to
// ~1s), and small enough that a permanent loss of quorum still surfaces
// to the caller in bounded time.
const dispatchLeaderRetryBudget = 5 * time.Second

// dispatchLeaderRetryInterval is the poll interval between retries.
const dispatchLeaderRetryInterval = 25 * time.Millisecond

// hlcPhysicalWindowMs is the duration in milliseconds that the Raft-agreed
// physical ceiling extends ahead of the current wall clock. Modelled after
// TiDB's TSO 3-second window: the leader commits ceiling = now + window, and
// renews before the window expires. A new leader inherits the committed ceiling
// so it never issues timestamps that collide with the previous leader's window.
const hlcPhysicalWindowMs int64 = 3_000

// hlcRenewalInterval controls how often the leader proposes a new ceiling.
// Must be less than hlcPhysicalWindowMs to guarantee the window never expires.
const hlcRenewalInterval = 1 * time.Second

// CoordinatorOption is a functional option for Coordinate constructors.
type CoordinatorOption func(*Coordinate)

// WithHLC sets a pre-created HLC on the coordinator. Use this together with
// NewKvFSMWithHLC so the FSM and coordinator share the same clock instance:
// the FSM advances physicalCeiling on every applied HLC lease entry, and the
// coordinator reads it inside Next() to floor new timestamps above the
// previous leader's committed window.
func WithHLC(hlc *HLC) CoordinatorOption {
	return func(c *Coordinate) {
		c.clock = hlc
	}
}

// LeaseReadObserver records lease-read fast-path vs slow-path outcomes
// without coupling kv to a concrete monitoring backend. It is called once
// per LeaseRead invocation that actually evaluates the lease (the initial
// type-assertion/LeaseDuration==0 short-circuits are NOT counted because
// they indicate the engine does not participate in lease reads at all).
//
// Implementations MUST be safe for concurrent use and MUST NOT block; the
// observer is invoked on the Redis GET hot path.
type LeaseReadObserver interface {
	// ObserveLeaseRead is called with hit=true when the lease fast path
	// served the read from local AppliedIndex, or hit=false when the
	// coordinator fell back to a full LinearizableRead (expired lease,
	// engine reported non-leader, or leader-loss callback raced with
	// the request).
	ObserveLeaseRead(hit bool)
}

// WithLeaseReadObserver wires a LeaseReadObserver onto a Coordinate.
// This is the mechanism monitoring uses to surface the lease-hit ratio
// panel on the Redis hot-path dashboard (see
// the "Hot Path" row in monitoring/grafana/dashboards/elastickv-redis-summary.json).
//
// Typed-nil guard: a caller passing a typed-nil pointer
// (e.g. `var o *myObserver; WithLeaseReadObserver(o)`) produces an
// interface value that is NOT equal to nil under the normal `!= nil`
// check, yet invoking ObserveLeaseRead would panic. Normalise here
// with reflect.Value.IsNil so the hot-path nil check in LeaseRead
// stays a single branch on a real nil interface.
func WithLeaseReadObserver(observer LeaseReadObserver) CoordinatorOption {
	return func(c *Coordinate) {
		c.leaseObserver = normalizeLeaseObserver(observer)
	}
}

// normalizeLeaseObserver flattens a typed-nil LeaseReadObserver to an
// untyped nil interface so downstream `observer != nil` checks behave
// as expected.
func normalizeLeaseObserver(observer LeaseReadObserver) LeaseReadObserver {
	if observer == nil {
		return nil
	}
	v := reflect.ValueOf(observer)
	switch v.Kind() { //nolint:exhaustive
	case reflect.Ptr, reflect.Interface, reflect.Func, reflect.Chan, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return nil
		}
	}
	return observer
}

func NewCoordinatorWithEngine(txm Transactional, engine raftengine.Engine, opts ...CoordinatorOption) *Coordinate {
	c := &Coordinate{
		transactionManager: txm,
		engine:             engine,
		clock:              NewHLC(),
		log:                slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	// Register a leader-loss hook so the lease is invalidated the instant
	// the engine notices a state transition out of the leader role,
	// rather than waiting for wall-clock expiry of the current lease.
	// Keep the deregister func so Close() can release the callback
	// slot; owners with a shorter lifetime than the engine (tests,
	// one-shot tools) MUST call Close() to avoid leaking a closure
	// pointing into this Coordinate.
	if lp, ok := engine.(raftengine.LeaseProvider); ok {
		c.deregisterLeaseCb = lp.RegisterLeaderLossCallback(c.lease.invalidate)
	}
	return c
}

// Close releases any engine-side registrations (currently the
// leader-loss callback) held by this Coordinate. It is safe to call
// on a nil receiver and multiple times. Owners whose lifetime matches
// the engine's do not need to call Close; owners who discard the
// Coordinate before closing the engine MUST.
func (c *Coordinate) Close() error {
	if c == nil {
		return nil
	}
	if c.deregisterLeaseCb != nil {
		c.deregisterLeaseCb()
		c.deregisterLeaseCb = nil
	}
	return nil
}

// hlcLeaseEntryLen is the byte length of a serialised HLC lease Raft entry:
// 1 tag byte + 8 bytes big-endian int64 ceiling ms.
const hlcLeaseEntryLen = 9 //nolint:mnd

// marshalHLCLeaseRenew encodes a physical ceiling into a Raft log entry.
// Format: [raftEncodeHLCLease][8 bytes big-endian int64 ceiling ms].
func marshalHLCLeaseRenew(ceilingMs int64) []byte {
	out := make([]byte, hlcLeaseEntryLen)
	out[0] = raftEncodeHLCLease
	binary.BigEndian.PutUint64(out[1:], uint64(ceilingMs)) //nolint:gosec // ceilingMs is a Unix ms timestamp, always positive
	return out
}

type CoordinateResponse struct {
	CommitIndex uint64
}

type Coordinate struct {
	transactionManager Transactional
	engine             raftengine.Engine
	clock              *HLC
	connCache          GRPCConnCache
	log                *slog.Logger
	lease              leaseState
	// deregisterLeaseCb removes the leader-loss callback registered
	// against engine at construction. Long-lived Coordinates don't
	// need to call it (the engine will be closed after them), but
	// short-lived test coordinators sharing an engine MUST invoke
	// Close() to release the callback slot.
	deregisterLeaseCb func()
	// leaseObserver records lease-read hit/miss outcomes for metrics
	// (nil when no observer is attached; LeaseRead short-circuits the
	// nil check so production does not pay an interface call when
	// monitoring is disabled).
	leaseObserver LeaseReadObserver
}

var _ Coordinator = (*Coordinate)(nil)

type Coordinator interface {
	Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error)
	IsLeader() bool
	VerifyLeader() error
	LinearizableRead(ctx context.Context) (uint64, error)
	RaftLeader() string
	IsLeaderForKey(key []byte) bool
	VerifyLeaderForKey(key []byte) error
	RaftLeaderForKey(key []byte) string
	Clock() *HLC
}

// LeaseReadableCoordinator is the optional capability implemented by
// coordinators that participate in the leader-local lease read path
// (see docs/design/2026_04_20_implemented_lease_read.md). Callers that want lease reads
// should type-assert to this interface and fall back to
// LinearizableRead when the assertion fails, following the same
// pattern as raftengine.LeaseProvider. Keeping the lease methods OFF
// the Coordinator interface avoids breaking existing external
// implementations that predate the lease-read feature.
type LeaseReadableCoordinator interface {
	LeaseRead(ctx context.Context) (uint64, error)
	LeaseReadForKey(ctx context.Context, key []byte) (uint64, error)
}

// LeaseReadThrough is a helper that calls LeaseRead when the
// coordinator supports it, falling back to LinearizableRead otherwise.
// Adapter call sites use this so they don't have to repeat the
// type-assertion dance.
func LeaseReadThrough(c Coordinator, ctx context.Context) (uint64, error) {
	if lr, ok := c.(LeaseReadableCoordinator); ok {
		idx, err := lr.LeaseRead(ctx)
		return idx, errors.WithStack(err)
	}
	idx, err := c.LinearizableRead(ctx)
	return idx, errors.WithStack(err)
}

// LeaseReadForKeyThrough is the key-routed counterpart of
// LeaseReadThrough.
func LeaseReadForKeyThrough(c Coordinator, ctx context.Context, key []byte) (uint64, error) {
	if lr, ok := c.(LeaseReadableCoordinator); ok {
		idx, err := lr.LeaseReadForKey(ctx, key)
		return idx, errors.WithStack(err)
	}
	idx, err := c.LinearizableRead(ctx)
	return idx, errors.WithStack(err)
}

func (c *Coordinate) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Validate the request before any use to avoid panics on malformed input.
	// Validation errors are not retryable, so do this once outside the loop.
	if err := validateOperationGroup(reqs); err != nil {
		return nil, err
	}

	// Wrap the actual dispatch in a bounded retry loop so that transient
	// leader-unavailable errors (no leader resolvable yet, local node just
	// stepped down, forwarded RPC bounced off a stale leader) are absorbed
	// inside the coordinator instead of leaking out through the gRPC API.
	// The gRPC contract is linearizable: a single client call either
	// commits atomically or returns a definitive error. "Leader not found"
	// during a re-election is neither, so we wait briefly for the cluster
	// to re-stabilise. Non-leader errors that exceed the retry budget are
	// surfaced unchanged for callers to observe.
	leaderAssignsTS := coordinatorAssignsTimestamps(reqs)
	deadline := time.Now().Add(dispatchLeaderRetryBudget)
	// Reuse a single Timer across retries. time.After would allocate a
	// fresh timer per iteration whose Go runtime entry lingers until the
	// interval elapses, producing a short-term leak proportional to the
	// retry rate. Under heavy mid-dispatch leader churn this is a hot
	// loop, so we Reset the timer in place instead. Go 1.23+ timer
	// semantics make Reset on an unfired/expired Timer safe without an
	// explicit drain.
	timer := time.NewTimer(dispatchLeaderRetryInterval)
	defer timer.Stop()
	// boundedCtx caps every dispatchOnce call by the retry deadline so
	// that a forward RPC inside redirect (which itself uses
	// context.WithTimeout(ctx, redirectForwardTimeout)) can never run
	// past the advertised dispatchLeaderRetryBudget. Without this bound,
	// a near-expiry iteration could legitimately enter dispatchOnce and
	// then sit in cli.Forward for the full 5s redirectForwardTimeout —
	// the wall-clock check between iterations would trip on the next
	// pass, but the offending call has already exceeded the budget.
	// context.WithDeadline picks the earlier of the caller's deadline
	// and ours, so callers with a tighter deadline still get their
	// own cancellation semantics.
	boundedCtx, cancelBounded := context.WithDeadline(ctx, deadline)
	defer cancelBounded()
	var lastResp *CoordinateResponse
	var lastErr error
	for {
		lastResp, lastErr = c.dispatchOnce(boundedCtx, reqs)
		// Caller-supplied ctx cancellation/deadline takes precedence
		// over any error dispatchOnce returned (which may itself wrap
		// context.Canceled / context.DeadlineExceeded propagated
		// through boundedCtx). gRPC clients rely on the wrapped
		// ctx.Err() to distinguish "I gave up" from "system was
		// unavailable".
		if err := ctx.Err(); err != nil {
			return lastResp, errors.WithStack(err)
		}
		if !shouldRetryDispatch(lastErr) {
			return lastResp, lastErr
		}
		if !time.Now().Before(deadline) {
			return lastResp, lastErr
		}
		if leaderAssignsTS {
			// Force dispatchOnce to mint a fresh StartTS on the next
			// attempt; keep CommitTS tied to StartTS by clearing it too
			// (the same invariant dispatchOnce enforces). Re-using a
			// stale StartTS would race the OCC conflict check in
			// fsm.validateConflicts (LatestCommitTS > startTS) against
			// writes that committed during the election window.
			reqs.StartTS = 0
			reqs.CommitTS = 0
		}
		if err := waitForDispatchRetry(ctx, timer, dispatchLeaderRetryInterval); err != nil {
			return lastResp, err
		}
		// Re-check the deadline AFTER the back-off sleep. If the budget
		// expired while we slept, do not start another dispatchOnce —
		// boundedCtx would just cancel it immediately, but exiting here
		// keeps the surfaced error as the last transient leader signal
		// instead of a context-deadline error from inside the gRPC
		// stack.
		if !time.Now().Before(deadline) {
			return lastResp, lastErr
		}
	}
}

// coordinatorAssignsTimestamps reports whether the caller expects the
// coordinator to mint StartTS/CommitTS on this dispatch. When true, each
// retry MUST reset the timestamps back to zero so dispatchOnce re-issues
// against the post-churn leader's HLC.
func coordinatorAssignsTimestamps(reqs *OperationGroup[OP]) bool {
	if !reqs.IsTxn {
		return false
	}
	return reqs.StartTS == 0
}

// shouldRetryDispatch reports whether Dispatch should loop again on the
// error returned by dispatchOnce. Only transient leader-unavailable
// signals qualify; a nil error and every other non-retryable error
// (write conflict, validation, etc.) must surface unchanged.
func shouldRetryDispatch(err error) bool {
	if err == nil {
		return false
	}
	return isTransientLeaderError(err)
}

// waitForDispatchRetry sleeps for interval on timer or until ctx fires,
// whichever comes first. A ctx cancellation returns a wrapped ctx.Err()
// so gRPC clients can distinguish "I gave up waiting" from "cluster is
// unavailable"; timer expiry returns nil and the caller loops again.
func waitForDispatchRetry(ctx context.Context, timer *time.Timer, interval time.Duration) error {
	timer.Reset(interval)
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-timer.C:
		return nil
	}
}

// dispatchOnce runs a single Dispatch attempt without retry. It is the
// transactional unit retried by Dispatch on transient leader errors.
//
// StartTS issuance is intentionally inside the per-attempt path: if a
// previous attempt was rejected by a stale leader, the new leader's
// HLC must mint a fresh StartTS so it floors above any committed
// physical-ceiling lease. Re-using the previous StartTS could violate
// monotonicity across the leader transition.
func (c *Coordinate) dispatchOnce(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if !c.IsLeader() {
		return c.redirect(ctx, reqs)
	}

	if reqs.IsTxn && reqs.StartTS == 0 {
		// Leader-only timestamp issuance to avoid divergence across shards.
		// When the leader assigns StartTS, also clear any caller-provided
		// CommitTS so dispatchTxn generates both timestamps consistently.
		// A caller-supplied CommitTS without a matching StartTS could produce
		// CommitTS <= StartTS (an invalid transaction).
		reqs.StartTS = c.nextStartTS()
		reqs.CommitTS = 0
	}

	// Sample the clock AND the lease generation BEFORE dispatching.
	//   * dispatchStart: any real quorum confirmation happens at or
	//     after this instant, so using it as the lease-extension base
	//     is strictly conservative (window can only be SHORTER than
	//     the actual safety window, never longer). Sampled from the
	//     monotonic-raw clock so NTP slew/step cannot push the lease
	//     past its true safety window (see internal/monoclock).
	//   * expectedGen: if a leader-loss callback fires between this
	//     sample and the post-dispatch extend, the generation will
	//     have advanced; extend(expectedGen) will see the mismatch
	//     and refuse to resurrect the lease. Capturing gen INSIDE
	//     extend would observe the post-invalidate value as current.
	dispatchStart := monoclock.Now()
	expectedGen := c.lease.generation()
	var resp *CoordinateResponse
	var err error
	if reqs.IsTxn {
		resp, err = c.dispatchTxn(reqs.Elems, reqs.ReadKeys, reqs.StartTS, reqs.CommitTS)
	} else {
		resp, err = c.dispatchRaw(reqs.Elems)
	}
	c.refreshLeaseAfterDispatch(resp, err, dispatchStart, expectedGen)
	return resp, err
}

// isTransientLeaderError reports whether err is a transient
// leader-unavailable signal worth retrying inside Dispatch.
//
// Three distinct conditions qualify:
//   - ErrLeaderNotFound — no leader address resolvable on this node yet
//     (election in progress, or the previous leader stepped down and the
//     successor has not propagated). Recoverable as soon as a new leader
//     publishes its identity.
//   - isLeadershipLossError — the etcd/raft engine rejected a Propose
//     because it just lost leadership (ErrNotLeader / ErrLeadershipLost
//     / ErrLeadershipTransferInProgress). Recoverable by re-routing
//     through the new leader (redirect path).
//   - Forwarded "not leader" / "leader not found" strings — when
//     Coordinate.redirect forwards to a stale leader, the destination
//     node returns adapter.ErrNotLeader (its own sentinel) which gRPC
//     transports as a generic Unknown status carrying only the message
//     "not leader". errors.Is cannot traverse that wire boundary, so we
//     fall back to a case-insensitive substring match on the final error
//     text. leaderErrorPhrases enumerates the exact phrases the adapter
//     and coordinator layers emit so the match cannot accidentally
//     swallow an unrelated business-logic error that happens to contain
//     "leader" in its text.
//
// Business-logic failures (write conflict, validation, etc.) are NOT
// covered here — those must surface to the caller unchanged so client
// retry logic can distinguish "system was unavailable" from "your write
// was rejected on its merits".
func isTransientLeaderError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrLeaderNotFound) {
		return true
	}
	if isLeadershipLossError(err) {
		return true
	}
	return hasTransientLeaderPhrase(err)
}

// leaderErrorPhrases is the closed set of wire-level error strings the
// coordinator is willing to treat as transient once the typed sentinel
// has been dropped by a gRPC boundary. Keep this list tight — any
// addition must correspond to an error the system actually emits for
// leader churn, not a generic "failed" message that happens to mention
// leaders.
var leaderErrorPhrases = []string{
	"not leader",       // adapter.ErrNotLeader, raftengine.ErrNotLeader, ErrLeadershipLost messages
	"leader not found", // kv.ErrLeaderNotFound, adapter.ErrLeaderNotFound
}

// hasTransientLeaderPhrase reports whether err.Error() contains one of
// the well-known transient-leader substrings. It is the last resort used
// after errors.Is fails across a gRPC boundary that strips the original
// sentinel chain.
func hasTransientLeaderPhrase(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, phrase := range leaderErrorPhrases {
		if strings.Contains(msg, phrase) {
			return true
		}
	}
	return false
}

// refreshLeaseAfterDispatch extends the lease only when the dispatch
// produced a real Raft commit. CommitIndex == 0 means the underlying
// transaction manager short-circuited (empty-input Commit, no-op
// Abort), and refreshing would be unsound because no quorum
// confirmation happened.
//
// On err != nil the lease is invalidated ONLY when isLeadershipLossError
// reports a real leadership-loss signal (non-leader rejection,
// ErrLeadershipLost, transfer-in-progress). Business-logic failures
// such as write conflicts or validation errors are NOT leadership
// signals and must not invalidate the lease -- doing so would force
// every subsequent read onto the slow LinearizableRead path and defeat
// the lease's purpose. RegisterLeaderLossCallback plus the
// State()==StateLeader fast-path guard cover real leader loss.
func (c *Coordinate) refreshLeaseAfterDispatch(resp *CoordinateResponse, err error, dispatchStart monoclock.Instant, expectedGen uint64) {
	if err != nil {
		// Only invalidate on errors that actually signal leadership
		// loss. Write conflicts and validation errors are business-
		// logic failures that do NOT mean this node stopped being
		// leader; invalidating for them would force every subsequent
		// read into the slow LinearizableRead path and defeat the
		// lease. Engine.RegisterLeaderLossCallback and the fast-path
		// State() == StateLeader guard cover real leader loss.
		if isLeadershipLossError(err) {
			c.lease.invalidate()
		}
		return
	}
	if resp == nil || resp.CommitIndex == 0 {
		return
	}
	lp, ok := c.engine.(raftengine.LeaseProvider)
	if !ok {
		return
	}
	c.lease.extend(dispatchStart.Add(lp.LeaseDuration()), expectedGen)
}

func (c *Coordinate) IsLeader() bool {
	return isLeaderEngine(c.engine)
}

// IsLeaderAcceptingWrites reports whether this node is leader and not currently
// transferring leadership. Background proposers should gate on this to avoid
// piling up dropped proposals while a transfer is in flight.
func (c *Coordinate) IsLeaderAcceptingWrites() bool {
	return isLeaderAcceptingWrites(c.engine)
}

func (c *Coordinate) VerifyLeader() error {
	return verifyLeaderEngine(c.engine)
}

// RaftLeader returns the current leader's address as known by this node.
func (c *Coordinate) RaftLeader() string {
	return leaderAddrFromEngine(c.engine)
}

func (c *Coordinate) Clock() *HLC {
	return c.clock
}

// ProposeHLCLease proposes a new physical ceiling to the Raft cluster.
// Only the current leader should call this; followers silently ignore
// proposals from non-leaders via Raft's leader-only write guarantee.
func (c *Coordinate) ProposeHLCLease(ctx context.Context, ceilingMs int64) error {
	_, err := c.engine.Propose(ctx, marshalHLCLeaseRenew(ceilingMs))
	return errors.WithStack(err)
}

// RunHLCLeaseRenewal runs a background loop that periodically proposes a new
// physical ceiling to the Raft cluster while this node is the leader.
//
// The ceiling is set to now + hlcPhysicalWindowMs (3 s) and is renewed every
// hlcRenewalInterval (1 s), mirroring TiDB's TSO window strategy. Because the
// window is always at least 2 s ahead of any real timestamp, a new leader will
// never issue timestamps that overlap with the previous leader's window.
//
// RunHLCLeaseRenewal blocks until ctx is cancelled; call it in a goroutine.
func (c *Coordinate) RunHLCLeaseRenewal(ctx context.Context) {
	// Use a Timer rather than a Ticker so the next renewal is scheduled
	// relative to the completion of the previous one. This prevents a burst
	// of back-to-back proposals if ProposeHLCLease stalls (e.g. waiting for
	// Raft quorum during a slow leader election).
	timer := time.NewTimer(hlcRenewalInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if c.IsLeaderAcceptingWrites() {
				ceilingMs := time.Now().UnixMilli() + hlcPhysicalWindowMs
				if err := c.ProposeHLCLease(ctx, ceilingMs); err != nil {
					c.log.WarnContext(ctx, "hlc lease renewal failed",
						slog.Int64("ceiling_ms", ceilingMs),
						slog.Any("err", err),
					)
				}
			}
			timer.Reset(hlcRenewalInterval)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Coordinate) IsLeaderForKey(_ []byte) bool {
	return c.IsLeader()
}

func (c *Coordinate) VerifyLeaderForKey(_ []byte) error {
	return c.VerifyLeader()
}

func (c *Coordinate) RaftLeaderForKey(_ []byte) string {
	return c.RaftLeader()
}

func (c *Coordinate) LinearizableRead(ctx context.Context) (uint64, error) {
	return linearizableReadEngineCtx(ctx, c.engine)
}

func (c *Coordinate) LinearizableReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return c.LinearizableRead(ctx)
}

// LeaseRead returns a read fence backed by a leader-local lease when
// available, falling back to a full LinearizableRead when no fast
// path is live or the engine does not implement LeaseProvider.
//
// The PRIMARY lease path is maintained inside the engine from ongoing
// MsgAppResp / MsgHeartbeatResp traffic, so that path does not rely
// on callers sampling time.Now() before the slow path to "extend" a
// lease afterwards. The earlier pre-read sampling was racy under
// congestion: if a LinearizableRead took longer than LeaseDuration,
// the extension would land already expired and the lease never
// warmed up. The engine-driven anchor is refreshed every heartbeat
// independent of read latency.
//
// The SECONDARY caller-side lease remains as a rollout fallback,
// still populated by the original pre-read sampling; it covers the
// narrow window between startup and the first quorum heartbeat round
// landing on the engine.
//
// The returned index is the engine's current applied index (fast
// path) or the index returned by LinearizableRead (slow path).
// Callers that resolve timestamps via store.LastCommitTS may discard
// the value.
func (c *Coordinate) LeaseRead(ctx context.Context) (uint64, error) {
	lp, ok := c.engine.(raftengine.LeaseProvider)
	if !ok {
		return c.LinearizableRead(ctx)
	}
	leaseDur := lp.LeaseDuration()
	if leaseDur <= 0 {
		// Misconfigured tick settings: lease is disabled.
		return c.LinearizableRead(ctx)
	}
	// Single monoclock.Now() sample so the primary, secondary, and
	// extension steps all reason about the same monotonic-raw instant.
	// Using CLOCK_MONOTONIC_RAW (via internal/monoclock) keeps the
	// lease-vs-safety-window comparison immune to NTP rate adjustment
	// and wall-clock steps; a misconfigured time daemon cannot slip
	// the lease past electionTimeout - leaseSafetyMargin.
	now := monoclock.Now()
	state := c.engine.State()
	if engineLeaseAckValid(state, lp.LastQuorumAck(), now, leaseDur) {
		c.observeLeaseRead(true)
		return lp.AppliedIndex(), nil
	}
	// Secondary: caller-side lease warmed by a previous successful
	// slow-path read. Preserved so tests can prime the lease directly
	// and so we still benefit on paths where LastQuorumAck is not yet
	// populated (e.g. very first read after startup before the first
	// quorum heartbeat round has landed).
	expectedGen := c.lease.generation()
	if c.lease.valid(now) && state == raftengine.StateLeader {
		c.observeLeaseRead(true)
		return lp.AppliedIndex(), nil
	}
	c.observeLeaseRead(false)
	idx, err := c.LinearizableRead(ctx)
	if err != nil {
		if isLeadershipLossError(err) {
			c.lease.invalidate()
		}
		return 0, err
	}
	c.lease.extend(now.Add(leaseDur), expectedGen)
	return idx, nil
}

// observeLeaseRead forwards a hit / miss signal to the configured
// LeaseReadObserver. Nil-safe so the LeaseRead hot path stays a
// single branch on a real nil interface (typed-nil is normalised at
// wiring time in WithLeaseReadObserver).
func (c *Coordinate) observeLeaseRead(hit bool) {
	if c.leaseObserver != nil {
		c.leaseObserver.ObserveLeaseRead(hit)
	}
}

// engineLeaseAckValid returns whether the engine-driven lease anchor
// published via LastQuorumAck is fresh enough to serve a leader-local
// read. Enforces the safety contract from raftengine.LeaseProvider:
//   - local state must be Leader
//   - now must be a real reading; a zero now signals that the
//     caller's monoclock.Now() read failed (e.g. clock_gettime denied
//     under seccomp) and the lease fast path must fail closed
//   - ack must be non-zero (a quorum was ever observed)
//   - ack must not be after now (defensive guard: the monotonic-raw
//     clock cannot go backwards, but a zero / bogus ack reading should
//     still fail closed)
//   - now − ack must be strictly less than leaseDur
//
// Both ack and now are monoclock.Instant readings from
// CLOCK_MONOTONIC_RAW, so the comparison is immune to NTP rate
// adjustment and wall-clock steps. See docs/lease_read_design.md §3.2
// for why the raw monotonic source matters once leaseSafetyMargin is
// tightened below ~5 ms.
func engineLeaseAckValid(state raftengine.State, ack, now monoclock.Instant, leaseDur time.Duration) bool {
	if state != raftengine.StateLeader || now.IsZero() || ack.IsZero() || ack.After(now) {
		return false
	}
	return now.Sub(ack) < leaseDur
}

func (c *Coordinate) LeaseReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return c.LeaseRead(ctx)
}

func (c *Coordinate) nextStartTS() uint64 {
	return c.clock.Next()
}

func (c *Coordinate) dispatchTxn(reqs []*Elem[OP], readKeys [][]byte, startTS uint64, commitTS uint64) (*CoordinateResponse, error) {
	if len(readKeys) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	primary := primaryKeyForElems(reqs)
	if len(primary) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}

	if commitTS == 0 {
		commitTS = c.clock.Next()
		if commitTS <= startTS {
			c.clock.Observe(startTS)
			commitTS = c.clock.Next()
		}
	} else {
		// Observe the caller-provided commitTS so the HLC never issues
		// a smaller timestamp in subsequent calls, preserving monotonicity.
		c.clock.Observe(commitTS)
	}
	if commitTS <= startTS {
		return nil, errors.WithStack(ErrTxnCommitTSRequired)
	}

	// ReadKeys are included in the Raft log entry so the FSM validates
	// read-write conflicts atomically under applyMu, eliminating the TOCTOU
	// window that exists between the adapter's pre-Raft validateReadSet call
	// and FSM application. The adapter's validateReadSet is kept as a fast
	// path to fail early without a Raft round-trip, but the FSM check is
	// the authoritative, serializable validation.
	r, err := c.transactionManager.Commit([]*pb.Request{
		onePhaseTxnRequest(startTS, commitTS, primary, reqs, readKeys),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &CoordinateResponse{
		CommitIndex: r.CommitIndex,
	}, nil
}

func (c *Coordinate) dispatchRaw(req []*Elem[OP]) (*CoordinateResponse, error) {
	muts := make([]*pb.Mutation, 0, len(req))
	for _, elem := range req {
		muts = append(muts, elemToMutation(elem))
	}

	logs := []*pb.Request{{
		IsTxn:     false,
		Phase:     pb.Phase_NONE,
		Ts:        c.clock.Next(),
		Mutations: muts,
	}}

	r, err := c.transactionManager.Commit(logs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &CoordinateResponse{
		CommitIndex: r.CommitIndex,
	}, nil
}

func (c *Coordinate) toRawRequest(req *Elem[OP]) *pb.Request {
	switch req.Op {
	case Put:
		return &pb.Request{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    c.clock.Next(),
			Mutations: []*pb.Mutation{
				{
					Op:    pb.Op_PUT,
					Key:   req.Key,
					Value: req.Value,
				},
			},
		}

	case Del:
		return &pb.Request{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    c.clock.Next(),
			Mutations: []*pb.Mutation{
				{
					Op:  pb.Op_DEL,
					Key: req.Key,
				},
			},
		}

	case DelPrefix:
		return &pb.Request{
			IsTxn: false,
			Phase: pb.Phase_NONE,
			Ts:    c.clock.Next(),
			Mutations: []*pb.Mutation{
				{
					Op:  pb.Op_DEL_PREFIX,
					Key: req.Key,
				},
			},
		}
	}

	panic("unreachable")
}

var ErrInvalidRequest = errors.New("invalid request")

// maxReadKeys caps the number of keys that may appear in a transaction's read
// set. Exceeding this limit is rejected to prevent unbounded memory growth.
const maxReadKeys = 10_000

var ErrLeaderNotFound = errors.New("leader not found")

func (c *Coordinate) redirect(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if len(reqs.Elems) == 0 {
		return nil, ErrInvalidRequest
	}

	addr := leaderAddrFromEngine(c.engine)
	if addr == "" {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}

	conn, err := c.connCache.ConnFor(addr)
	if err != nil {
		return nil, err
	}

	cli := pb.NewInternalClient(conn)

	requests, err := c.buildRedirectRequests(reqs)
	if err != nil {
		return nil, err
	}

	fwdCtx, cancel := context.WithTimeout(ctx, redirectForwardTimeout)
	defer cancel()
	r, err := cli.Forward(fwdCtx, c.toForwardRequest(requests))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if !r.Success {
		return nil, ErrInvalidRequest
	}

	return &CoordinateResponse{
		CommitIndex: r.CommitIndex,
	}, nil
}

func (c *Coordinate) buildRedirectRequests(reqs *OperationGroup[OP]) ([]*pb.Request, error) {
	if !reqs.IsTxn {
		requests := make([]*pb.Request, 0, len(reqs.Elems))
		for _, req := range reqs.Elems {
			requests = append(requests, c.toRawRequest(req))
		}
		return requests, nil
	}
	if len(reqs.ReadKeys) > maxReadKeys {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	primary := primaryKeyForElems(reqs.Elems)
	if len(primary) == 0 {
		return nil, errors.WithStack(ErrTxnPrimaryKeyRequired)
	}
	// When StartTS is absent (leader will assign it), also clear CommitTS
	// so the leader assigns both timestamps consistently. A caller-provided
	// CommitTS without a StartTS would produce an invalid txn where
	// CommitTS <= StartTS (because StartTS=0 at the forwarding site).
	commitTS := reqs.CommitTS
	if reqs.StartTS == 0 {
		commitTS = 0
	}
	return []*pb.Request{
		onePhaseTxnRequest(reqs.StartTS, commitTS, primary, reqs.Elems, reqs.ReadKeys),
	}, nil
}

func (c *Coordinate) toForwardRequest(reqs []*pb.Request) *pb.ForwardRequest {
	if len(reqs) == 0 {
		return nil
	}

	out := &pb.ForwardRequest{
		IsTxn:    reqs[0].IsTxn,
		Requests: make([]*pb.Request, 0, len(reqs)),
	}
	out.Requests = reqs

	return out
}

func elemToMutation(req *Elem[OP]) *pb.Mutation {
	switch req.Op {
	case Put:
		return &pb.Mutation{
			Op:    pb.Op_PUT,
			Key:   req.Key,
			Value: req.Value,
		}
	case Del:
		return &pb.Mutation{
			Op:  pb.Op_DEL,
			Key: req.Key,
		}
	case DelPrefix:
		return &pb.Mutation{
			Op:  pb.Op_DEL_PREFIX,
			Key: req.Key, // prefix (may be empty for "all keys")
		}
	}
	panic("unreachable")
}

func onePhaseTxnRequest(startTS, commitTS uint64, primaryKey []byte, reqs []*Elem[OP], readKeys [][]byte) *pb.Request {
	muts := make([]*pb.Mutation, 0, len(reqs)+1)
	muts = append(muts, txnMetaMutation(primaryKey, 0, commitTS))
	for _, req := range reqs {
		muts = append(muts, elemToMutation(req))
	}
	return &pb.Request{
		IsTxn:     true,
		Phase:     pb.Phase_NONE,
		Ts:        startTS,
		Mutations: muts,
		ReadKeys:  readKeys,
	}
}

func primaryKeyForElems(reqs []*Elem[OP]) []byte {
	var primary []byte
	seen := make(map[string]struct{}, len(reqs))
	for _, e := range reqs {
		if e == nil || len(e.Key) == 0 {
			continue
		}
		k := string(e.Key)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		if primary == nil || bytes.Compare(e.Key, primary) < 0 {
			primary = e.Key
		}
	}
	return primary
}

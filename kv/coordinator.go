package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const redirectForwardTimeout = 5 * time.Second

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

func NewCoordinator(txm Transactional, r *raft.Raft, opts ...CoordinatorOption) *Coordinate {
	return NewCoordinatorWithEngine(txm, hashicorpraftengine.New(r), opts...)
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
}

var _ Coordinator = (*Coordinate)(nil)

type Coordinator interface {
	Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error)
	IsLeader() bool
	VerifyLeader() error
	LinearizableRead(ctx context.Context) (uint64, error)
	LeaseRead(ctx context.Context) (uint64, error)
	RaftLeader() raft.ServerAddress
	IsLeaderForKey(key []byte) bool
	VerifyLeaderForKey(key []byte) error
	LeaseReadForKey(ctx context.Context, key []byte) (uint64, error)
	RaftLeaderForKey(key []byte) raft.ServerAddress
	Clock() *HLC
}

func (c *Coordinate) Dispatch(ctx context.Context, reqs *OperationGroup[OP]) (*CoordinateResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Validate the request before any use to avoid panics on malformed input.
	if err := validateOperationGroup(reqs); err != nil {
		return nil, err
	}

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
	//     the actual safety window, never longer).
	//   * expectedGen: if a leader-loss callback fires between this
	//     sample and the post-dispatch extend, the generation will
	//     have advanced; extend(expectedGen) will see the mismatch
	//     and refuse to resurrect the lease. Capturing gen INSIDE
	//     extend would observe the post-invalidate value as current.
	dispatchStart := time.Now()
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

// refreshLeaseAfterDispatch extends the lease only when the dispatch
// produced a real Raft commit. CommitIndex == 0 means the underlying
// transaction manager short-circuited (empty-input Commit, no-op
// Abort), and refreshing would be unsound because no quorum
// confirmation happened.
//
// On err != nil the lease is invalidated: a Propose error commonly
// signals leadership loss (non-leader rejection, transfer in
// progress, quorum lost, etc.) and the design doc lists
// "any error from engine.Propose" as an invalidation trigger.
func (c *Coordinate) refreshLeaseAfterDispatch(resp *CoordinateResponse, err error, dispatchStart time.Time, expectedGen uint64) {
	if err != nil {
		c.lease.invalidate()
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
func (c *Coordinate) RaftLeader() raft.ServerAddress {
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

func (c *Coordinate) RaftLeaderForKey(_ []byte) raft.ServerAddress {
	return c.RaftLeader()
}

func (c *Coordinate) LinearizableRead(ctx context.Context) (uint64, error) {
	return linearizableReadEngineCtx(ctx, c.engine)
}

func (c *Coordinate) LinearizableReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return c.LinearizableRead(ctx)
}

// LeaseRead returns a read fence backed by a leader-local lease when
// available, falling back to a full LinearizableRead when the lease has
// expired or the underlying engine does not implement LeaseProvider.
//
// The returned index is the engine's current applied index (fast path) or
// the index returned by LinearizableRead (slow path). Callers that resolve
// timestamps via store.LastCommitTS may discard the value.
func (c *Coordinate) LeaseRead(ctx context.Context) (uint64, error) {
	lp, ok := c.engine.(raftengine.LeaseProvider)
	if !ok {
		return c.LinearizableRead(ctx)
	}
	leaseDur := lp.LeaseDuration()
	if leaseDur <= 0 {
		// Misconfigured tick settings (Engine.Open warned about this):
		// the lease can never be valid. Fall back without touching
		// lease state so we do not waste extend/invalidate work.
		return c.LinearizableRead(ctx)
	}
	// Capture time.Now() and the lease generation exactly once before
	// any quorum work. `now` is reused for both the fast-path validity
	// check and (on slow path) the extend base; `expectedGen` guards
	// against a leader-loss invalidation that fires during
	// LinearizableRead from being overwritten by this caller's extend.
	// See Coordinate.Dispatch for the same rationale.
	now := time.Now()
	expectedGen := c.lease.generation()
	if c.lease.valid(now) {
		return lp.AppliedIndex(), nil
	}
	idx, err := c.LinearizableRead(ctx)
	if err != nil {
		c.lease.invalidate()
		return 0, err
	}
	c.lease.extend(now.Add(leaseDur), expectedGen)
	return idx, nil
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

package kv

import (
	"context"
	stderrors "errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultTSORouteRetryBudget   = 5 * time.Second
	defaultTSORouteRetryInterval = 25 * time.Millisecond
)

var (
	ErrTSOGroupRequired       = errors.New("tso: dedicated raft group is required")
	ErrTSOStateRequired       = errors.New("tso: dedicated state machine is required")
	ErrTSOFloorProviderNeeded = errors.New("tso: authoritative commit floor provider is required")
	ErrTSONotLeader           = errors.New("tso: not leader")
	ErrTSOProtocolUnsupported = errors.New("tso: leader does not support durable timestamp windows")
)

// TSOReservation describes one group-0-serialized timestamp window.
// PreviousAllocationFloor is sampled before this request's reservation and is
// used by shadow migration to reject overlapping legacy candidates.
type TSOReservation struct {
	Base                    uint64
	Count                   int
	PreviousAllocationFloor uint64
	CutoverActive           bool
	PhaseDActive            bool
	PhaseDFloor             uint64
}

// TSOReservationAllocator is the migration-aware extension exposed by the
// dedicated group leader. Ordinary callers continue to use TSOAllocator.
type TSOReservationAllocator interface {
	ReserveBatchAfter(context.Context, int, uint64, bool, bool) (TSOReservation, error)
}

type TSOShadowReservationAllocator interface {
	ValidateShadowTimestamp(context.Context, uint64) (TSOReservation, error)
}

type tsoCutoverState interface {
	CutoverActive() bool
}

// RaftTSOAllocator reserves timestamp windows on the dedicated TSO leader.
// A window is returned only after its inclusive end has committed to Raft.
// Failed proposals may leak a local window, but they can never expose an
// uncommitted timestamp or make a later leader reuse a returned timestamp.
type RaftTSOAllocator struct {
	group           *ShardGroup
	clock           *HLC
	state           *TSOStateMachine
	floorProvider   TSOCutoverFloorProvider
	initializedTerm uint64
	mu              sync.Mutex
}

type RaftTSOAllocatorOption func(*RaftTSOAllocator)

func WithTSOCutoverFloorProvider(provider TSOCutoverFloorProvider) RaftTSOAllocatorOption {
	return func(a *RaftTSOAllocator) {
		a.floorProvider = provider
	}
}

func NewRaftTSOAllocator(group *ShardGroup, clock *HLC, opts ...RaftTSOAllocatorOption) (*RaftTSOAllocator, error) {
	if group == nil || group.Engine == nil {
		return nil, errors.WithStack(ErrTSOGroupRequired)
	}
	if clock == nil {
		return nil, errors.WithStack(ErrTSOClockNil)
	}
	if group.TSOState == nil {
		return nil, errors.WithStack(ErrTSOStateRequired)
	}
	a := &RaftTSOAllocator{group: group, clock: clock, state: group.TSOState}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	if a.floorProvider == nil {
		return nil, errors.WithStack(ErrTSOFloorProviderNeeded)
	}
	return a, nil
}

func (a *RaftTSOAllocator) Next(ctx context.Context) (uint64, error) {
	return a.NextBatch(ctx, 1)
}

func (a *RaftTSOAllocator) NextBatch(ctx context.Context, n int) (uint64, error) {
	return a.nextBatchAfter(ctx, n, 0)
}

func (a *RaftTSOAllocator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	return a.NextBatchAfter(ctx, 1, min)
}

func (a *RaftTSOAllocator) NextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return a.nextBatchAfter(ctx, n, min)
}

func (a *RaftTSOAllocator) nextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error) {
	reservation, err := a.ReserveBatchAfter(ctx, n, min, false, false)
	return reservation.Base, err
}

// ReserveBatchAfter serializes floor discovery, the one-way cutover marker,
// and window reservation under the TSO leader. A returned window is always
// above both the caller's minimum and every authoritative data-group commit
// observed when this leader term first serves a request.
func (a *RaftTSOAllocator) ReserveBatchAfter(
	ctx context.Context,
	n int,
	min uint64,
	activateCutover bool,
	activatePhaseD bool,
) (TSOReservation, error) {
	var empty TSOReservation
	if err := validateTSOBatchSize(n); err != nil {
		return empty, err
	}
	if min == ^uint64(0) {
		return empty, errors.WithStack(ErrTxnCommitTSRequired)
	}
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reserveBatchAfterLocked(ctx, n, min, activateCutover, activatePhaseD)
}

func (a *RaftTSOAllocator) reserveBatchAfterLocked(
	ctx context.Context,
	n int,
	min uint64,
	activateCutover bool,
	activatePhaseD bool,
) (TSOReservation, error) {
	var empty TSOReservation
	engine, err := a.verifiedLeader(ctx)
	if err != nil {
		return empty, err
	}
	term := engine.Status().Term
	if term == 0 {
		return empty, errors.Wrap(ErrTSONotLeader, "tso leader has no active term")
	}
	previousFloor, err := a.prepareLeaderTermReservation(ctx, engine, term, activateCutover, activatePhaseD)
	if err != nil {
		return empty, err
	}
	minimum := max(min, previousFloor)
	if minimum > 0 {
		a.clock.Observe(minimum)
	}
	base, err := a.clock.NextBatchFenced(n)
	if err != nil {
		return empty, errors.Wrap(err, "tso reserve local batch")
	}
	end := base + uint64(n) - 1 //nolint:gosec // n is positive and bounded above.
	if err := a.commitAllocationFloor(ctx, engine, end); err != nil {
		return empty, err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, false); err != nil {
		return empty, err
	}
	a.initializedTerm = term
	return TSOReservation{
		Base:                    base,
		Count:                   n,
		PreviousAllocationFloor: previousFloor,
		CutoverActive:           a.state.CutoverActive(),
		PhaseDActive:            a.state.PhaseDActive(),
		PhaseDFloor:             a.state.PhaseDFloor(),
	}, nil
}

func (a *RaftTSOAllocator) prepareLeaderTermReservation(
	ctx context.Context,
	engine raftengine.Engine,
	term uint64,
	activateCutover bool,
	activatePhaseD bool,
) (uint64, error) {
	termFloor, err := a.termCommitFloor(ctx, term)
	if err != nil {
		return 0, err
	}
	previousFloor := max(a.state.AllocationFloor(), termFloor, a.state.PhaseDFloor())
	if err := a.activateDurableMarkers(ctx, engine, previousFloor, activateCutover, activatePhaseD); err != nil {
		return 0, err
	}
	if err := verifyTSOLeaderTerm(ctx, engine, term, true); err != nil {
		return 0, err
	}
	return previousFloor, nil
}

func (a *RaftTSOAllocator) activateDurableMarkers(
	ctx context.Context,
	engine raftengine.Engine,
	previousFloor uint64,
	activateCutover bool,
	activatePhaseD bool,
) error {
	if activateCutover && !a.state.CutoverActive() {
		if err := a.commitCutover(ctx, engine); err != nil {
			return err
		}
	}
	if !activatePhaseD || a.state.PhaseDActive() {
		return nil
	}
	if !a.state.CutoverActive() {
		return errors.Wrap(ErrTSOPhaseDInactive, "phase D activation requires durable cutover")
	}
	return a.commitPhaseD(ctx, engine, previousFloor)
}

func (a *RaftTSOAllocator) termCommitFloor(ctx context.Context, term uint64) (uint64, error) {
	if a.initializedTerm == term {
		return a.state.AllocationFloor(), nil
	}
	floor, err := a.floorProvider.GlobalCommittedTimestampFloor(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "tso initialize leader term commit floor")
	}
	return max(floor, a.state.AllocationFloor()), nil
}

func (a *RaftTSOAllocator) verifiedLeader(ctx context.Context) (raftengine.Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrap(err, "tso reserve batch")
	}
	engine := engineForGroup(a.group)
	if !isLeaderEngine(engine) {
		return nil, errors.WithStack(ErrTSONotLeader)
	}
	if err := verifyLeaderEngineCtx(ctx, engine); err != nil {
		return nil, errors.Wrap(stderrors.Join(ErrTSONotLeader, err), "tso verify leader")
	}
	return engine, nil
}

func verifyTSOLeaderTerm(ctx context.Context, engine raftengine.Engine, expectedTerm uint64, fence bool) error {
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "tso verify leader term")
	}
	if !isLeaderEngine(engine) {
		return errors.WithStack(ErrTSONotLeader)
	}
	if fence {
		if err := verifyLeaderEngineCtx(ctx, engine); err != nil {
			return errors.Wrap(stderrors.Join(ErrTSONotLeader, err), "tso fence leader term")
		}
	}
	status := engine.Status()
	if status.State != raftengine.StateLeader || status.Term != expectedTerm {
		return errors.Wrapf(ErrTSONotLeader,
			"tso leader term changed: expected=%d current=%d state=%v",
			expectedTerm, status.Term, status.State)
	}
	return nil
}

func (a *RaftTSOAllocator) commitAllocationFloor(ctx context.Context, engine raftengine.Engine, end uint64) error {
	if _, err := a.group.Proposer().Propose(ctx, marshalTSOAllocationFloor(end)); err != nil {
		wrapped := errors.Wrap(err, "tso commit allocation floor")
		if tsoProposalLostLeadership(engine, err) {
			return stderrors.Join(ErrTSONotLeader, wrapped)
		}
		return wrapped
	}
	return nil
}

func (a *RaftTSOAllocator) commitCutover(ctx context.Context, engine raftengine.Engine) error {
	if _, err := a.group.Proposer().Propose(ctx, marshalTSOCutover()); err != nil {
		wrapped := errors.Wrap(err, "tso commit cutover marker")
		if tsoProposalLostLeadership(engine, err) {
			return stderrors.Join(ErrTSONotLeader, wrapped)
		}
		return wrapped
	}
	if !a.state.CutoverActive() {
		return errors.New("tso cutover proposal committed without applied marker")
	}
	return nil
}

func (a *RaftTSOAllocator) commitPhaseD(ctx context.Context, engine raftengine.Engine, floor uint64) error {
	if _, err := a.group.Proposer().Propose(ctx, marshalTSOPhaseD(floor)); err != nil {
		wrapped := errors.Wrap(err, "tso commit phase-D marker")
		if tsoProposalLostLeadership(engine, err) {
			return stderrors.Join(ErrTSONotLeader, wrapped)
		}
		return wrapped
	}
	if !a.state.PhaseDActive() || a.state.PhaseDFloor() != floor {
		return errors.New("tso phase-D proposal committed without applied marker")
	}
	return nil
}

func (a *RaftTSOAllocator) ValidateDurableTimestamp(ctx context.Context, timestamp uint64) error {
	ctx = nonNilTSOContext(ctx)
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.verifiedLeader(ctx); err != nil {
		return err
	}
	if !a.state.PhaseDActive() {
		return errors.WithStack(ErrTSOPhaseDInactive)
	}
	floor := a.state.PhaseDFloor()
	end := a.state.AllocationFloor()
	if timestamp == 0 || timestamp > end {
		return errors.Wrapf(ErrTSOTimestampInvalid,
			"timestamp=%d phase_d_floor=%d allocation_floor=%d", timestamp, floor, end)
	}
	if timestamp <= floor {
		return errors.Wrapf(stderrors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD),
			"timestamp=%d phase_d_floor=%d allocation_floor=%d", timestamp, floor, end)
	}
	return nil
}

func (a *RaftTSOAllocator) PhaseDActive() bool {
	return a != nil && a.state != nil && a.state.PhaseDActive()
}

func (a *RaftTSOAllocator) PhaseDRequired() bool {
	return a.PhaseDActive()
}

func (a *RaftTSOAllocator) PhaseDFloor() uint64 {
	if a == nil || a.state == nil {
		return 0
	}
	return a.state.PhaseDFloor()
}

func (a *RaftTSOAllocator) AllocationFloor() uint64 {
	if a == nil || a.state == nil {
		return 0
	}
	return a.state.AllocationFloor()
}

func tsoProposalLostLeadership(engine raftengine.Engine, err error) bool {
	return !isLeaderEngine(engine) || errors.Is(err, raftengine.ErrNotLeader) || isTransientLeaderError(err)
}

func (a *RaftTSOAllocator) IsLeader() bool {
	return a != nil && isLeaderEngine(engineForGroup(a.group))
}

func (a *RaftTSOAllocator) RunLeaseRenewal(ctx context.Context) {
	<-nonNilTSOContext(ctx).Done()
}

type tsoRemoteRequest func(context.Context, string, int, uint64, bool, bool) (TSOReservation, error)

type tsoRemoteValidation func(context.Context, string, uint64) error

// LeaderRoutedTSOAllocator serves local requests on the TSO leader and sends
// follower requests to the leader address published by the group-0 engine.
// It re-resolves that address after transient errors so a leadership change
// does not pin a BatchAllocator refill to a stale endpoint.
type LeaderRoutedTSOAllocator struct {
	local          TSOAllocator
	leader         raftengine.LeaderView
	connCache      GRPCConnCache
	remoteRequest  tsoRemoteRequest
	retryBudget    time.Duration
	retryInterval  time.Duration
	activation     atomic.Uint32
	clock          *HLC
	remoteValidate tsoRemoteValidation
	observer       TSOObserver
}

const (
	tsoActivationInactive uint32 = iota
	tsoActivationCutover
	tsoActivationPhaseD
)

type LeaderRoutedTSOAllocatorOption func(*LeaderRoutedTSOAllocator)

func WithTSOCutoverActivation() LeaderRoutedTSOAllocatorOption {
	return func(a *LeaderRoutedTSOAllocator) { a.setActivation(true, false) }
}

func WithTSOPhaseDActivation() LeaderRoutedTSOAllocatorOption {
	return func(a *LeaderRoutedTSOAllocator) { a.setActivation(true, true) }
}

func WithTSORoutedClock(clock *HLC) LeaderRoutedTSOAllocatorOption {
	return func(a *LeaderRoutedTSOAllocator) { a.clock = clock }
}

func WithTSOObserver(observer TSOObserver) LeaderRoutedTSOAllocatorOption {
	return func(a *LeaderRoutedTSOAllocator) { a.observer = observer }
}

func NewLeaderRoutedTSOAllocator(
	local TSOAllocator,
	leader raftengine.LeaderView,
	opts ...LeaderRoutedTSOAllocatorOption,
) (*LeaderRoutedTSOAllocator, error) {
	if local == nil {
		return nil, errors.WithStack(ErrTSOAllocatorRequired)
	}
	if leader == nil {
		return nil, errors.WithStack(ErrTSOGroupRequired)
	}
	a := &LeaderRoutedTSOAllocator{
		local:         local,
		leader:        leader,
		retryBudget:   defaultTSORouteRetryBudget,
		retryInterval: defaultTSORouteRetryInterval,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	a.remoteRequest = a.requestRemoteBatch
	a.remoteValidate = a.requestRemoteValidation
	return a, nil
}

func (a *LeaderRoutedTSOAllocator) Next(ctx context.Context) (uint64, error) {
	return a.NextBatch(ctx, 1)
}

func (a *LeaderRoutedTSOAllocator) NextBatch(ctx context.Context, n int) (uint64, error) {
	return a.nextBatchAfter(ctx, n, 0)
}

func (a *LeaderRoutedTSOAllocator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	return a.NextBatchAfter(ctx, 1, min)
}

func (a *LeaderRoutedTSOAllocator) NextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return a.nextBatchAfter(ctx, n, min)
}

func (a *LeaderRoutedTSOAllocator) nextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error) {
	activate, activatePhaseD := a.activationState()
	reservation, err := a.nextReservation(ctx, n, min, activate, activatePhaseD, activate)
	if err != nil {
		return 0, err
	}
	a.observeReservation(reservation)
	return reservation.Base, nil
}

func (a *LeaderRoutedTSOAllocator) ValidateShadowTimestamp(ctx context.Context, min uint64) (TSOReservation, error) {
	reservation, err := a.nextReservation(ctx, 1, min, false, false, true)
	if err == nil {
		a.observeReservation(reservation)
	}
	return reservation, err
}

func (a *LeaderRoutedTSOAllocator) nextReservation(
	ctx context.Context,
	n int,
	min uint64,
	activate bool,
	activatePhaseD bool,
	requireReservation bool,
) (TSOReservation, error) {
	var empty TSOReservation
	if err := validateTSOBatchSize(n); err != nil {
		return empty, err
	}
	ctx = nonNilTSOContext(ctx)
	deadline := time.Now().Add(a.retryBudget)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	var lastErr error
	for {
		reservation, err := a.tryBatch(ctx, n, min, activate, activatePhaseD, requireReservation)
		if err == nil {
			return reservation, nil
		}
		lastErr = err
		if !isTransientTSORouteError(err) {
			return empty, err
		}
		if err := waitTSORouteRetry(ctx, a.retryInterval); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				if lastErr != nil {
					return empty, errors.Wrap(stderrors.Join(ctxErr, lastErr), "tso route retry exhausted")
				}
				return empty, errors.Wrap(ctxErr, "tso route retry exhausted")
			}
			return empty, err
		}
	}
}

func (a *LeaderRoutedTSOAllocator) tryBatch(
	ctx context.Context,
	n int,
	min uint64,
	activate bool,
	activatePhaseD bool,
	requireReservation bool,
) (TSOReservation, error) {
	var empty TSOReservation
	if a.local.IsLeader() {
		started := time.Now()
		reservation, err := a.tryLocalBatch(ctx, n, min, activate, activatePhaseD, requireReservation)
		a.observeRequest("reserve", "local", err, time.Since(started))
		return reservation, err
	}
	addr := leaderAddrFromEngine(a.leader)
	if addr == "" {
		err := errors.WithStack(ErrLeaderNotFound)
		a.observeRequest("reserve", "remote", err, 0)
		return empty, err
	}
	started := time.Now()
	reservation, err := a.remoteRequest(ctx, addr, n, min, activate, activatePhaseD)
	if err != nil {
		a.observeRequest("reserve", "remote", err, time.Since(started))
		return empty, err
	}
	if err := validateTSOReservation(reservation, n, min, requireReservation); err != nil {
		a.observeRequest("reserve", "remote", err, time.Since(started))
		return empty, err
	}
	a.observeRequest("reserve", "remote", nil, time.Since(started))
	return reservation, nil
}

func (a *LeaderRoutedTSOAllocator) tryLocalBatch(
	ctx context.Context,
	n int,
	min uint64,
	activate bool,
	activatePhaseD bool,
	requireReservation bool,
) (TSOReservation, error) {
	var empty TSOReservation
	reservationAllocator, ok := a.local.(TSOReservationAllocator)
	if !ok {
		if requireReservation {
			return empty, errors.WithStack(ErrTSOProtocolUnsupported)
		}
		base, err := nextTSOBatchAfter(ctx, a.local, n, min)
		if err != nil {
			return empty, err
		}
		reservation := TSOReservation{Base: base, Count: n}
		return reservation, validateTSOReservation(reservation, n, min, false)
	}
	reservation, err := reservationAllocator.ReserveBatchAfter(ctx, n, min, activate, activatePhaseD)
	if err != nil {
		return empty, errors.Wrap(err, "reserve local TSO window")
	}
	return reservation, validateTSOReservation(reservation, n, min, true)
}

func (a *LeaderRoutedTSOAllocator) requestRemoteBatch(
	ctx context.Context,
	addr string,
	n int,
	min uint64,
	activate bool,
	activatePhaseD bool,
) (TSOReservation, error) {
	var empty TSOReservation
	conn, err := a.connCache.ConnFor(addr)
	if err != nil {
		return empty, errors.Wrap(err, "tso dial leader")
	}
	resp, err := pb.NewDistributionClient(conn).GetTimestamp(ctx, &pb.GetTimestampRequest{
		Count:           uint32(n), //nolint:gosec // n is bounded by maxHLCBatchSize.
		MinTimestamp:    min,
		ActivateCutover: activate,
		ActivatePhaseD:  activatePhaseD,
	})
	if err != nil {
		return empty, errors.Wrap(err, "tso request leader batch")
	}
	count := uint32(n) //nolint:gosec // n is validated before this request.
	if !resp.GetCommittedByDedicatedTso() || resp.GetCount() != count {
		return empty, errors.Wrapf(ErrTSOProtocolUnsupported,
			"leader response committed=%t count=%d, want committed=true count=%d",
			resp.GetCommittedByDedicatedTso(), resp.GetCount(), n)
	}
	if activate && !resp.GetCutoverActive() {
		return empty, errors.Wrap(ErrTSOProtocolUnsupported,
			"leader did not confirm durable TSO cutover")
	}
	if activatePhaseD && !resp.GetPhaseDActive() {
		return empty, errors.Wrap(ErrTSOProtocolUnsupported,
			"leader did not confirm durable TSO phase D")
	}
	return TSOReservation{
		Base:                    resp.GetTimestamp(),
		Count:                   n,
		PreviousAllocationFloor: resp.GetPreviousAllocationFloor(),
		CutoverActive:           resp.GetCutoverActive(),
		PhaseDActive:            resp.GetPhaseDActive(),
		PhaseDFloor:             resp.GetPhaseDFloor(),
	}, nil
}

func (a *LeaderRoutedTSOAllocator) ValidateDurableTimestamp(ctx context.Context, timestamp uint64) error {
	if timestamp == 0 {
		return errors.WithStack(ErrTSOTimestampInvalid)
	}
	ctx = nonNilTSOContext(ctx)
	deadline := time.Now().Add(a.retryBudget)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var lastErr error
	for {
		if a.local.IsLeader() {
			validator, ok := a.local.(DurableTimestampValidator)
			if !ok {
				return errors.WithStack(ErrTSOProtocolUnsupported)
			}
			started := time.Now()
			lastErr = errors.WithStack(validator.ValidateDurableTimestamp(ctx, timestamp))
			a.observeRequest("validate", "local", lastErr, time.Since(started))
		} else {
			addr := leaderAddrFromEngine(a.leader)
			if addr == "" {
				lastErr = errors.WithStack(ErrLeaderNotFound)
				a.observeRequest("validate", "remote", lastErr, 0)
			} else {
				started := time.Now()
				lastErr = a.remoteValidate(ctx, addr, timestamp)
				a.observeRequest("validate", "remote", lastErr, time.Since(started))
			}
		}
		if lastErr == nil {
			return nil
		}
		if !isTransientTSORouteError(lastErr) {
			return lastErr
		}
		if err := waitTSORouteRetry(ctx, a.retryInterval); err != nil {
			return errors.Wrap(stderrors.Join(ctx.Err(), lastErr), "tso durable timestamp validation")
		}
	}
}

func (a *LeaderRoutedTSOAllocator) requestRemoteValidation(ctx context.Context, addr string, timestamp uint64) error {
	conn, err := a.connCache.ConnFor(addr)
	if err != nil {
		return errors.Wrap(err, "tso dial validation leader")
	}
	resp, err := pb.NewDistributionClient(conn).ValidateTimestamp(ctx, &pb.ValidateTimestampRequest{Timestamp: timestamp})
	if err != nil {
		code := status.Code(err)
		if code == codes.OutOfRange {
			return errors.Wrap(stderrors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD), err.Error())
		}
		if code == codes.InvalidArgument {
			return errors.Wrap(ErrTSOTimestampInvalid, err.Error())
		}
		return errors.Wrap(err, "tso validate timestamp at leader")
	}
	if !resp.GetValid() || !resp.GetPhaseDActive() {
		return errors.Wrapf(ErrTSOTimestampInvalid,
			"leader validation valid=%t phase_d_active=%t", resp.GetValid(), resp.GetPhaseDActive())
	}
	return nil
}

func (a *LeaderRoutedTSOAllocator) PhaseDActive() bool {
	state, ok := a.local.(TSOPhaseDState)
	return ok && state.PhaseDActive()
}

func (a *LeaderRoutedTSOAllocator) PhaseDRequired() bool {
	_, activatePhaseD := a.activationState()
	return a != nil && (activatePhaseD || a.PhaseDActive())
}

func (a *LeaderRoutedTSOAllocator) observeReservation(reservation TSOReservation) {
	if a == nil || a.clock == nil || reservation.Base == 0 || reservation.Count <= 0 {
		return
	}
	end := reservation.Base + uint64(reservation.Count) - 1 //nolint:gosec // validated reservation.
	a.clock.Observe(end)
	if a.observer != nil {
		a.observer.ObserveTSODurableState(reservation.CutoverActive, reservation.PhaseDActive)
	}
}

// setActivation atomically publishes the durable markers future reservations
// must request. In-flight reservations may complete under the prior mode; all
// such modes still use group 0, and committed markers remain one-way.
func (a *LeaderRoutedTSOAllocator) setActivation(cutover, phaseD bool) {
	if a == nil {
		return
	}
	state := tsoActivationInactive
	if cutover || phaseD {
		state = tsoActivationCutover
	}
	if phaseD {
		state = tsoActivationPhaseD
	}
	a.activation.Store(state)
}

// promoteActivation advances the requested durable marker without allowing a
// concurrent observer to lower an already-requested Phase D activation.
func (a *LeaderRoutedTSOAllocator) promoteActivation(cutover, phaseD bool) {
	if a == nil {
		return
	}
	target := tsoActivationInactive
	if cutover || phaseD {
		target = tsoActivationCutover
	}
	if phaseD {
		target = tsoActivationPhaseD
	}
	for current := a.activation.Load(); current < target; current = a.activation.Load() {
		if a.activation.CompareAndSwap(current, target) {
			return
		}
	}
}

func (a *LeaderRoutedTSOAllocator) activationState() (bool, bool) {
	if a == nil {
		return false, false
	}
	state := a.activation.Load()
	return state >= tsoActivationCutover, state >= tsoActivationPhaseD
}

func (a *LeaderRoutedTSOAllocator) observeRequest(operation, path string, err error, duration time.Duration) {
	if a == nil || a.observer == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	a.observer.ObserveTSORequest(operation, path, outcome, duration)
}

func validateRoutedTSOWindow(base uint64, n int, min uint64) error {
	if base == 0 || base <= min {
		return errors.Wrapf(ErrTxnCommitTSRequired,
			"tso leader returned base %d at or below minimum %d", base, min)
	}
	size := uint64(n) //nolint:gosec // n was validated as positive and bounded.
	if base > ^uint64(0)-(size-1) {
		return errors.Wrapf(ErrTxnCommitTSRequired,
			"tso leader window base %d count %d overflows", base, n)
	}
	return nil
}

func validateTSOReservation(reservation TSOReservation, n int, min uint64, requireMetadata bool) error {
	if err := validateRoutedTSOWindow(reservation.Base, n, min); err != nil {
		return err
	}
	if reservation.Count != n {
		return errors.Wrapf(ErrTSOProtocolUnsupported,
			"tso reservation count=%d, want %d", reservation.Count, n)
	}
	if requireMetadata && reservation.PreviousAllocationFloor >= reservation.Base {
		return errors.Wrapf(ErrTSOProtocolUnsupported,
			"tso reservation base=%d does not exceed previous floor=%d",
			reservation.Base, reservation.PreviousAllocationFloor)
	}
	return nil
}

func (a *LeaderRoutedTSOAllocator) IsLeader() bool {
	return a != nil && a.local != nil && a.local.IsLeader()
}

func (a *LeaderRoutedTSOAllocator) RunLeaseRenewal(ctx context.Context) {
	if a == nil || a.local == nil {
		<-nonNilTSOContext(ctx).Done()
		return
	}
	a.local.RunLeaseRenewal(ctx)
}

func (a *LeaderRoutedTSOAllocator) Close() error {
	if a == nil {
		return nil
	}
	return a.connCache.Close()
}

func nextTSOBatchAfter(ctx context.Context, alloc TSOAllocator, n int, min uint64) (uint64, error) {
	if after, ok := alloc.(tsoBatchAfterAllocator); ok && min > 0 {
		base, err := after.NextBatchAfter(ctx, n, min)
		return base, errors.Wrap(err, "tso allocate batch after minimum")
	}
	base, err := alloc.NextBatch(ctx, n)
	if err != nil {
		return 0, errors.Wrap(err, "tso allocate batch")
	}
	if base <= min {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return base, nil
}

func isTransientTSORouteError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrTSONotLeader) || errors.Is(err, ErrTSOProtocolUnsupported) || isTransientLeaderError(err) {
		return true
	}
	code := status.Code(err)
	return code == codes.Aborted || code == codes.FailedPrecondition || code == codes.Unavailable
}

func validateTSOBatchSize(n int) error {
	if n <= 0 || n > maxHLCBatchSize {
		return errors.WithStack(ErrInvalidTSOBatchSize)
	}
	return nil
}

func waitTSORouteRetry(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "tso route retry")
	}
}

func nonNilTSOContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// ShadowTimestampAllocator serializes each legacy candidate through group 0
// before returning it. Candidates at or below a prior TSO floor are discarded
// and retried; once the durable cutover marker is active, the allocator returns
// the reserved TSO timestamp directly so rolling restarts cannot mix sources.
type ShadowTimestampAllocator struct {
	legacy   *HLC
	shadow   TSOShadowReservationAllocator
	cutover  tsoCutoverState
	log      *slog.Logger
	observer TSOObserver
}

type ShadowTimestampAllocatorOption func(*ShadowTimestampAllocator)

func WithTSOShadowCutoverState(state tsoCutoverState) ShadowTimestampAllocatorOption {
	return func(a *ShadowTimestampAllocator) { a.cutover = state }
}

func WithTSOShadowObserver(observer TSOObserver) ShadowTimestampAllocatorOption {
	return func(a *ShadowTimestampAllocator) { a.observer = observer }
}

func NewShadowTimestampAllocator(
	legacy *HLC,
	shadow TSOShadowReservationAllocator,
	logger *slog.Logger,
	opts ...ShadowTimestampAllocatorOption,
) (*ShadowTimestampAllocator, error) {
	if legacy == nil {
		return nil, errors.WithStack(ErrTSOClockNil)
	}
	if shadow == nil {
		return nil, errors.WithStack(ErrTSOAllocatorRequired)
	}
	if logger == nil {
		logger = slog.Default()
	}
	a := &ShadowTimestampAllocator{
		legacy: legacy,
		shadow: shadow,
		log:    logger,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	return a, nil
}

func (a *ShadowTimestampAllocator) Next(ctx context.Context) (uint64, error) {
	return a.nextAfter(ctx, 0)
}

func (a *ShadowTimestampAllocator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return a.nextAfter(ctx, min)
}

func (a *ShadowTimestampAllocator) nextAfter(ctx context.Context, min uint64) (uint64, error) {
	ctx = nonNilTSOContext(ctx)
	if a.cutover != nil && a.cutover.CutoverActive() {
		a.observeShadowComparison("cutover_active", 0)
		return a.nextDedicatedAfter(ctx, min)
	}
	a.legacy.Observe(min)
	for {
		if err := ctx.Err(); err != nil {
			return 0, errors.Wrap(err, "tso shadow migration")
		}
		legacyTS, err := a.legacy.NextFenced()
		if err != nil {
			return 0, errors.Wrap(err, "tso shadow allocate legacy timestamp")
		}
		reservation, err := a.shadow.ValidateShadowTimestamp(ctx, legacyTS)
		if err != nil {
			a.observeShadowComparison("error", 0)
			a.log.ErrorContext(ctx, "tso shadow allocation failed",
				slog.Uint64("legacy_ts", legacyTS),
				slog.Any("err", err),
			)
			return 0, errors.Wrap(err, "tso shadow validation")
		}
		a.legacy.Observe(reservation.Base)
		if reservation.CutoverActive {
			a.observeShadowComparison("cutover_active", 0)
			return reservation.Base, nil
		}
		if legacyTS > reservation.PreviousAllocationFloor {
			a.observeShadowComparison("legacy_ahead", legacyTS-reservation.PreviousAllocationFloor)
			return legacyTS, nil
		}
		a.observeShadowComparison("legacy_overlap", reservation.PreviousAllocationFloor-legacyTS)
		a.log.WarnContext(ctx, "tso shadow discarded overlapping legacy timestamp",
			slog.Uint64("legacy_ts", legacyTS),
			slog.Uint64("previous_tso_floor", reservation.PreviousAllocationFloor),
			slog.Uint64("reserved_tso_ts", reservation.Base),
		)
	}
}

func (a *ShadowTimestampAllocator) nextDedicatedAfter(ctx context.Context, min uint64) (uint64, error) {
	alloc, ok := a.shadow.(TimestampAllocator)
	if !ok {
		return 0, errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return nextTimestampAfterFromAllocator(ctx, alloc, min, "tso post-cutover shadow bypass")
}

func (a *ShadowTimestampAllocator) ValidateDurableTimestamp(ctx context.Context, timestamp uint64) error {
	validator, ok := a.shadow.(DurableTimestampValidator)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return errors.WithStack(validator.ValidateDurableTimestamp(ctx, timestamp))
}

func (a *ShadowTimestampAllocator) PhaseDActive() bool {
	state, ok := a.shadow.(TSOPhaseDState)
	return ok && state.PhaseDActive()
}

func (a *ShadowTimestampAllocator) PhaseDRequired() bool {
	state, ok := a.shadow.(TSOPhaseDState)
	return ok && state.PhaseDRequired()
}

func (a *ShadowTimestampAllocator) Close() error {
	return nil
}

func (a *ShadowTimestampAllocator) observeShadowComparison(result string, divergence uint64) {
	if a != nil && a.observer != nil {
		a.observer.ObserveTSOShadowComparison(result, divergence)
	}
}

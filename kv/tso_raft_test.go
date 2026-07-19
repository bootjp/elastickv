package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRaftTSOAllocatorCommitsWindowEndBeforeReturning(t *testing.T) {
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{state: raftengine.StateLeader, leader: raftengine.LeaderInfo{Address: "self"}}
	engine.apply = func(payload []byte) error {
		if result := fsm.Apply(payload); result != nil {
			if err, ok := result.(error); ok {
				return err
			}
			return errors.Newf("unexpected TSO FSM result %T", result)
		}
		return nil
	}
	alloc, err := newTestRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock)
	require.NoError(t, err)

	base, err := alloc.NextBatch(context.Background(), testTSOBatchSize)
	require.NoError(t, err)
	require.NotZero(t, base)

	payloads := engine.proposedPayloads()
	require.Len(t, payloads, 1)
	require.True(t, bytes.HasPrefix(payloads[0], []byte(tsoAllocationFloorEnvelope)))
	wantEnd := base + uint64(testTSOBatchSize) - 1
	require.Equal(t, wantEnd, binary.BigEndian.Uint64(payloads[0][len(tsoAllocationFloorEnvelope):]))

	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, snapshot.Close()) })
	var encoded snapshotBuffer
	_, err = snapshot.WriteTo(&encoded)
	require.NoError(t, err)
	require.Equal(t, wantEnd, binary.BigEndian.Uint64(encoded.Bytes()[hlcLeasePayloadLen:]))
}

func TestRaftTSOAllocatorRequiresCommitFloorProvider(t *testing.T) {
	clock := NewHLC()
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{state: raftengine.StateLeader}

	_, err := NewRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock)
	require.ErrorIs(t, err, ErrTSOFloorProviderNeeded)
}

func TestRaftTSOAllocatorDoesNotReturnFailedReservation(t *testing.T) {
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	engine := &recordingTSOEngine{
		state:      raftengine.StateLeader,
		leader:     raftengine.LeaderInfo{Address: "self"},
		proposeErr: raftengine.ErrNotLeader,
	}
	fsm := NewTSOStateMachine(clock)
	engine.apply = applyTSOTestFSM(fsm)
	alloc, err := newTestRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock)
	require.NoError(t, err)

	_, err = alloc.NextBatch(context.Background(), testTSOBatchSize)
	require.ErrorIs(t, err, ErrTSONotLeader)
	leakedEnd := clock.Current()
	require.NotZero(t, leakedEnd)

	engine.setProposeError(nil)
	base, err := alloc.NextBatch(context.Background(), testTSOBatchSize)
	require.NoError(t, err)
	require.Greater(t, base, leakedEnd)
}

func TestRaftTSOAllocatorInitializesEveryLeaderTermAboveDataFloor(t *testing.T) {
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{
		state:  raftengine.StateLeader,
		leader: raftengine.LeaderInfo{Address: "self"},
		term:   7,
		apply:  applyTSOTestFSM(fsm),
	}
	floor := uint64(time.Now().Add(time.Minute).UnixMilli()) << hlcLogicalBits //nolint:gosec // future test HLC.
	provider := &recordingTSOFloorProvider{floor: floor}
	alloc, err := NewRaftTSOAllocator(
		&ShardGroup{Engine: engine, TSOState: fsm},
		clock,
		WithTSOCutoverFloorProvider(provider),
	)
	require.NoError(t, err)

	first, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.Greater(t, first, floor)
	require.Equal(t, 1, provider.callCount())

	_, err = alloc.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, provider.callCount(), "one leader term must read the data floor once")

	engine.setTerm(8)
	_, err = alloc.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, provider.callCount(), "a new leader term must fence against data groups again")
}

func TestRaftTSOAllocatorRejectsTermChangeDuringCommitFloorRead(t *testing.T) {
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{
		state:  raftengine.StateLeader,
		leader: raftengine.LeaderInfo{Address: "self"},
		term:   1,
		apply:  applyTSOTestFSM(fsm),
	}
	provider := &recordingTSOFloorProvider{afterRead: func() { engine.setTerm(2) }}
	alloc, err := NewRaftTSOAllocator(
		&ShardGroup{Engine: engine, TSOState: fsm},
		clock,
		WithTSOCutoverFloorProvider(provider),
	)
	require.NoError(t, err)

	_, err = alloc.Next(context.Background())
	require.ErrorIs(t, err, ErrTSONotLeader)
	require.Empty(t, engine.proposedPayloads())
}

func TestRaftTSOAllocatorDropsCommittedWindowAfterTermChange(t *testing.T) {
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{
		state:  raftengine.StateLeader,
		leader: raftengine.LeaderInfo{Address: "self"},
		term:   1,
		apply:  applyTSOTestFSM(fsm),
	}
	var changeTerm sync.Once
	engine.afterPropose = func(payload []byte) {
		if bytes.HasPrefix(payload, []byte(tsoAllocationFloorEnvelope)) {
			changeTerm.Do(func() { engine.setTerm(2) })
		}
	}
	alloc, err := newTestRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock)
	require.NoError(t, err)

	_, err = alloc.Next(context.Background())
	require.ErrorIs(t, err, ErrTSONotLeader)
	leakedFloor := fsm.AllocationFloor()
	require.NotZero(t, leakedFloor)

	next, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.Greater(t, next, leakedFloor)
}

func TestRaftTSOAllocatorCommitsCutoverBeforeProductionWindow(t *testing.T) {
	clock := NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(clock)
	engine := &recordingTSOEngine{
		state:  raftengine.StateLeader,
		leader: raftengine.LeaderInfo{Address: "self"},
		term:   1,
		apply:  applyTSOTestFSM(fsm),
	}
	alloc, err := newTestRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, clock)
	require.NoError(t, err)

	reservation, err := alloc.ReserveBatchAfter(context.Background(), testTSOBatchSize, 0, true)
	require.NoError(t, err)
	require.True(t, reservation.CutoverActive)
	payloads := engine.proposedPayloads()
	require.Len(t, payloads, 2)
	require.Equal(t, []byte(tsoCutoverEnvelope), payloads[0])
	require.True(t, bytes.HasPrefix(payloads[1], []byte(tsoAllocationFloorEnvelope)))
}

func TestLeaderRoutedTSOAllocatorReResolvesAfterStaleLeader(t *testing.T) {
	local := &fakeTSOAllocator{leader: false, nextBase: testTSOInitialBase}
	engine := &recordingTSOEngine{state: raftengine.StateFollower, leader: raftengine.LeaderInfo{Address: "old"}}
	alloc, err := NewLeaderRoutedTSOAllocator(local, engine)
	require.NoError(t, err)
	alloc.retryBudget = time.Second
	alloc.retryInterval = time.Millisecond

	var addresses []string
	alloc.remoteRequest = func(_ context.Context, addr string, n int, min uint64, activate bool) (TSOReservation, error) {
		addresses = append(addresses, addr)
		require.Equal(t, testTSOBatchSize, n)
		require.Equal(t, uint64(testTSOInitialBase), min)
		require.False(t, activate)
		if addr == "old" {
			engine.setLeaderAddress("new")
			return TSOReservation{}, status.Error(codes.FailedPrecondition, "tso: not leader")
		}
		return TSOReservation{Base: testTSOInitialBase + 1, Count: n}, nil
	}

	base, err := alloc.NextBatchAfter(context.Background(), testTSOBatchSize, testTSOInitialBase)
	require.NoError(t, err)
	require.Equal(t, uint64(testTSOInitialBase+1), base)
	require.Equal(t, []string{"old", "new"}, addresses)
}

func TestLeaderRoutedTSOAllocatorUsesLocalLeader(t *testing.T) {
	local := &fakeTSOAllocator{leader: true, nextBase: testTSOInitialBase}
	engine := &recordingTSOEngine{state: raftengine.StateLeader, leader: raftengine.LeaderInfo{Address: "self"}}
	alloc, err := NewLeaderRoutedTSOAllocator(local, engine)
	require.NoError(t, err)
	alloc.remoteRequest = func(context.Context, string, int, uint64, bool) (TSOReservation, error) {
		t.Fatal("local TSO leader must not call remote RPC")
		return TSOReservation{}, nil
	}

	base, err := alloc.NextBatch(context.Background(), testTSOBatchSize)
	require.NoError(t, err)
	require.Equal(t, uint64(testTSOInitialBase), base)
}

func TestLeaderRoutedTSOAllocatorRejectsLocalShadowWithoutReservationMetadata(t *testing.T) {
	local := &fakeTSOAllocator{leader: true, nextBase: testTSOInitialBase}
	engine := &recordingTSOEngine{state: raftengine.StateLeader, leader: raftengine.LeaderInfo{Address: "self"}}
	alloc, err := NewLeaderRoutedTSOAllocator(local, engine)
	require.NoError(t, err)

	_, err = alloc.ValidateShadowTimestamp(context.Background(), testTSOInitialBase-1)
	require.ErrorIs(t, err, ErrTSOProtocolUnsupported)
}

func TestLeaderRoutedTSOAllocatorRejectsRemoteWindowAtMinimum(t *testing.T) {
	local := &fakeTSOAllocator{leader: false}
	engine := &recordingTSOEngine{state: raftengine.StateFollower, leader: raftengine.LeaderInfo{Address: "leader"}}
	alloc, err := NewLeaderRoutedTSOAllocator(local, engine)
	require.NoError(t, err)
	alloc.remoteRequest = func(context.Context, string, int, uint64, bool) (TSOReservation, error) {
		return TSOReservation{Base: testTSOInitialBase, Count: testTSOBatchSize}, nil
	}

	_, err = alloc.NextBatchAfter(context.Background(), testTSOBatchSize, testTSOInitialBase)
	require.ErrorIs(t, err, ErrTxnCommitTSRequired)
}

func TestLeaderRoutedTSOAllocatorPreservesDeadlineAfterTransientErrors(t *testing.T) {
	local := &fakeTSOAllocator{leader: false}
	engine := &recordingTSOEngine{state: raftengine.StateFollower, leader: raftengine.LeaderInfo{Address: "leader"}}
	alloc, err := NewLeaderRoutedTSOAllocator(local, engine)
	require.NoError(t, err)
	alloc.retryBudget = time.Second
	alloc.retryInterval = time.Millisecond

	var attempts int
	alloc.remoteRequest = func(context.Context, string, int, uint64, bool) (TSOReservation, error) {
		attempts++
		return TSOReservation{}, status.Error(codes.Unavailable, "leader restarting")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = alloc.NextBatch(ctx, testTSOBatchSize)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Greater(t, attempts, 1)
}

func TestShadowTimestampAllocatorReturnsLegacyAndAdvancesTSO(t *testing.T) {
	legacy := NewHLC()
	legacy.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	shadow := &recordingShadowReservationAllocator{}
	alloc, err := NewShadowTimestampAllocator(legacy, shadow, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, alloc.Close()) })

	legacyTS, err := alloc.NextAfter(context.Background(), testTSOInitialBase)
	require.NoError(t, err)
	require.Greater(t, legacyTS, uint64(testTSOInitialBase))
	min, returned, calls := shadow.values()
	require.Equal(t, legacyTS, min)
	require.Equal(t, legacyTS+1, returned)
	require.Equal(t, 1, calls)
	require.Equal(t, returned, legacy.Current(), "shadow reservation must keep the rollback clock warm")
}

func TestShadowTimestampAllocatorFailsClosedOnShadowError(t *testing.T) {
	legacy := NewHLC()
	legacy.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	shadowErr := errors.New("shadow unavailable")
	shadow := &recordingShadowReservationAllocator{err: shadowErr}
	alloc, err := NewShadowTimestampAllocator(legacy, shadow, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, alloc.Close()) })

	_, err = alloc.Next(context.Background())
	require.ErrorIs(t, err, shadowErr)
}

func TestShadowTimestampAllocatorHonorsDeadlineWhileShadowBlocked(t *testing.T) {
	legacy := NewHLC()
	legacy.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	shadow := &blockingShadowTimestampAllocator{started: make(chan struct{})}
	alloc, err := NewShadowTimestampAllocator(legacy, shadow, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = alloc.Next(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, alloc.Close())
}

func TestShadowTimestampAllocatorDiscardsCandidateBelowPriorFloor(t *testing.T) {
	legacy := NewHLC()
	legacy.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	shadow := &recordingShadowReservationAllocator{overlapFirst: true}
	alloc, err := NewShadowTimestampAllocator(legacy, shadow, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	legacyTS, err := alloc.Next(context.Background())
	require.NoError(t, err)
	_, reserved, calls := shadow.values()
	require.Equal(t, 2, calls)
	require.Equal(t, legacyTS+1, reserved)
}

func TestShadowTimestampAllocatorReturnsTSOAfterDurableCutover(t *testing.T) {
	legacy := NewHLC()
	legacy.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	shadow := &recordingShadowReservationAllocator{cutover: true}
	alloc, err := NewShadowTimestampAllocator(legacy, shadow, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	issued, err := alloc.Next(context.Background())
	require.NoError(t, err)
	legacyCandidate, reserved, calls := shadow.values()
	require.Equal(t, 1, calls)
	require.Equal(t, reserved, issued)
	require.Greater(t, issued, legacyCandidate)
}

func TestShadowAndCutoverAllocatorsSerializeMigrationOnGroupZero(t *testing.T) {
	tsoClock := NewHLC()
	tsoClock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	fsm := NewTSOStateMachine(tsoClock)
	engine := &recordingTSOEngine{
		state:  raftengine.StateLeader,
		leader: raftengine.LeaderInfo{Address: "self"},
		term:   1,
		apply:  applyTSOTestFSM(fsm),
	}
	local, err := newTestRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: fsm}, tsoClock)
	require.NoError(t, err)

	legacyClock := NewHLC()
	legacyClock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	shadowRoute, err := NewLeaderRoutedTSOAllocator(local, engine, WithTSORoutedClock(legacyClock))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shadowRoute.Close()) })
	shadow, err := NewShadowTimestampAllocator(legacyClock, shadowRoute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	legacyIssued, err := shadow.Next(context.Background())
	require.NoError(t, err)
	require.False(t, fsm.CutoverActive())

	cutoverRoute, err := NewLeaderRoutedTSOAllocator(local, engine, WithTSOCutoverActivation())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cutoverRoute.Close()) })
	cutoverIssued, err := cutoverRoute.Next(context.Background())
	require.NoError(t, err)
	require.True(t, fsm.CutoverActive())
	require.Greater(t, cutoverIssued, legacyIssued)

	shadowAfterCutover, err := shadow.Next(context.Background())
	require.NoError(t, err)
	require.Greater(t, shadowAfterCutover, cutoverIssued)
	require.Equal(t, fsm.AllocationFloor(), shadowAfterCutover)
}

type recordingShadowReservationAllocator struct {
	mu           sync.Mutex
	min          uint64
	returned     uint64
	calls        int
	err          error
	overlapFirst bool
	cutover      bool
}

func (a *recordingShadowReservationAllocator) ValidateShadowTimestamp(_ context.Context, min uint64) (TSOReservation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.min = min
	a.calls++
	if a.err != nil {
		return TSOReservation{}, a.err
	}
	a.returned = min + 1
	previousFloor := min - 1
	if a.overlapFirst && a.calls == 1 {
		previousFloor = min
	}
	return TSOReservation{
		Base:                    a.returned,
		Count:                   1,
		PreviousAllocationFloor: previousFloor,
		CutoverActive:           a.cutover,
	}, nil
}

func (a *recordingShadowReservationAllocator) values() (uint64, uint64, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.min, a.returned, a.calls
}

type blockingShadowTimestampAllocator struct {
	once    sync.Once
	started chan struct{}
}

func (a *blockingShadowTimestampAllocator) ValidateShadowTimestamp(ctx context.Context, _ uint64) (TSOReservation, error) {
	a.once.Do(func() { close(a.started) })
	<-ctx.Done()
	return TSOReservation{}, ctx.Err()
}

type recordingTSOEngine struct {
	mu           sync.Mutex
	state        raftengine.State
	leader       raftengine.LeaderInfo
	proposeErr   error
	proposals    [][]byte
	apply        func([]byte) error
	afterPropose func([]byte)
	term         uint64
}

func (e *recordingTSOEngine) Propose(_ context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	e.mu.Lock()
	if e.proposeErr != nil {
		e.mu.Unlock()
		return nil, e.proposeErr
	}
	copyPayload := append([]byte(nil), payload...)
	e.proposals = append(e.proposals, copyPayload)
	if e.apply != nil {
		if err := e.apply(copyPayload); err != nil {
			e.mu.Unlock()
			return nil, err
		}
	}
	afterPropose := e.afterPropose
	e.mu.Unlock()
	if afterPropose != nil {
		afterPropose(copyPayload)
	}
	return &raftengine.ProposalResult{}, nil
}

func (e *recordingTSOEngine) ProposeAdmin(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	return e.Propose(ctx, payload)
}

func (e *recordingTSOEngine) State() raftengine.State {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

func (e *recordingTSOEngine) Leader() raftengine.LeaderInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leader
}

func (e *recordingTSOEngine) VerifyLeader(context.Context) error {
	if e.State() != raftengine.StateLeader {
		return raftengine.ErrNotLeader
	}
	return nil
}

func (e *recordingTSOEngine) LinearizableRead(context.Context) (uint64, error) { return 0, nil }

func (e *recordingTSOEngine) Status() raftengine.Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	term := e.term
	if term == 0 {
		term = 1
	}
	return raftengine.Status{State: e.state, Term: term}
}

func (e *recordingTSOEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (e *recordingTSOEngine) Close() error { return nil }

func (e *recordingTSOEngine) setProposeError(err error) {
	e.mu.Lock()
	e.proposeErr = err
	e.mu.Unlock()
}

func (e *recordingTSOEngine) setLeaderAddress(addr string) {
	e.mu.Lock()
	e.leader.Address = addr
	e.mu.Unlock()
}

func (e *recordingTSOEngine) setTerm(term uint64) {
	e.mu.Lock()
	e.term = term
	e.mu.Unlock()
}

func (e *recordingTSOEngine) proposedPayloads() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]byte, len(e.proposals))
	for i := range e.proposals {
		out[i] = append([]byte(nil), e.proposals[i]...)
	}
	return out
}

type snapshotBuffer struct {
	data []byte
}

func (b *snapshotBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *snapshotBuffer) Bytes() []byte { return b.data }

func applyTSOTestFSM(fsm *TSOStateMachine) func([]byte) error {
	return func(payload []byte) error {
		if result := fsm.Apply(payload); result != nil {
			if err, ok := result.(error); ok {
				return err
			}
			return errors.Newf("unexpected TSO FSM result %T", result)
		}
		return nil
	}
}

type recordingTSOFloorProvider struct {
	mu        sync.Mutex
	floor     uint64
	calls     int
	afterRead func()
}

func newTestRaftTSOAllocator(group *ShardGroup, clock *HLC) (*RaftTSOAllocator, error) {
	return NewRaftTSOAllocator(group, clock,
		WithTSOCutoverFloorProvider(&recordingTSOFloorProvider{}))
}

func (p *recordingTSOFloorProvider) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	p.mu.Lock()
	p.calls++
	floor := p.floor
	afterRead := p.afterRead
	p.mu.Unlock()
	if afterRead != nil {
		afterRead()
	}
	return floor, nil
}

func (p *recordingTSOFloorProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

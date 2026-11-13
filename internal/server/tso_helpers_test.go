package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/tsowiring"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TSO test fakes shared by main tests; internal/tsowiring defines its own
// copies for the tests that moved there.
func newActiveMainTSOCutover(t *testing.T) (*kv.HLC, *kv.TSOStateMachine, *mainTSOEngine, map[uint64]*kv.ShardGroup) {
	t.Helper()
	clock := kv.NewHLC()
	clock.SetPhysicalCeiling(time.Now().Add(time.Minute).UnixMilli())
	fsm := kv.NewTSOStateMachine(clock)
	engine := &mainTSOEngine{state: raftengine.StateLeader, tsoState: fsm}
	group := &kv.ShardGroup{Engine: engine, TSOState: fsm}
	allocator, err := kv.NewRaftTSOAllocator(group, clock, kv.WithTSOCutoverFloorProvider(mainTSOFloorProvider{}))
	require.NoError(t, err)
	_, err = allocator.ReserveBatchAfter(context.Background(), 1, 0, true, false)
	require.NoError(t, err)
	require.True(t, fsm.CutoverActive())
	engine.proposals.Store(0)
	return clock, fsm, engine, map[uint64]*kv.ShardGroup{noderuntime.DedicatedTSORaftGroupID: group}
}

// testTSOFlags returns the registered TSO flag defaults with the given
// legacy stage and a small batch size.
func testTSOFlags(enabled, shadow bool) tsowiring.Flags {
	f := defaultServerFlags().tso
	f.Enabled = enabled
	f.ShadowEnabled = shadow
	f.BatchSize = 8
	return f
}

func newMainTSOCoordinator(clock *kv.HLC, groups map[uint64]*kv.ShardGroup) *kv.ShardedCoordinator {
	return kv.NewShardedCoordinator(distribution.NewEngine(), groups, 1, clock, nil)
}

type mainTSOEngine struct {
	state     raftengine.State
	proposals atomic.Uint64
	tsoState  *kv.TSOStateMachine
}

type mainTSOFloorProvider struct {
	floor uint64
}

type mainTimestampAllocator struct {
	next uint64
}

func (a *mainTimestampAllocator) Next(context.Context) (uint64, error) {
	return a.next, nil
}

type mainForwardTxn struct {
	requests []*pb.Request
}

type mainForwardObservingCoordinator struct {
	*kv.ShardedCoordinator
	observed atomic.Uint64
}

func (c *mainForwardObservingCoordinator) ObserveForwardedRequests(reqs []*pb.Request) {
	for range reqs {
		c.observed.Add(1)
	}
}

func (t *mainForwardTxn) Commit(_ context.Context, reqs []*pb.Request) (*kv.TransactionResponse, error) {
	t.requests = reqs
	return &kv.TransactionResponse{CommitIndex: 1}, nil
}

func (t *mainForwardTxn) Abort(context.Context, []*pb.Request) (*kv.TransactionResponse, error) {
	return &kv.TransactionResponse{}, nil
}

func (p mainTSOFloorProvider) GlobalCommittedTimestampFloor(context.Context) (uint64, error) {
	return p.floor, nil
}

// These fixtures have no data groups; the tests exercise allocator/decorator
// wiring rather than the production data-group activation gate.
func (mainTSOFloorProvider) CheckPhaseDProtocol(context.Context) error { return nil }

func (mainTSOFloorProvider) SealPhaseDCreation(context.Context) error { return nil }

func (mainTSOFloorProvider) PreparePhaseD(context.Context, uint64) error { return nil }

func (mainTSOFloorProvider) FinishPhaseD(context.Context, uint64) error { return nil }

func (e *mainTSOEngine) Propose(_ context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	e.proposals.Add(1)
	if e.tsoState != nil {
		if result := e.tsoState.Apply(payload); result != nil {
			if err, ok := result.(error); ok {
				return nil, err
			}
			return nil, fmt.Errorf("unexpected TSO apply result %T", result)
		}
	}
	return &raftengine.ProposalResult{}, nil
}

func (e *mainTSOEngine) ProposeAdmin(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	return e.Propose(ctx, payload)
}

func (e *mainTSOEngine) State() raftengine.State { return e.state }

func (e *mainTSOEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "self", Address: "127.0.0.1:50051"}
}

func (e *mainTSOEngine) VerifyLeader(context.Context) error {
	if e.state != raftengine.StateLeader {
		return raftengine.ErrNotLeader
	}
	return nil
}

func (e *mainTSOEngine) LinearizableRead(context.Context) (uint64, error) { return 0, nil }

func (e *mainTSOEngine) Status() raftengine.Status {
	return raftengine.Status{State: e.state, Term: 1}
}

func (e *mainTSOEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (e *mainTSOEngine) SnapshotEvery() uint64 { return 0 }

func (e *mainTSOEngine) Close() error { return nil }

// phaseDMainAllocator carries the Phase-D surfaces the forwarded-timestamp
// validation depends on.
type phaseDMainAllocator struct {
	floor uint64
	next  atomic.Uint64
}

func (a *phaseDMainAllocator) Next(context.Context) (uint64, error) {
	return a.floor + a.next.Add(1), nil
}

func (a *phaseDMainAllocator) ValidateDurableTimestamp(_ context.Context, timestamp uint64) error {
	if timestamp <= a.floor {
		return errors.Join(kv.ErrTSOTimestampInvalid, kv.ErrTSOTimestampPrePhaseD)
	}
	return nil
}

func (a *phaseDMainAllocator) PhaseDActive() bool { return true }

func (a *phaseDMainAllocator) PhaseDRequired() bool { return true }

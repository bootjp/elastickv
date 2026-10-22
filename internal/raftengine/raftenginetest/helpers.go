package raftenginetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
)

// NewSingleNode starts a self-bootstrapping engine and registers ordered cleanup.
func NewSingleNode(tb testing.TB, factory raftengine.Factory, sm raftengine.StateMachine) *raftengine.FactoryResult {
	tb.Helper()
	result, err := factory.Create(raftengine.FactoryConfig{
		LocalID: "n1", LocalAddress: "127.0.0.1:0", DataDir: tb.TempDir(),
		Bootstrap: true, StateMachine: sm,
	})
	if err != nil {
		tb.Fatalf("create single-node engine: %v", err)
	}
	tb.Cleanup(func() {
		if err := result.Engine.Close(); err != nil {
			tb.Errorf("close engine: %v", err)
		}
		if result.Close != nil {
			if err := result.Close(); err != nil {
				tb.Errorf("close engine resources: %v", err)
			}
		}
	})
	WaitForLeader(tb, leaderElectionTimeout, result.Engine)
	return result
}

// WaitForLeader returns the index of the first view observed as leader.
func WaitForLeader(tb testing.TB, d time.Duration, views ...raftengine.LeaderView) int {
	tb.Helper()
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	ticker := time.NewTicker(leaderElectionInterval)
	defer ticker.Stop()
	for {
		for i, view := range views {
			if view.State() == raftengine.StateLeader {
				return i
			}
		}
		select {
		case <-deadline.C:
			tb.Fatal("no engine became leader")
			return -1
		case <-ticker.C:
		}
	}
}

// StaticLeaderView implements only LeaderView. Configure it before use.
type StaticLeaderView struct {
	StateValue  raftengine.State
	LeaderValue raftengine.LeaderInfo
	VerifyErr   error
	ReadIndex   uint64
	ReadErr     error
}

func (s *StaticLeaderView) State() raftengine.State       { return s.StateValue }
func (s *StaticLeaderView) Leader() raftengine.LeaderInfo { return s.LeaderValue }
func (s *StaticLeaderView) VerifyLeader(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.VerifyErr != nil {
		return s.VerifyErr
	}
	if s.StateValue != raftengine.StateLeader {
		return raftengine.ErrNotLeader
	}
	return nil
}
func (s *StaticLeaderView) LinearizableRead(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.ReadIndex, s.ReadErr
}

// StubEngine implements exactly Engine, with no optional capabilities.
// Its exported configuration is immutable once calls begin.
type StubEngine struct {
	StaticLeaderView
	ProposeFunc  func(context.Context, []byte) (*raftengine.ProposalResult, error)
	StatusValue  raftengine.Status
	Config       raftengine.Configuration
	ConfigErr    error
	SnapshotStep uint64
}

func (s *StubEngine) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.ProposeFunc != nil {
		return s.ProposeFunc(ctx, data)
	}
	return &raftengine.ProposalResult{}, nil
}
func (s *StubEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return s.Propose(ctx, data)
}
func (s *StubEngine) Status() raftengine.Status { return s.StatusValue }
func (s *StubEngine) Configuration(ctx context.Context) (raftengine.Configuration, error) {
	if err := ctx.Err(); err != nil {
		return raftengine.Configuration{}, err
	}
	cfg := s.Config
	cfg.Servers = append([]raftengine.Server(nil), cfg.Servers...)
	return cfg, s.ConfigErr
}
func (s *StubEngine) SnapshotEvery() uint64 { return s.SnapshotStep }
func (s *StubEngine) Close() error          { return nil }

// RecordedProposal preserves whether the admin entry point was used.
type RecordedProposal struct {
	Data  []byte
	Admin bool
}

// RecordingProposer owns copies of submitted bytes and is safe for parallel calls.
type RecordingProposer struct {
	mu    sync.Mutex
	calls []RecordedProposal
}

func (p *RecordingProposer) record(ctx context.Context, data []byte, admin bool) (*raftengine.ProposalResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, RecordedProposal{Data: append([]byte(nil), data...), Admin: admin})
	return &raftengine.ProposalResult{CommitIndex: uint64(len(p.calls))}, nil
}
func (p *RecordingProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.record(ctx, data, false)
}
func (p *RecordingProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.record(ctx, data, true)
}
func (p *RecordingProposer) Calls() []RecordedProposal {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]RecordedProposal, len(p.calls))
	for i, call := range p.calls {
		out[i] = RecordedProposal{Data: append([]byte(nil), call.Data...), Admin: call.Admin}
	}
	return out
}

type applyingProposer struct {
	mu    sync.Mutex
	sm    raftengine.StateMachine
	index uint64
}

// ApplyingProposer serializes proposals directly into sm, without a Raft cluster.
func ApplyingProposer(sm raftengine.StateMachine) raftengine.Proposer {
	return &applyingProposer{sm: sm}
}
func (p *applyingProposer) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.index++
	return &raftengine.ProposalResult{CommitIndex: p.index, Response: p.sm.Apply(append([]byte(nil), data...))}, nil
}
func (p *applyingProposer) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return p.Propose(ctx, data)
}

// StubLeaseEngine opts into LeaseProvider without enriching the base stub.
type StubLeaseEngine struct {
	StubEngine
	LeaseDurationValue time.Duration
	AppliedIndexValue  uint64
	QuorumAck          monoclock.Instant
	mu                 sync.Mutex
	nextCallback       uint64
	callbacks          map[uint64]func()
}

func (s *StubLeaseEngine) LeaseDuration() time.Duration { return s.LeaseDurationValue }
func (s *StubLeaseEngine) AppliedIndex() uint64         { return s.AppliedIndexValue }
func (s *StubLeaseEngine) LastQuorumAck() monoclock.Instant {
	if s.State() != raftengine.StateLeader {
		return monoclock.Instant{}
	}
	return s.QuorumAck
}
func (s *StubLeaseEngine) RegisterLeaderLossCallback(fn func()) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.callbacks == nil {
		s.callbacks = make(map[uint64]func())
	}
	s.nextCallback++
	id := s.nextCallback
	s.callbacks[id] = fn
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.callbacks, id)
	}
}

// NotifyLeaderLoss invokes registered callbacks without holding the registry lock.
func (s *StubLeaseEngine) NotifyLeaderLoss() {
	s.mu.Lock()
	callbacks := make([]func(), 0, len(s.callbacks))
	for _, fn := range s.callbacks {
		callbacks = append(callbacks, fn)
	}
	s.mu.Unlock()
	for _, fn := range callbacks {
		notifyLeaderLossCallback(fn)
	}
}

func notifyLeaderLossCallback(fn func()) {
	// One broken holder must not prevent other holders from losing their lease.
	defer func() { _ = recover() }()
	fn()
}

var (
	_ raftengine.Engine        = (*StubEngine)(nil)
	_ raftengine.LeaderView    = (*StaticLeaderView)(nil)
	_ raftengine.Proposer      = (*RecordingProposer)(nil)
	_ raftengine.Proposer      = (*applyingProposer)(nil)
	_ raftengine.LeaseProvider = (*StubLeaseEngine)(nil)
)

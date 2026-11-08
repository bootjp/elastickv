// Package runtimehealth reports local stateful readiness without performing writes.
package runtimehealth

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
)

const (
	reasonReady                = "ready"
	DefaultObservationMaxAge   = 3 * time.Second
	DefaultObservationInterval = time.Second
	DefaultStallTimeout        = 30 * time.Second
	DefaultContactTimeout      = 10 * time.Second
)

// Engine exposes applied local state. It must not implement these reads as RPCs.
type Engine interface {
	raftengine.StatusReader
	raftengine.ConfigReader
	ReadinessState() raftengine.ReadinessState
	ReadinessObservedAt() monoclock.Instant
}

type Source struct {
	GroupID uint64
	Engine  func() Engine
}

type Result struct {
	Ready  bool   `json:"ready"`
	Reason string `json:"reason"`
}

type progress struct {
	cut       uint64
	applied   uint64
	advanced  time.Time
	regressed bool
	backlog   bool
}

// Probe retains only observation history; it does not alter Raft or store state.
type observation struct {
	result Result
	at     time.Time
}

type Probe struct {
	latest         atomic.Pointer[observation]
	mu             sync.Mutex
	serving        bool
	localID        string
	sources        []Source
	progress       map[uint64]*progress
	now            func() time.Time
	stallTimeout   time.Duration
	contactTimeout time.Duration
}

func New(localID string) *Probe {
	return &Probe{localID: localID, progress: make(map[uint64]*progress), now: time.Now,
		stallTimeout: DefaultStallTimeout, contactTimeout: DefaultContactTimeout}
}

// MarkServing opens the public-service startup gate after all service preparation.
func (p *Probe) MarkServing() { p.mu.Lock(); p.serving = true; p.mu.Unlock() }

func (p *Probe) SetSources(sources []Source) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sources = append([]Source(nil), sources...)
	p.progress = make(map[uint64]*progress)
}

// Evaluate samples all local groups, including followers. Idle groups remain ready.
func (p *Probe) Evaluate(ctx context.Context) Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.serving || len(p.sources) == 0 || p.localID == "" {
		return unavailable("starting")
	}
	first := Result{Ready: true, Reason: reasonReady}
	for _, source := range p.sources {
		if ctx.Err() != nil {
			return unavailable("observation_failed")
		}
		result := p.evaluateSource(ctx, source)
		if first.Ready && !result.Ready {
			first = result
		}
	}
	if ctx.Err() != nil {
		return unavailable("observation_failed")
	}
	return first
}

func unavailable(reason string) Result { return Result{Reason: reason} }

func lifecycleResult(s raftengine.ReadinessState) Result {
	switch {
	case s.Failed:
		return unavailable("engine_failed")
	case s.Stopped:
		return unavailable("engine_stopped")
	case s.Restoring:
		return unavailable("snapshot_restoring")
	case !s.Started:
		return unavailable("starting")
	default:
		return Result{Ready: true, Reason: reasonReady}
	}
}

func (p *Probe) evaluateSource(ctx context.Context, source Source) Result {
	if source.Engine == nil {
		return unavailable("starting")
	}
	engine := source.Engine()
	if engine == nil {
		return unavailable("starting")
	}
	if result := lifecycleResult(engine.ReadinessState()); !result.Ready {
		return result
	}
	observed := engine.ReadinessObservedAt()
	if observed.IsZero() {
		return unavailable("observation_unavailable")
	}
	age := monoclock.Now().Sub(observed)
	if age < 0 || age >= p.stallTimeout {
		return unavailable("observation_stale")
	}
	status := engine.Status()
	configuration, err := engine.Configuration(ctx)
	if err != nil {
		return unavailable("configuration_unavailable")
	}
	if result := p.memberResult(status, configuration, age); !result.Ready {
		return result
	}
	if result := p.progressResult(source.GroupID, status); !result.Ready {
		return result
	}
	// Do not reopen readiness if snapshot installation or shutdown began during observation.
	return lifecycleResult(engine.ReadinessState())
}

func (p *Probe) memberResult(s raftengine.Status, configuration raftengine.Configuration, age time.Duration) Result {
	voters := voterIDs(configuration)
	if _, ok := voters[p.localID]; !ok {
		return unavailable("not_a_voter")
	}
	if _, ok := voters[s.Leader.ID]; !ok {
		return unavailable("leader_unknown")
	}
	if s.PendingConfChange {
		return unavailable("membership_changing")
	}
	return p.roleResult(s, age)
}

func voterIDs(configuration raftengine.Configuration) map[string]struct{} {
	voters := make(map[string]struct{}, len(configuration.Servers))
	for _, member := range configuration.Servers {
		if member.Suffrage == "voter" && member.ID != "" {
			voters[member.ID] = struct{}{}
		}
	}
	return voters
}

func (p *Probe) roleResult(s raftengine.Status, age time.Duration) Result {
	switch s.State {
	case raftengine.StateLeader:
		if s.Leader.ID != p.localID {
			return unavailable("leader_unknown")
		}
	case raftengine.StateFollower:
		if !p.recentContact(s.LastContact, age) {
			return unavailable("leader_contact_stale")
		}
	case raftengine.StateCandidate, raftengine.StateShutdown, raftengine.StateUnknown:
		return unavailable("not_participating")
	default:
		return unavailable("not_participating")
	}
	return Result{Ready: true, Reason: reasonReady}
}

func (p *Probe) recentContact(contact, age time.Duration) bool {
	return contact >= 0 && age >= 0 && contact <= p.contactTimeout && age <= p.contactTimeout-contact
}

func (p *Probe) observeProgress(id uint64, s raftengine.Status, now time.Time) *progress {
	history := p.progress[id]
	if history == nil {
		history = &progress{cut: s.CommitIndex, applied: s.AppliedIndex, advanced: now}
		p.progress[id] = history
	}
	backlog := s.AppliedIndex < s.CommitIndex || s.FSMPending > 0
	if backlog && !history.backlog {
		history.advanced = now
	}
	history.backlog = backlog
	if s.AppliedIndex < history.applied {
		history.regressed = true
	}
	if s.AppliedIndex > history.applied {
		history.applied = s.AppliedIndex
		history.advanced = now
	}
	return history
}

func (p *Probe) progressResult(id uint64, s raftengine.Status) Result {
	now := p.now()
	history := p.observeProgress(id, s, now)
	if history.regressed || s.AppliedIndex > s.CommitIndex {
		return unavailable("apply_regressed")
	}
	if s.AppliedIndex < s.LastSnapshotIndex || s.AppliedIndex < history.cut {
		return unavailable("catching_up")
	}
	if s.AppliedIndex == s.CommitIndex && s.FSMPending == 0 {
		history.advanced = now
		return Result{Ready: true, Reason: reasonReady}
	}
	if now.Sub(history.advanced) >= p.stallTimeout {
		return unavailable("apply_stalled")
	}
	return Result{Ready: true, Reason: reasonReady}
}

// Run owns the single observation worker. HTTP handlers never acquire the history
// mutex or start additional workers, even when an engine's local lock blocks.
func (p *Probe) Run(ctx context.Context) {
	ticker := time.NewTicker(DefaultObservationInterval)
	defer ticker.Stop()
	for {
		sample, cancel := context.WithTimeout(ctx, observationTimeout)
		result := p.Evaluate(sample)
		cancel()
		p.latest.Store(&observation{result: result, at: p.now()})
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Probe) Current(ctx context.Context) Result {
	if ctx.Err() != nil {
		return unavailable("observation_failed")
	}
	value := p.latest.Load()
	if value == nil {
		return unavailable("starting")
	}
	age := p.now().Sub(value.at)
	if age < 0 || age > DefaultObservationMaxAge {
		return unavailable("observation_stale")
	}
	return value.result
}

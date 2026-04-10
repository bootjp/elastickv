package hashicorp

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

const defaultProposalTimeout = time.Second
const unknownLastContact = time.Duration(-1)

var errNilEngine = errors.New("raft engine is not configured")

type Engine struct {
	raft *raft.Raft

	barrierMu   sync.Mutex    // serialises Barrier execution (slow path only)
	barrierTerm atomic.Uint64 // term of last successful Barrier (lock-free read)
}

func New(r *raft.Raft) *Engine {
	if r == nil {
		return nil
	}
	return &Engine{raft: r}
}

func (e *Engine) Close() error {
	return nil
}

func (e *Engine) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	timeout, err := timeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if e == nil || e.raft == nil {
		return nil, errors.WithStack(errNilEngine)
	}

	af := e.raft.Apply(data, timeout)
	if err := af.Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, errors.WithStack(err)
	}

	return &raftengine.ProposalResult{
		CommitIndex: af.Index(),
		Response:    af.Response(),
	}, nil
}

func (e *Engine) State() raftengine.State {
	if e == nil || e.raft == nil {
		return raftengine.StateUnknown
	}

	switch e.raft.State() {
	case raft.Follower:
		return raftengine.StateFollower
	case raft.Candidate:
		return raftengine.StateCandidate
	case raft.Leader:
		return raftengine.StateLeader
	case raft.Shutdown:
		return raftengine.StateShutdown
	default:
		return raftengine.StateUnknown
	}
}

func (e *Engine) Leader() raftengine.LeaderInfo {
	if e == nil || e.raft == nil {
		return raftengine.LeaderInfo{}
	}

	addr, id := e.raft.LeaderWithID()
	return raftengine.LeaderInfo{
		ID:      string(id),
		Address: string(addr),
	}
}

func (e *Engine) VerifyLeader(ctx context.Context) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if e == nil || e.raft == nil {
		return errors.WithStack(errNilEngine)
	}

	if err := e.raft.VerifyLeader().Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return ctxErr
		}
		return errors.WithStack(err)
	}
	return nil
}

// readIndexPollInterval is the interval between AppliedIndex polls while
// waiting for the FSM to catch up to the commit index. 10ms balances
// latency against CPU overhead from polling the atomic AppliedIndex.
const readIndexPollInterval = 10 * time.Millisecond

func (e *Engine) CheckServing(ctx context.Context) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if e == nil || e.raft == nil {
		return errors.WithStack(errNilEngine)
	}
	if e.State() != raftengine.StateLeader {
		return errors.WithStack(raft.ErrNotLeader)
	}
	return nil
}

// LinearizableRead blocks until the local FSM has applied all entries up to
// the current commit index, guaranteeing that a subsequent local read
// observes the latest committed state (linearizable read). It first
// ensures that at least one Barrier has been issued since the last
// leadership transition (Raft §5.4.2), then records CommitIndex, and
// finally confirms leadership via a quorum check (VerifyLeader) so that
// a partitioned leader cannot return stale data. When Barrier was
// executed in this call, VerifyLeader is skipped because Barrier already
// implies a quorum commit.
func (e *Engine) LinearizableRead(ctx context.Context) (uint64, error) {
	if e == nil || e.raft == nil {
		return 0, errors.WithStack(errNilEngine)
	}
	if e.raft.State() != raft.Leader {
		return 0, errors.WithStack(raft.ErrNotLeader)
	}

	// Raft §5.4.2: ensure at least one Barrier has been issued in the
	// current term so that CommitIndex is authoritative.
	performedBarrier, err := e.ensureBarrierForTerm(ctx)
	if err != nil {
		return 0, err
	}

	// Record CommitIndex before the quorum check. The ReadIndex protocol
	// requires the leadership verification to happen after the read index
	// has been determined. When Barrier was just performed it already
	// confirmed leadership via quorum, so VerifyLeader is redundant.
	commitIndex := e.raft.CommitIndex()
	if !performedBarrier {
		if err := e.VerifyLeader(ctx); err != nil {
			return 0, err
		}
	}

	return e.waitForApplied(ctx, commitIndex)
}

// waitForApplied blocks until the FSM has applied all entries up to the
// given commit index, or the context is cancelled.
func (e *Engine) waitForApplied(ctx context.Context, commitIndex uint64) (uint64, error) {
	if e.raft.AppliedIndex() >= commitIndex {
		return commitIndex, nil
	}

	ticker := time.NewTicker(readIndexPollInterval)
	defer ticker.Stop()

	for {
		if e.raft.AppliedIndex() >= commitIndex {
			return commitIndex, nil
		}
		select {
		case <-ctx.Done():
			return 0, errors.WithStack(ctx.Err())
		case <-ticker.C:
		}
	}
}

// ensureBarrierForTerm issues a single Barrier per Raft term so that
// CommitIndex reflects all entries from previous terms (Raft §5.4.2).
// It returns true when a Barrier was actually performed (which implies a
// quorum commit), false when the current term already has a valid Barrier.
//
// The fast path uses an atomic load of barrierTerm to avoid mutex
// contention on concurrent reads. Only the slow path (new term detected)
// acquires barrierMu to serialise a single Barrier per term.
func (e *Engine) ensureBarrierForTerm(ctx context.Context) (bool, error) {
	// Fast path: lock-free check using the atomically published barrierTerm.
	term, err := parseTermFromStats(e.raft.Stats())
	if err != nil {
		return false, err
	}
	if e.barrierTerm.Load() >= term {
		return false, nil
	}

	return e.executeBarrier(ctx)
}

// executeBarrier is the slow path of ensureBarrierForTerm: it acquires
// barrierMu so that only one goroutine runs Barrier per term.
func (e *Engine) executeBarrier(ctx context.Context) (bool, error) {
	e.barrierMu.Lock()
	defer e.barrierMu.Unlock()

	// Re-check under lock; another goroutine may have completed Barrier.
	term, err := parseTermFromStats(e.raft.Stats())
	if err != nil {
		return false, err
	}
	if e.barrierTerm.Load() >= term {
		return false, nil
	}

	if e.raft.State() != raft.Leader {
		return false, errors.WithStack(raft.ErrNotLeader)
	}

	timeout, err := timeoutFromContext(ctx)
	if err != nil {
		return false, err
	}
	if err := e.raft.Barrier(timeout).Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return false, ctxErr
		}
		return false, errors.WithStack(err)
	}

	e.barrierTerm.Store(term)
	return true, nil
}

func (e *Engine) Status() raftengine.Status {
	if e == nil || e.raft == nil {
		return raftengine.Status{State: raftengine.StateUnknown}
	}

	stats := e.raft.Stats()
	state := e.State()
	return raftengine.Status{
		State:             state,
		Leader:            e.Leader(),
		Term:              parseUint(stats["term"]),
		CommitIndex:       e.raft.CommitIndex(),
		AppliedIndex:      e.raft.AppliedIndex(),
		LastLogIndex:      e.raft.LastIndex(),
		LastSnapshotIndex: parseUint(stats["last_snapshot_index"]),
		FSMPending:        parseUint(stats["fsm_pending"]),
		NumPeers:          parseUint(stats["num_peers"]),
		LastContact:       lastContact(state, e.raft.LastContact()),
	}
}

func (e *Engine) Configuration(ctx context.Context) (raftengine.Configuration, error) {
	if err := contextErr(ctx); err != nil {
		return raftengine.Configuration{}, err
	}
	if e == nil || e.raft == nil {
		return raftengine.Configuration{}, errors.WithStack(errNilEngine)
	}

	future := e.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return raftengine.Configuration{}, ctxErr
		}
		return raftengine.Configuration{}, errors.WithStack(err)
	}

	cfg := raftengine.Configuration{Servers: make([]raftengine.Server, 0, len(future.Configuration().Servers))}
	for _, server := range future.Configuration().Servers {
		cfg.Servers = append(cfg.Servers, raftengine.Server{
			ID:       string(server.ID),
			Address:  string(server.Address),
			Suffrage: normalizeSuffrage(server.Suffrage),
		})
	}
	return cfg, nil
}

func (e *Engine) AddVoter(ctx context.Context, id string, address string, prevIndex uint64) (uint64, error) {
	timeout, err := timeoutFromContext(ctx)
	if err != nil {
		return 0, err
	}
	if e == nil || e.raft == nil {
		return 0, errors.WithStack(errNilEngine)
	}

	future := e.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), prevIndex, timeout)
	if err := future.Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, errors.WithStack(err)
	}
	return future.Index(), nil
}

func (e *Engine) RemoveServer(ctx context.Context, id string, prevIndex uint64) (uint64, error) {
	timeout, err := timeoutFromContext(ctx)
	if err != nil {
		return 0, err
	}
	if e == nil || e.raft == nil {
		return 0, errors.WithStack(errNilEngine)
	}

	future := e.raft.RemoveServer(raft.ServerID(id), prevIndex, timeout)
	if err := future.Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, errors.WithStack(err)
	}
	return future.Index(), nil
}

func (e *Engine) TransferLeadership(ctx context.Context) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if e == nil || e.raft == nil {
		return errors.WithStack(errNilEngine)
	}

	if err := e.raft.LeadershipTransfer().Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return ctxErr
		}
		return errors.WithStack(err)
	}
	return nil
}

func (e *Engine) TransferLeadershipToServer(ctx context.Context, id string, address string) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if e == nil || e.raft == nil {
		return errors.WithStack(errNilEngine)
	}

	if err := e.raft.LeadershipTransferToServer(raft.ServerID(id), raft.ServerAddress(address)).Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return ctxErr
		}
		return errors.WithStack(err)
	}
	return nil
}

func timeoutFromContext(ctx context.Context) (time.Duration, error) {
	if ctx == nil {
		return defaultProposalTimeout, nil
	}
	if err := contextErr(ctx); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return defaultProposalTimeout, nil
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return 0, errors.WithStack(context.DeadlineExceeded)
	}
	return timeout, nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func normalizeSuffrage(s raft.ServerSuffrage) string {
	switch s {
	case raft.Voter:
		return "voter"
	case raft.Nonvoter:
		return "nonvoter"
	case raft.Staging:
		return "staging"
	default:
		return "unknown"
	}
}

func parseUint(raw string) uint64 {
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

var errTermParse = errors.New("failed to determine raft term from stats")

func parseTermFromStats(stats map[string]string) (uint64, error) {
	raw, ok := stats["term"]
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, errors.WithStack(errTermParse)
	}
	term := parseUint(raw)
	if term == 0 {
		return 0, errors.WithStack(errTermParse)
	}
	return term, nil
}

func lastContact(state raftengine.State, last time.Time) time.Duration {
	if state == raftengine.StateLeader {
		return 0
	}
	if last.IsZero() {
		return unknownLastContact
	}
	return time.Since(last)
}

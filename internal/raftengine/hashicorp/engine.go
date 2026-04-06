package hashicorp

import (
	"context"
	"strconv"
	"strings"
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

func (e *Engine) LinearizableRead(ctx context.Context) (uint64, error) {
	if err := e.VerifyLeader(ctx); err != nil {
		return 0, err
	}

	timeout, err := timeoutFromContext(ctx)
	if err != nil {
		return 0, err
	}
	if err := e.raft.Barrier(timeout).Error(); err != nil {
		if ctxErr := contextErr(ctx); ctxErr != nil {
			return 0, ctxErr
		}
		return 0, errors.WithStack(err)
	}
	return e.raft.CommitIndex(), nil
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

func lastContact(state raftengine.State, last time.Time) time.Duration {
	if state == raftengine.StateLeader {
		return 0
	}
	if last.IsZero() {
		return unknownLastContact
	}
	return time.Since(last)
}

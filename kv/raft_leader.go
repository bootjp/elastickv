package kv

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/singleflight"
)

// HashiCorp Raft's leader lease is used for leader step-down, not as a read
// lease. We therefore keep only a very short local cache of successful
// VerifyLeader probes to collapse bursty calls without treating the lease as
// authoritative for serving reads.
//
// The cache window is intentionally shorter than the leader lease used in this
// repository's runtime/test configs (50ms-100ms).
const raftLeaderVerifySuccessCacheTTL = 20 * time.Millisecond

type raftLeaderVerifier interface {
	State() raft.RaftState
	VerifyLeader() raft.Future
}

type raftLeaderVerifyCache struct {
	ttl time.Duration

	states sync.Map // map[raftLeaderVerifier]*raftLeaderVerifyState
}

type raftLeaderVerifyState struct {
	group      singleflight.Group
	verifiedAt atomic.Int64
}

var defaultRaftLeaderVerifyCache = newRaftLeaderVerifyCache(raftLeaderVerifySuccessCacheTTL)

func newRaftLeaderVerifyCache(ttl time.Duration) *raftLeaderVerifyCache {
	return &raftLeaderVerifyCache{ttl: ttl}
}

func verifyRaftLeader(r *raft.Raft) error {
	if r == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return defaultRaftLeaderVerifyCache.verify(r)
}

func (c *raftLeaderVerifyCache) verify(r raftLeaderVerifier) error {
	if r.State() != raft.Leader {
		c.clear(r)
		return errors.WithStack(raft.ErrNotLeader)
	}

	state := c.stateFor(r)
	if c.isFresh(state.verifiedAt.Load()) {
		return nil
	}

	_, err, _ := state.group.Do("verify", func() (any, error) {
		if r.State() != raft.Leader {
			state.verifiedAt.Store(0)
			return nil, errors.WithStack(raft.ErrNotLeader)
		}
		if c.isFresh(state.verifiedAt.Load()) {
			return nil, nil
		}
		if err := r.VerifyLeader().Error(); err != nil {
			state.verifiedAt.Store(0)
			return nil, errors.WithStack(err)
		}
		state.verifiedAt.Store(time.Now().UnixNano())
		return nil, nil
	})
	return errors.WithStack(err)
}

func (c *raftLeaderVerifyCache) stateFor(r raftLeaderVerifier) *raftLeaderVerifyState {
	if v, ok := c.states.Load(r); ok {
		if state, ok := v.(*raftLeaderVerifyState); ok {
			return state
		}
		return &raftLeaderVerifyState{}
	}
	state := &raftLeaderVerifyState{}
	actual, _ := c.states.LoadOrStore(r, state)
	if stored, ok := actual.(*raftLeaderVerifyState); ok {
		return stored
	}
	return state
}

func (c *raftLeaderVerifyCache) clear(r raftLeaderVerifier) {
	if v, ok := c.states.Load(r); ok {
		if state, ok := v.(*raftLeaderVerifyState); ok {
			state.verifiedAt.Store(0)
		}
	}
}

func (c *raftLeaderVerifyCache) isFresh(verifiedAt int64) bool {
	if c == nil || c.ttl <= 0 || verifiedAt == 0 {
		return false
	}
	return time.Since(time.Unix(0, verifiedAt)) < c.ttl
}

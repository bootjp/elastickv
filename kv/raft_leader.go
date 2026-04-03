package kv

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"golang.org/x/sync/singleflight"
)

// Linearizable read fences need a fresh quorum verification for each read. The
// shared verifier therefore deduplicates only concurrent VerifyLeader probes by
// default; time-based reuse of successful probes is disabled unless a non-zero
// TTL is requested explicitly.
//
// HashiCorp Raft's leader lease remains a step-down mechanism, not a read
// lease.
const raftLeaderVerifySuccessCacheTTL time.Duration = 0

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
	return verifyFreshRaftLeader(r)
}

func verifyRaftLeaderCached(r *raft.Raft) error {
	if r == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	return defaultRaftLeaderVerifyCache.verify(r)
}

func UnregisterRaftLeaderVerifier(r *raft.Raft) {
	if r == nil {
		return
	}
	defaultRaftLeaderVerifyCache.unregister(r)
}

func (c *raftLeaderVerifyCache) verify(r raftLeaderVerifier) error {
	state := c.stateFor(r)
	if r.State() != raft.Leader {
		state.verifiedAt.Store(0)
		return errors.WithStack(raft.ErrNotLeader)
	}
	if c.isFresh(state.verifiedAt.Load()) {
		return nil
	}

	_, err, _ := state.group.Do("verify", func() (any, error) {
		if err := verifyFreshRaftLeader(r); err != nil {
			state.verifiedAt.Store(0)
			return nil, err
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
		c.states.Delete(r)
	}
	state := &raftLeaderVerifyState{}
	actual, _ := c.states.LoadOrStore(r, state)
	if stored, ok := actual.(*raftLeaderVerifyState); ok {
		return stored
	}
	return state
}

func verifyFreshRaftLeader(r raftLeaderVerifier) error {
	if r == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	if r.State() != raft.Leader {
		return errors.WithStack(raft.ErrNotLeader)
	}
	return errors.WithStack(r.VerifyLeader().Error())
}

func (c *raftLeaderVerifyCache) unregister(r raftLeaderVerifier) {
	if c == nil || r == nil {
		return
	}
	c.states.Delete(r)
}

func (c *raftLeaderVerifyCache) isFresh(verifiedAt int64) bool {
	if c == nil || c.ttl <= 0 || verifiedAt == 0 {
		return false
	}
	now := time.Now().UnixNano()
	if now < verifiedAt {
		return false
	}
	return now-verifiedAt < c.ttl.Nanoseconds()
}

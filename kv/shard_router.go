package kv

import (
	"sync"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// ShardRouter routes requests to multiple raft groups based on key ranges.
// It does not provide transactional guarantees across shards; commits are executed
// per shard and failures may leave partial results.
type ShardRouter struct {
	engine *distribution.Engine
	mu     sync.RWMutex
	groups map[uint64]Transactional
}

// NewShardRouter creates a new router.
func NewShardRouter(e *distribution.Engine) *ShardRouter {
	return &ShardRouter{
		engine: e,
		groups: make(map[uint64]Transactional),
	}
}

// Register associates a raft group ID with a Transactional.
func (s *ShardRouter) Register(group uint64, tm Transactional) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups[group] = tm
}

func (s *ShardRouter) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	return s.process(reqs, func(tm Transactional, rs []*pb.Request) (*TransactionResponse, error) {
		return tm.Commit(rs)
	})
}

// Abort dispatches aborts to the correct raft group.
func (s *ShardRouter) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	return s.process(reqs, func(tm Transactional, rs []*pb.Request) (*TransactionResponse, error) {
		return tm.Abort(rs)
	})
}

func (s *ShardRouter) process(reqs []*pb.Request, fn func(Transactional, []*pb.Request) (*TransactionResponse, error)) (*TransactionResponse, error) {
	grouped, err := s.groupRequests(reqs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var max uint64
	for gid, rs := range grouped {
		tm, ok := s.getGroup(gid)
		if !ok {
			return nil, errors.Wrapf(ErrInvalidRequest, "unknown group %d", gid)
		}
		r, err := fn(tm, rs)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if r.CommitIndex > max {
			max = r.CommitIndex
		}
	}
	return &TransactionResponse{CommitIndex: max}, nil
}

func (s *ShardRouter) getGroup(id uint64) (Transactional, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tm, ok := s.groups[id]
	return tm, ok
}

func (s *ShardRouter) groupRequests(reqs []*pb.Request) (map[uint64][]*pb.Request, error) {
	batches := make(map[uint64][]*pb.Request)
	for _, r := range reqs {
		if len(r.Mutations) == 0 {
			return nil, ErrInvalidRequest
		}
		key := r.Mutations[0].Key
		route, ok := s.engine.GetRoute(key)
		if !ok {
			return nil, errors.Wrapf(ErrInvalidRequest, "no route for key %q", key)
		}
		batches[route.GroupID] = append(batches[route.GroupID], r)
	}
	return batches, nil
}

var _ Transactional = (*ShardRouter)(nil)

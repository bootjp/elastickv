package kv

import (
	"sync"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// ShardedTransactionManager routes requests to multiple raft groups based on key ranges.
type ShardedTransactionManager struct {
	engine *distribution.Engine
	mu     sync.RWMutex
	groups map[uint64]Transactional
}

// NewShardedTransactionManager creates a new manager.
func NewShardedTransactionManager(e *distribution.Engine) *ShardedTransactionManager {
	return &ShardedTransactionManager{
		engine: e,
		groups: make(map[uint64]Transactional),
	}
}

// Register associates a raft group ID with a Transactional.
func (s *ShardedTransactionManager) Register(group uint64, tm Transactional) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups[group] = tm
}

// Commit dispatches requests to the correct raft group.
func (s *ShardedTransactionManager) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
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
		r, err := tm.Commit(rs)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if r.CommitIndex > max {
			max = r.CommitIndex
		}
	}
	return &TransactionResponse{CommitIndex: max}, nil
}

// Abort dispatches aborts to the correct raft group.
func (s *ShardedTransactionManager) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
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
		r, err := tm.Abort(rs)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if r.CommitIndex > max {
			max = r.CommitIndex
		}
	}
	return &TransactionResponse{CommitIndex: max}, nil
}

func (s *ShardedTransactionManager) getGroup(id uint64) (Transactional, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tm, ok := s.groups[id]
	return tm, ok
}

func (s *ShardedTransactionManager) groupRequests(reqs []*pb.Request) (map[uint64][]*pb.Request, error) {
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

var _ Transactional = (*ShardedTransactionManager)(nil)

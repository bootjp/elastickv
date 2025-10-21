package kv

import (
	"context"
	"sync"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// ShardRouter routes requests to multiple raft groups based on key ranges.
// It does not provide transactional guarantees across shards; commits are executed
// per shard and failures may leave partial results.
type ShardRouter struct {
	engine *distribution.Engine
	mu     sync.RWMutex
	groups map[uint64]*routerGroup
}

type routerGroup struct {
	tm    Transactional
	store store.Store
}

// NewShardRouter creates a new router.
func NewShardRouter(e *distribution.Engine) *ShardRouter {
	return &ShardRouter{
		engine: e,
		groups: make(map[uint64]*routerGroup),
	}
}

// Register associates a raft group ID with its transactional manager and store.
func (s *ShardRouter) Register(group uint64, tm Transactional, st store.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups[group] = &routerGroup{tm: tm, store: st}
}

func (s *ShardRouter) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	return s.process(reqs, func(g *routerGroup, rs []*pb.Request) (*TransactionResponse, error) {
		return g.tm.Commit(rs)
	})
}

// Abort dispatches aborts to the correct raft group.
func (s *ShardRouter) Abort(reqs []*pb.Request) (*TransactionResponse, error) {
	return s.process(reqs, func(g *routerGroup, rs []*pb.Request) (*TransactionResponse, error) {
		return g.tm.Abort(rs)
	})
}

func (s *ShardRouter) process(reqs []*pb.Request, fn func(*routerGroup, []*pb.Request) (*TransactionResponse, error)) (*TransactionResponse, error) {
	grouped, err := s.groupRequests(reqs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var max uint64
	var errs error
	for gid, rs := range grouped {
		g, ok := s.getGroup(gid)
		if !ok {
			err := errors.Wrapf(ErrInvalidRequest, "unknown group %d", gid)
			errs = errors.CombineErrors(errs, err)
			continue
		}
		r, err := fn(g, rs)
		if err != nil {
			errs = errors.CombineErrors(errs, errors.WithStack(err))
			continue
		}
		if r.CommitIndex > max {
			max = r.CommitIndex
		}
	}
	resp := &TransactionResponse{CommitIndex: max}
	return resp, errs
}

func (s *ShardRouter) getGroup(id uint64) (*routerGroup, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.groups[id]
	return g, ok
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

// Get retrieves a key routed to the correct shard.
func (s *ShardRouter) Get(ctx context.Context, key []byte) ([]byte, error) {
	route, ok := s.engine.GetRoute(key)
	if !ok {
		return nil, errors.Wrapf(ErrInvalidRequest, "no route for key %q", key)
	}
	g, ok := s.getGroup(route.GroupID)
	if !ok {
		return nil, errors.Wrapf(ErrInvalidRequest, "unknown group %d", route.GroupID)
	}
	v, err := g.store.Get(ctx, key)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return v, nil
}

var _ Transactional = (*ShardRouter)(nil)

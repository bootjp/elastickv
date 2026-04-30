package kv

import (
	"context"
	"sync"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// PartitionResolver maps a key to its owning Raft group when the
// key belongs to a partition-scheme keyspace (e.g. SQS HT-FIFO,
// where each (queue, partition) pair lives on a different group).
// ShardRouter consults the resolver before falling through to the
// byte-range engine, so partition routing can override the default
// shard-range layout without breaking the engine's non-overlapping-
// cover invariant.
//
// Implementations must be safe for concurrent use — ResolveGroup is
// called on the request hot path. Returning (0, false) for a key
// the resolver does not recognise lets the router fall through to
// the engine.
type PartitionResolver interface {
	ResolveGroup(key []byte) (uint64, bool)
}

// ShardRouter routes requests to multiple raft groups based on key ranges.
//
// Cross-shard transactions are not supported. They require distributed
// coordination (for example, 2PC) to ensure atomicity.
//
// Non-transactional request batches may still partially succeed across shards.
type ShardRouter struct {
	engine            *distribution.Engine
	partitionResolver PartitionResolver
	mu                sync.RWMutex
	groups            map[uint64]*routerGroup
}

var ErrCrossShardTransactionNotSupported = errors.New("cross-shard transactions are not supported")

type routerGroup struct {
	tm    Transactional
	store store.MVCCStore
}

// NewShardRouter creates a new router.
func NewShardRouter(e *distribution.Engine) *ShardRouter {
	return &ShardRouter{
		engine: e,
		groups: make(map[uint64]*routerGroup),
	}
}

// WithPartitionResolver installs a partition-keyspace resolver that
// is consulted before the byte-range engine on every dispatch. A
// nil resolver clears any previously-installed resolver. Returns
// the receiver so callers can chain.
//
// Intended for use during startup, before the router begins handling
// requests. Interface assignment in Go is not atomic, so a call that
// races with a concurrent ResolveGroup in resolveGroup may produce a
// torn read; callers must wire the resolver once during construction
// (parseRuntimeConfig → NewShardedCoordinator → WithPartitionResolver)
// and treat any post-startup re-assignment as undefined behaviour.
func (s *ShardRouter) WithPartitionResolver(r PartitionResolver) *ShardRouter {
	s.partitionResolver = r
	return s
}

// ResolveGroup tries the partition resolver first (when installed),
// then falls through to the byte-range engine. Exposed at package
// scope so ShardedCoordinator's per-key helpers (groupForKey,
// routeAndGroupForKey, engineGroupIDForKey, groupMutations) can
// consult the same dispatch path Commit / Abort / Get use — without
// it those helpers would bypass the resolver and partitioned-FIFO
// traffic would silently mis-route through 2PC and the read paths.
//
// The resolver runs on the RAW key before any user-key
// normalization. SQS keys in particular are collapsed to
// !sqs|route|global by routeKey to keep the engine's per-shard
// layout simple, but that collapse hides the partitioned-prefix
// information the resolver needs (issue: codex P1 / gemini high on
// PR #715). The engine still sees the post-normalization key, so
// legacy routing (catalog → !sqs|route|global → default group)
// stays unchanged.
//
// Returns (0, false) when neither the resolver nor the engine
// recognises the key. Caller surfaces this as an "unknown group"
// error so a partitioned-prefix key whose queue is missing from the
// resolver map fails closed rather than landing on whichever
// engine-default group happens to cover the raw bytes.
func (s *ShardRouter) ResolveGroup(rawKey []byte) (uint64, bool) {
	if s.partitionResolver != nil {
		if gid, ok := s.partitionResolver.ResolveGroup(rawKey); ok {
			return gid, true
		}
	}
	// Engine routes against the user-key view of the byte-range
	// space; routeKey may rewrite SQS / DynamoDB / Redis-internal
	// keys to a stable per-table or per-namespace route key so the
	// engine sees one route per logical entity.
	route, ok := s.engine.GetRoute(routeKey(rawKey))
	if !ok {
		return 0, false
	}
	return route.GroupID, true
}

// Register associates a raft group ID with its transactional manager and store.
func (s *ShardRouter) Register(group uint64, tm Transactional, st store.MVCCStore) {
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
	if len(reqs) == 0 {
		return nil, errors.WithStack(ErrInvalidRequest)
	}

	grouped, err := s.groupRequests(reqs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := validateShardBatch(reqs[0], grouped); err != nil {
		return nil, errors.WithStack(err)
	}

	return s.processGrouped(grouped, fn)
}

func validateShardBatch(first *pb.Request, grouped map[uint64][]*pb.Request) error {
	// Avoid partial commits for transactional batches spanning shards.
	if first.IsTxn && len(grouped) > 1 {
		return ErrCrossShardTransactionNotSupported
	}
	return nil
}

func (s *ShardRouter) processGrouped(grouped map[uint64][]*pb.Request, fn func(*routerGroup, []*pb.Request) (*TransactionResponse, error)) (*TransactionResponse, error) {
	var firstErr error
	var maxIndex uint64
	for gid, rs := range grouped {
		g, ok := s.getGroup(gid)
		if !ok {
			return nil, errors.Wrapf(ErrInvalidRequest, "unknown group %d", gid)
		}
		r, err := fn(g, rs)
		if err != nil {
			if firstErr == nil {
				firstErr = errors.WithStack(err)
			}
			continue
		}
		if r.CommitIndex > maxIndex {
			maxIndex = r.CommitIndex
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return &TransactionResponse{CommitIndex: maxIndex}, nil
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
		if r == nil {
			return nil, ErrInvalidRequest
		}
		if len(r.Mutations) == 0 || r.Mutations[0] == nil {
			return nil, ErrInvalidRequest
		}
		rawKey := r.Mutations[0].Key
		if len(rawKey) == 0 {
			return nil, ErrInvalidRequest
		}
		gid, ok := s.ResolveGroup(rawKey)
		if !ok {
			return nil, errors.Wrapf(ErrInvalidRequest, "no route for key %q", rawKey)
		}
		batches[gid] = append(batches[gid], r)
	}
	return batches, nil
}

// Get retrieves a key routed to the correct shard.
func (s *ShardRouter) Get(ctx context.Context, key []byte) ([]byte, error) {
	gid, ok := s.ResolveGroup(key)
	if !ok {
		return nil, errors.Wrapf(ErrInvalidRequest, "no route for key %q", key)
	}
	g, ok := s.getGroup(gid)
	if !ok {
		return nil, errors.Wrapf(ErrInvalidRequest, "unknown group %d", gid)
	}
	v, err := g.store.GetAt(ctx, key, ^uint64(0))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return v, nil
}

var _ Transactional = (*ShardRouter)(nil)

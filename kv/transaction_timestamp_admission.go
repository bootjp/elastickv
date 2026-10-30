package kv

import (
	"context"
	"io"
	"sync/atomic"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

type transactionTimestampAdmission struct {
	store     store.MVCCStore
	allocator func() TimestampAllocator
	groupID   func() uint64
}

// WithTimestampAdmission guards the transaction manager before requests enter
// its raw queue. The getter follows the coordinator's current runtime allocator.
func WithTimestampAdmission(st store.MVCCStore, allocator func() TimestampAllocator) TransactionOption {
	return func(t *TransactionManager) {
		t.timestampAdmission.Store(&transactionTimestampAdmission{store: st, allocator: allocator})
	}
}

// WithTimestampAdmissionStore equips a single-group coordinator with the data
// store required to stamp local requests, including its LeaderProxy local path.
func WithTimestampAdmissionStore(st store.MVCCStore) CoordinatorOption {
	return func(c *Coordinate) {
		c.transactionManager = configureTimestampAdmission(c.transactionManager, st, func() TimestampAllocator { return c.tsAllocator }, func() uint64 { return c.claimGroupID })
	}
}

func (c *ShardedCoordinator) configureTimestampAdmissionForGroup(group *ShardGroup) {
	timestampGroup := c.groups[0]
	if timestampGroup == nil || timestampGroup.TSOState == nil {
		// This immutable runtime cannot activate the protocol. Keep the default
		// legacy path unchanged; an explicit transaction option remains intact.
		return
	}
	var groupID uint64
	for id, candidate := range c.groups {
		if candidate == group {
			groupID = id
			break
		}
	}
	group.Txn = configureTimestampAdmission(group.Txn, group.Store, func() TimestampAllocator { return c.tsAllocator }, func() uint64 { return groupID })
}

func (a *transactionTimestampAdmission) prepare(ctx context.Context, reqs []*pb.Request) ([]*pb.Request, error) {
	if a == nil {
		return reqs, nil
	}
	// Keep the admitted epoch immutable while queued, even if a caller reuses
	// the original request for a retry after the data group releases creation.
	owned := make([]*pb.Request, len(reqs))
	for i, request := range reqs {
		owned[i] = proto.CloneOf(request)
	}
	var allocator TimestampAllocator
	if a.allocator != nil {
		allocator = a.allocator()
	}
	var groupID uint64
	if a.groupID != nil {
		groupID = a.groupID()
	}
	if err := StampTimestampAdmission(ctx, allocator, a.store, owned, groupID); err != nil {
		return nil, err
	}
	return owned, nil
}

func configureTimestampAdmission(tx Transactional, st store.MVCCStore, allocator func() TimestampAllocator, groupID func() uint64) Transactional {
	admission := &transactionTimestampAdmission{store: st, allocator: allocator, groupID: groupID}
	switch t := tx.(type) {
	case *TransactionManager:
		t.timestampAdmission.Store(admission)
	case *LeaderProxy:
		t.tm.timestampAdmission.Store(admission)
	case *leaseRefreshingTxn:
		t.inner = configureTimestampAdmission(t.inner, st, allocator, groupID)
	case *timestampAdmissionTransactional:
		t.admission.Store(admission)
	case nil:
		return nil
	default:
		wrapped := &timestampAdmissionTransactional{inner: tx}
		wrapped.admission.Store(admission)
		return wrapped
	}
	return tx
}

type timestampAdmissionTransactional struct {
	inner     Transactional
	admission atomic.Pointer[transactionTimestampAdmission]
}

func (t *timestampAdmissionTransactional) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	owned, err := t.admission.Load().prepare(ctx, reqs)
	if err != nil {
		return nil, err
	}
	response, err := t.inner.Commit(ctx, owned)
	return response, errors.WithStack(err)
}

func (t *timestampAdmissionTransactional) Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	response, err := t.inner.Abort(ctx, reqs)
	return response, errors.WithStack(err)
}

func (t *timestampAdmissionTransactional) Close() error {
	if closer, ok := t.inner.(io.Closer); ok {
		return errors.WithStack(closer.Close())
	}
	return nil
}

// Package fstest provides in-memory test doubles for exercising the
// filesystem service without a Raft cluster.
package fstest

import (
	"context"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/kv/kvtest"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// MemoryDispatcher applies operation groups directly to an MVCC store. It
// satisfies filesystem.Dispatcher and the optional linearizable-read fence so
// tests can drive filesystem.Service end to end without replication.
type MemoryDispatcher struct {
	local *kvtest.LocalCoordinator
}

// NewMemoryDispatcher wraps st in a dispatcher that commits every group at
// max(store+1, requested, StartTS+1), validates OCC read keys on every group,
// patches commit-timestamp value offsets, and rejects prefix deletes.
func NewMemoryDispatcher(st store.MVCCStore) *MemoryDispatcher {
	return &MemoryDispatcher{local: kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{
		DelPrefix:        kvtest.PrefixReject,
		CommitTimestamp:  kvtest.TimestampStoreIncrement,
		AdvanceCommitTS:  true,
		OCC:              kvtest.OCCAllGroups,
		ValidateReadKeys: true,
		PatchCommitTS:    true,
	})}
}

// LinearizableRead returns the latest applied commit timestamp.
func (d *MemoryDispatcher) LinearizableRead(ctx context.Context) (uint64, error) {
	ts, err := d.local.LinearizableRead(ctx)
	return ts, errors.WithStack(err)
}

// Dispatch applies the group's mutations atomically at a fresh commit
// timestamp, validating OCC read keys against the group's start timestamp.
func (d *MemoryDispatcher) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	resp, err := d.local.Dispatch(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "apply memory dispatcher mutations")
	}
	return resp, nil
}

// NewMemoryService builds a filesystem.Service over a fresh in-memory MVCC
// store and a MemoryDispatcher.
func NewMemoryService(opts ...filesystem.Option) (*filesystem.Service, error) {
	st := store.NewMVCCStore()
	svc, err := filesystem.NewService(st, NewMemoryDispatcher(st), opts...)
	if err != nil {
		return nil, errors.Wrap(err, "create memory filesystem service")
	}
	return svc, nil
}

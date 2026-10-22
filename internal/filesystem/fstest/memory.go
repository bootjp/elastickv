// Package fstest provides in-memory test doubles for exercising the
// filesystem service without a Raft cluster.
package fstest

import (
	"context"
	"sync"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// MemoryDispatcher applies operation groups directly to an MVCC store. It
// satisfies filesystem.Dispatcher and the optional linearizable-read fence so
// tests can drive filesystem.Service end to end without replication.
type MemoryDispatcher struct {
	mu    sync.Mutex
	store store.MVCCStore
}

// NewMemoryDispatcher wraps st in a dispatcher that commits every group at
// the next commit timestamp.
func NewMemoryDispatcher(st store.MVCCStore) *MemoryDispatcher {
	return &MemoryDispatcher{store: st}
}

// LinearizableRead returns the latest applied commit timestamp.
func (d *MemoryDispatcher) LinearizableRead(context.Context) (uint64, error) {
	return d.store.LastCommitTS(), nil
}

// Dispatch applies the group's mutations atomically at a fresh commit
// timestamp, validating OCC read keys against the group's start timestamp.
func (d *MemoryDispatcher) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	commitTS := d.store.LastCommitTS() + 1
	if req.CommitTS > commitTS {
		commitTS = req.CommitTS
	}
	if req.IsTxn && commitTS <= req.StartTS {
		commitTS = req.StartTS + 1
	}
	mutations := make([]*store.KVPairMutation, 0, len(req.Elems))
	for _, elem := range req.Elems {
		switch elem.Op {
		case kv.Put:
			mutations = append(mutations, &store.KVPairMutation{
				Op:    store.OpTypePut,
				Key:   append([]byte(nil), elem.Key...),
				Value: append([]byte(nil), elem.Value...),
			})
		case kv.Del:
			mutations = append(mutations, &store.KVPairMutation{
				Op:  store.OpTypeDelete,
				Key: append([]byte(nil), elem.Key...),
			})
		case kv.DelPrefix:
			return nil, errors.WithStack(store.ErrUnknownOp)
		default:
			return nil, errors.WithStack(store.ErrUnknownOp)
		}
	}
	if err := d.store.ApplyMutations(ctx, mutations, req.ReadKeys, req.StartTS, commitTS); err != nil {
		return nil, errors.Wrap(err, "apply memory dispatcher mutations")
	}
	return &kv.CoordinateResponse{CommitIndex: commitTS}, nil
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

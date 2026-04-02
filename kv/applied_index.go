package kv

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

// AppliedIndexWaiter reports the highest raft log index whose FSM application
// has fully completed and allows callers to wait for a target index.
type AppliedIndexWaiter interface {
	AppliedIndex() uint64
	WaitForAppliedIndex(ctx context.Context, index uint64) error
}

type appliedIndexTracker struct {
	mu    sync.Mutex
	index uint64
	ch    chan struct{}
}

func newAppliedIndexTracker() *appliedIndexTracker {
	return &appliedIndexTracker{
		ch: make(chan struct{}),
	}
}

func (t *appliedIndexTracker) AppliedIndex() uint64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.index
}

func (t *appliedIndexTracker) WaitForAppliedIndex(ctx context.Context, index uint64) error {
	if t == nil || index == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		t.mu.Lock()
		if t.index >= index {
			t.mu.Unlock()
			return nil
		}
		ch := t.ch
		t.mu.Unlock()

		select {
		case <-ctx.Done():
			return errors.WithStack(context.Cause(ctx))
		case <-ch:
		}
	}
}

func (t *appliedIndexTracker) markAppliedIndex(index uint64) {
	if t == nil || index == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if index <= t.index {
		return
	}
	t.index = index
	close(t.ch)
	t.ch = make(chan struct{})
}

var raftAppliedIndexWaiters sync.Map // map[*raft.Raft]AppliedIndexWaiter

func RegisterRaftAppliedIndexWaiter(r *raft.Raft, waiter AppliedIndexWaiter) {
	if r == nil || waiter == nil {
		return
	}
	raftAppliedIndexWaiters.Store(r, waiter)
}

func UnregisterRaftAppliedIndexWaiter(r *raft.Raft) {
	if r == nil {
		return
	}
	raftAppliedIndexWaiters.Delete(r)
}

func appliedIndexWaiterForRaft(r *raft.Raft) AppliedIndexWaiter {
	if r == nil {
		return nil
	}
	v, ok := raftAppliedIndexWaiters.Load(r)
	if !ok {
		return nil
	}
	waiter, ok := v.(AppliedIndexWaiter)
	if !ok {
		return nil
	}
	return waiter
}

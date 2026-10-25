package fuseadapter

import (
	"context"
	"sync"
	"syscall"
	"time"
)

const (
	maxPendingMutations     = 1024
	mutationQueueWaitBudget = 30 * time.Second
)

// mutationTracker orders a local fsync after operations that entered this
// adapter before the barrier. Distributed ordering still comes from OCC.
type mutationTracker struct {
	mu      sync.Mutex
	pending map[uint64]map[*mutationTicket]struct{}
	count   int
	sticky  map[uint64]syscall.Errno
}

type mutationTicket struct {
	keys []uint64
	done chan struct{}
}

func (m *mutationTracker) begin(ctx context.Context, keys ...uint64) (*mutationTicket, syscall.Errno) {
	if err := ctx.Err(); err != nil {
		return nil, Errno(err)
	}
	ticket, predecessors, errno := m.register(keys)
	if errno != 0 {
		return nil, errno
	}
	waitCtx, cancel := context.WithTimeout(ctx, mutationQueueWaitBudget)
	defer cancel()
	for preceding := range predecessors {
		select {
		case <-preceding.done:
		case <-waitCtx.Done():
			errno := Errno(waitCtx.Err())
			m.end(ticket, 0)
			return nil, errno
		}
	}
	if errno := m.stickyFor(ticket.keys); errno != 0 {
		m.end(ticket, 0)
		return nil, errno
	}
	return ticket, 0
}

func (m *mutationTracker) register(keys []uint64) (*mutationTicket, map[*mutationTicket]struct{}, syscall.Errno) {
	ticket := &mutationTicket{done: make(chan struct{})}
	m.mu.Lock()
	if m.count >= maxPendingMutations {
		m.mu.Unlock()
		return nil, nil, syscall.EBUSY
	}
	m.count++
	if m.pending == nil {
		m.pending = make(map[uint64]map[*mutationTicket]struct{})
	}
	predecessors := make(map[*mutationTicket]struct{})
	for _, key := range keys {
		if m.pending[key] == nil {
			m.pending[key] = make(map[*mutationTicket]struct{})
		}
		if _, duplicate := m.pending[key][ticket]; duplicate {
			continue
		}
		for preceding := range m.pending[key] {
			predecessors[preceding] = struct{}{}
		}
		m.pending[key][ticket] = struct{}{}
		ticket.keys = append(ticket.keys, key)
	}
	m.mu.Unlock()
	return ticket, predecessors, 0
}

func (m *mutationTracker) stickyFor(keys []uint64) syscall.Errno {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		if errno := m.sticky[key]; errno != 0 {
			return errno
		}
	}
	return 0
}

func (m *mutationTracker) end(ticket *mutationTicket, errno syscall.Errno) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count--
	for _, key := range ticket.keys {
		if errno == syscall.EIO {
			if m.sticky == nil {
				m.sticky = make(map[uint64]syscall.Errno)
			}
			m.sticky[key] = errno
		}
		delete(m.pending[key], ticket)
		if len(m.pending[key]) == 0 {
			delete(m.pending, key)
		}
	}
	close(ticket.done)
}

func (m *mutationTracker) wait(ctx context.Context, key uint64) syscall.Errno {
	waitCtx, cancel := context.WithTimeout(ctx, mutationQueueWaitBudget)
	defer cancel()
	m.mu.Lock()
	predecessors := make([]*mutationTicket, 0, len(m.pending[key]))
	for ticket := range m.pending[key] {
		predecessors = append(predecessors, ticket)
	}
	m.mu.Unlock()
	for _, ticket := range predecessors {
		select {
		case <-ticket.done:
		case <-waitCtx.Done():
			return Errno(waitCtx.Err())
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sticky[key]
}

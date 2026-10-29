package etcd

import (
	"sync"

	"github.com/cockroachdb/errors"
)

var (
	errStorageWorkBackpressure       = errors.New("storage work budget exhausted")
	errStorageWorkInvalidReservation = errors.New("invalid storage work reservation")
)

// storageWorkBudget bounds outstanding reservations, independently of work stage.
// Its limits are immutable; mu protects both usage and each permit's release.
type storageWorkBudget struct {
	mu        sync.Mutex
	maxBytes  uint64
	maxItems  int
	usedBytes uint64
	usedItems int
}

type storageWorkPermit struct {
	budget   *storageWorkBudget
	bytes    uint64
	released bool
}

// Invalid limits leave a budget that rejects every acquisition rather than
// interpreting zero or negative limits as unbounded capacity.
func newStorageWorkBudget(maxBytes uint64, maxItems int) *storageWorkBudget {
	return &storageWorkBudget{maxBytes: maxBytes, maxItems: maxItems}
}

func (b *storageWorkBudget) tryAcquire(bytes uint64) (*storageWorkPermit, error) {
	if b == nil || b.maxBytes == 0 || b.maxItems <= 0 || bytes > b.maxBytes {
		return nil, errors.WithStack(errStorageWorkInvalidReservation)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Subtraction avoids overflow even when the byte limit is MaxUint64.
	if b.usedItems >= b.maxItems || bytes > b.maxBytes-b.usedBytes {
		return nil, errors.WithStack(errStorageWorkBackpressure)
	}
	b.usedBytes += bytes
	b.usedItems++
	return &storageWorkPermit{budget: b, bytes: bytes}, nil
}

// Release retires a reservation exactly once, including concurrent calls.
func (p *storageWorkPermit) Release() {
	if p == nil || p.budget == nil {
		return
	}
	b := p.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.released {
		return
	}
	b.usedBytes -= p.bytes
	b.usedItems--
	p.released = true
}

func (b *storageWorkBudget) usage() (uint64, int) {
	if b == nil {
		return 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usedBytes, b.usedItems
}

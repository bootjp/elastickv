package etcd

import (
	"math"
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
	parent    *storageWorkBudget
	owner     bool
	// Only an aggregate has an emergency allowance. All owner reservations
	// share this ONE allowance; no acquisition creates another Ready window.
	overshootBytes uint64
	overshootItems int
}

type storageWorkPermit struct {
	budget   *storageWorkBudget
	bytes    uint64
	items    int
	parent   *storageWorkPermit
	released bool
}

// Invalid limits leave a budget that rejects every acquisition rather than
// interpreting zero or negative limits as unbounded capacity.
func newStorageWorkBudget(maxBytes uint64, maxItems int) *storageWorkBudget {
	return &storageWorkBudget{maxBytes: maxBytes, maxItems: maxItems}
}

func (b *storageWorkBudget) tryAcquire(bytes uint64) (*storageWorkPermit, error) {
	return b.acquire(bytes, 1, false)
}

func (b *storageWorkBudget) acquire(bytes uint64, items int, owner bool) (*storageWorkPermit, error) {
	if !b.validAcquisition(items) {
		return nil, errors.WithStack(errStorageWorkInvalidReservation)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	maxBytes, maxItems, err := b.acquisitionLimits(owner)
	if err != nil {
		return nil, err
	}
	if bytes > maxBytes || items > maxItems {
		return nil, errors.WithStack(errStorageWorkInvalidReservation)
	}
	// Subtraction avoids overflow even when the byte limit is MaxUint64.
	if b.usedItems > maxItems-items || b.usedBytes > maxBytes-bytes {
		return nil, errors.WithStack(errStorageWorkBackpressure)
	}
	var parent *storageWorkPermit
	if b.parent != nil {
		var err error
		parent, err = b.parent.acquire(bytes, items, b.owner)
		if err != nil {
			return nil, err
		}
	}
	b.usedBytes += bytes
	b.usedItems += items
	return &storageWorkPermit{budget: b, bytes: bytes, items: items, parent: parent}, nil
}

func (b *storageWorkBudget) validAcquisition(items int) bool {
	return b != nil && b.maxBytes != 0 && b.maxItems > 0 && items >= 0
}

func (b *storageWorkBudget) acquisitionLimits(owner bool) (uint64, int, error) {
	if !owner {
		return b.maxBytes, b.maxItems, nil
	}
	if b.overshootBytes > math.MaxUint64-b.maxBytes || b.overshootItems > math.MaxInt-b.maxItems {
		return 0, 0, errors.WithStack(errStorageWorkInvalidReservation)
	}
	return b.maxBytes + b.overshootBytes, b.maxItems + b.overshootItems, nil
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
	b.usedItems -= p.items
	p.released = true
	p.parent.Release()
}

func (b *storageWorkBudget) debt() (uint64, int) {
	if b == nil {
		return 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var bytes uint64
	var items int
	if b.usedBytes > b.maxBytes {
		bytes = b.usedBytes - b.maxBytes
	}
	if b.usedItems > b.maxItems {
		items = b.usedItems - b.maxItems
	}
	return bytes, items
}

func (b *storageWorkBudget) snapshotUsage() (uint64, int, uint64, int) {
	if b == nil {
		return 0, 0, 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var bytes uint64
	var items int
	if b.usedBytes > b.maxBytes {
		bytes = b.usedBytes - b.maxBytes
	}
	if b.usedItems > b.maxItems {
		items = b.usedItems - b.maxItems
	}
	return b.usedBytes, b.usedItems, bytes, items
}

func (b *storageWorkBudget) usage() (uint64, int) {
	if b == nil {
		return 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usedBytes, b.usedItems
}

package kv

import "sync"

// ActiveTimestampTracker tracks in-flight read or transaction timestamps that
// must remain readable while background compaction is running.
type ActiveTimestampTracker struct {
	mu     sync.Mutex
	nextID uint64
	active map[uint64]uint64
}

// ActiveTimestampToken releases one tracked timestamp when the owning
// operation completes.
type ActiveTimestampToken struct {
	tracker *ActiveTimestampTracker
	id      uint64
	once    sync.Once
}

func NewActiveTimestampTracker() *ActiveTimestampTracker {
	return &ActiveTimestampTracker{
		active: make(map[uint64]uint64),
	}
}

func (t *ActiveTimestampTracker) Pin(ts uint64) *ActiveTimestampToken {
	if t == nil || ts == 0 || ts == ^uint64(0) {
		return &ActiveTimestampToken{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id := t.nextID
	t.active[id] = ts
	return &ActiveTimestampToken{
		tracker: t,
		id:      id,
	}
}

func (t *ActiveTimestampTracker) Oldest() uint64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var oldest uint64
	for _, ts := range t.active {
		if oldest == 0 || ts < oldest {
			oldest = ts
		}
	}
	return oldest
}

func (t *ActiveTimestampToken) Release() {
	if t == nil || t.tracker == nil || t.id == 0 {
		return
	}
	t.once.Do(func() {
		t.tracker.mu.Lock()
		defer t.tracker.mu.Unlock()
		delete(t.tracker.active, t.id)
	})
}

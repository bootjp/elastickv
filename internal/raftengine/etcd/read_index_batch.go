package etcd

const maxReadIndexBatch = 32

// A batch's first ID identifies the one Raft read context. Each member
// remains separately cancellable and retains its own applied-index policy.
// The pending lock protects both the batch and the individual requests.
type readIndexBatch struct {
	ids       []uint64
	remaining int
}

func (e *Engine) handleReadQueue(first readRequest) {
	// Native admission must authorize the original request at its own
	// execution boundary. These requests keep their individual ReadIndex.
	if e.nativeAdmission != nil {
		e.handleRead(first)
		return
	}

	var queued [maxReadIndexBatch]readRequest
	queued[0] = first
	count := 1
drain:
	for count < len(queued) {
		select {
		case queued[count] = <-e.readCh:
			count++
		default:
			break drain
		}
	}
	if count == 1 {
		e.handleRead(first)
		return
	}
	if err := e.checkReadAdmission(false); err != nil {
		for _, req := range queued[:count] {
			req.done <- readResult{err: err}
		}
		return
	}
	// Register all members before issuing the quorum round. Later requests
	// cannot join this round, even while its result is pending.
	if id := e.storeReadIndexBatch(queued[:count]); id != 0 {
		e.rawNode.ReadIndex(encodeReadContext(id))
	}
}

func (e *Engine) storeReadIndexBatch(requests []readRequest) uint64 {
	e.pending.Lock()
	defer e.pending.Unlock()

	batch := &readIndexBatch{ids: make([]uint64, 0, len(requests))}
	for _, req := range requests {
		// Checking under the pending lock prevents cancellation just before
		// registration from leaving a dead waiter behind.
		if err := contextErr(req.ctx); err != nil {
			req.done <- readResult{err: err}
			continue
		}
		req.batch = batch
		e.pendingReads[req.id] = req
		batch.ids = append(batch.ids, req.id)
	}
	if len(batch.ids) == 0 {
		return 0
	}
	batch.remaining = len(batch.ids)
	if e.pendingReadBatches == nil {
		e.pendingReadBatches = make(map[uint64]*readIndexBatch)
	}
	id := batch.ids[0]
	e.pendingReadBatches[id] = batch
	return id
}

func (e *Engine) recordReadIndex(id, index uint64) {
	e.pending.Lock()
	defer e.pending.Unlock()

	if batch := e.pendingReadBatches[id]; batch != nil {
		delete(e.pendingReadBatches, id)
		for _, member := range batch.ids {
			e.recordReadIndexLocked(member, index)
		}
		return
	}
	if req := e.pendingReads[id]; req.batch == nil {
		e.recordReadIndexLocked(id, index)
	}
}

func (e *Engine) recordReadIndexLocked(id, index uint64) {
	req, ok := e.pendingReads[id]
	if !ok || req.target != 0 {
		return
	}
	if req.waitApplied {
		req.target = index
		req.batch = nil
		e.pendingReads[id] = req
		return
	}
	delete(e.pendingReads, id)
	req.done <- readResult{index: index}
}

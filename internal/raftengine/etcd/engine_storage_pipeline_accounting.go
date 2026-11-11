package etcd

import (
	"math"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

const (
	pipelinePointerBytes        = 8
	pipelinePhysicalEntryBytes  = 1024
	pipelineCommittedBatchBytes = 256 << 10
	pipelineMaxOwnedInputBytes  = pipelineEntryRoom/2 + pipelineControlBytes
	pipelineReadyOvershootBytes = pipelineBulkBytes
	pipelineReadyOvershootItems = 7
)

func pipelineAddBytes(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

func pipelineArrayBytes(capacity int, elementBytes uint64) uint64 {
	if capacity < 0 || elementBytes == 0 {
		return math.MaxUint64
	}
	if uint64(capacity) > math.MaxUint64/elementBytes {
		return math.MaxUint64
	}
	return uint64(capacity) * elementBytes
}

// This includes a conservative Entry object/optional-header allowance plus
// ledger shadow, normalApplyInput and receipt descriptors. Payload aliases are
// deliberately charged again for independent retained owners. It is not a
// whole-heap measurement, and does not charge durable MemoryStorage or FSM data.
func pipelineOwnedEntryBytes(entry *raftpb.Entry) uint64 {
	return pipelineEntryAccounting(entry, false)
}

func pipelineBackingBytes(data []byte, cloning bool) uint64 {
	if cloning {
		return pipelineArrayBytes(len(data), outboundCapacityFactor)
	}
	return uint64(cap(data))
}

func pipelineEntryAccounting(entry *raftpb.Entry, cloning bool) uint64 {
	if entry == nil {
		return 0
	}
	bytes := pipelineAddBytes(pipelinePhysicalEntryBytes, pipelineBackingBytes(entry.Data, cloning))
	return pipelineAddBytes(bytes, pipelineBackingBytes(entry.ProtoReflect().GetUnknown(), cloning))
}

func pipelineOwnedMessageBytes(message *raftpb.Message) uint64 {
	return pipelineMessageAccounting(message, false)
}

func pipelineCloneMessageBytes(message *raftpb.Message) uint64 {
	return pipelineMessageAccounting(message, true)
}

func pipelinePointerArrayBytes(length, capacity int, cloning bool) uint64 {
	if cloning {
		return pipelineArrayBytes(length, pipelinePointerBytes*outboundCapacityFactor)
	}
	return pipelineArrayBytes(capacity, pipelinePointerBytes)
}

func pipelineMessageAccounting(message *raftpb.Message, cloning bool) uint64 {
	if message == nil {
		return 0
	}
	bytes := pipelineAddBytes(pipelineTaskOverhead, pipelineBackingBytes(message.Context, cloning))
	bytes = pipelineAddBytes(bytes, pipelineBackingBytes(message.ProtoReflect().GetUnknown(), cloning))
	bytes = pipelineAddBytes(bytes, pipelinePointerArrayBytes(len(message.Entries), cap(message.Entries), cloning))
	for _, entry := range message.Entries {
		bytes = pipelineAddBytes(bytes, pipelineEntryAccounting(entry, cloning))
	}
	bytes = pipelineAddBytes(bytes, pipelineSnapshotAccounting(message.Snapshot, cloning))
	bytes = pipelineAddBytes(bytes, pipelinePointerArrayBytes(len(message.Responses), cap(message.Responses), cloning))
	for _, response := range message.Responses {
		bytes = pipelineAddBytes(bytes, pipelineMessageAccounting(response, cloning))
	}
	return bytes
}

// Borrowed Ready pointer arrays may be subslices of durable MemoryStorage's
// entire log. Tasks detach those arrays before retention. Reserve their compact
// projection, not all unrelated durable entries beyond the slice's length.
func pipelineTaskMessageBytes(message *raftpb.Message) uint64 {
	if message == nil {
		return 0
	}
	view := *message
	view.Entries = view.Entries[:len(view.Entries):len(view.Entries)]
	view.Responses = view.Responses[:len(view.Responses):len(view.Responses)]
	return pipelineOwnedMessageBytes(&view)
}

func pipelineOwnedSnapshotBytes(snapshot *raftpb.Snapshot) uint64 {
	return pipelineSnapshotAccounting(snapshot, false)
}

func pipelineSnapshotAccounting(snapshot *raftpb.Snapshot, cloning bool) uint64 {
	if snapshot == nil {
		return 0
	}
	bytes := pipelineAddBytes(pipelineTaskOverhead, pipelineBackingBytes(snapshot.Data, cloning))
	bytes = pipelineAddBytes(bytes, pipelineBackingBytes(snapshot.ProtoReflect().GetUnknown(), cloning))
	if metadata := snapshot.Metadata; metadata != nil {
		bytes = pipelineAddBytes(bytes, pipelineTaskOverhead)
		bytes = pipelineAddBytes(bytes, pipelineBackingBytes(metadata.ProtoReflect().GetUnknown(), cloning))
		bytes = pipelineAddBytes(bytes, pipelineConfStateAccounting(metadata.ConfState, cloning))
	}
	return bytes
}

func pipelineOwnedConfStateBytes(state *raftpb.ConfState) uint64 {
	return pipelineConfStateAccounting(state, false)
}

func pipelineConfStateAccounting(state *raftpb.ConfState, cloning bool) uint64 {
	if state == nil {
		return 0
	}
	bytes := pipelineAddBytes(pipelineTaskOverhead, pipelineBackingBytes(state.ProtoReflect().GetUnknown(), cloning))
	for _, ids := range [][]uint64{state.Voters, state.VotersOutgoing, state.Learners, state.LearnersNext} {
		bytes = pipelineAddBytes(bytes, pipelinePointerArrayBytes(len(ids), cap(ids), cloning))
	}
	return bytes
}

func (p *engineStoragePipeline) connectOwnershipBudgets() {
	p.bulkOwnership = newStorageWorkBudget(pipelineBulkBytes, pipelineBulkItems)
	p.bulkOwnership.overshootBytes = pipelineReadyOvershootBytes
	p.bulkOwnership.overshootItems = pipelineReadyOvershootItems
	p.controlOwnership = newStorageWorkBudget(pipelineControlBytes, pipelineControlItems)
	for _, budget := range []*storageWorkBudget{p.proposalBudget, p.bulkIngressBudget, p.snapshotIngressBudget} {
		budget.parent = p.bulkOwnership
	}
	for _, budget := range []*storageWorkBudget{p.diskBulk, p.applyBudget, p.ledger.budget, p.snapshotBudget} {
		budget.parent, budget.owner = p.bulkOwnership, true
	}
	for _, budget := range []*storageWorkBudget{p.diskControl, p.priorityIngressBudget, p.configurationBudget, p.configurationHistoryBudget} {
		budget.parent = p.controlOwnership
	}
	// Actor queue permits only track occupancy. Their task's ownership permit
	// already spans deferred/queued/executing/unconsumed completion stages.
}

// StoragePipelineUsage is conservative retained-owner accounting, not live Go
// heap size, durable log size or an assertion of production async activation.
type StoragePipelineUsage struct {
	BulkBytes, BulkDebtBytes                uint64
	BulkWork, BulkDebtWork                  int
	ControlBytes                            uint64
	ControlWork                             int
	BulkLimit, ReadyAllowance               uint64
	BulkWorkLimit, ReadyWorkAllowance       int
	OutboundBulkBytes, OutboundControlBytes uint64
	OutboundBulkWork, OutboundControlWork   int
	ReservedBytes, ReservationCeilingBytes  uint64
	ReservedWork, ReservationCeilingWork    int
}

// StoragePipelineUsage returns a concurrency-safe view of the internal runtime.
// A synchronous engine returns zero; observing counters does not activate it.
func (e *Engine) StoragePipelineUsage() StoragePipelineUsage {
	if e == nil || e.storagePipeline == nil || e.storagePipeline.bulkOwnership == nil {
		return StoragePipelineUsage{}
	}
	p := e.storagePipeline
	bytes, work, debtBytes, debtWork := p.bulkOwnership.snapshotUsage()
	controlBytes, controlWork := p.controlOwnership.usage()
	outboundBulkBytes, outboundBulkWork := p.outboundBulk.usage()
	outboundControlBytes, outboundControlWork := p.outboundControl.usage()
	reserved := pipelineAddBytes(pipelineAddBytes(bytes, controlBytes), pipelineAddBytes(outboundBulkBytes, outboundControlBytes))
	ceiling := pipelineAddBytes(pipelineAddBytes(p.bulkOwnership.maxBytes, p.bulkOwnership.overshootBytes), p.controlOwnership.maxBytes)
	ceiling = pipelineAddBytes(ceiling, pipelineAddBytes(p.outboundBulk.maxBytes, p.outboundControl.maxBytes))
	return StoragePipelineUsage{BulkBytes: bytes, BulkWork: work, BulkDebtBytes: debtBytes, BulkDebtWork: debtWork,
		ControlBytes: controlBytes, ControlWork: controlWork, BulkLimit: p.bulkOwnership.maxBytes, ReadyAllowance: p.bulkOwnership.overshootBytes,
		BulkWorkLimit: p.bulkOwnership.maxItems, ReadyWorkAllowance: p.bulkOwnership.overshootItems,
		OutboundBulkBytes: outboundBulkBytes, OutboundBulkWork: outboundBulkWork,
		OutboundControlBytes: outboundControlBytes, OutboundControlWork: outboundControlWork,
		ReservedBytes: reserved, ReservedWork: work + controlWork + outboundBulkWork + outboundControlWork,
		ReservationCeilingBytes: ceiling,
		ReservationCeilingWork: p.bulkOwnership.maxItems + p.bulkOwnership.overshootItems + p.controlOwnership.maxItems +
			p.outboundBulk.maxItems + p.outboundControl.maxItems}
}

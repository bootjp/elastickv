package etcd

import (
	"math"

	raftpb "go.etcd.io/raft/v3/raftpb"
)

const outboundCapacityFactor = 2

// Reserve before copying payloads. A queue handoff, an RPC return or a retained
// completion report is not retirement: Close follows the last dispatch use.
func (e *Engine) tryPrepareOwnedDispatchRequest(message raftpb.Message) (dispatchRequest, bool) {
	if e.storagePipeline == nil {
		return prepareDispatchRequest(message), true
	}
	p := e.storagePipeline
	budget := p.outboundBulk
	if isPriorityMsg(message.GetType()) || message.GetType() == raftpb.MsgAppResp {
		budget = p.outboundControl
	}
	permit, err := budget.tryAcquire(outboundDispatchBytes(&message))
	if err != nil {
		return dispatchRequest{}, false
	}
	request := prepareDispatchRequest(message)
	request.permit = permit
	return request, true
}

// Billing is conservative, not a complete Go-heap measurement. Byte slices
// cloned with append can retain allocator-rounded capacity above their length.
// ConfState's uint64 slices also need a fixed-width allowance: packed protobuf
// varints are not their in-memory size. Include nested opaque responses too.
func outboundDispatchBytes(message *raftpb.Message) uint64 {
	bytes := pipelineMessageBytes(message)
	if bytes > math.MaxUint64/outboundCapacityFactor {
		return math.MaxUint64
	}
	return outboundSnapshotArrayBytes(message, bytes*outboundCapacityFactor)
}

func outboundSnapshotArrayBytes(message *raftpb.Message, bytes uint64) uint64 {
	if message == nil {
		return bytes
	}
	state := message.GetSnapshot().GetMetadata().GetConfState()
	for _, ids := range [][]uint64{state.GetVoters(), state.GetVotersOutgoing(), state.GetLearners(), state.GetLearnersNext()} {
		bytes = outboundArrayBytes(bytes, uint64(len(ids)))
	}
	for _, response := range message.Responses {
		bytes = outboundSnapshotArrayBytes(response, bytes)
	}
	return bytes
}

func outboundArrayBytes(bytes, elements uint64) uint64 {
	const roundedUint64Bytes = outboundCapacityFactor * 8
	if elements > (math.MaxUint64-bytes)/roundedUint64Bytes {
		return math.MaxUint64
	}
	return bytes + elements*roundedUint64Bytes
}

func (e *Engine) replacePlainHeartbeatResponse(request dispatchRequest, message raftpb.Message) (dispatchRequest, bool) {
	if request.msg.GetType() != raftpb.MsgHeartbeatResp || len(request.msg.GetContext()) != 0 {
		return request, false
	}
	replacement, admitted := e.tryPrepareOwnedDispatchRequest(message)
	if !admitted {
		return request, false
	}
	closeDispatchRequest(request)
	return replacement, true
}

// The owner removes a peer before cancellation and never enqueues after stop.
// Each lane has one worker, so its exit owns all remaining queued requests.
func retireQueuedDispatchRequests(ch chan dispatchRequest) {
	for {
		request, ok := tryReceiveDispatchRequest(ch)
		if !ok {
			return
		}
		closeDispatchRequest(request)
	}
}

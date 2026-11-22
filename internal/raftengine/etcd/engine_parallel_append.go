package etcd

import (
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// dispatchParallelAppend overlaps follower replication with the leader's WAL
// Save, as allowed by the upstream Ready contract. Every earlier Ready is
// durable and this Ready carries no HardState change. The owner still blocks
// on Save before processing replies, applying, resolving callers or Advance.
// enqueueDispatchMessage clones on the owner before persistence can reuse data.
func (e *Engine) dispatchParallelAppend(rd etcdraft.Ready) (etcdraft.Ready, error) {
	if !e.canDispatchParallelAppend(rd) {
		return rd, nil
	}
	deferred := rd
	deferred.Messages = nil
	for _, msg := range rd.Messages {
		if msg.GetType() != raftpb.MsgApp {
			deferred.Messages = append(deferred.Messages, msg)
			continue
		}
		if err := e.sendMessages([]raftpb.Message{*msg}); err != nil {
			return etcdraft.Ready{}, err
		}
	}
	return deferred, nil
}

func (e *Engine) canDispatchParallelAppend(rd etcdraft.Ready) bool {
	if !e.parallelAppendUnprotected() {
		return false
	}
	if !etcdraft.IsEmptyHardState(rd.HardState) || !etcdraft.IsEmptySnap(rd.Snapshot) || len(rd.Entries) == 0 || len(rd.CommittedEntries) != 0 {
		return false
	}
	if e.rawNode.BasicStatus().RaftState != etcdraft.StateLeader {
		return false
	}
	for _, entry := range rd.Entries {
		if entry.GetType() != raftpb.EntryNormal {
			return false
		}
	}
	return true
}

func (e *Engine) parallelAppendUnprotected() bool {
	return e.nativeAdmission == nil && e.admissionChanged == nil &&
		e.recoveredAdmission == nil && e.storagePipeline == nil
}

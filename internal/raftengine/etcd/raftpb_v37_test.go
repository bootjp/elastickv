package etcd

import raftpb "go.etcd.io/raft/v3/raftpb"

func testHardState(term, commit uint64) raftpb.HardState {
	return raftpb.HardState{
		Term:   uint64Ptr(term),
		Commit: uint64Ptr(commit),
	}
}

func raftTestSnapshot(index, term uint64, voters []uint64, data []byte) raftpb.Snapshot {
	return raftpb.Snapshot{
		Data:     data,
		Metadata: testSnapshotMetadata(index, term, voters),
	}
}

func testSnapshotMetadata(index, term uint64, voters []uint64) *raftpb.SnapshotMetadata {
	return &raftpb.SnapshotMetadata{
		ConfState: &raftpb.ConfState{Voters: append([]uint64(nil), voters...)},
		Index:     uint64Ptr(index),
		Term:      uint64Ptr(term),
	}
}

func testEntry(index, term uint64, data []byte) raftpb.Entry {
	return raftpb.Entry{
		Type:  entryTypePtr(raftpb.EntryNormal),
		Term:  uint64Ptr(term),
		Index: uint64Ptr(index),
		Data:  data,
	}
}

func testMessagePointers(messages []raftpb.Message) []*raftpb.Message {
	if len(messages) == 0 {
		return nil
	}
	out := make([]*raftpb.Message, 0, len(messages))
	for _, message := range messages {
		messageCopy := message
		out = append(out, &messageCopy)
	}
	return out
}

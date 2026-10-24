package etcd

import (
	"strconv"
	"testing"

	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// Isolate the added Ready scan from network/storage work and outgoing messages.
// Normal committed entries should not allocate removal bookkeeping.
func BenchmarkSendReadyMessagesNormal(b *testing.B) {
	for _, size := range []int{1, 64, 1024} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			entries := make([]*raftpb.Entry, size)
			for i := range entries {
				entries[i] = &raftpb.Entry{Type: entryTypePtr(raftpb.EntryNormal)}
			}
			ready := etcdraft.Ready{CommittedEntries: entries}
			engine := &Engine{}
			b.ReportAllocs()
			for b.Loop() {
				if err := engine.sendReadyMessages(ready); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

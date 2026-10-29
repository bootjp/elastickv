package adapter

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/bootjp/elastickv/store"
)

// This measures snapshot head discovery on a local MVCC store, not Raft/network
// receive throughput. The true head is beyond the first outer/tie scan pages.
func BenchmarkSQSResolveFifoBucketHead(b *testing.B) {
	for _, count := range []int{4, 33, 256} {
		b.Run(fmt.Sprintf("messages_%d", count), func(b *testing.B) {
			fixture := newFifoHeadBenchmark(b, count)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				head, state, _, err := fixture.server.resolveFifoCandidate(ctx, fixture.meta.Name, fixture.meta, fixture.candidate, fixture.record, 20)
				if err != nil || head == nil || state != fifoLockAcquire || head.candidate.messageID != fixture.headID {
					b.Fatalf("invalid head discovery: state=%v head=%+v err=%v", state, head, err)
				}
			}
		})
	}
}

type fifoHeadBenchmark struct {
	server    *SQSServer
	meta      *sqsQueueMeta
	candidate sqsMsgCandidate
	record    *sqsMessageRecord
	headID    string
}

func newFifoHeadBenchmark(b *testing.B, count int) fifoHeadBenchmark {
	b.Helper()
	st := store.NewMVCCStore()
	b.Cleanup(func() {
		if err := st.Close(); err != nil {
			b.Fatal(err)
		}
	})
	meta := &sqsQueueMeta{Name: "bench.fifo", Generation: 1, IsFIFO: true}
	const visibleAt = int64(1_700_000_000_000)
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("%032x", i)
	}
	key := func(id string) []byte {
		return sqsMsgVisKeyDispatch(meta, meta.Name, 0, meta.Generation, visibleAt, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(key(ids[i]), key(ids[j])) < 0 })
	fixture := fifoHeadBenchmark{server: &SQSServer{store: st}, meta: meta, headID: ids[len(ids)-1]}
	fixture.candidate = sqsMsgCandidate{visKey: key(ids[0]), messageID: ids[0]}
	for i, id := range ids {
		seq := uint64(i + 2)
		if i == len(ids)-1 {
			seq = 1
		}
		stored := putFifoTieRecord(b, st, meta, id, "g", seq, visibleAt)
		if i == 0 {
			fixture.record = stored
		}
	}
	return fixture
}

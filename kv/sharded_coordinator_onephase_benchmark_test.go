package kv

import (
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/distribution"
)

// Keep the Raft transport out of the measurement. Dispatch still routes and
// validates the request and builds the exact log entry passed to Commit.
func BenchmarkShardedCoordinatorOnePhaseDispatch(b *testing.B) {
	for _, size := range []int{1, 32} {
		b.Run(fmt.Sprintf("%d-writes", size), func(b *testing.B) {
			engine := distribution.NewEngine()
			engine.UpdateRoute(nil, []byte("m"), 1)
			engine.UpdateRoute([]byte("m"), nil, 2)
			txn := &fakeTM{}
			coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
				1: {Txn: txn}, 2: {Txn: &fakeTM{}},
			}, 1, NewHLC(), nil)
			defer coord.Close()
			req := &OperationGroup[OP]{IsTxn: true, StartTS: 10, CommitTS: 20}
			for i := range size {
				key := []byte(fmt.Sprintf("a%03d", i))
				req.Elems = append(req.Elems, &Elem[OP]{Op: Put, Key: key, Value: []byte("value")})
				req.ReadKeys = append(req.ReadKeys, key)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := coord.Dispatch(b.Context(), req); err != nil {
					b.Fatal(err)
				}
			}
			if txn.commitCalls != b.N {
				b.Fatalf("got %d Commit calls for %d dispatches", txn.commitCalls, b.N)
			}
		})
	}
}

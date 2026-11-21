package kv

import (
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
)

type ordinaryBenchmarkLeader struct {
	raftengine.Engine
	verifications uint64
	proposals     uint64
	frame         []byte
}

func (*ordinaryBenchmarkLeader) State() raftengine.State { return raftengine.StateLeader }
func (*ordinaryBenchmarkLeader) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{}
}
func (*ordinaryBenchmarkLeader) Status() raftengine.Status { return raftengine.Status{} }
func (e *ordinaryBenchmarkLeader) VerifyLeader(context.Context) error {
	e.verifications++
	return nil
}
func (e *ordinaryBenchmarkLeader) Propose(_ context.Context, frame []byte) (*raftengine.ProposalResult, error) {
	e.proposals++
	e.frame = frame
	return &raftengine.ProposalResult{CommitIndex: e.proposals}, nil
}

func BenchmarkOrdinaryLeaderLocal1PC(b *testing.B) {
	for _, count := range []int{1, 64} {
		b.Run(fmt.Sprintf("Mutations%d", count), func(b *testing.B) {
			ctx := b.Context()
			leader := &ordinaryBenchmarkLeader{}
			proxy := NewLeaderProxyWithEngine(leader)
			b.Cleanup(func() { _ = proxy.Close() })
			key := []byte("key-0")
			mutations := []*pb.Mutation{{
				Op: pb.Op_PUT, Key: []byte(txnMetaPrefix),
				Value: EncodeTxnMeta(TxnMeta{PrimaryKey: key, CommitTS: 11}),
			}}
			for i := range count {
				mutations = append(mutations, &pb.Mutation{
					Op: pb.Op_PUT, Key: fmt.Appendf(nil, "key-%d", i), Value: []byte("ordinary-value"),
				})
			}
			req := &pb.Request{IsTxn: true, Phase: pb.Phase_NONE, Ts: 10, Mutations: mutations}
			reqs := []*pb.Request{req}
			b.ReportAllocs()
			for b.Loop() {
				res, err := proxy.Commit(ctx, reqs)
				if err != nil || res.CommitIndex != leader.proposals {
					b.Fatalf("Commit returned %v, %v", res, err)
				}
			}
			if leader.proposals != uint64(b.N) || leader.verifications != uint64(b.N) {
				b.Fatalf("got %d proposals and %d verifications for %d commits", leader.proposals, leader.verifications, b.N)
			}
			if len(leader.frame) == 0 || leader.frame[0] != raftEncodeSingle {
				b.Fatal("expected ordinary single-request Raft framing")
			}
			b.ReportMetric(float64(leader.proposals)/float64(b.N), "proposals/op")
			b.ReportMetric(float64(leader.verifications)/float64(b.N), "verifications/op")
		})
	}
}

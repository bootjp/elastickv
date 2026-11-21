package etcd

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Rejected ingress isolates the pre-admission work without mutating Raft or
// growing its log. The commit floor must still be read before admission.
func BenchmarkRejectedPeerStep(b *testing.B) {
	for _, leader := range []bool{false, true} {
		name := "follower"
		if leader {
			name = "leader"
		}
		b.Run(name, func(b *testing.B) {
			storage := etcdraft.NewMemoryStorage()
			require.NoError(b, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
				Index: proto.Uint64(1), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
			}}))
			raw, err := etcdraft.NewRawNode(&etcdraft.Config{ID: 1, Storage: storage,
				ElectionTick: defaultElectionTick, HeartbeatTick: defaultHeartbeatTick,
				MaxInflightMsgs: defaultMaxInflightMsg, MaxSizePerMsg: defaultMaxSizePerMsg})
			require.NoError(b, err)
			if leader {
				require.NoError(b, raw.Campaign())
				require.NoError(b, raw.Step(&raftpb.Message{Type: raftpb.MsgVoteResp.Enum(), From: proto.Uint64(2), To: proto.Uint64(1), Term: proto.Uint64(raw.BasicStatus().GetTerm())}))
				require.NoError(b, raw.Step(&raftpb.Message{Type: raftpb.MsgVoteResp.Enum(), From: proto.Uint64(3), To: proto.Uint64(1), Term: proto.Uint64(raw.BasicStatus().GetTerm())}))
				require.Equal(b, etcdraft.StateLeader, raw.BasicStatus().RaftState)
			}
			e := &Engine{rawNode: raw}
			request := &raftengine.NativeRequest{Operation: raftengine.NativePeerStep}
			message := raftpb.Message{Type: raftpb.MsgHeartbeatResp.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)}
			deny := func(func() error) error { return raftengine.ErrNativeAdmissionDenied }
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				e.handleStepWithAdmission(message, request, deny)
			}
		})
	}
}

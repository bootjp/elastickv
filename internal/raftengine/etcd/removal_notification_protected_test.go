package etcd

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestProtectedRemovalNeverUsesLegacyNotification(t *testing.T) {
	t.Parallel()
	payload, err := proto.Marshal(&raftpb.ConfChange{
		Type: confChangeTypePtr(raftpb.ConfChangeRemoveNode), NodeId: uint64Ptr(2),
	})
	require.NoError(t, err)
	engine := &Engine{nativeAdmission: fixtureNativeAdmission{}}
	err = engine.sendReadyMessages(etcdraft.Ready{CommittedEntries: []*raftpb.Entry{{
		Index: uint64Ptr(10), Type: raftpb.EntryConfChange.Enum(), Data: payload,
	}}})
	require.ErrorIs(t, err, raftengine.ErrNativeAdmissionDenied)
	engine.dispatchWG.Wait()
}

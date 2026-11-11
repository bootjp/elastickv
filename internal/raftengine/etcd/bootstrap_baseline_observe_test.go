package etcd

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestOfflineBaselineObserveUnsignedCompactedDataCut(t *testing.T) {
	t.Parallel()
	o, key := offlineBaselineFixture(t, false)
	before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	observed, err := ObserveOfflineBaseline(t.Context(), OfflineBaselineObservationOptions{DataDir: o.DataDir, Ownership: o.Ownership, MemberID: o.MemberID,
		Identity: o.Expected.Identity, OperationID: o.Expected.OperationID, Minimum: o.Expected.Minimum, Members: o.Expected.Configuration.Members, ProbeStore: o.ProbeStore})
	require.NoError(t, err)
	require.Empty(t, observed.Evidence.Signature)
	require.Empty(t, observed.Evidence.PublicKeyDER)
	require.Equal(t, uint64(1), observed.Configuration.Index)
	require.Equal(t, uint64(3), observed.CommitIndex)
	require.Equal(t, uint64(4), observed.CommitTerm)
	require.Equal(t, o.Expected.DataDigest, observed.DataDigest)
	require.Equal(t, o.Expected.Watermark, observed.Watermark)
	signed, err := raftbootstrap.SignBaselineProbe(observed, key)
	require.NoError(t, err)
	require.NoError(t, raftbootstrap.VerifyBaselineProbe(signed, observed.BaselineContext))
	after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestOfflineBaselineCompactedEpochIsProvenByCommittedConfChangeTail(t *testing.T) {
	t.Parallel()
	change, err := proto.Marshal(&raftpb.ConfChange{Type: raftpb.ConfChangeAddLearnerNode.Enum(), NodeId: proto.Uint64(8)})
	require.NoError(t, err)
	image := newBaselineWALImage()
	image.hardState = &raftpb.HardState{Term: proto.Uint64(9), Commit: proto.Uint64(5)}
	image.entries[4] = &raftpb.Entry{Index: proto.Uint64(4), Term: proto.Uint64(5), Type: raftpb.EntryConfChange.Enum(), Data: change}
	image.entries[5] = &raftpb.Entry{Index: proto.Uint64(5), Term: proto.Uint64(5), Type: raftpb.EntryNormal.Enum()}
	snapshot := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(3), Term: proto.Uint64(4), ConfState: &raftpb.ConfState{Voters: []uint64{7}}}}
	conf, n, err := baselineReplayedConfiguration(image, snapshot, BaselineStoreProof{})
	require.NoError(t, err)
	require.Equal(t, uint64(4), n)
	require.Equal(t, []uint64{8}, conf.GetLearners())
	_, _, err = baselineReplayedConfiguration(image, snapshot, BaselineStoreProof{ConfigurationIndex: 3})
	require.Error(t, err, "a stale existing store epoch is not max-normalized")
	delete(image.entries, 5)
	_, _, err = baselineReplayedConfiguration(image, snapshot, BaselineStoreProof{})
	require.Error(t, err, "all slots after snapshot remain mandatory")
}

func TestOfflineBaselineObserveRefusesUnprovenOrUnappliedCut(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"no configuration proof", "unapplied", "wrong pinned address"} {
		t.Run(kind, func(t *testing.T) {
			o, _ := offlineBaselineFixture(t, false)
			probe := o.ProbeStore
			if kind == "wrong pinned address" {
				o.Expected.Configuration.Members[0].Address = "wrong:7007"
			}
			o.ProbeStore = func(ctx context.Context) (BaselineStoreProof, error) {
				proof, err := probe(ctx)
				if kind == "no configuration proof" {
					proof.ConfigurationIndex = 0
				}
				if kind == "unapplied" {
					proof.AppliedIndex = 2
				}
				return proof, err
			}
			_, err := ObserveOfflineBaseline(t.Context(), OfflineBaselineObservationOptions{DataDir: o.DataDir, Ownership: o.Ownership, MemberID: o.MemberID,
				Identity: o.Expected.Identity, OperationID: o.Expected.OperationID, Minimum: o.Expected.Minimum, Members: o.Expected.Configuration.Members, ProbeStore: o.ProbeStore})
			require.Error(t, err)
		})
	}
}

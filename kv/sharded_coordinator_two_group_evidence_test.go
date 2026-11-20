package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type twoGroupEvidenceProbeStub struct {
	group uint64
	calls int
	err   error
}

func (p *twoGroupEvidenceProbeStub) RequireTwoGroupPrepareEvidence(_ context.Context, group uint64) error {
	p.calls++
	if group != p.group {
		return ErrTwoGroupAdmission
	}
	return p.err
}

func TestTwoGroupRemoteOnlyEvidenceBuildsCanonicalManifest(t *testing.T) {
	st := store.NewMVCCStore()
	require.Nil(t, NewKvFSMWithHLC(st, NewHLC()).Apply(EncodePrepareEvidenceFence(0)))
	probe := &twoGroupEvidenceProbeStub{group: 2}
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{1: {Store: st}}, tsAllocator: twoGroupTestAllocator{}}
	coord.WithTwoGroupPrepareAdmission(func(uint64) (TwoGroupHomeLookup, error) { return nil, ErrTwoGroupAdmission })
	coord.WithTwoGroupRemoteEvidence(func(group uint64) (TwoGroupPrepareEvidenceProbe, error) {
		require.Equal(t, uint64(2), group)
		return probe, nil
	})
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_PUT, Key: []byte("home-key"), Value: []byte("home")}},
		2: {{Op: pb.Op_PUT, Key: []byte("secondary-key"), Value: []byte("secondary")}},
	}
	manifest, ids, err := coord.buildTwoGroupManifest(t.Context(), 10, 42, []byte("home-key"), grouped,
		[]uint64{1, 2}, nil, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, ids)
	require.Equal(t, 1, probe.calls)
	id := twoGroupOperationID([]byte("home-key"), 10)
	require.Equal(t, id[:], manifest.GetOperationId())
	for _, participant := range manifest.GetParticipants() {
		meta, _, err := extractTxnMeta(participant.GetPrepare().GetMutations())
		require.NoError(t, err)
		require.Equal(t, uint64(42), meta.PrepareCommitTS)
	}
	probe.err = ErrTwoGroupAdmission
	_, _, err = coord.buildTwoGroupManifest(t.Context(), 10, 42, []byte("home-key"), grouped,
		[]uint64{1, 2}, nil, 0, nil)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
	coord.twoGroupRemoteEvidence = nil
	_, _, err = coord.buildTwoGroupManifest(t.Context(), 10, 42, []byte("home-key"), grouped,
		[]uint64{1, 2}, nil, 0, nil)
	require.ErrorIs(t, err, ErrTwoGroupAdmission)
}

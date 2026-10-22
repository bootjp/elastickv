package kv

import (
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestPrepareEvidenceMetasReservesSharedTimestampOnce(t *testing.T) {
	for _, phaseD := range []bool{false, true} {
		t.Run(map[bool]string{false: "pre-phase-d", true: "phase-d"}[phaseD], func(t *testing.T) {
			groups := make(map[uint64]*ShardGroup)
			for _, id := range []uint64{1, 2, 3} {
				st := store.NewMVCCStore()
				groups[id] = &ShardGroup{Store: st, Txn: NewTransactionWithProposer(&recordingTSOEngine{})}
				if id != 3 {
					require.Nil(t, NewKvFSMWithHLC(st, NewHLC()).Apply(EncodePrepareEvidenceFence(0)))
				}
			}
			allocator := &phaseDTestAllocator{phaseDActive: phaseD, phaseDRequired: phaseD}
			coord := NewShardedCoordinator(distribution.NewEngine(), groups, 1, NewHLC(), nil).WithTSOAllocator(allocator)
			metas, err := coord.prepareEvidenceMetas(t.Context(), []byte("primary"), 100, []uint64{1, 2, 3})
			require.NoError(t, err)
			require.Len(t, metas, 3)
			for id, mutation := range metas {
				meta, err := DecodeTxnMeta(mutation.Value)
				require.NoError(t, err)
				if id == 3 {
					require.Zero(t, meta.PrepareCommitTS)
				} else {
					require.Equal(t, uint64(100), meta.PrepareCommitTS)
				}
			}
			require.Equal(t, uint64(1), allocator.nextCalls.Load()+allocator.validateCalls.Load(),
				"one transaction shares one reservation/validation across enabled groups")
		})
	}
}

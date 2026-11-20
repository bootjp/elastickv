package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// The invariant is that at least one switch stays on. Both clients read both
// switches at the same snapshot. A single-key CAS protects only its own switch.
func TestVersionCASAndReadSetWriteSkew(t *testing.T) {
	for _, mode := range []string{"exact-key", "validate-read-set"} {
		t.Run(mode, func(t *testing.T) {
			fsm, st := newCommitTSFenceFSM(t)
			enableCASApplyForTest(t, fsm)
			keys := [][]byte{[]byte("a"), []byte("b")}
			expected := []uint64{10, 20}
			for i, k := range keys {
				fsm.SetApplyIndex(uint64(i + 2))
				require.NoError(t, applyFSMRequest(t, fsm, rawPut(expected[i], string(k), "on")))
			}
			snapshot := st.LastCommitTS()
			for _, k := range keys {
				value, err := st.GetAt(t.Context(), k, snapshot)
				require.NoError(t, err)
				require.Equal(t, []byte("on"), value)
			}

			for i, k := range keys {
				req := onePhaseReq(snapshot, uint64(30+i), 0, k, []byte("off"))
				if mode == "exact-key" {
					req.Mutations[1].Op = pb.Op_COMPARE_AND_SWAP
					req.Mutations[1].ExpectedCommitTs = expected[i]
				} else {
					req.ReadKeys = keys
				}
				fsm.SetApplyIndex(uint64(100 + i))
				payload, err := marshalRaftCommand([]*pb.Request{req})
				require.NoError(t, err)
				result := fsm.Apply(payload)
				if mode == "validate-read-set" && i == 1 {
					rejection, ok := result.(error)
					require.True(t, ok)
					require.ErrorIs(t, rejection, store.ErrWriteConflict)
				} else {
					require.Nil(t, result)
				}
			}
			value, err := st.GetAt(t.Context(), keys[1], st.LastCommitTS())
			require.NoError(t, err)
			if mode == "validate-read-set" {
				require.Equal(t, []byte("on"), value)
			} else {
				require.Equal(t, []byte("off"), value)
			}
		})
	}
}

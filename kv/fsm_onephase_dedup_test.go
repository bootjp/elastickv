package kv

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// onePhaseReq builds a single-shard (Phase_NONE) transactional request whose
// meta carries commitTS and, when non-zero, prevCommitTS — the option-2
// idempotency dedup probe key. The mutation set is a single PUT of key=value,
// mirroring the failed attempt's reused write set on a retry.
func onePhaseReq(startTS, commitTS, prevCommitTS uint64, key, value []byte) *pb.Request {
	return &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_NONE,
		Ts:    startTS,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{
				PrimaryKey:   key,
				CommitTS:     commitTS,
				PrevCommitTS: prevCommitTS,
			})},
			{Op: pb.Op_PUT, Key: key, Value: value},
		},
	}
}

// TestOnePhaseDedup_NoOpsWhenPriorAttemptLanded is the core option-2 case:
// the first attempt committed at T1 (its raft entry survived the churn that
// returned an ambiguous error), then the adapter retried with the SAME write
// set under a fresh commit_ts T2 and prev_commit_ts=T1. The FSM probe finds
// the prior version at exactly T1 and no-ops the whole apply, so no second
// version is written at T2 — preventing the duplicate. This runs at the
// retry entry's deterministic apply point, so every node computes the same
// no-op decision.
func TestOnePhaseDedup_NoOpsWhenPriorAttemptLanded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	key := []byte("list-item")

	// Attempt 1 lands at commit_ts 20.
	require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(10, 20, 0, key, []byte("v"))))
	landed, err := st.CommittedVersionAt(ctx, key, 20)
	require.NoError(t, err)
	require.True(t, landed)

	// Retry: reused write set, fresh commit_ts 40, prev_commit_ts=20.
	require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(30, 40, 20, key, []byte("v"))))

	// The retry must be a no-op: no version at 40, and the newest version is
	// still the one from attempt 1 at 20.
	at40, err := st.CommittedVersionAt(ctx, key, 40)
	require.NoError(t, err)
	require.False(t, at40, "retry must not write a second version at the fresh commit_ts")

	latest, exists, err := st.LatestCommitTS(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(20), latest, "newest version must remain attempt 1's at 20")
}

// TestOnePhaseDedup_AppliesWhenPriorAttemptDidNotLand covers the truncated /
// never-applied case: prev_commit_ts is set but no version exists at exactly
// that timestamp (attempt 1's entry lost the log race). The probe misses and
// the retry applies its reused write set at the fresh commit_ts.
func TestOnePhaseDedup_AppliesWhenPriorAttemptDidNotLand(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	key := []byte("list-item")

	// No attempt 1 ever landed; the retry carries prev_commit_ts=20 anyway.
	require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(30, 40, 20, key, []byte("v"))))

	at40, err := st.CommittedVersionAt(ctx, key, 40)
	require.NoError(t, err)
	require.True(t, at40, "with no prior version at 20, the retry must apply at 40")

	val, err := st.GetAt(ctx, key, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

// TestOnePhaseDedup_FirstAttemptSkipsProbe confirms the probe is inert when
// prev_commit_ts is zero (the first attempt / pre-feature path): the apply
// proceeds exactly as before, leaving the FSM byte-identical to today for
// every non-retry request.
func TestOnePhaseDedup_FirstAttemptSkipsProbe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	key := []byte("k")
	require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(10, 20, 0, key, []byte("v"))))

	at20, err := st.CommittedVersionAt(ctx, key, 20)
	require.NoError(t, err)
	require.True(t, at20)
}

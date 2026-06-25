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

// TestOnePhaseDedup_OtherTxnVersionDoesNotMaskRetry closes the M2 open item
// (design doc §M2 "Still open"): exactness must be pinned at the apply layer
// for the OTHER-txn case. The store layer's TestCommittedVersionAt_PebbleStore
// /-_MVCCStore pin (load-bearing exactness suite) already proves that a
// version at T_other ≠ T1 does not satisfy a probe at T1. This test extends
// that pin to the FSM apply path: if a third party committed a version at
// some `T_other ≠ prev_commit_ts` for the SAME primary key, the FSM probe
// at prev_commit_ts must miss and the retry must apply at the fresh
// commit_ts. Without exactness, the probe would alias on T_other and
// incorrectly no-op the retry — silently dropping the user's write.
//
// Scenario:
//  1. Third-party txn writes key=other at T_other=20.
//  2. Adapter's attempt 1 at commit_ts T1=30 returns ambiguous error and
//     was actually truncated (no version at T1).
//  3. Retry arrives at fresh commit_ts T2=40 with prev_commit_ts=T1=30.
//  4. FSM probes CommittedVersionAt(key, 30) → MISS (only T_other=20 exists),
//     falls through to apply, writing key=v at T2=40.
//
// If exactness were broken (probe at 30 returned true because T_other=20
// existed), the retry would no-op and v would be permanently lost.
func TestOnePhaseDedup_OtherTxnVersionDoesNotMaskRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	fsm, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
	require.True(t, ok)

	key := []byte("list-item")

	// Third party commits an unrelated version at T_other=20.
	require.NoError(t, st.PutAt(ctx, key, []byte("other"), 20, 0))

	// Retry: claims prev_commit_ts=T1=30 (an "attempt 1" that never landed,
	// no version at 30 exists) and fresh commit_ts T2=40. The third-party
	// version at 20 must NOT cause the probe at 30 to hit.
	require.NoError(t, applyFSMRequest(t, fsm, onePhaseReq(35, 40, 30, key, []byte("v"))))

	// FSM apply layer pin: probe MUST miss exact 30, retry MUST apply at 40.
	at30, err := st.CommittedVersionAt(ctx, key, 30)
	require.NoError(t, err)
	require.False(t, at30, "no attempt 1 landed at 30; the third-party version at 20 must not satisfy the exact-30 probe")

	at40, err := st.CommittedVersionAt(ctx, key, 40)
	require.NoError(t, err)
	require.True(t, at40, "with the exact-30 probe missing, the retry must apply at the fresh commit_ts 40")

	// LatestCommitTS reflects T2=40 (the retry's fresh write supersedes both
	// the third-party version at 20 and the never-landed attempt 1 at 30).
	latest, exists, err := st.LatestCommitTS(ctx, key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(40), latest, "newest version must be the retry's fresh apply at 40, not the third-party 20")

	val, err := st.GetAt(ctx, key, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val, "retry's write must be readable; exactness loss would have lost it")
}

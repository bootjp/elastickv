package kv

import (
	"bytes"
	"io"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestApplyObservesCommitTSIntoHLC verifies HLC-4 precondition (ii) /
// strategy (c) at the FSM level: every successful Apply of a write
// request advances the shared HLC's `last` via Observe(commitTS),
// regardless of whether this node is a leader or a follower.  On a
// follower this is what closes the logical-handoff gap surfaced by
// the tla-check gap configuration on PR #856 — when a follower is
// later elected leader, its first HLC.Next() returns a value strictly
// greater than every commit it has previously applied.
//
// See:
//   - docs/design/2026_05_28_implemented_tla_safety_spec.md §5.1 HLC-4 (ii)
//   - tla/hlc/HLC.tla BecomeLeader_HLC (strategy (c))
func TestApplyObservesCommitTSIntoHLC(t *testing.T) {
	st := store.NewMVCCStore()
	hlc := NewHLC()
	fsm, ok := NewKvFSMWithHLC(st, hlc).(*kvFSM)
	require.True(t, ok)

	// Sanity: HLC starts at 0 (no in-memory issuance, no Observe).
	require.Equal(t, uint64(0), hlc.Current(),
		"newly constructed HLC should have last = 0")

	// Apply a write at commitTS = 100.  This simulates a leader
	// replicating to this node (the FSM does not care whether this
	// node is the leader; Apply is called identically on leader and
	// follower).
	put := &pb.Request{
		Ts: 100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v1")},
		},
	}
	data, err := proto.Marshal(put)
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(data))

	// HLC.last must now be at least 100.  Observe advances `last` to
	// max(last, ts), so after this single apply, last >= 100.
	require.GreaterOrEqual(t, hlc.Current(), uint64(100),
		"Apply(commitTS=100) must Observe the commitTS into HLC.last")

	// Apply a higher commit_ts — HLC.last must advance.
	put2 := &pb.Request{
		Ts: 250,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v2")},
		},
	}
	data2, err := proto.Marshal(put2)
	require.NoError(t, err)
	require.Nil(t, fsm.Apply(data2))
	require.GreaterOrEqual(t, hlc.Current(), uint64(250),
		"Apply(commitTS=250) must advance HLC.last")

	// Apply a lower commit_ts (unrealistic in real traffic but valid
	// for the model) — HLC.last must NOT regress.
	put3 := &pb.Request{
		Ts: 200,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v3")},
		},
	}
	data3, err := proto.Marshal(put3)
	require.NoError(t, err)
	// Apply may return a write-conflict error (since 200 < 250 on key
	// "k") — but it must NOT regress hlc.last.  We don't assert the
	// apply outcome, only the HLC monotonicity property.
	fsm.Apply(data3)
	require.GreaterOrEqual(t, hlc.Current(), uint64(250),
		"HLC.last must never regress, even on a stale-ts Apply")
}

// TestNewKvFSMWithHLCObservesStoreLastCommitTS simulates the new-leader
// handoff scenario the spec doc §5.1 HLC-4 (ii) describes: a follower
// whose HLC was reset (e.g. process restart, all in-memory `last` lost)
// may skip WAL replay when the durable FSM is already past the snapshot
// pointer.  By the time the FSM is constructed, HLC.last must dominate
// every previously committed timestamp — otherwise a subsequent election
// could let this node issue an HLC strictly less than a prior commit.
func TestNewKvFSMWithHLCObservesStoreLastCommitTS(t *testing.T) {
	st := store.NewMVCCStore()

	// Pre-populate the store with a write at commit_ts = 12345 — the
	// "previous leader's last committed entry" that must not be
	// regressed by a fresh leader on this node.
	const priorCommit uint64 = 12345
	require.NoError(t, st.PutAt(t.Context(), []byte("k"), []byte("v"), priorCommit, 0))

	// Construct a fresh HLC + FSM: hlc.last starts at 0 before the
	// FSM wires it to the store.
	hlc := NewHLC()
	fsm, ok := NewKvFSMWithHLC(st, hlc).(*kvFSM)
	require.True(t, ok)

	require.GreaterOrEqual(t, hlc.Current(), priorCommit,
		"post-restart HLC.last must dominate the prior leader's max commit_ts before replay")
	require.Greater(t, hlc.Next(), priorCommit,
		"first Next() after cold-start skip must be strictly above the prior commit")
	require.NotNil(t, fsm)
}

// TestRestoreObservesStoreLastCommitTSIntoHLC covers the snapshot-install path
// that bypasses Apply replay: after Restore succeeds, a node's fresh HLC must
// still dominate the restored store's max commit timestamp before it can serve.
func TestRestoreObservesStoreLastCommitTSIntoHLC(t *testing.T) {
	src := store.NewMVCCStore()
	const restoredCommit uint64 = 67890
	require.NoError(t, src.PutAt(t.Context(), []byte("restored"), []byte("v"), restoredCommit, 0))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())

	hlc := NewHLC()
	fsm, ok := NewKvFSMWithHLC(store.NewMVCCStore(), hlc).(*kvFSM)
	require.True(t, ok)
	require.Equal(t, uint64(0), hlc.Current(), "fresh target HLC starts below the snapshot data")

	require.NoError(t, fsm.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))
	require.GreaterOrEqual(t, hlc.Current(), restoredCommit,
		"Restore must observe the restored store LastCommitTS into HLC.last")
	require.Greater(t, hlc.Next(), restoredCommit,
		"first Next() after Restore must be strictly above restored commit_ts")
}

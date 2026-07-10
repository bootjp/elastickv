package kv

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type fakeRaftStatus struct {
	status raftengine.Status
}

func (f fakeRaftStatus) Status() raftengine.Status {
	return f.status
}

type deadlineCapturingStore struct {
	store.MVCCStore
	store.RetentionController

	lastCommitTS  uint64
	minRetainedTS uint64
	compactCalled bool
	compactMinTS  uint64
	hasDeadline   bool
	deadline      time.Time
}

func (s *deadlineCapturingStore) LastCommitTS() uint64 {
	return s.lastCommitTS
}

func (s *deadlineCapturingStore) MinRetainedTS() uint64 {
	return s.minRetainedTS
}

func (s *deadlineCapturingStore) SetMinRetainedTS(ts uint64) {
	s.minRetainedTS = ts
}

func (s *deadlineCapturingStore) Compact(ctx context.Context, minTS uint64) error {
	s.compactCalled = true
	s.compactMinTS = minTS
	s.deadline, s.hasDeadline = ctx.Deadline()
	return nil
}

func TestFSMCompactorCompactsEligibleRuntime(t *testing.T) {
	st := store.NewMVCCStore()
	ctx := context.Background()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v30"), 30, 0))

	compactor := NewFSMCompactor(
		[]FSMCompactRuntime{{
			GroupID: 1,
			StatusReader: fakeRaftStatus{status: raftengine.Status{
				State:        raftengine.StateFollower,
				FSMPending:   0,
				AppliedIndex: 10,
				CommitIndex:  10,
			}},
			Store: st,
		}},
		WithFSMCompactorInterval(time.Hour),
		WithFSMCompactorRetentionWindow(time.Millisecond),
	)

	require.NoError(t, compactor.SyncOnce(ctx))

	_, err := st.GetAt(ctx, []byte("k"), 20)
	require.ErrorIs(t, err, store.ErrReadTSCompacted)

	val, err := st.GetAt(ctx, []byte("k"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v30"), val)
}

func TestFSMCompactorRespectsPinnedTimestamp(t *testing.T) {
	st := store.NewMVCCStore()
	ctx := context.Background()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v30"), 30, 0))

	tracker := NewActiveTimestampTracker()
	token := tracker.Pin(20)
	defer token.Release()

	compactor := NewFSMCompactor(
		[]FSMCompactRuntime{{
			GroupID: 1,
			StatusReader: fakeRaftStatus{status: raftengine.Status{
				State:        raftengine.StateFollower,
				FSMPending:   0,
				AppliedIndex: 10,
				CommitIndex:  10,
			}},
			Store: st,
		}},
		WithFSMCompactorActiveTimestampTracker(tracker),
		WithFSMCompactorInterval(time.Hour),
		WithFSMCompactorRetentionWindow(time.Millisecond),
	)

	require.NoError(t, compactor.SyncOnce(ctx))

	val, err := st.GetAt(ctx, []byte("k"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("v20"), val)
}

func TestFSMCompactorSkipsLaggingRuntime(t *testing.T) {
	st := store.NewMVCCStore()
	ctx := context.Background()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))

	compactor := NewFSMCompactor(
		[]FSMCompactRuntime{{
			GroupID: 1,
			StatusReader: fakeRaftStatus{status: raftengine.Status{
				State:        raftengine.StateFollower,
				FSMPending:   1,
				AppliedIndex: 9,
				CommitIndex:  10,
			}},
			Store: st,
		}},
		WithFSMCompactorInterval(time.Hour),
		WithFSMCompactorRetentionWindow(time.Millisecond),
	)

	require.NoError(t, compactor.SyncOnce(ctx))

	val, err := st.GetAt(ctx, []byte("k"), 10)
	require.NoError(t, err)
	require.Equal(t, []byte("v10"), val)
}

func TestFSMCompactorCompactsLeaderRuntimeWithShortTimeout(t *testing.T) {
	const leaderTimeout = 20 * time.Millisecond
	st := &deadlineCapturingStore{lastCommitTS: 20}
	ctx := context.Background()

	compactor := NewFSMCompactor(
		[]FSMCompactRuntime{{
			GroupID: 1,
			StatusReader: fakeRaftStatus{status: raftengine.Status{
				State:        raftengine.StateLeader,
				FSMPending:   0,
				AppliedIndex: 10,
				CommitIndex:  10,
			}},
			Store: st,
		}},
		WithFSMCompactorInterval(time.Hour),
		WithFSMCompactorRetentionWindow(time.Millisecond),
		WithFSMCompactorTimeout(time.Hour),
		WithFSMCompactorLeaderTimeout(leaderTimeout),
	)

	start := time.Now()
	require.NoError(t, compactor.SyncOnce(ctx))

	require.True(t, st.compactCalled)
	require.Equal(t, uint64(20), st.compactMinTS)
	require.True(t, st.hasDeadline)
	require.LessOrEqual(t, st.deadline.Sub(start), leaderTimeout+50*time.Millisecond)
	require.Greater(t, st.deadline.Sub(start), time.Duration(0))
}

func TestFSMCompactorCompactsEligiblePebbleRuntime(t *testing.T) {
	dir, err := os.MkdirTemp("", "fsm-compactor-pebble-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	st, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	defer st.Close()

	ctx := context.Background()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v30"), 30, 0))

	compactor := NewFSMCompactor(
		[]FSMCompactRuntime{{
			GroupID: 1,
			StatusReader: fakeRaftStatus{status: raftengine.Status{
				State:        raftengine.StateFollower,
				FSMPending:   0,
				AppliedIndex: 10,
				CommitIndex:  10,
			}},
			Store: st,
		}},
		WithFSMCompactorInterval(time.Hour),
		WithFSMCompactorRetentionWindow(time.Millisecond),
	)

	require.NoError(t, compactor.SyncOnce(ctx))

	_, err = st.GetAt(ctx, []byte("k"), 20)
	require.ErrorIs(t, err, store.ErrReadTSCompacted)

	val, err := st.GetAt(ctx, []byte("k"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v30"), val)
}

package kv

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type fakeRaftStats struct {
	stats map[string]string
}

func (f fakeRaftStats) Stats() map[string]string {
	return f.stats
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
			Raft: fakeRaftStats{stats: map[string]string{
				"state":         "Follower",
				"fsm_pending":   "0",
				"applied_index": "10",
				"commit_index":  "10",
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
			Raft: fakeRaftStats{stats: map[string]string{
				"state":         "Follower",
				"fsm_pending":   "0",
				"applied_index": "10",
				"commit_index":  "10",
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
			Raft: fakeRaftStats{stats: map[string]string{
				"state":         "Follower",
				"fsm_pending":   "1",
				"applied_index": "9",
				"commit_index":  "10",
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
			Raft: fakeRaftStats{stats: map[string]string{
				"state":         "Follower",
				"fsm_pending":   "0",
				"applied_index": "10",
				"commit_index":  "10",
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

package store

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func absenceTestStores(t *testing.T) map[string]func(*testing.T) MVCCStore {
	t.Helper()
	return map[string]func(*testing.T) MVCCStore{
		"memory": func(t *testing.T) MVCCStore { t.Helper(); return NewMVCCStore() },
		"pebble": func(t *testing.T) MVCCStore {
			t.Helper()
			s, err := NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			return s
		},
	}
}

func TestApplyMutationsRequireAbsentRejectsOldVersionsAtomically(t *testing.T) {
	t.Parallel()
	for name, factory := range absenceTestStores(t) {
		for _, deleted := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/value", true: "/tombstone"}[deleted], func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				s := factory(t)
				key := []byte("metadata")
				require.NoError(t, s.PutAt(ctx, key, []byte("user"), 5, 0))
				if deleted {
					require.NoError(t, s.DeleteAt(ctx, key, 6))
				}
				// Ordinary OCC at StartTS=100 does not reject this older version.
				err := s.ApplyMutationsRaftAt(ctx, []*KVPairMutation{
					{Op: OpTypePut, Key: []byte("other"), Value: []byte("must not land")},
					{Op: OpTypePut, Key: key, Value: []byte("migration"), RequireAbsent: true},
				}, nil, 100, 101, 1)
				require.Error(t, err)
				_, err = s.GetAt(ctx, []byte("other"), 200)
				require.ErrorIs(t, err, ErrKeyNotFound)
				got, err := s.GetAt(ctx, key, 200)
				if deleted {
					require.ErrorIs(t, err, ErrKeyNotFound)
				} else {
					require.NoError(t, err)
					require.Equal(t, []byte("user"), got)
				}
			})
		}
	}
}

func TestApplyMutationsRequireAbsentAllowsOnlyOneConcurrentWinner(t *testing.T) {
	t.Parallel()
	for name, factory := range absenceTestStores(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := factory(t)
			start := make(chan struct{})
			results := make(chan error, 2)
			var workers sync.WaitGroup
			for _, startTS := range []uint64{100, 200} {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					results <- s.ApplyMutations(context.Background(), []*KVPairMutation{{Op: OpTypePut, Key: []byte("key"), Value: []byte("value"), RequireAbsent: true}}, nil, startTS, startTS+1)
				}()
			}
			close(start)
			workers.Wait()
			close(results)
			var successes int
			for err := range results {
				if err == nil {
					successes++
				}
			}
			require.Equal(t, 1, successes)
		})
	}
}

func TestApplyRaftEntryAbsenceGuardSeesEarlierRequest(t *testing.T) {
	ctx := context.Background()
	s := newApplyIndexPebbleStore(t)
	applier := raftEntryApplier(t, s)
	require.NoError(t, applier.ApplyRaftEntry(ctx, 7, func(ctx context.Context) error {
		require.NoError(t, s.ApplyMutationsRaftAt(ctx, raftEntryPut("metadata", "user"), nil, 4, 5, 7))
		err := s.ApplyMutationsRaftAt(ctx, []*KVPairMutation{
			{Op: OpTypePut, Key: []byte("other"), Value: []byte("must not land")},
			{Op: OpTypePut, Key: []byte("metadata"), Value: []byte("migration"), RequireAbsent: true},
		}, nil, 100, 101, 7)
		require.ErrorIs(t, err, ErrWriteConflict)
		return nil
	}))
	require.False(t, raftEntryExists(t, s, "other"))
	got, err := s.GetAt(ctx, []byte("metadata"), 200)
	require.NoError(t, err)
	require.Equal(t, []byte("user"), got)
}

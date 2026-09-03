package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// snapshotTS answers ^uint64(0) for a store with no committed record. That is
// the "latest of everything" sentinel a direct MVCC read wants, but Phase D
// rejects it, so a fenced read of a nonexistent key on a fresh cluster would
// fail instead of returning empty -- and Phase D could never activate through
// that path.
func TestRedisFencedSelectTSNormalizesTheEmptyStoreSentinel(t *testing.T) {
	t.Parallel()

	empty := store.NewMVCCStore()
	t.Cleanup(func() { _ = empty.Close() })
	// The sentinel comes from the real production helper, not a literal.
	require.Equal(t, ^uint64(0), snapshotTS(nil, empty), "fixture must reproduce the sentinel")

	require.Equal(t, uint64(1), redisFencedSelectTS(func() uint64 { return snapshotTS(nil, empty) }))
	require.Equal(t, uint64(1), redisFencedSelectTS(func() uint64 { return 0 }))
	require.Equal(t, uint64(1), redisFencedSelectTS(nil))

	// A real watermark passes through untouched.
	ctx := context.Background()
	seeded := store.NewMVCCStore()
	t.Cleanup(func() { _ = seeded.Close() })
	require.NoError(t, seeded.PutAt(ctx, []byte("k"), []byte("v"), 42, 0))
	require.Equal(t, uint64(42), redisFencedSelectTS(func() uint64 { return snapshotTS(nil, seeded) }))
}

// The production path is the point: a fenced read on a Phase-D cluster whose
// store holds no committed record must return a usable read timestamp rather
// than ErrTSOTimestampInvalid. This is the path LRANGE takes through
// fenceRangeListReadGroups.
func TestRedisFencedReadTimestampAcceptsAnEmptyStoreUnderPhaseD(t *testing.T) {
	t.Parallel()

	empty := store.NewMVCCStore()
	t.Cleanup(func() { _ = empty.Close() })
	require.Equal(t, ^uint64(0), snapshotTS(nil, empty), "fixture must reproduce the sentinel")

	coord := newPhaseDVoucherCoordinator(empty)
	server := NewRedisServer(nil, "", empty, coord, nil, nil)
	t.Cleanup(server.Stop)

	readTimestamp, readPin, err := server.redisReadFencedTimestampForTargets(
		context.Background(), nil, server.readTS, "test: begin read timestamp")
	require.NoError(t, err)
	if readPin != nil {
		readPin.Release()
	}
	require.NotZero(t, readTimestamp.Timestamp())
	require.NotEqual(t, ^uint64(0), readTimestamp.Timestamp())
}

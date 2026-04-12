package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotTSDoesNotUseHLC(t *testing.T) {
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(context.Background(), []byte("key"), []byte("v"), 10, 0))

	clock := kv.NewHLC()
	clock.Observe(999) // Advance HLC far beyond LastCommitTS

	ts := snapshotTS(clock, st)
	assert.Equal(t, st.LastCommitTS(), ts,
		"snapshotTS must return LastCommitTS, not HLC.Current()")
	assert.Less(t, ts, uint64(999),
		"snapshotTS must not advance to HLC value")
}

func TestSnapshotTSFallbackWhenStoreEmpty(t *testing.T) {
	clock := kv.NewHLC()
	st := store.NewMVCCStore()

	ts := snapshotTS(clock, st)
	assert.Equal(t, ^uint64(0), ts,
		"snapshotTS must return MaxUint64 when store is empty")
}

func TestSnapshotTSNilStore(t *testing.T) {
	clock := kv.NewHLC()
	ts := snapshotTS(clock, nil)
	assert.Equal(t, ^uint64(0), ts,
		"snapshotTS must return MaxUint64 when store is nil")
}

func TestSnapshotTSNilClock(t *testing.T) {
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(context.Background(), []byte("k"), []byte("v"), 42, 0))

	ts := snapshotTS(nil, st)
	assert.Equal(t, uint64(42), ts,
		"snapshotTS must work with nil clock")
}

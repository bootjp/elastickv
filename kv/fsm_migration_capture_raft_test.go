package kv

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestFSMShadowCaptureRealEtcdRaftEntry(t *testing.T) {
	t.Parallel()
	st := g17OpenStore(t, t.TempDir())
	fsm := NewKvFSMWithHLC(st, NewHLC(), WithMigrationCaptureShadow(shadowCaptureConfig()))
	engine, stop := newSingleRaft(t, "capture", fsm)
	t.Cleanup(stop)
	result, err := engine.Propose(t.Context(), g17Entry(t, g17RawPut(100, "a", "replicated-entry")))
	require.NoError(t, err)
	require.Nil(t, result.Response)
	require.NotZero(t, result.CommitIndex)
	record, err := store.ReadMigrationCaptureEntry(t.Context(), st, shadowCaptureID(result.CommitIndex))
	require.NoError(t, err)
	require.True(t, record.Complete)
	require.Len(t, record.Effects, 1)
	require.Equal(t, result.CommitIndex, record.Identity.Index)
	require.Equal(t, []byte("replicated-entry"), record.Effects[0].Value)
	value, err := st.GetAt(t.Context(), []byte("a"), 101)
	require.NoError(t, err)
	require.Equal(t, record.Effects[0].Value, value)
	index, present := g17AppliedIndex(t, st)
	require.True(t, present)
	require.GreaterOrEqual(t, index, result.CommitIndex)
}

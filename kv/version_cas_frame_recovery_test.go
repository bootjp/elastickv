package kv

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestVersionCASFrameRecoveryAfterCommitFailure(t *testing.T) {
	dir := t.TempDir()
	open := func() (*kvFSM, store.MVCCStore) {
		st, err := store.NewPebbleStore(dir)
		require.NoError(t, err)
		f, ok := NewKvFSMWithHLC(st, NewHLC()).(*kvFSM)
		require.True(t, ok)
		return f, st
	}
	f, st := open()
	enableCASApplyForTest(t, f)
	key := []byte("original-key")
	require.NoError(t, st.PutAt(t.Context(), key, []byte("prior"), 10, 0))
	injected := errors.New("injected failure after staging CAS")
	metadata, ok := st.(store.RaftControlMetadataStore)
	require.True(t, ok)
	f.store = &struct {
		*g17FailingEntryStore
		store.RaftControlMetadataStore
	}{&g17FailingEntryStore{MVCCStore: st, err: injected}, metadata}
	req := casApplyRequest(15, 20, 10, string(key), "replacement")
	result := applyCASFrame(t, f, 2, req)
	halt, ok := result.(interface{ HaltApply() error })
	require.True(t, ok, "response = %v", result)
	require.ErrorIs(t, halt.HaltApply(), injected)
	require.NoError(t, st.Close())

	f, st = open()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	requireCASProgress(t, f, 1)
	old, err := store.GetVersionAt(t.Context(), st, key, 20)
	require.NoError(t, err)
	require.EqualValues(t, 10, old.TS)
	require.Equal(t, []byte("prior"), old.Value)
	require.Nil(t, applyCASFrame(t, f, 2, req))
	requireCASProgress(t, f, 2)
	require.Nil(t, applyCASFrame(t, f, 2, req), "durable replay skips the predicate")
	newValue, err := store.GetVersionAt(t.Context(), st, key, 20)
	require.NoError(t, err)
	require.EqualValues(t, 20, newValue.TS)
	require.Equal(t, []byte("replacement"), newValue.Value)
}

package kv

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestApplyConsumesFullEntryIdentity(t *testing.T) {
	localStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, localStore.Close()) })
	fsm, ok := NewKvFSMWithHLC(localStore, nil).(*kvFSM)
	require.True(t, ok)
	identity := raftengine.NewAppliedEntryIdentity(7, 3, 0, []byte{raftEncodeHLCLease, 0, 0, 0, 0, 0, 0, 0, 1})
	fsm.SetApplyEntryIdentity(identity)

	require.Nil(t, fsm.Apply([]byte{raftEncodeHLCLease, 0, 0, 0, 0, 0, 0, 0, 1}))
	require.Equal(t, uint64(7), fsm.pendingApplyIdx)
	require.Equal(t, raftengine.AppliedEntryIdentity{}, fsm.pendingApplyIdentity)
}

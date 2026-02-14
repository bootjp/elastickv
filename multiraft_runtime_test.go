package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestGroupDataDir(t *testing.T) {
	base := "/tmp/data"
	raftID := "n1"

	t.Run("single", func(t *testing.T) {
		require.Equal(t, filepath.Join(base, raftID), groupDataDir(base, raftID, 1, false))
	})

	t.Run("multi", func(t *testing.T) {
		require.Equal(t, filepath.Join(base, raftID, "group-2"), groupDataDir(base, raftID, 2, true))
	})
}

func TestNewRaftGroupBootstrap(t *testing.T) {
	baseDir := t.TempDir()

	st := store.NewMVCCStore()
	fsm := kv.NewKvFSM(st)

	r, tm, err := newRaftGroup(
		"n1",
		groupSpec{id: 1, address: "127.0.0.1:0"},
		baseDir,
		true, // multi
		true, // bootstrap
		fsm,
	)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.NotNil(t, tm)
	t.Cleanup(func() {
		_ = r.Shutdown().Error()
	})

	dir := groupDataDir(baseDir, "n1", 1, true)
	_, err = os.Stat(dir)
	require.NoError(t, err)

	_, err = os.Stat(filepath.Join(dir, "logs.dat"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, "stable.dat"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return r.State() == raft.Leader
	}, 5*time.Second, 10*time.Millisecond)
}

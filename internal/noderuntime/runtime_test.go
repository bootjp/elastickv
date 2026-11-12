package noderuntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupDataDir(t *testing.T) {
	t.Parallel()
	base := "/tmp/data"
	raftID := "n1"

	cases := []struct {
		name    string
		groupID uint64
		multi   bool
		want    string
	}{
		{name: "single", groupID: 1, want: filepath.Join(base, raftID)},
		{name: "reserved TSO group stays isolated in single data-group mode", groupID: 0, want: filepath.Join(base, raftID, "group-0")},
		{name: "multi", groupID: 2, multi: true, want: filepath.Join(base, raftID, "group-2")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, GroupDataDir(base, raftID, tc.groupID, tc.multi))
		})
	}
}

func TestParseRaftEngineType(t *testing.T) {
	t.Parallel()
	t.Run("default", func(t *testing.T) {
		engineType, err := ParseRaftEngineType("")
		require.NoError(t, err)
		require.Equal(t, RaftEngineEtcd, engineType)
	})

	t.Run("etcd", func(t *testing.T) {
		engineType, err := ParseRaftEngineType("etcd")
		require.NoError(t, err)
		require.Equal(t, RaftEngineEtcd, engineType)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := ParseRaftEngineType("nope")
		require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
	})
}

func TestEnsureRaftEngineDataDir(t *testing.T) {
	t.Parallel()
	t.Run("writes marker for empty dir", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, ensureRaftEngineDataDir(dir, RaftEngineEtcd))
		data, err := os.ReadFile(filepath.Join(dir, RaftEngineMarkerFile))
		require.NoError(t, err)
		require.Equal(t, "etcd\n", string(data))
	})

	t.Run("detects etcd peers metadata artifact", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "etcd-raft-peers.bin"), []byte("placeholder"), 0o600))

		engineType, ok, err := detectRaftEngineFromDataDir(dir)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, RaftEngineEtcd, engineType)
	})

	t.Run("detects bare wal dir as etcd artifact", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "wal"), 0o755))
		engineType, ok, err := detectRaftEngineFromDataDir(dir)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, RaftEngineEtcd, engineType)
	})

	t.Run("detects bare snap dir as etcd artifact", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "snap"), 0o755))
		engineType, ok, err := detectRaftEngineFromDataDir(dir)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, RaftEngineEtcd, engineType)
	})

	t.Run("rejects legacy hashicorp raft.db", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "raft.db"), []byte("legacy"), 0o600))
		err := ensureRaftEngineDataDir(dir, RaftEngineEtcd)
		require.ErrorIs(t, err, ErrLegacyHashicorpDataDir)
	})

	t.Run("rejects legacy hashicorp logs.dat", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "logs.dat"), []byte("legacy"), 0o600))
		err := ensureRaftEngineDataDir(dir, RaftEngineEtcd)
		require.ErrorIs(t, err, ErrLegacyHashicorpDataDir)
	})

	t.Run("rejects legacy hashicorp stable.dat", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "stable.dat"), []byte("legacy"), 0o600))
		err := ensureRaftEngineDataDir(dir, RaftEngineEtcd)
		require.ErrorIs(t, err, ErrLegacyHashicorpDataDir)
	})

}

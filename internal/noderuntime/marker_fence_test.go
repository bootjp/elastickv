package noderuntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadRaftEngineMarkerFence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		content string
		want    RaftEngineMarker
		bad     bool
	}{
		{name: "legacy", content: "etcd\n", want: RaftEngineMarker{Engine: RaftEngineEtcd}},
		{name: "legacy whitespace", content: " ETCD \n", want: RaftEngineMarker{Engine: RaftEngineEtcd}},
		{name: "fenced", content: fencedRaftEngineMarker, want: RaftEngineMarker{Engine: RaftEngineEtcd, Fenced: true}},
		{name: "malformed fence", content: "etcd+m1b+unknown\n", bad: true},
		{name: "partial fence", content: "etcd+m1b", bad: true},
		{name: "other engine", content: "hashicorp\n", bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), RaftEngineMarkerFile)
			require.NoError(t, os.WriteFile(path, []byte(tc.content), RaftEngineMarkerPerm))
			marker, ok, err := ReadRaftEngineMarkerFence(path)
			if tc.bad {
				require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
				require.False(t, ok)
				return
			}
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.want, marker)
		})
	}

	marker, ok, err := ReadRaftEngineMarkerFence(filepath.Join(t.TempDir(), RaftEngineMarkerFile))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, RaftEngineMarker{}, marker)
	_, err = ParseRaftEngineType(fencedRaftEngineMarker)
	require.ErrorIs(t, err, ErrUnsupportedRaftEngine)
}

func TestUpgradeRaftEngineMarkerFence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, RaftEngineMarkerFile)
	require.NoError(t, ensureRaftEngineDataDir(dir, RaftEngineEtcd))
	require.NoError(t, ensureRaftEngineDataDir(dir, RaftEngineEtcd))
	legacy, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "etcd\n", string(legacy))
	require.NoError(t, UpgradeRaftEngineMarkerFence(dir))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, fencedRaftEngineMarker, string(data))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(RaftEngineMarkerPerm), info.Mode().Perm())

	// Reopening the directory must recognize the fence without replacing it.
	require.NoError(t, ensureRaftEngineDataDir(dir, RaftEngineEtcd))
	marker, ok, err := ReadRaftEngineMarkerFence(path)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, RaftEngineMarker{Engine: RaftEngineEtcd, Fenced: true}, marker)
	engine, ok, err := ReadRaftEngineMarker(path)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, RaftEngineEtcd, engine)

	require.NoError(t, UpgradeRaftEngineMarkerFence(dir))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, RaftEngineMarkerFile, entries[0].Name())
}

func TestUpgradeRaftEngineMarkerFenceRejectsMissingAndMalformed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, RaftEngineMarkerFile)
	require.ErrorIs(t, UpgradeRaftEngineMarkerFence(dir), os.ErrNotExist)
	require.NoError(t, os.WriteFile(path, []byte("etcd+m1b+unknown\n"), RaftEngineMarkerPerm))
	require.ErrorIs(t, UpgradeRaftEngineMarkerFence(dir), ErrUnsupportedRaftEngine)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "etcd+m1b+unknown\n", string(data))
}

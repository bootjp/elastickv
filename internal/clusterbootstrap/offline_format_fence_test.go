package clusterbootstrap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func TestOfflineProtectedPreparationPreservesM1bFence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	backupRoot := t.TempDir()
	config := raftbootstrap.Configuration{Index: 1,
		State:   raftbootstrap.ConfState{Voters: []uint64{1}},
		Members: []raftbootstrap.Member{{ID: 1, Address: "127.0.0.1:50051", PublicKeyDigest: raftbootstrap.Digest{1}}}}
	c := etcd.BaselinePreparationContext{DataDir: dir, BackupDir: filepath.Join(backupRoot, "backup"),
		Manifest: raftbootstrap.BaselineManifest{BaselineContext: raftbootstrap.BaselineContext{
			Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1}, Configuration: config,
			Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		}}}
	markerPath := filepath.Join(dir, raftbootstrap.MarkerFile)
	require.NoError(t, os.WriteFile(markerPath, []byte(raftbootstrap.MarkerLegacyFenced), 0o600))
	require.NoError(t, stageBootstrapRequiredFormat(c))
	require.NoError(t, verifyBootstrapRequiredFormat(c))
	marker, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.MarkerV2, string(marker))
	require.NoError(t, stageBootstrapRequiredFormat(c), "retry must retain the fenced marker")
	marker, err = os.ReadFile(markerPath)
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.MarkerV2, string(marker))
}

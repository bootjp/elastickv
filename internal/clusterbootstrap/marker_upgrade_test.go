package clusterbootstrap

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func markerUpgradeFixture(t *testing.T) (string, raftbootstrap.Identity) {
	t.Helper()
	dir := t.TempDir()
	identity := raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 1}
	intent := raftbootstrap.Intent{Identity: identity, BaselineKind: raftbootstrap.Accepted,
		Baseline: raftbootstrap.Configuration{Index: 1,
			State:   raftbootstrap.ConfState{Voters: []uint64{7}},
			Members: []raftbootstrap.Member{{ID: 7, Address: "127.0.0.1:50051", PublicKeyDigest: raftbootstrap.Digest{1}}}},
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}}
	encoded, err := raftbootstrap.MarshalIntent(intent)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, raftbootstrap.IntentFile), encoded, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, raftbootstrap.MarkerFile), []byte(raftbootstrap.MarkerV1), 0o600))
	walDir := filepath.Join(dir, "wal")
	require.NoError(t, os.Mkdir(walDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "0000000000000000-0000000000000000.wal"), []byte("wal"), 0o600))
	return dir, identity
}

func TestBootstrapMarkerUpgradeCLI(t *testing.T) {
	t.Parallel()
	dir, identity := markerUpgradeFixture(t)
	args := []string{"upgrade-marker", "--raft-data-dir", dir, "--cluster-uuid", "01000000-0000-0000-0000-000000000000",
		"--group-id", "1", "--member-id", "7", "--dry-run"}
	request, err := ParseBootstrapMarkerUpgradeCLI(args)
	require.NoError(t, err)
	require.Equal(t, BootstrapMarkerUpgradeRequest{DataDir: dir, Identity: identity, MemberID: 7, DryRun: true}, request)
	for _, invalid := range [][]string{
		{"upgrade-marker"},
		append(append([]string{}, args...), "extra"),
		append(append([]string{}, args...), "--group-id", "2"),
		{"upgrade-marker", "--raft-data-dir", "relative", "--cluster-uuid", "01000000-0000-0000-0000-000000000000", "--group-id", "1", "--member-id", "7"},
	} {
		_, err := ParseBootstrapMarkerUpgradeCLI(invalid)
		require.Error(t, err)
	}
}

func TestBootstrapMarkerUpgradeDryRunAndApply(t *testing.T) {
	t.Parallel()
	dir, identity := markerUpgradeFixture(t)
	request := BootstrapMarkerUpgradeRequest{DataDir: dir, Identity: identity, MemberID: 7, DryRun: true}
	result, err := ExecuteBootstrapMarkerUpgrade(request)
	require.NoError(t, err)
	require.True(t, result.DryRun)
	require.False(t, result.Changed)
	require.False(t, result.AlreadyFenced)
	marker, err := os.ReadFile(filepath.Join(dir, raftbootstrap.MarkerFile))
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.MarkerV1, string(marker))

	request.DryRun = false
	var output bytes.Buffer
	args := []string{"bootstrap", "upgrade-marker", "--raft-data-dir", dir, "--cluster-uuid", "01000000-0000-0000-0000-000000000000",
		"--group-id", "1", "--member-id", "7"}
	handled, err := DispatchBootstrapOperator(args, &output)
	require.NoError(t, err)
	require.True(t, handled)
	require.Contains(t, output.String(), `"changed":true`)
	marker, err = os.ReadFile(filepath.Join(dir, raftbootstrap.MarkerFile))
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.MarkerV2, string(marker))

	result, err = ExecuteBootstrapMarkerUpgrade(request)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.True(t, result.AlreadyFenced)
}

func TestBootstrapMarkerUpgradeRejectsWrongMemberAndMissingWAL(t *testing.T) {
	t.Parallel()
	dir, identity := markerUpgradeFixture(t)
	request := BootstrapMarkerUpgradeRequest{DataDir: dir, Identity: identity, MemberID: 8}
	_, err := ExecuteBootstrapMarkerUpgrade(request)
	require.ErrorIs(t, err, raftbootstrap.ErrInconsistent)
	request.MemberID = 7
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "wal")))
	_, err = ExecuteBootstrapMarkerUpgrade(request)
	require.Error(t, err)
	marker, readErr := os.ReadFile(filepath.Join(dir, raftbootstrap.MarkerFile))
	require.NoError(t, readErr)
	require.Equal(t, raftbootstrap.MarkerV1, string(marker))
}

func TestBootstrapMarkerUpgradeRequiresOfflineOwnership(t *testing.T) {
	t.Parallel()
	dir, identity := markerUpgradeFixture(t)
	owner, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	_, err = ExecuteBootstrapMarkerUpgrade(BootstrapMarkerUpgradeRequest{DataDir: dir, Identity: identity, MemberID: 7})
	require.Error(t, err)
	marker, err := os.ReadFile(filepath.Join(dir, raftbootstrap.MarkerFile))
	require.NoError(t, err)
	require.Equal(t, raftbootstrap.MarkerV1, string(marker))
}

package etcd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRuntimeNamespacePreallocationPreservesStrictOfflineRules(t *testing.T) {
	o, _ := offlineBaselineFixture(t, true)
	require.NoError(t, o.Ownership.Close())
	wdir := filepath.Join(o.DataDir, walDirName)
	original, err := readBaselineWALPayloads(wdir, true)
	require.NoError(t, err)
	for _, name := range []string{"0.tmp", "1.tmp"} {
		require.NoError(t, os.WriteFile(filepath.Join(wdir, name), []byte("unpublished native preallocation"), baselinePrivateFile))
		before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
		require.NoError(t, err)
		image, err := readRuntimeNamespaceWAL(wdir)
		require.NoError(t, err)
		require.True(t, original.equivalent(image))
		ownership, err := AcquireNamespaceRecoveryOwnership(o.DataDir)
		require.NoError(t, err)
		_, err = verifyBaselineOwnership(OfflineBaselineOptions{DataDir: o.DataDir, Ownership: ownership})
		require.Error(t, err, "runtime token cannot authorize offline migration or signing")
		require.NoError(t, ownership.Close())
		_, err = readBaselineWALPayloads(wdir, true)
		require.ErrorIs(t, err, ErrOfflineBaseline)
		offline, err := AcquireOfflineBaselineOwnership(o.DataDir)
		if offline != nil {
			require.NoError(t, offline.Close())
		}
		require.ErrorIs(t, err, ErrOfflineBaseline)
		after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
		require.NoError(t, err)
		require.Equal(t, before, after, "runtime readers/ownership must not delete or rewrite leftovers")
	}
}

func TestRuntimeNamespacePreallocationNativeRecoveryAndFence(t *testing.T) {
	o, identity, oldGuard := namespaceRecoveryFixture(t)
	require.NoError(t, oldGuard.Close())
	wdir := filepath.Join(o.DataDir, walDirName)
	for _, name := range []string{"0.tmp", "1.tmp"} {
		require.NoError(t, os.WriteFile(filepath.Join(wdir, name), nil, baselinePrivateFile))
	}
	ownership, err := AcquireNamespaceRecoveryOwnership(o.DataDir)
	require.NoError(t, err)
	defer ownership.Close()
	guard, err := claimNamespaceRecoveryOwnership(OpenConfig{DataDir: o.DataDir, namespaceOwnership: ownership})
	require.NoError(t, err)
	defer guard.Close()
	require.NoError(t, ownership.Close())
	other, err := AcquireNamespaceRecoveryOwnership(o.DataDir)
	if other != nil {
		require.NoError(t, other.Close())
	}
	require.Error(t, err, "handoff must retain the root fence")
	probe := &namespaceRecoveryRestoreProbe{}
	called := false
	disk, err := loadWalStateRecovery(zap.NewNop(), wdir, filepath.Join(o.DataDir, snapDirName), filepath.Join(o.DataDir, fsmSnapDirName), probe, nil,
		func(_ context.Context, source NamespaceRecoverySource) (uint64, bool, error) {
			called = true
			require.Equal(t, *identity, source.Identity())
			return source.CommitIndex(), true, nil
		}, identity)
	require.NoError(t, err)
	require.True(t, called)
	require.False(t, probe.restored)
	require.NoError(t, disk.Persist.Close())
	require.NoError(t, guard.Close())
	other, err = AcquireNamespaceRecoveryOwnership(o.DataDir)
	require.NoError(t, err)
	require.NoError(t, other.Close())
}

func TestRuntimeNamespacePreallocationRejectsUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"unknown", "symlink", "directory", "wal-alias", "snapshot-alias", "external-alias", "temporary-alias"} {
		t.Run(kind, func(t *testing.T) {
			o, _ := offlineBaselineFixture(t, true)
			require.NoError(t, o.Ownership.Close())
			wdir := filepath.Join(o.DataDir, walDirName)
			stageUnsafeNamespacePreallocation(t, o.DataDir, kind)
			_, err := readRuntimeNamespaceWAL(wdir)
			require.Error(t, err)
			ownership, err := AcquireNamespaceRecoveryOwnership(o.DataDir)
			if ownership != nil {
				require.NoError(t, ownership.Close())
			}
			require.Error(t, err)
		})
	}
}

func stageUnsafeNamespacePreallocation(t *testing.T, dir, kind string) {
	t.Helper()
	wdir := filepath.Join(dir, walDirName)
	path := filepath.Join(wdir, "0.tmp")
	switch kind {
	case "unknown":
		require.NoError(t, os.WriteFile(filepath.Join(wdir, "x.tmp"), nil, baselinePrivateFile))
	case "symlink":
		require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "missing"), path))
	case "directory":
		require.NoError(t, os.Mkdir(path, baselinePrivateDirectory))
	case "wal-alias":
		names, err := baselineWALNames(wdir)
		require.NoError(t, err)
		require.NoError(t, os.Link(filepath.Join(wdir, names[0]), path))
	case "snapshot-alias":
		files, err := os.ReadDir(filepath.Join(dir, snapDirName))
		require.NoError(t, err)
		require.NotEmpty(t, files)
		require.NoError(t, os.Link(filepath.Join(dir, snapDirName, files[0].Name()), path))
	case "external-alias":
		outside := filepath.Join(t.TempDir(), "retained")
		require.NoError(t, os.WriteFile(outside, []byte("retained bytes"), baselinePrivateFile))
		require.NoError(t, os.Link(outside, path))
	case "temporary-alias":
		require.NoError(t, os.WriteFile(path, nil, baselinePrivateFile))
		require.NoError(t, os.Link(path, filepath.Join(wdir, "1.tmp")))
	default:
		t.Fatalf("unknown fixture %q", kind)
	}
}

func TestRuntimeNamespaceOwnershipRechecksBeforeHandoff(t *testing.T) {
	o, _ := offlineBaselineFixture(t, true)
	require.NoError(t, o.Ownership.Close())
	path := filepath.Join(o.DataDir, walDirName, "0.tmp")
	require.NoError(t, os.WriteFile(path, []byte("original"), baselinePrivateFile))
	ownership, err := AcquireNamespaceRecoveryOwnership(o.DataDir)
	require.NoError(t, err)
	defer ownership.Close()
	require.NoError(t, os.Link(path, filepath.Join(t.TempDir(), "new-alias")))
	guard, err := claimNamespaceRecoveryOwnership(OpenConfig{DataDir: o.DataDir, namespaceOwnership: ownership})
	if guard != nil {
		require.NoError(t, guard.Close())
	}
	require.Error(t, err, "a new hardlink must fail before native preallocation can reuse the inode")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "original", string(got))
}

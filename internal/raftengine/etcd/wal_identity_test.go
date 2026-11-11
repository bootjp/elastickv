package etcd

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.uber.org/zap"
)

func identityFixture() *WALIdentity {
	return &WALIdentity{Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 3}, NodeID: 7, BaselineDigest: raftbootstrap.Digest{2}}
}

func TestNativeWALIdentityRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	wdir, sdir, fdir := filepath.Join(dir, "wal"), filepath.Join(dir, "snap"), filepath.Join(dir, "fsm")
	id := identityFixture()
	disk, err := persistBootState(zap.NewNop(), wdir, sdir, fdir, nil, persistedState{}, id)
	require.NoError(t, err)
	require.Equal(t, id, disk.VerifiedIdentity)
	require.NotSame(t, id, disk.VerifiedIdentity)
	require.NoError(t, disk.Persist.Close())
	selected, err := readWALRestoreState(zap.NewNop(), wdir, sdir, id)
	require.NoError(t, err)
	require.Equal(t, id, selected.verifiedIdentity)
	require.NoError(t, selected.w.Close())
	for _, mutate := range []func(*WALIdentity){
		func(v *WALIdentity) { v.NodeID++ },
		func(v *WALIdentity) { v.Identity.Group++ },
		func(v *WALIdentity) { v.Identity.Cluster[0]++ },
		func(v *WALIdentity) { v.BaselineDigest[0]++ },
	} {
		wrong := *id
		mutate(&wrong)
		_, err := readWALRestoreState(zap.NewNop(), wdir, sdir, &wrong)
		require.ErrorIs(t, err, ErrWALIdentity)
	}
	selected, err = readWALRestoreState(zap.NewNop(), wdir, sdir)
	require.ErrorIs(t, err, ErrWALIdentity)
	require.Nil(t, selected)
	_, err = openDiskState(OpenConfig{DataDir: dir, NodeID: id.NodeID}, nil)
	require.ErrorIs(t, err, ErrWALIdentity)
	selected, err = readWALRestoreState(zap.NewNop(), wdir, sdir, id)
	require.NoError(t, err)
	require.NoError(t, selected.w.Close())
}

func TestNativeWALRejectsMissingOrCorruptMetadata(t *testing.T) {
	t.Parallel()
	canonical, err := encodeWALIdentity(identityFixture())
	require.NoError(t, err)
	corrupt := append([]byte(nil), canonical...)
	corrupt[len(corrupt)-1] ^= 1
	for _, metadata := range [][]byte{nil, []byte("unknown"), canonical[:20], append(canonical, 0), corrupt} {
		t.Run(stringSize(metadata), func(t *testing.T) {
			dir := t.TempDir()
			wdir := filepath.Join(dir, "wal")
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "snap"), 0o700))
			w, err := wal.Create(zap.NewNop(), wdir, metadata)
			require.NoError(t, err)
			require.NoError(t, w.Close())
			_, err = readWALRestoreState(zap.NewNop(), wdir, filepath.Join(dir, "snap"), identityFixture())
			require.ErrorIs(t, err, ErrWALIdentity)
			selected, err := readWALRestoreState(zap.NewNop(), wdir, filepath.Join(dir, "snap"))
			if len(metadata) >= 8 && string(metadata[:8]) == "EKVWID01" {
				require.ErrorIs(t, err, ErrWALIdentity)
				require.Nil(t, selected)
				return
			}
			require.NoError(t, err)
			require.Nil(t, selected.verifiedIdentity)
			require.NoError(t, selected.w.Close())
		})
	}
}

func stringSize(b []byte) string {
	return strconv.Itoa(len(b))
}

func TestProtectedWALConfigRejectsUnsafePaths(t *testing.T) {
	t.Parallel()
	id := identityFixture()
	require.ErrorIs(t, validateNativeIdentityConfig(OpenConfig{NodeID: id.NodeID + 1, NativeIdentity: id}), ErrWALIdentity)
	require.NoError(t, validateNativeIdentityConfig(OpenConfig{NodeID: id.NodeID, NativeIdentity: id}))
	require.NoError(t, validateNativeIdentityConfig(OpenConfig{}))
	_, err := migrateUnprotectedState(zap.NewNop(), "", "", "", OpenConfig{NativeIdentity: id}, persistedState{})
	require.ErrorIs(t, err, ErrWALIdentity)
	_, err = bootstrapNewCluster(zap.NewNop(), "", "", "", OpenConfig{NativeIdentity: id}, nil)
	require.ErrorIs(t, err, ErrWALIdentity)
	for _, invalid := range []*WALIdentity{
		{},
		{Identity: id.Identity, NodeID: id.NodeID},
		{Identity: id.Identity, BaselineDigest: id.BaselineDigest},
	} {
		_, err := encodeWALIdentity(invalid)
		require.ErrorIs(t, err, ErrWALIdentity)
	}
}

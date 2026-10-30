//go:build linux || darwin

package kv

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func claimSeedTestDirectory(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.Chmod(directory, 0o700))
	return directory
}

func TestTSOClaimSeedLoadNeverCreatesOrRepairs(t *testing.T) {
	dir := claimSeedTestDirectory(t)
	path := filepath.Join(dir, "missing")
	identity, err := LoadTSOClaimIdentity(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, identity)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
	_, err = ProvisionTSOClaimIdentity(filepath.Join(dir, "absent-parent", "identity"), bytes.Repeat([]byte{1}, ed25519.SeedSize))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = ProvisionTSOClaimIdentity(path, make([]byte, ed25519.SeedSize))
	require.ErrorIs(t, err, ErrTSOClaimInvalid)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestTSOClaimSeedStrictFormat(t *testing.T) {
	path := filepath.Join(claimSeedTestDirectory(t), "identity")
	_, err := ProvisionTSOClaimIdentity(path, bytes.Repeat([]byte{17}, ed25519.SeedSize))
	require.NoError(t, err)
	valid, err := os.ReadFile(path)
	require.NoError(t, err)
	version := bytes.Clone(valid)
	version[7] = '2'
	checksum := sha256.Sum256(version[:40])
	copy(version[40:], checksum[:])
	corrupt := bytes.Clone(valid)
	corrupt[8] ^= 1
	zero := bytes.Clone(valid)
	clear(zero[8:40])
	checksum = sha256.Sum256(zero[:40])
	copy(zero[40:], checksum[:])
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"empty", nil},
		{"legacy-unversioned", bytes.Repeat([]byte{17}, ed25519.SeedSize)},
		{"truncated", valid[:len(valid)-1]},
		{"trailing", append(bytes.Clone(valid), 0)},
		{"unknown-version-valid-checksum", version},
		{"corrupt-seed", corrupt},
		{"zero-seed-valid-checksum", zero},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := filepath.Join(claimSeedTestDirectory(t), "identity")
			require.NoError(t, os.WriteFile(candidate, tc.raw, 0o600))
			identity, err := LoadTSOClaimIdentity(candidate)
			require.ErrorIs(t, err, ErrTSOClaimSeedInvalid)
			require.Nil(t, identity)
			after, err := os.ReadFile(candidate)
			require.NoError(t, err)
			require.Equal(t, len(tc.raw), len(after))
			require.True(t, bytes.Equal(tc.raw, after))
		})
	}
}

func TestTSOClaimSeedRejectsUnsafePathsAndPermissions(t *testing.T) {
	dir := claimSeedTestDirectory(t)
	path := filepath.Join(dir, "identity")
	seed := bytes.Repeat([]byte{17}, ed25519.SeedSize)
	_, err := ProvisionTSOClaimIdentity(path, seed)
	require.NoError(t, err)
	for _, mode := range []os.FileMode{0o644, 0o400, 0o700} {
		require.NoError(t, os.Chmod(path, mode))
		_, err := LoadTSOClaimIdentity(path)
		require.ErrorIs(t, err, ErrTSOClaimSeedUnsafe)
	}
	require.NoError(t, os.Chmod(path, 0o600))
	link := filepath.Join(dir, "file-symlink")
	require.NoError(t, os.Symlink(path, link))
	_, err = LoadTSOClaimIdentity(link)
	require.Error(t, err)
	_, err = ProvisionTSOClaimIdentity(link, seed)
	require.ErrorIs(t, err, os.ErrExist)
	alias := filepath.Join(claimSeedTestDirectory(t), "directory-symlink")
	require.NoError(t, os.Symlink(dir, alias))
	_, err = LoadTSOClaimIdentity(filepath.Join(alias, "identity"))
	require.Error(t, err)
	_, err = ProvisionTSOClaimIdentity(filepath.Join(alias, "new-identity"), seed)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(dir, "new-identity"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = LoadTSOClaimIdentity(dir + "/../" + filepath.Base(dir) + "/identity")
	require.ErrorIs(t, err, ErrTSOClaimSeedUnsafe)
	_, err = LoadTSOClaimIdentity("relative-identity")
	require.ErrorIs(t, err, ErrTSOClaimSeedUnsafe)
	require.NoError(t, os.Chmod(dir, 0o755))
	_, err = LoadTSOClaimIdentity(path)
	require.ErrorIs(t, err, ErrTSOClaimSeedUnsafe)
	require.NoError(t, os.Chmod(dir, 0o700))
	loaded, err := LoadTSOClaimIdentity(path)
	require.NoError(t, err)
	expected, err := NewTSOClaimIdentity(seed)
	require.NoError(t, err)
	require.Equal(t, expected.PublicKey(), loaded.PublicKey())
}

func TestTSOClaimSeedRejectsHardlinksAndNonRegularFiles(t *testing.T) {
	dir := claimSeedTestDirectory(t)
	path := filepath.Join(dir, "identity")
	_, err := ProvisionTSOClaimIdentity(path, bytes.Repeat([]byte{1}, ed25519.SeedSize))
	require.NoError(t, err)
	alias := filepath.Join(dir, "hardlink")
	require.NoError(t, os.Link(path, alias))
	_, err = LoadTSOClaimIdentity(path)
	require.ErrorIs(t, err, ErrTSOClaimSeedUnsafe)
	_, err = LoadTSOClaimIdentity(alias)
	require.ErrorIs(t, err, ErrTSOClaimSeedUnsafe)
	require.NoError(t, os.Remove(alias))
	_, err = LoadTSOClaimIdentity(path)
	require.NoError(t, err)
	directory := filepath.Join(dir, "not-a-file")
	require.NoError(t, os.Mkdir(directory, 0o700))
	_, err = LoadTSOClaimIdentity(directory)
	require.ErrorIs(t, err, ErrTSOClaimSeedUnsafe)
}

func TestTSOClaimSeedConcurrentProvisionNeverReplaces(t *testing.T) {
	dir := claimSeedTestDirectory(t)
	path := filepath.Join(dir, "identity")
	const workers = 8
	identities := make([]*TSOClaimIdentity, workers)
	failures := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			identities[i], failures[i] = ProvisionTSOClaimIdentity(path, bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize))
		})
	}
	wg.Wait()
	var winner *TSOClaimIdentity
	for i, err := range failures {
		if err == nil {
			require.Nil(t, winner, "exactly one no-replace publication may win")
			winner = identities[i]
			continue
		}
		require.ErrorIs(t, err, os.ErrExist)
		require.Nil(t, identities[i])
	}
	require.NotNil(t, winner)
	loaded, err := LoadTSOClaimIdentity(path)
	require.NoError(t, err)
	require.Equal(t, winner.PublicKey(), loaded.PublicKey())
	_, err = ProvisionTSOClaimIdentity(path, bytes.Repeat([]byte{99}, ed25519.SeedSize))
	require.ErrorIs(t, err, os.ErrExist)
	after, err := LoadTSOClaimIdentity(path)
	require.NoError(t, err)
	require.Equal(t, winner.PublicKey(), after.PublicKey())
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "successful and losing provisioners must leave no temporary files")
}

func TestTSOClaimSeedExplicitProvisionAndLoad(t *testing.T) {
	path := filepath.Join(claimSeedTestDirectory(t), "identity")
	seed := bytes.Repeat([]byte{17}, ed25519.SeedSize)
	expected, err := NewTSOClaimIdentity(seed)
	require.NoError(t, err)
	provisioned, err := ProvisionTSOClaimIdentity(path, seed)
	require.NoError(t, err)
	require.Equal(t, expected.PublicKey(), provisioned.PublicKey())
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), stat.Mode().Perm())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, raw, 72)
	require.Equal(t, []byte("EKVCKY01"), raw[:8])
	for range 2 {
		loaded, err := LoadTSOClaimIdentity(path)
		require.NoError(t, err)
		require.Equal(t, expected.PublicKey(), loaded.PublicKey())
		binding := claimTestBinding(1)
		binding.Owner = expected.owner()
		require.Equal(t, expected.credential(binding), loaded.credential(binding))
	}
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, raw, after, "load must never rewrite or regenerate the identity")
}

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

func offlineDataStoreFixture(t *testing.T, configuration bool) (string, raftbootstrap.BaselineManifest, kv.PreparedBootstrapActivation) {
	t.Helper()
	root := t.TempDir()
	st, err := store.NewPebbleStore(filepath.Join(root, bootstrapOfflineDataPath))
	require.NoError(t, err)
	require.NoError(t, st.PutAt(t.Context(), []byte("k"), []byte("old history"), 10, 0))
	require.NoError(t, st.PutAt(t.Context(), []byte("k"), []byte("latest"), 20, 0))
	if configuration {
		require.NoError(t, st.PutAt(t.Context(), store.MigrationConfigurationIndexKey(), binary.BigEndian.AppendUint64(nil, 8), 1, 0))
	}
	indexed, ok := st.(interface{ SetDurableAppliedIndex(uint64) error })
	require.True(t, ok)
	require.NoError(t, indexed.SetDurableAppliedIndex(10))
	require.NoError(t, st.Close())
	proof, err := bootstrapOfflineDataStoreProbe(t.Context(), root)
	require.NoError(t, err)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(public)
	require.NoError(t, err)
	cut := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
		Identity: raftbootstrap.Identity{Cluster: raftbootstrap.UUID{1}, Group: 7}, OperationID: raftbootstrap.OperationID{2},
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, CommitIndex: 10, CommitTerm: 3,
		Watermark: proof.Watermark, DataDigest: proof.DataDigest,
		Configuration: raftbootstrap.Configuration{Index: 8, State: raftbootstrap.ConfState{Voters: []uint64{1}},
			Members: []raftbootstrap.Member{{ID: 1, Address: "local:9000", PublicKeyDigest: sha256.Sum256(der)}}}}
	digest, err := raftbootstrap.ConfigurationDigest(cut.Configuration)
	require.NoError(t, err)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: cut,
		Evidence: raftbootstrap.BaselineMemberEvidence{MemberID: 1, CommitIndex: 10, CommitTerm: 3,
			AppliedIndex: 10, LastLogIndex: 10, ConfigurationDigest: digest,
			NativeStateDigest: raftbootstrap.Digest{3}, DataDigest: proof.DataDigest, Watermark: proof.Watermark}}, private)
	require.NoError(t, err)
	manifest := raftbootstrap.BaselineManifest{BaselineContext: cut, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}
	manifestDigest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	// The unit fixture supplies trusted native evidence. Actual WAL/history and
	// full-backup authentication are independently enforced by the native owner.
	return root, manifest, kv.PreparedBootstrapActivation{Probe: probe, ManifestDigest: manifestDigest}
}

func offlineDataFiles(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	result := make(map[string][32]byte)
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[rel] = sha256.Sum256(data)
		return nil
	}))
	return result
}

func offlineDataBackup(t *testing.T, source string) string {
	t.Helper()
	backup := t.TempDir()
	require.NoError(t, filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(backup, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	}))
	require.Equal(t, offlineDataFiles(t, source), offlineDataFiles(t, backup))
	return backup
}

func TestBootstrapOfflineDataProbeReadOnlyFullHistoryAndConfiguration(t *testing.T) {
	t.Parallel()
	for _, configuration := range []bool{true, false} {
		root, _, _ := offlineDataStoreFixture(t, configuration)
		before := offlineDataFiles(t, root)
		proof, err := bootstrapOfflineDataStoreProbe(t.Context(), root)
		require.NoError(t, err)
		require.Equal(t, uint64(10), proof.AppliedIndex)
		require.Equal(t, uint64(20), proof.Watermark)
		require.Empty(t, proof.ConfigurationMembers, "do not manufacture native membership")
		if configuration {
			require.Equal(t, uint64(8), proof.ConfigurationIndex)
		} else {
			require.Zero(t, proof.ConfigurationIndex, "absent C requires native complete-history verification")
		}
		require.Equal(t, before, offlineDataFiles(t, root))
	}
}

func TestBootstrapOfflineDataProbeRejectsMissingSourcesWithoutCreating(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	before := offlineDataFiles(t, root)
	for _, path := range []string{root, filepath.Join(root, "does-not-exist")} {
		proof, err := bootstrapOfflineDataStoreProbe(t.Context(), path)
		require.Error(t, err)
		require.Equal(t, etcd.BaselineStoreProof{}, proof)
	}
	require.Equal(t, before, offlineDataFiles(t, root))
	require.NoDirExists(t, filepath.Join(root, bootstrapOfflineDataPath))
	require.NoDirExists(t, filepath.Join(root, "does-not-exist"))
}

func TestBootstrapOfflineDataProbeRejectsMissingLockWithoutRecreating(t *testing.T) {
	t.Parallel()
	root, _, _ := offlineDataStoreFixture(t, true)
	lock := filepath.Join(root, bootstrapOfflineDataPath, "LOCK")
	require.NoError(t, os.Remove(lock))
	before := offlineDataFiles(t, root)
	_, err := bootstrapOfflineDataStoreProbe(t.Context(), root)
	require.Error(t, err)
	require.NoFileExists(t, lock)
	require.Equal(t, before, offlineDataFiles(t, root))
}

func TestBootstrapOfflineDataProbeRejectsMissingDurableMetadataAndLocks(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"watermark", "applied", "pending"} {
		t.Run(kind, func(t *testing.T) {
			root, _, _ := offlineDataStoreFixture(t, true)
			db, err := pebble.Open(filepath.Join(root, bootstrapOfflineDataPath), &pebble.Options{ErrorIfNotExists: true})
			require.NoError(t, err)
			switch kind {
			case "watermark":
				require.NoError(t, db.Delete([]byte("_meta_last_commit_ts"), pebble.Sync))
			case "applied":
				require.NoError(t, db.Delete([]byte("_meta_applied_index"), pebble.Sync))
			case "pending":
				key := binary.BigEndian.AppendUint64([]byte("!txn|lock|k"), ^uint64(20))
				value := make([]byte, 9+len("pending"))
				copy(value[9:], "pending")
				require.NoError(t, db.Set(key, value, pebble.Sync))
			}
			require.NoError(t, db.Close())
			before := offlineDataFiles(t, root)
			proof, err := bootstrapOfflineDataStoreProbe(t.Context(), root)
			require.Error(t, err)
			require.Equal(t, etcd.BaselineStoreProof{}, proof)
			require.Equal(t, before, offlineDataFiles(t, root))
		})
	}
}

func TestBootstrapOfflineDataPreparationStageVerifyAndRestart(t *testing.T) {
	// Not parallel: store.VerifyBootstrapBaselinePhysicalEquality calls Error() on a closed pebble iterator, whose pooled allocation another test's pebble DB may be reusing.
	root, manifest, prepared := offlineDataStoreFixture(t, true)
	backup := offlineDataBackup(t, root)
	backupBefore := offlineDataFiles(t, backup)
	callbacks, err := bootstrapOfflineDataPreparation(prepared)
	require.NoError(t, err)
	require.Equal(t, []string{bootstrapOfflineDataPath}, callbacks.MutablePaths)
	c := etcd.BaselinePreparationContext{DataDir: root, BackupDir: backup, Manifest: manifest}
	_, err = callbacks.VerifyPreparedStore(t.Context(), c)
	require.Error(t, err, "original image is not prepared")
	require.NoError(t, callbacks.StagePreparation(t.Context(), c))
	proof, err := callbacks.VerifyPreparedStore(t.Context(), c)
	require.NoError(t, err)
	require.Equal(t, prepared.Probe.DataDigest, proof.Baseline.DataDigest)
	require.NotEqual(t, proof.Baseline.DataDigest, proof.PreparedDataDigest)
	require.Equal(t, backupBefore, offlineDataFiles(t, backup))
	preparedFiles := offlineDataFiles(t, root)
	// A fresh callback owner after restart recognizes only the exact transform
	// and does not reopen RW merely to repeat an already completed stage.
	restarted, err := bootstrapOfflineDataPreparation(prepared)
	require.NoError(t, err)
	require.NoError(t, restarted.StagePreparation(t.Context(), c))
	again, err := restarted.VerifyPreparedStore(t.Context(), c)
	require.NoError(t, err)
	require.Equal(t, proof, again)
	require.Equal(t, preparedFiles, offlineDataFiles(t, root))
	require.Equal(t, backupBefore, offlineDataFiles(t, backup))
	key, value, err := kv.PreparedBootstrapActivationMetadata(prepared.Probe.Identity, prepared)
	require.NoError(t, err)
	physicalValue, err := store.EncodeOfflinePlainControlMetadata(value)
	require.NoError(t, err)
	require.NoError(t, bootstrapOfflineWithData(t.Context(), root, true, func(db *pebble.DB) error {
		snapshot := db.NewSnapshot()
		defer snapshot.Close()
		raw, present, err := bootstrapOfflineGet(snapshot, append([]byte(store.RaftControlMetadataPrefix), key...))
		require.NoError(t, err)
		require.True(t, present)
		require.Equal(t, physicalValue, raw)
		return nil
	}))
}

func TestBootstrapOfflineDataPreparationRuntimeMetadataAfterRestart(t *testing.T) {
	// Not parallel: store.VerifyBootstrapBaselinePhysicalEquality calls Error() on a closed pebble iterator, whose pooled allocation another test's pebble DB may be reusing.
	root, manifest, prepared := offlineDataStoreFixture(t, true)
	backup := offlineDataBackup(t, root)
	callbacks, err := bootstrapOfflineDataPreparation(prepared)
	require.NoError(t, err)
	c := etcd.BaselinePreparationContext{DataDir: root, BackupDir: backup, Manifest: manifest}
	require.NoError(t, callbacks.StagePreparation(t.Context(), c))
	key, canonical, err := kv.PreparedBootstrapActivationMetadata(prepared.Probe.Identity, prepared)
	require.NoError(t, err)
	// Reopen through the runtime codec, not the raw offline snapshot reader.
	// Canonical BAP1 bytes without the physical value header must fail here.
	for range 2 {
		st, err := store.NewPebbleStore(filepath.Join(root, bootstrapOfflineDataPath))
		require.NoError(t, err)
		func() {
			defer func() { require.NoError(t, st.Close()) }()
			metadata, ok := st.(store.RaftControlMetadataStore)
			require.True(t, ok)
			value, err := metadata.ReadRaftControlMetadata(t.Context(), key)
			require.NoError(t, err)
			require.Equal(t, canonical, value)
			got, err := kv.ReadPreparedBootstrapActivation(t.Context(), metadata, prepared.Probe.Identity)
			require.NoError(t, err)
			require.Equal(t, prepared.ManifestDigest, got.ManifestDigest)
			gotKey, gotCanonical, err := kv.PreparedBootstrapActivationMetadata(prepared.Probe.Identity, got)
			require.NoError(t, err)
			require.Equal(t, key, gotKey)
			require.Equal(t, canonical, gotCanonical)
		}()
	}
}

func TestBootstrapOfflineDataPreparationNeverWritesWithoutTrustedBackup(t *testing.T) {
	// Not parallel: store.VerifyBootstrapBaselinePhysicalEquality calls Error() on a closed pebble iterator, whose pooled allocation another test's pebble DB may be reusing.
	for _, kind := range []string{"missing backup", "same store", "wrong manifest", "edited target", "edited backup", "aliased backup", "nested backup"} {
		t.Run(kind, func(t *testing.T) {
			root, manifest, prepared := offlineDataStoreFixture(t, true)
			backup := offlineDataBackup(t, root)
			c := etcd.BaselinePreparationContext{DataDir: root, BackupDir: backup, Manifest: manifest}
			switch kind {
			case "missing backup":
				c.BackupDir = filepath.Join(t.TempDir(), "absent")
			case "same store":
				c.BackupDir = root
			case "wrong manifest":
				c.Manifest.OperationID[0]++
			case "aliased backup":
				description, err := pebble.Peek(filepath.Join(root, bootstrapOfflineDataPath), vfs.Default)
				require.NoError(t, err)
				original := description.ManifestFilename
				alias := filepath.Join(backup, bootstrapOfflineDataPath, filepath.Base(original))
				require.NoError(t, os.Remove(alias))
				require.NoError(t, os.Link(original, alias))
			case "nested backup":
				c.BackupDir = filepath.Join(root, "nested-backup")
				require.NoError(t, os.Rename(backup, c.BackupDir))
			case "edited target", "edited backup":
				changed := root
				if kind == "edited backup" {
					changed = backup
				}
				st, err := store.NewPebbleStore(filepath.Join(changed, bootstrapOfflineDataPath))
				require.NoError(t, err)
				require.NoError(t, st.PutAt(t.Context(), []byte("k"), []byte("changed old history"), 10, 0))
				require.NoError(t, st.Close())
			}
			before := offlineDataFiles(t, root)
			callbacks, err := bootstrapOfflineDataPreparation(prepared)
			require.NoError(t, err)
			require.Error(t, callbacks.StagePreparation(t.Context(), c))
			require.Equal(t, before, offlineDataFiles(t, root), "all RO refusals precede RW open")
		})
	}
}

func TestBootstrapOfflineDataProbeCancellationAndSymlink(t *testing.T) {
	t.Parallel()
	root, _, _ := offlineDataStoreFixture(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := bootstrapOfflineDataStoreProbe(ctx, root)
	require.ErrorIs(t, err, context.Canceled)
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(root, alias))
	_, err = bootstrapOfflineDataStoreProbe(t.Context(), alias)
	require.Error(t, err)
	_, err = bootstrapOfflineDataStoreProbe(t.Context(), alias+string(filepath.Separator))
	require.Error(t, err, "trailing separators cannot hide a symlink root")
}

func TestBootstrapOfflineDataWalkRejectsEscapingSymlink(t *testing.T) {
	t.Parallel()
	root, _, _ := offlineDataStoreFixture(t, false)
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("preserved external file"), 0o600))
	link := filepath.Join(root, bootstrapOfflineDataPath, "foreign-table")
	require.NoError(t, os.Symlink(outside, link))
	_, err := bootstrapOfflineDataStoreProbe(t.Context(), root)
	require.ErrorContains(t, err, "unsupported store tree")
	value, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, []byte("preserved external file"), value)
	info, err := os.Lstat(link)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
}

func TestBootstrapOfflineDataPreparationRejectsPostStageEditsAfterRestart(t *testing.T) {
	// Not parallel: store.VerifyBootstrapBaselinePhysicalEquality calls Error() on a closed pebble iterator, whose pooled allocation another test's pebble DB may be reusing.
	for _, kind := range []string{"preparation", "old history", "extra metadata"} {
		t.Run(kind, func(t *testing.T) {
			root, manifest, prepared := offlineDataStoreFixture(t, false)
			backup := offlineDataBackup(t, root)
			callbacks, err := bootstrapOfflineDataPreparation(prepared)
			require.NoError(t, err)
			c := etcd.BaselinePreparationContext{DataDir: root, BackupDir: backup, Manifest: manifest}
			require.NoError(t, callbacks.StagePreparation(t.Context(), c))
			db, err := pebble.Open(filepath.Join(root, bootstrapOfflineDataPath), &pebble.Options{ErrorIfNotExists: true})
			require.NoError(t, err)
			switch kind {
			case "preparation":
				key, _, err := kv.PreparedBootstrapActivationMetadata(prepared.Probe.Identity, prepared)
				require.NoError(t, err)
				require.NoError(t, db.Set(append([]byte(store.RaftControlMetadataPrefix), key...), []byte("untrusted"), pebble.Sync))
			case "old history":
				require.NoError(t, db.Delete(binary.BigEndian.AppendUint64([]byte("k"), ^uint64(10)), pebble.Sync))
			case "extra metadata":
				require.NoError(t, db.Set([]byte(store.RaftControlMetadataPrefix+"J/unapproved"), []byte("extra"), pebble.Sync))
			}
			require.NoError(t, db.Close())
			before := offlineDataFiles(t, root)
			restarted, err := bootstrapOfflineDataPreparation(prepared)
			require.NoError(t, err)
			_, err = restarted.VerifyPreparedStore(t.Context(), c)
			require.Error(t, err)
			require.Error(t, restarted.StagePreparation(t.Context(), c))
			require.Equal(t, before, offlineDataFiles(t, root), "do not repair or overwrite ambiguous edits")
		})
	}
}

func TestBootstrapOfflineDataFSNeverDeletesOrReplaces(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "preserved")
	require.NoError(t, os.WriteFile(path, []byte("preserved"), 0o600))
	f := bootstrapOfflineFS{FS: vfs.Default}
	require.Error(t, f.Remove(path))
	require.Error(t, f.RemoveAll(dir))
	_, err := f.Create(path, vfs.WriteCategoryUnspecified)
	require.Error(t, err)
	require.Error(t, f.Rename(path, path))
	_, err = f.ReuseForWrite(path, filepath.Join(dir, "reused"), vfs.WriteCategoryUnspecified)
	require.Error(t, err)
	require.Error(t, f.MkdirAll(filepath.Join(dir, "missing"), 0o700))
	readOnly := bootstrapOfflineFS{FS: vfs.Default, readOnly: true}
	_, err = readOnly.OpenReadWrite(path, vfs.WriteCategoryUnspecified)
	require.Error(t, err)
	_, err = readOnly.Create(filepath.Join(dir, "new"), vfs.WriteCategoryUnspecified)
	require.Error(t, err)
	value, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("preserved"), value)
	require.NoFileExists(t, filepath.Join(dir, "new"))
	require.NoFileExists(t, filepath.Join(dir, "reused"))
	require.NoDirExists(t, filepath.Join(dir, "missing"))
}

func TestBootstrapOfflineDataProbeWALReplayBound(t *testing.T) {
	t.Parallel()
	root, _, _ := offlineDataStoreFixture(t, false)
	path := filepath.Join(root, bootstrapOfflineDataPath, "999999.log")
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(bootstrapOfflineWALLimit+1))
	require.NoError(t, f.Close())
	_, err = bootstrapOfflineDataStoreProbe(t.Context(), root)
	require.ErrorContains(t, err, "WAL replay byte bound")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(bootstrapOfflineWALLimit+1), info.Size())
}

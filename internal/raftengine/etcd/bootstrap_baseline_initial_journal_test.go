package etcd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

func initialBaselineJournalFixture(t *testing.T, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest) (OfflineBaselineMigration, []byte) {
	t.Helper()
	inspection, err := InspectOfflineBaseline(t.Context(), o)
	require.NoError(t, err)
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	transformation, err := baselineTransformationDigest(o)
	require.NoError(t, err)
	paths := baselineMigrationPaths(o)
	require.NoError(t, os.Mkdir(paths.Directory, baselinePrivateDirectory))
	return paths, encodeBaselineJournal(baselineMigrationJournal{phase: baselinePreparing, manifest: digest,
		source: inspection.Evidence.NativeStateDigest, transformation: transformation})
}

func stageInitialBaselineCrash(t *testing.T, dir, state string, data []byte) {
	t.Helper()
	if state == "mkdir" {
		return
	}
	file, err := os.OpenFile(filepath.Join(dir, "journal.tmp"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, baselinePrivateFile)
	require.NoError(t, err)
	switch state {
	case "tmp-created":
	case "partial-write":
		_, err = file.Write(data[:baselineJournalSize-1])
	default:
		_, err = file.Write(data)
	}
	require.NoError(t, err)
	if state == "tmp-fsync" || state == "pre-rename" {
		require.NoError(t, file.Sync())
	}
	require.NoError(t, file.Close())
	if state == "pre-rename" {
		require.NoError(t, baselineSyncDirectory(dir))
	}
}

func TestOfflineBaselineInitialJournalCrashRecovery(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"prepare", "resume"} {
		for _, state := range []string{"mkdir", "tmp-created", "partial-write", "tmp-fsync", "pre-rename"} {
			t.Run(method+"/"+state, func(t *testing.T) {
				o, key := offlineBaselineFixture(t, true)
				manifest := signedOfflineBaseline(t, o, key)
				paths, data := initialBaselineJournalFixture(t, o, manifest)
				stageInitialBaselineCrash(t, paths.Directory, state, data)
				original, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
				require.NoError(t, err)
				// Reacquire the native fence as a process restart would.
				require.NoError(t, o.Ownership.Close())
				o.Ownership, err = AcquireOfflineBaselineOwnership(o.DataDir)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, o.Ownership.Close()) })
				var result OfflineBaselineMigration
				if method == "prepare" {
					result, err = PrepareOfflineBaseline(t.Context(), o, manifest)
				} else {
					result, err = ResumeOfflineBaseline(t.Context(), o, manifest)
				}
				require.NoError(t, err)
				require.Equal(t, "prepared", result.Phase)
				live, err := readBaselineWAL(filepath.Join(o.DataDir, walDirName))
				require.NoError(t, err)
				require.Equal(t, original.fileDigest, live.fileDigest, "recovery must not exchange the WAL")
				result, err = ResumeOfflineBaseline(t.Context(), o, manifest)
				require.NoError(t, err)
				require.Equal(t, "bound", result.Phase)
			})
		}
	}
}

func TestOfflineBaselineInitialJournalRejectsUnexpectedContents(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unknown-file", "unknown-directory", "tmp-directory", "tmp-symlink", "root-symlink", "foreign-journal", "oversized", "published-journal"} {
		t.Run(kind, func(t *testing.T) {
			o, key := offlineBaselineFixture(t, true)
			manifest := signedOfflineBaseline(t, o, key)
			paths, data := initialBaselineJournalFixture(t, o, manifest)
			initialBaselineUnexpectedContent(t, paths.Directory, kind, data)
			sourceBefore, err := baselineTreeDigest(t.Context(), o.DataDir, "")
			require.NoError(t, err)
			_, err = PrepareOfflineBaseline(t.Context(), o, manifest)
			require.Error(t, err)
			if kind != "published-journal" {
				_, err = ResumeOfflineBaseline(t.Context(), o, manifest)
				require.Error(t, err)
			}
			sourceAfter, err := baselineTreeDigest(t.Context(), o.DataDir, "")
			require.NoError(t, err)
			require.Equal(t, sourceBefore, sourceAfter)
		})
	}
}

func initialBaselineUnexpectedContent(t *testing.T, dir, kind string, data []byte) {
	t.Helper()
	switch kind {
	case "unknown-file":
		require.NoError(t, os.WriteFile(filepath.Join(dir, "journal.tmp"), data, baselinePrivateFile))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "unrecognized"), nil, baselinePrivateFile))
	case "unknown-directory":
		require.NoError(t, os.Mkdir(filepath.Join(dir, "backup"), baselinePrivateDirectory))
	case "tmp-directory":
		require.NoError(t, os.Mkdir(filepath.Join(dir, "journal.tmp"), baselinePrivateDirectory))
	case "tmp-symlink":
		target := filepath.Join(t.TempDir(), "unrelated")
		require.NoError(t, os.WriteFile(target, data, baselinePrivateFile))
		require.NoError(t, os.Symlink(target, filepath.Join(dir, "journal.tmp")))
	case "root-symlink":
		require.NoError(t, os.Remove(dir))
		require.NoError(t, os.Symlink(t.TempDir(), dir))
	case "foreign-journal":
		foreign := encodeBaselineJournal(baselineMigrationJournal{phase: baselinePreparing, manifest: raftbootstrap.Digest{9}})
		require.NoError(t, os.WriteFile(filepath.Join(dir, "journal.tmp"), foreign, baselinePrivateFile))
	case "oversized":
		require.NoError(t, os.WriteFile(filepath.Join(dir, "journal.tmp"), append(data, 0), baselinePrivateFile))
	case "published-journal":
		require.NoError(t, os.WriteFile(filepath.Join(dir, "journal"), data, baselinePrivateFile))
	default:
		t.Fatalf("unknown case %q", kind)
	}
}

func TestOfflineBaselineInitialJournalResumeDoesNotCreateDirectory(t *testing.T) {
	t.Parallel()
	o, key := offlineBaselineFixture(t, true)
	manifest := signedOfflineBaseline(t, o, key)
	paths := baselineMigrationPaths(o)
	_, err := ResumeOfflineBaseline(t.Context(), o, manifest)
	require.Error(t, err)
	_, err = os.Lstat(paths.Directory)
	require.True(t, os.IsNotExist(err))
}

func TestOfflineBaselineInitialJournalDoesNotTruncateLinkedTemp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	journal := baselineMigrationJournal{phase: baselinePreparing, manifest: raftbootstrap.Digest{1}}
	prefix := encodeBaselineJournal(journal)[:len("EKVBLJ02")]
	outside := filepath.Join(t.TempDir(), "preserved")
	require.NoError(t, os.WriteFile(outside, prefix, baselinePrivateFile))
	require.NoError(t, os.Link(outside, filepath.Join(dir, "journal.tmp")))
	require.NoError(t, initializeBaselineJournal(dir, journal))
	got, err := os.ReadFile(outside)
	require.NoError(t, err)
	require.Equal(t, prefix, got)
	published, err := readBaselineJournal(dir)
	require.NoError(t, err)
	require.Equal(t, journal, published)
}

func TestOfflineBaselineInitialJournalRevalidatesSource(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"native-source", "store-proof"} {
		t.Run(kind, func(t *testing.T) {
			o, key := offlineBaselineFixture(t, true)
			manifest := signedOfflineBaseline(t, o, key)
			paths, data := initialBaselineJournalFixture(t, o, manifest)
			stageInitialBaselineCrash(t, paths.Directory, "tmp-fsync", data)
			if kind == "native-source" {
				require.NoError(t, os.WriteFile(filepath.Join(o.DataDir, "changed-source"), []byte("changed"), baselinePrivateFile))
			} else {
				probe := o.ProbeStore
				o.ProbeStore = func(ctx context.Context) (BaselineStoreProof, error) {
					proof, err := probe(ctx)
					proof.Watermark++
					return proof, err
				}
			}
			before, err := baselineTreeDigest(t.Context(), o.DataDir, "")
			require.NoError(t, err)
			_, err = ResumeOfflineBaseline(t.Context(), o, manifest)
			require.Error(t, err)
			got, err := os.ReadFile(filepath.Join(paths.Directory, "journal.tmp"))
			require.NoError(t, err)
			require.Equal(t, data, got)
			_, err = os.Lstat(filepath.Join(paths.Directory, "journal"))
			require.True(t, os.IsNotExist(err))
			after, err := baselineTreeDigest(t.Context(), o.DataDir, "")
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

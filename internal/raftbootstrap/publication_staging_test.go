package raftbootstrap

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func stagingStore(t *testing.T) (*Store, string, string) {
	t.Helper()
	root := t.TempDir()
	data, staging := filepath.Join(root, "data"), filepath.Join(root, "operator-staging")
	require.NoError(t, os.Mkdir(data, 0o700))
	require.NoError(t, os.Mkdir(staging, 0o700))
	s, err := NewStoreWithStagingDirectory(data, staging)
	require.NoError(t, err)
	return s, data, staging
}

func stagedSourceFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	files := make(map[string]string)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		files[entry.Name()] = string(data)
	}
	return files
}

// Preserve the Store's external rename rather than instrument's local-only
// rename, while reusing its real-file fault/sync boundary wrappers.
func instrumentStaging(s *Store, before func(string) error, after func(string)) {
	real := s.ops
	current := ""
	s.ops.createTemp = func(dir, pattern string) (durableFile, error) {
		current = MarkerFile
		if strings.Contains(pattern, IntentFile) {
			current = IntentFile
		}
		if err := before(current + ":create"); err != nil {
			return nil, err
		}
		f, err := real.createTemp(dir, pattern)
		if err != nil {
			return nil, err
		}
		after(current + ":create")
		return &boundaryFile{durableFile: f, name: current, before: before, after: after}, nil
	}
	s.ops.rename = func(from, to string) error {
		if err := before(current + ":rename"); err != nil {
			return err
		}
		err := real.rename(from, to)
		after(current + ":rename")
		return err
	}
	s.ops.openDir = func(dir string) (syncDirectory, error) {
		if err := before(current + ":dir-open"); err != nil {
			return nil, err
		}
		d, err := real.openDir(dir)
		if err != nil {
			return nil, err
		}
		after(current + ":dir-open")
		return &boundaryDir{syncDirectory: d, name: current, before: before, after: after}, nil
	}
}

func TestPublicationStagingSuccessSyncsDestinationAndPreservesOrphans(t *testing.T) {
	t.Parallel()
	in, evidence := fixture(t)
	s, data, staging := stagingStore(t)
	put(t, staging, ".bootstrap-intent-abandoned.tmp", []byte("not authority"))
	var destinations []string
	real := s.ops
	s.ops.createTemp = func(dir, pattern string) (durableFile, error) {
		require.Equal(t, s.staging.temporary, dir)
		return real.createTemp(dir, pattern)
	}
	s.ops.openDir = func(dir string) (syncDirectory, error) {
		require.Equal(t, s.dir, dir)
		destinations = append(destinations, dir)
		return real.openDir(dir)
	}
	var events []string
	instrumentStaging(s, func(string) error { return nil }, func(event string) { events = append(events, event) })
	require.NoError(t, s.Publish(in, evidence))
	require.Equal(t, []string{s.dir, s.dir}, destinations)
	var expected []string
	for _, name := range []string{IntentFile, MarkerFile} {
		for _, step := range steps {
			expected = append(expected, name+":"+step)
		}
	}
	require.Equal(t, expected, events, "file sync and BOTH destination directory syncs retain original order")
	inspection, err := NewStore(data).Inspect(evidence)
	require.NoError(t, err)
	require.Equal(t, Required, inspection.Class)
	require.Len(t, stagedSourceFiles(t, data), 2, "no source temporary files")
	require.Equal(t, map[string]string{".bootstrap-intent-abandoned.tmp": "not authority"}, stagedSourceFiles(t, staging))
}

func TestPublicationStagingPreRenameFailuresLeaveSourceUnchanged(t *testing.T) {
	t.Parallel()
	for _, step := range []string{"create", "chmod", "write", "file-sync", "file-close", "rename"} {
		t.Run(step, func(t *testing.T) {
			in, evidence := fixture(t)
			s, data, _ := stagingStore(t)
			put(t, data, MarkerFile, []byte("etcd\n"))
			put(t, data, "unrelated", []byte("preserved"))
			before := stagedSourceFiles(t, data)
			instrumentStaging(s, func(event string) error {
				if event == IntentFile+":"+step {
					return errDisk
				}
				return nil
			}, func(string) {})
			require.ErrorIs(t, s.Publish(in, evidence), errDisk)
			require.Equal(t, before, stagedSourceFiles(t, data))
		})
	}
}

func TestPublicationStagingDestinationSyncFailureIsNotSuccess(t *testing.T) {
	t.Parallel()
	for _, name := range []string{IntentFile, MarkerFile} {
		t.Run(name, func(t *testing.T) {
			in, evidence := fixture(t)
			s, data, staging := stagingStore(t)
			put(t, data, MarkerFile, []byte("etcd\n"))
			instrumentStaging(s, func(event string) error {
				if event == name+":dir-sync" {
					return errDisk
				}
				return nil
			}, func(string) {})
			require.ErrorIs(t, s.Publish(in, evidence), errDisk)
			if name == IntentFile {
				require.Equal(t, "etcd\n", string(diskBytes(t, data, MarkerFile)))
			}
			restarted, err := NewStoreWithStagingDirectory(data, staging)
			require.NoError(t, err)
			require.NoError(t, restarted.Publish(in, evidence), "retry must reconcile and sync actual destination files")
		})
	}
}

func TestPublicationStagingCrossDeviceRenameDoesNotCopy(t *testing.T) {
	t.Parallel()
	in, evidence := fixture(t)
	s, data, _ := stagingStore(t)
	put(t, data, MarkerFile, []byte("etcd\n"))
	before := stagedSourceFiles(t, data)
	s.ops.rename = func(from, to string) error {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: syscall.EXDEV}
	}
	require.ErrorIs(t, s.Publish(in, evidence), syscall.EXDEV)
	require.Equal(t, before, stagedSourceFiles(t, data), "no copy/remove fallback")
}

func TestPublicationStagingRejectsInvalidOrOverlappingDirectories(t *testing.T) {
	t.Parallel()
	_, data, staging := stagingStore(t)
	file := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(file, []byte("preserve"), filePerm))
	alias := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(staging, alias))
	child := filepath.Join(data, "inside-source")
	require.NoError(t, os.Mkdir(child, 0o700))
	missing := filepath.Join(t.TempDir(), "missing")
	for _, pair := range [][2]string{
		{"", staging}, {data, ""}, {missing, staging}, {data, missing},
		{file, staging}, {data, file}, {data, data}, {data, child}, {child, data},
		{data, alias}, {alias, data}, {data, alias + string(filepath.Separator)},
	} {
		s, err := NewStoreWithStagingDirectory(pair[0], pair[1])
		require.Error(t, err)
		require.Nil(t, s)
	}
	require.NoDirExists(t, missing)
	value, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, []byte("preserve"), value)
}

func TestPublicationStagingRevalidatesPinnedDirectories(t *testing.T) {
	t.Parallel()
	for _, replaced := range []string{"source", "staging"} {
		t.Run(replaced, func(t *testing.T) {
			in, evidence := fixture(t)
			s, data, staging := stagingStore(t)
			path := staging
			if replaced == "source" {
				path = data
			}
			require.NoError(t, os.Rename(path, path+".preserved"))
			require.NoError(t, os.Mkdir(path, 0o700))
			require.ErrorIs(t, s.Publish(in, evidence), ErrInvalid)
			require.Empty(t, stagedSourceFiles(t, data))
			require.Empty(t, stagedSourceFiles(t, staging))
		})
	}
}

func TestPublicationStagingCrashChild(t *testing.T) {
	boundary := os.Getenv("RAFTBOOTSTRAP_STAGING_BOUNDARY")
	if boundary == "" {
		t.Skip("subprocess only")
	}
	in, evidence := fixture(t)
	s, err := NewStoreWithStagingDirectory(os.Getenv("RAFTBOOTSTRAP_STAGING_DATA"), os.Getenv("RAFTBOOTSTRAP_STAGING_DIR"))
	require.NoError(t, err)
	instrumentStaging(s, func(string) error { return nil }, func(event string) {
		if event == boundary {
			os.Exit(crashExit)
		}
	})
	require.NoError(t, s.Publish(in, evidence))
	t.Fatal("crash boundary was not reached")
}

func TestPublicationStagingCrashLeavesNoSourceTempsAndResumes(t *testing.T) {
	t.Parallel()
	// Process crashes, not power-loss persistence. The latter still depends on
	// the unchanged file-sync -> rename -> destination-dir-sync contract.
	for _, boundary := range []string{IntentFile + ":file-sync", IntentFile + ":rename", MarkerFile + ":file-sync"} {
		t.Run(boundary, func(t *testing.T) {
			in, evidence := fixture(t)
			_, data, staging := stagingStore(t)
			put(t, data, MarkerFile, []byte("etcd\n"))
			cmd := exec.Command(os.Args[0], "-test.run=^TestPublicationStagingCrashChild$")
			cmd.Env = append(os.Environ(), "RAFTBOOTSTRAP_STAGING_BOUNDARY="+boundary,
				"RAFTBOOTSTRAP_STAGING_DATA="+data, "RAFTBOOTSTRAP_STAGING_DIR="+staging)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			require.Equal(t, crashExit, exit.ExitCode())
			for name := range stagedSourceFiles(t, data) {
				require.NotContains(t, name, ".tmp")
			}
			if boundary == IntentFile+":file-sync" {
				require.Equal(t, map[string]string{MarkerFile: "etcd\n"}, stagedSourceFiles(t, data))
			}
			orphans := stagedSourceFiles(t, staging)
			s, err := NewStoreWithStagingDirectory(data, staging)
			require.NoError(t, err)
			require.NoError(t, s.Publish(in, evidence))
			require.Equal(t, orphans, stagedSourceFiles(t, staging), "outside crash files stay preserved, never trusted as publication")
		})
	}
}

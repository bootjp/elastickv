package raftbootstrap

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var errDisk = errors.New("injected disk boundary failure")

type boundaryFile struct {
	durableFile
	name       string
	before     func(string) error
	after      func(string)
	chunk      int
	writeFault string
}

func (f *boundaryFile) Chmod(mode os.FileMode) error {
	if err := f.before(f.name + ":chmod"); err != nil {
		return err
	}
	err := f.durableFile.Chmod(mode)
	f.after(f.name + ":chmod")
	return err
}

func (f *boundaryFile) Write(b []byte) (int, error) {
	if err := f.before(f.name + ":write"); err != nil {
		return 0, err
	}
	switch f.writeFault {
	case "zero":
		return 0, nil
	case "invalid":
		return len(b) + 1, nil
	case "partial-error":
		n, _ := f.durableFile.Write(b[:1])
		return n, errDisk
	}
	if f.chunk > 0 && len(b) > f.chunk {
		b = b[:f.chunk]
	}
	n, err := f.durableFile.Write(b)
	f.after(f.name + ":write")
	return n, err
}

func (f *boundaryFile) Sync() error {
	if err := f.before(f.name + ":file-sync"); err != nil {
		return err
	}
	err := f.durableFile.Sync()
	f.after(f.name + ":file-sync")
	return err
}

func (f *boundaryFile) Close() error {
	// Close the real descriptor even when simulating a close-time I/O error.
	err := f.durableFile.Close()
	injected := f.before(f.name + ":file-close")
	f.after(f.name + ":file-close")
	return errors.CombineErrors(err, injected)
}

type boundaryDir struct {
	syncDirectory
	name   string
	before func(string) error
	after  func(string)
}

func (d *boundaryDir) Sync() error {
	if err := d.before(d.name + ":dir-sync"); err != nil {
		return err
	}
	err := d.syncDirectory.Sync()
	d.after(d.name + ":dir-sync")
	return err
}

func (d *boundaryDir) Close() error {
	err := d.syncDirectory.Close()
	injected := d.before(d.name + ":dir-close")
	d.after(d.name + ":dir-close")
	return errors.CombineErrors(err, injected)
}

func instrument(s *Store, before func(string) error, after func(string), chunk int, writeFault string) {
	real := realFileOps()
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
		return &boundaryFile{durableFile: f, name: current, before: before, after: after, chunk: chunk, writeFault: writeFault}, nil
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

var steps = []string{"create", "chmod", "write", "file-sync", "file-close", "rename", "dir-open", "dir-sync", "dir-close"}

func TestPublicationEveryIOFailure(t *testing.T) {
	for _, name := range []string{IntentFile, MarkerFile} {
		for _, step := range steps {
			t.Run(name+"/"+step, func(t *testing.T) {
				in, e := fixture(t)
				dir := t.TempDir()
				s := NewStore(dir)
				legacy := []byte("etcd\n")
				put(t, dir, MarkerFile, legacy)
				var events []string
				instrument(s, func(event string) error {
					events = append(events, event)
					if event == name+":"+step {
						return errDisk
					}
					return nil
				}, func(string) {}, 0, "")
				require.ErrorIs(t, s.Publish(in, e), errDisk, "no upgrade-ready success on any boundary error")
				marker := diskBytes(t, dir, MarkerFile)
				require.True(t, bytes.Equal(marker, legacy) || bytes.Equal(marker, []byte(MarkerV1)))
				if name == IntentFile {
					require.Equal(t, legacy, marker)
					require.NotContains(t, events, MarkerFile+":create")
				}
				r, err := NewStore(dir).Inspect(e)
				require.NoError(t, err)
				require.Contains(t, []Classification{Legacy, Recovery, Required}, r.Class)
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				for _, entry := range entries {
					require.NotContains(t, entry.Name(), ".tmp")
				}
				// A new owner must explicitly republish/fsync before acknowledging
				// readiness even if both renames happened before the I/O error.
				require.NoError(t, NewStore(dir).Publish(in, e))
			})
		}
	}
}

func TestPublicationOrderingAndShortWrites(t *testing.T) {
	in, e := fixture(t)
	dir := t.TempDir()
	s := NewStore(dir)
	var events []string
	instrument(s, func(string) error { return nil }, func(event string) { events = append(events, event) }, 3, "")
	require.NoError(t, s.Publish(in, e))
	var compressed []string
	for _, event := range events {
		if len(compressed) == 0 || compressed[len(compressed)-1] != event {
			compressed = append(compressed, event)
		}
	}
	var want []string
	for _, name := range []string{IntentFile, MarkerFile} {
		for _, step := range steps {
			want = append(want, name+":"+step)
		}
	}
	require.Equal(t, want, compressed)
	r, err := s.Inspect(e)
	require.NoError(t, err)
	require.Equal(t, Required, r.Class)
}

func TestWriteAllRejectsBrokenWriters(t *testing.T) {
	for _, fault := range []string{"zero", "invalid", "partial-error"} {
		t.Run(fault, func(t *testing.T) {
			in, e := fixture(t)
			dir := t.TempDir()
			s := NewStore(dir)
			put(t, dir, MarkerFile, []byte("etcd\n"))
			instrument(s, func(string) error { return nil }, func(string) {}, 0, fault)
			err := s.Publish(in, e)
			if fault == "partial-error" {
				require.ErrorIs(t, err, errDisk)
			} else {
				require.ErrorIs(t, err, io.ErrShortWrite)
			}
			require.Equal(t, []byte("etcd\n"), diskBytes(t, dir, MarkerFile))
		})
	}
}

const crashExit = 73

// A child process exits without defers immediately AFTER the real operation.
// This tests process-crash boundaries, not simulated power-loss persistence.
func TestCrashChild(t *testing.T) {
	boundary := os.Getenv("RAFTBOOTSTRAP_CRASH_BOUNDARY")
	if boundary == "" {
		t.Skip("subprocess only")
	}
	in, e := fixture(t)
	s := NewStore(os.Getenv("RAFTBOOTSTRAP_CRASH_DIR"))
	instrument(s, func(string) error { return nil }, func(event string) {
		if event == boundary {
			os.Exit(crashExit)
		}
	}, 0, "")
	require.NoError(t, s.Publish(in, e))
	t.Fatal("crash boundary was not reached")
}

func TestProcessCrashEveryBoundary(t *testing.T) {
	for _, name := range []string{IntentFile, MarkerFile} {
		for _, step := range steps {
			t.Run(name+"/"+step, func(t *testing.T) {
				in, e := fixture(t)
				dir := t.TempDir()
				legacy := []byte("etcd\n")
				put(t, dir, MarkerFile, legacy)
				cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
				cmd.Env = append(os.Environ(), "RAFTBOOTSTRAP_CRASH_BOUNDARY="+name+":"+step, "RAFTBOOTSTRAP_CRASH_DIR="+dir)
				out, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(out))
				require.Equal(t, crashExit, exit.ExitCode(), string(out))
				marker := diskBytes(t, dir, MarkerFile)
				require.True(t, bytes.Equal(marker, legacy) || bytes.Equal(marker, []byte(MarkerV1)))
				if name == IntentFile {
					require.Equal(t, legacy, marker)
				}
				// The old binary's actual whole-trimmed-name rule rejects v1.
				if bytes.Equal(marker, []byte(MarkerV1)) {
					require.NotContains(t, []string{"", "etcd"}, strings.ToLower(strings.TrimSpace(string(marker))))
				}
				s := NewStore(dir)
				r, err := s.Inspect(e)
				require.NoError(t, err)
				if bytes.Equal(marker, legacy) && r.Intent != nil {
					require.Equal(t, Recovery, r.Class)
				}
				require.NoError(t, s.Publish(in, e), "stale exclusive temporary files do not prevent retry")
				require.Equal(t, []byte(MarkerV1), diskBytes(t, dir, MarkerFile))
			})
		}
	}
}

func TestExistingIntentSurvivesReplacementFailure(t *testing.T) {
	in, e := fixture(t)
	dir := t.TempDir()
	s := NewStore(dir)
	require.NoError(t, s.Publish(in, e))
	before := diskBytes(t, dir, IntentFile)
	updated, recovered := active(t)
	instrument(s, func(event string) error {
		if event == IntentFile+":rename" {
			return errDisk
		}
		return nil
	}, func(string) {}, 0, "")
	require.ErrorIs(t, s.Publish(updated, recovered), errDisk)
	require.Equal(t, before, diskBytes(t, dir, IntentFile))
	require.Equal(t, []byte(MarkerV1), diskBytes(t, dir, MarkerFile))
	_, err := os.Stat(filepath.Join(dir, IntentFile))
	require.NoError(t, err)
}

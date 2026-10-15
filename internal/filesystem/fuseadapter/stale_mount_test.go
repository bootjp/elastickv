package fuseadapter

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mountTableLine renders one /proc/self/mounts row for mountPoint, escaping
// the characters the kernel escapes.
func mountTableLine(device, mountPoint, fsType string) string {
	escaped := strings.NewReplacer(`\`, `\134`, " ", `\040`, "\t", `\011`, "\n", `\012`).Replace(mountPoint)
	return device + " " + escaped + " " + fsType + " rw,relatime 0 0\n"
}

func writeMountTable(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mounts")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "")), 0o600))
	return path
}

func statReturning(err error) func(string) error {
	return func(string) error { return err }
}

func TestStaleFUSEMountDetection(t *testing.T) {
	base := t.TempDir()
	mountPoint := filepath.Join(base, "mount point")
	require.NoError(t, os.Mkdir(mountPoint, 0o755))
	other := filesystemNameForTest(base, "elsewhere")
	tests := []struct {
		name      string
		table     []string
		stat      func(string) error
		wantStale bool
		wantType  string
	}{
		{
			name:      "dead go-fuse mount",
			table:     []string{mountTableLine("elastickv", mountPoint, "fuse.elastickv")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: true,
			wantType:  "fuse.elastickv",
		},
		{
			name:      "dead bare fuse mount",
			table:     []string{mountTableLine("fuse", mountPoint, "fuse")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: true,
			wantType:  "fuse",
		},
		{
			name:      "live fuse mount answers stat",
			table:     []string{mountTableLine("elastickv", mountPoint, "fuse.elastickv")},
			stat:      statReturning(nil),
			wantStale: false,
		},
		{
			name:      "fuse mount with a non-transport stat error",
			table:     []string{mountTableLine("elastickv", mountPoint, "fuse.elastickv")},
			stat:      statReturning(syscall.EACCES),
			wantStale: false,
		},
		{
			name:      "non-FUSE filesystem is never touched",
			table:     []string{mountTableLine("/dev/sda1", mountPoint, "ext4")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: false,
		},
		{
			name:      "fuseblk is not a go-fuse mount",
			table:     []string{mountTableLine("/dev/sdb", mountPoint, "fuseblk")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: false,
		},
		{
			name:      "no entry for the path",
			table:     []string{mountTableLine("elastickv", other, "fuse.elastickv")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: false,
		},
		{
			name: "non-FUSE mount stacked over a dead fuse mount",
			table: []string{
				mountTableLine("elastickv", mountPoint, "fuse.elastickv"),
				mountTableLine("tmpfs", mountPoint, "tmpfs"),
			},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: false,
		},
		{
			name: "dead fuse mount stacked over another filesystem",
			table: []string{
				mountTableLine("tmpfs", mountPoint, "tmpfs"),
				mountTableLine("elastickv", mountPoint, "fuse.elastickv"),
			},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: true,
			wantType:  "fuse.elastickv",
		},
		{
			name:      "malformed rows are skipped",
			table:     []string{"garbage\n", mountTableLine("elastickv", mountPoint, "fuse.elastickv")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: true,
			wantType:  "fuse.elastickv",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inspector := &staleMountInspector{mountTable: writeMountTable(t, tt.table...), stat: tt.stat}
			entry, stale, err := inspector.staleFUSEMount(mountPoint)
			require.NoError(t, err)
			require.Equal(t, tt.wantStale, stale)
			if tt.wantStale {
				require.Equal(t, mountPoint, entry.mountPoint)
				require.Equal(t, tt.wantType, entry.fsType)
			}
		})
	}
}

func filesystemNameForTest(base, name string) string {
	return filepath.Join(base, name)
}

func TestStaleFUSEMountMatchesSymlinkedParent(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	require.NoError(t, os.Mkdir(real, 0o755))
	link := filepath.Join(base, "link")
	require.NoError(t, os.Symlink(real, link))
	resolvedBase, err := filepath.EvalSymlinks(real)
	require.NoError(t, err)

	table := writeMountTable(t, mountTableLine("elastickv", filepath.Join(resolvedBase, "mnt"), "fuse.elastickv"))
	inspector := &staleMountInspector{mountTable: table, stat: statReturning(syscall.ENOTCONN)}
	entry, stale, err := inspector.staleFUSEMount(filepath.Join(link, "mnt"))
	require.NoError(t, err)
	require.True(t, stale)
	require.Equal(t, filepath.Join(resolvedBase, "mnt"), entry.mountPoint)
}

func TestStaleFUSEMountMatchesSymlinkedMountPoint(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real-mount")
	require.NoError(t, os.Mkdir(real, 0o755))
	link := filepath.Join(base, "mount-link")
	require.NoError(t, os.Symlink("real-mount", link))
	resolvedReal, err := filepath.EvalSymlinks(real)
	require.NoError(t, err)

	// The kernel mounts on the symlink's target and records the target.
	table := writeMountTable(t, mountTableLine("elastickv", resolvedReal, "fuse.elastickv"))
	inspector := &staleMountInspector{mountTable: table, stat: statReturning(syscall.ENOTCONN)}
	entry, stale, err := inspector.staleFUSEMount(link)
	require.NoError(t, err)
	require.True(t, stale)
	require.Equal(t, resolvedReal, entry.mountPoint)
}

func TestResolveMountPointSymlinksStopsAtNonSymlink(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	require.NoError(t, os.Mkdir(real, 0o755))
	first := filepath.Join(base, "first")
	second := filepath.Join(base, "second")
	require.NoError(t, os.Symlink(real, second))
	require.NoError(t, os.Symlink("second", first))

	require.Equal(t, real, resolveMountPointSymlinks(first))
	require.Equal(t, real, resolveMountPointSymlinks(real))
	missing := filepath.Join(base, "missing")
	require.Equal(t, missing, resolveMountPointSymlinks(missing))
}

func TestStaleFUSEMountReportsUnreadableTable(t *testing.T) {
	inspector := &staleMountInspector{
		mountTable: filepath.Join(t.TempDir(), "missing"),
		stat:       statReturning(syscall.ENOTCONN),
	}
	_, _, err := inspector.staleFUSEMount(t.TempDir())
	var unavailable *mountTableUnavailableError
	require.ErrorAs(t, err, &unavailable)
	require.ErrorContains(t, err, "open mount table")
}

func TestDetachAllSkipsCheckWithoutMountTable(t *testing.T) {
	// A chroot or minimal container may have /dev/fuse but no /proc; the
	// stale check must not turn that into a new mount prerequisite.
	inspector := &staleMountInspector{
		mountTable: filepath.Join(t.TempDir(), "missing"),
		stat:       statReturning(syscall.ENOTCONN),
	}
	detach := func(context.Context, string) (string, error) {
		t.Fatal("detach must not run without a mount table")
		return "", nil
	}

	detached, err := inspector.detachAll(context.Background(), t.TempDir(), slog.Default(), detach)
	require.NoError(t, err)
	require.Zero(t, detached)
}

func TestDetachAllDetachesUntilClean(t *testing.T) {
	mountPoint := t.TempDir()
	stale := mountTableLine("elastickv", mountPoint, "fuse.elastickv")
	// One table per state: two stacked dead mounts, one, none. Each fake
	// detach advances the inspector to the next table, as the kernel would
	// drop the topmost row after a real unmount.
	tables := []string{
		writeMountTable(t, stale, stale),
		writeMountTable(t, stale),
		writeMountTable(t),
	}
	inspector := &staleMountInspector{mountTable: tables[0], stat: statReturning(syscall.ENOTCONN)}
	var detachedPaths []string
	detach := func(_ context.Context, path string) (string, error) {
		detachedPaths = append(detachedPaths, path)
		inspector.mountTable = tables[len(detachedPaths)]
		return "fake", nil
	}

	detached, err := inspector.detachAll(context.Background(), mountPoint, slog.Default(), detach)
	require.NoError(t, err)
	require.Equal(t, 2, detached)
	require.Equal(t, []string{mountPoint, mountPoint}, detachedPaths)
}

func TestDetachAllLeavesHealthyPathAlone(t *testing.T) {
	mountPoint := t.TempDir()
	table := writeMountTable(t, mountTableLine("/dev/sda1", mountPoint, "ext4"))
	inspector := &staleMountInspector{mountTable: table, stat: statReturning(syscall.ENOTCONN)}
	detach := func(context.Context, string) (string, error) {
		t.Fatal("detach must not run for a non-FUSE mount")
		return "", nil
	}

	detached, err := inspector.detachAll(context.Background(), mountPoint, nil, detach)
	require.NoError(t, err)
	require.Zero(t, detached)
}

func TestDetachAllWrapsDetachFailure(t *testing.T) {
	mountPoint := t.TempDir()
	table := writeMountTable(t, mountTableLine("elastickv", mountPoint, "fuse.elastickv"))
	inspector := &staleMountInspector{mountTable: table, stat: statReturning(syscall.ENOTCONN)}
	boom := errors.New("fusermount3 exploded")
	detach := func(context.Context, string) (string, error) { return "", boom }

	detached, err := inspector.detachAll(context.Background(), mountPoint, slog.Default(), detach)
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "detach stale FUSE mount at "+mountPoint)
	require.Zero(t, detached)
}

func TestDetachAllClearsExactlyTheMaximum(t *testing.T) {
	mountPoint := t.TempDir()
	stale := mountTableLine("elastickv", mountPoint, "fuse.elastickv")
	// tables[k] holds maxStaleMountDetaches-k stacked dead mounts; the last
	// one is clean. Exactly the maximum must succeed, not report "more than".
	tables := make([]string, 0, maxStaleMountDetaches+1)
	for remaining := maxStaleMountDetaches; remaining >= 0; remaining-- {
		lines := make([]string, remaining)
		for index := range lines {
			lines[index] = stale
		}
		tables = append(tables, writeMountTable(t, lines...))
	}
	inspector := &staleMountInspector{mountTable: tables[0], stat: statReturning(syscall.ENOTCONN)}
	calls := 0
	detach := func(context.Context, string) (string, error) {
		calls++
		inspector.mountTable = tables[calls]
		return "fake", nil
	}

	detached, err := inspector.detachAll(context.Background(), mountPoint, slog.Default(), detach)
	require.NoError(t, err)
	require.Equal(t, maxStaleMountDetaches, detached)
	require.Equal(t, maxStaleMountDetaches, calls)
}

func TestDetachAllBoundsTheLoop(t *testing.T) {
	mountPoint := t.TempDir()
	table := writeMountTable(t, mountTableLine("elastickv", mountPoint, "fuse.elastickv"))
	inspector := &staleMountInspector{mountTable: table, stat: statReturning(syscall.ENOTCONN)}
	calls := 0
	// The fake detach "succeeds" without changing the table, as a helper
	// that lies about the unmount would.
	detach := func(context.Context, string) (string, error) {
		calls++
		return "noop", nil
	}

	detached, err := inspector.detachAll(context.Background(), mountPoint, slog.Default(), detach)
	require.ErrorContains(t, err, "more than")
	require.Equal(t, maxStaleMountDetaches, detached)
	require.Equal(t, maxStaleMountDetaches, calls)
}

func TestUnescapeMountTableField(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{in: "/mnt/plain", want: "/mnt/plain"},
		{in: `/mnt/with\040space`, want: "/mnt/with space"},
		{in: `/mnt/tab\011here`, want: "/mnt/tab\there"},
		{in: `/mnt/back\134slash`, want: `/mnt/back\slash`},
		{in: `/mnt/trailing\04`, want: `/mnt/trailing\04`},
		{in: `/mnt/not\0zzoctal`, want: `/mnt/not\0zzoctal`},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, unescapeMountTableField(tt.in))
		})
	}
}

func TestIsFUSEType(t *testing.T) {
	require.True(t, isFUSEType("fuse"))
	require.True(t, isFUSEType("fuse.elastickv"))
	require.True(t, isFUSEType("fuse.sshfs"))
	require.False(t, isFUSEType("fuseblk"))
	require.False(t, isFUSEType("fusectl"))
	require.False(t, isFUSEType("ext4"))
	require.False(t, isFUSEType(""))
}

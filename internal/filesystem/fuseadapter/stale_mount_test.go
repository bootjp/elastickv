package fuseadapter

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

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
			name:      "dead bare fuse mount is not this server's",
			table:     []string{mountTableLine("fuse", mountPoint, "fuse")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: false,
		},
		{
			name:      "dead foreign FUSE filesystem is not this server's",
			table:     []string{mountTableLine("sshfs#host:/", mountPoint, "fuse.sshfs")},
			stat:      statReturning(syscall.ENOTCONN),
			wantStale: false,
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
			entry, stale, err := inspector.staleFUSEMount(context.Background(), mountPoint)
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
	entry, stale, err := inspector.staleFUSEMount(context.Background(), filepath.Join(link, "mnt"))
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
	entry, stale, err := inspector.staleFUSEMount(context.Background(), link)
	require.NoError(t, err)
	require.True(t, stale)
	require.Equal(t, resolvedReal, entry.mountPoint)
}

// TestStaleFUSEMountMatchesRelativeSymlinkUnderSymlinkedParent covers
// /base/link/mp with link -> /base/real/deep and mp -> ../target: the kernel
// resolves the parent first and mounts on /base/real/target, so a relative
// link target must be joined to the canonical parent, not the typed one.
func TestStaleFUSEMountMatchesRelativeSymlinkUnderSymlinkedParent(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	deep := filepath.Join(base, "real", "deep")
	target := filepath.Join(base, "real", "target")
	require.NoError(t, os.MkdirAll(deep, 0o755))
	require.NoError(t, os.MkdirAll(target, 0o755))
	require.NoError(t, os.Symlink(deep, filepath.Join(base, "link")))
	require.NoError(t, os.Symlink("../target", filepath.Join(deep, "mp")))
	typed := filepath.Join(base, "link", "mp")

	require.Equal(t, target, resolveMountPointSymlinks(typed))

	table := writeMountTable(t, mountTableLine("elastickv", target, "fuse.elastickv"))
	inspector := &staleMountInspector{mountTable: table, stat: statReturning(syscall.ENOTCONN)}
	entry, stale, err := inspector.staleFUSEMount(context.Background(), typed)
	require.NoError(t, err)
	require.True(t, stale)
	require.Equal(t, target, entry.mountPoint)
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
	_, _, err := inspector.staleFUSEMount(context.Background(), t.TempDir())
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

// TestDetachFUSEMountReportsCancellation pins that a shutdown requested while
// a detach is in flight surfaces as context.Canceled rather than as the
// EPERM the privileged fallback would return for an unprivileged server:
// onlyContextCanceled in main relies on that to exit 0 after a signal. The
// path is a plain directory, so nothing is unmounted whether or not a
// fusermount helper exists on this machine.
// blockingStat imitates stat on a FUSE mount whose server is alive but
// wedged: the syscall never returns until release is closed.
func blockingStat(release <-chan struct{}) func(string) error {
	return func(string) error {
		<-release
		return nil
	}
}

func TestStaleFUSEMountProbeStopsOnCancellation(t *testing.T) {
	mountPoint := t.TempDir()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	inspector := &staleMountInspector{
		mountTable:   writeMountTable(t, mountTableLine("elastickv", mountPoint, "fuse.elastickv")),
		stat:         blockingStat(release),
		probeTimeout: time.Minute,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, stale, err := inspector.staleFUSEMount(ctx, mountPoint)
	require.ErrorIs(t, err, context.Canceled)
	var probeErr *mountProbeError
	require.ErrorAs(t, err, &probeErr)
	require.False(t, stale)
}

func TestStaleFUSEMountProbeTimesOutOnWedgedServer(t *testing.T) {
	mountPoint := t.TempDir()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	inspector := &staleMountInspector{
		mountTable:   writeMountTable(t, mountTableLine("elastickv", mountPoint, "fuse.elastickv")),
		stat:         blockingStat(release),
		probeTimeout: 20 * time.Millisecond,
	}

	_, stale, err := inspector.staleFUSEMount(context.Background(), mountPoint)
	require.ErrorContains(t, err, "unresponsive FUSE server")
	var probeErr *mountProbeError
	require.ErrorAs(t, err, &probeErr)
	require.False(t, stale)

	// The verdict error must stop the detach loop rather than be read as
	// "not stale and proceed".
	detach := func(context.Context, string) (string, error) {
		t.Fatal("detach must not run when the probe reached no verdict")
		return "", nil
	}
	detached, err := inspector.detachAll(context.Background(), mountPoint, slog.Default(), detach)
	require.ErrorAs(t, err, &probeErr)
	require.Zero(t, detached)
}

// useFakeDetachMountTable points the detach path's mounts-table check at a
// table listing mountPoint as a FUSE mount, so detachFUSEMount on a plain
// test directory gets past the verification to the mechanism under test.
func useFakeDetachMountTable(t *testing.T, lines ...string) string {
	t.Helper()
	table := writeMountTable(t, lines...)
	old := detachMountTable
	detachMountTable = table
	t.Cleanup(func() { detachMountTable = old })
	return table
}

func fuseTableLine(mountPoint string) string {
	return mountTableLine("elastickv", mountPoint, "fuse.elastickv")
}

// writeFakeMountHelper drops an executable stand-in for a fusermount binary
// into dir and returns its path.
func writeFakeMountHelper(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	return path
}

// onlyMountHelpersIn makes dir the only place lookupMountHelper finds a
// fusermount helper: PATH and the go-fuse style fallback directory both
// point at it, so a real /bin/fusermount or /bin/fusermount3 on the host
// (GitHub's Ubuntu runners ship /bin/fusermount) cannot run after the
// stand-ins and change what the test observes.
func onlyMountHelpersIn(t *testing.T, dir string) {
	t.Helper()
	t.Setenv("PATH", dir)
	old := mountHelperFallbackDir
	mountHelperFallbackDir = dir
	t.Cleanup(func() { mountHelperFallbackDir = old })
}

func TestLookupMountHelperFallsBackLikeGoFuse(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // empty PATH: nothing found there
	fallback := t.TempDir()
	oldFallback := mountHelperFallbackDir
	mountHelperFallbackDir = fallback
	t.Cleanup(func() { mountHelperFallbackDir = oldFallback })

	_, err := lookupMountHelper("fusermount3")
	require.Error(t, err, "nothing on PATH or in the fallback dir")

	want := writeFakeMountHelper(t, fallback, "fusermount3")
	got, err := lookupMountHelper("fusermount3")
	require.NoError(t, err)
	require.Equal(t, want, got)

	// PATH still wins when it has the helper.
	onPath := t.TempDir()
	t.Setenv("PATH", onPath)
	wantOnPath := writeFakeMountHelper(t, onPath, "fusermount3")
	got, err = lookupMountHelper("fusermount3")
	require.NoError(t, err)
	require.Equal(t, wantOnPath, got)
}

// TestDetachFUSEMountReachesKernelFallbackWhenHelperHangs puts a fusermount3
// stand-in that never finishes on PATH and gives the detach a short deadline.
// The helper must be cut off before the deadline so the privileged
// unmount(2) fallback is still attempted (here on a plain directory, so it
// fails with its own error rather than the deadline's).
func TestDetachFUSEMountReachesKernelFallbackWhenHelperHangs(t *testing.T) {
	helperDir := t.TempDir()
	helper := filepath.Join(helperDir, "fusermount3")
	require.NoError(t, os.WriteFile(helper, []byte("#!/bin/sh\nexec /bin/sleep 5\n"), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	onlyMountHelpersIn(t, helperDir)
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	// Half of this budget is the helper's share; the reserved pipe wait
	// leaves it 500 ms of real running time before it is killed.
	budget := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	method, err := detachFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName))
	t.Logf("detach error: %v", err)
	require.Error(t, err)
	require.Empty(t, method)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the deadline must not pre-empt the kernel fallback")
	require.ErrorContains(t, err, lazyUnmountMethod)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, budget/2-detachHelperWaitDelay-50*time.Millisecond, "the helper must have been given its share of the deadline")
	require.Less(t, elapsed, budget, "the fallback must run inside the caller's budget")
}

func TestDetachHelperTimeoutSharesRemainingDeadline(t *testing.T) {
	require.Equal(t, detachCommandTimeout-detachHelperWaitDelay, detachHelperTimeout(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	timeout := detachHelperTimeout(ctx)
	require.Greater(t, timeout, time.Duration(0))
	require.LessOrEqual(t, timeout, 2*time.Second/detachHelperBudgetShare-detachHelperWaitDelay,
		"the pipe wait after the kill must fit inside the helper's share")

	// A share that the pipe wait alone would use up leaves nothing for the
	// helper itself: the timeout is not positive, so exec skips it.
	tight, cancelTight := context.WithTimeout(context.Background(), 2*detachHelperWaitDelay)
	defer cancelTight()
	require.LessOrEqual(t, detachHelperTimeout(tight), time.Duration(0))

	expired, cancelExpired := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancelExpired()
	time.Sleep(time.Millisecond)
	require.LessOrEqual(t, detachHelperTimeout(expired), time.Duration(0))
}

// TestDetachFUSEMountReachesKernelFallbackWhenHelpersHoldPipes is the case
// the share must survive: both helpers are present, each hangs, and each
// leaves a background child holding its output pipes open, so every
// invocation costs its context timeout plus the WaitDelay pipe wait. With
// the pipe wait reserved out of the share, the first helper uses at most
// half of the budget, the second gets a share the pipe wait would exhaust
// and is skipped, and the kernel fallback still runs before the deadline.
// Before the reservation the two helpers overran the budget and the
// fallback was skipped behind the expired context.
func TestDetachFUSEMountReachesKernelFallbackWhenHelpersHoldPipes(t *testing.T) {
	helperDir := t.TempDir()
	const script = "#!/bin/sh\n/bin/sleep 5 &\nexec /bin/sleep 5\n"
	for _, name := range []string{"fusermount3", "fusermount"} {
		require.NoError(t, os.WriteFile(filepath.Join(helperDir, name), []byte(script), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	}
	onlyMountHelpersIn(t, helperDir)
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	budget := 4 * detachHelperWaitDelay
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	method, err := detachFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName))
	t.Logf("detach error: %v", err)
	require.Error(t, err)
	require.Empty(t, method)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the helpers' pipe waits must not pre-empt the kernel fallback")
	require.ErrorContains(t, err, lazyUnmountMethod)
	elapsed := time.Since(start)
	require.GreaterOrEqual(t, elapsed, budget/2-50*time.Millisecond, "the first helper must have been given its share, pipe wait included")
	require.Less(t, elapsed, budget, "the fallback must run inside the caller's budget")
}

func TestDetachFUSEMountReportsCancellation(t *testing.T) {
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	method, err := detachFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, method)
}

// TestDetachFUSEMountRefusesPathWithoutFUSEMount pins that the detach never
// acts on a path the mounts table does not list as a FUSE mount: nothing
// mounted, another filesystem on top, or a table it cannot read. No helper
// is run and the kernel fallback is not attempted, so a filesystem that
// sits where the FUSE mount used to be (go-fuse's own Unmount having
// already removed it while still waiting for its event loop) is left alone.
func TestDetachFUSEMountRefusesPathWithoutFUSEMount(t *testing.T) {
	mountPoint := t.TempDir()
	helperDir := t.TempDir()
	marker := filepath.Join(helperDir, "ran")
	script := "#!/bin/sh\n/bin/touch " + marker + "\nexit 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(helperDir, "fusermount3"), []byte(script), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	onlyMountHelpersIn(t, helperDir)

	tests := []struct {
		name  string
		table []string
	}{
		{name: "nothing mounted at the path", table: nil},
		{name: "another filesystem on top", table: []string{mountTableLine("/dev/sdb1", mountPoint, "ext4")}},
		{name: "another FUSE filesystem exposed at the path", table: []string{mountTableLine("sshfs#host:/", mountPoint, "fuse.sshfs")}},
		{name: "a bare fuse mount at the path", table: []string{mountTableLine("fuse", mountPoint, "fuse")}},
		{name: "foreign FUSE filesystem over a dead mount of ours", table: []string{fuseTableLine(mountPoint), mountTableLine("sshfs#host:/", mountPoint, "fuse.sshfs")}},
		{name: "foreign filesystem over a dead fuse mount", table: []string{fuseTableLine(mountPoint), mountTableLine("tmpfs", mountPoint, "tmpfs")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useFakeDetachMountTable(t, tt.table...)
			method, err := detachFUSEMount(context.Background(), mountPoint, fuseTypeFor(filesystemName))
			require.ErrorIs(t, err, errNoFUSEMount)
			require.NotContains(t, err.Error(), lazyUnmountMethod, "the kernel fallback must not be attempted")
			require.Empty(t, method)
			require.NoFileExists(t, marker, "no helper may run against a path without a FUSE mount")
		})
	}

	t.Run("mount table unavailable", func(t *testing.T) {
		old := detachMountTable
		detachMountTable = filepath.Join(t.TempDir(), "missing")
		t.Cleanup(func() { detachMountTable = old })
		_, err := detachFUSEMount(context.Background(), mountPoint, fuseTypeFor(filesystemName))
		var unavailable *mountTableUnavailableError
		require.ErrorAs(t, err, &unavailable)
		require.NotContains(t, err.Error(), lazyUnmountMethod)
		require.NoFileExists(t, marker)
	})
}

// TestDetachFUSEMountRechecksBeforeKernelFallback covers the mount vanishing
// while a helper runs: the stand-in rewrites the table to show another
// filesystem at the path and fails, and the kernel fallback must then be
// refused instead of detaching that filesystem.
func TestDetachFUSEMountRechecksBeforeKernelFallback(t *testing.T) {
	mountPoint := t.TempDir()
	table := useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	helperDir := t.TempDir()
	// The helper replaces the table with one that lists ext4 at the path,
	// as if the FUSE mount had been removed underneath it, then fails so the
	// code reaches the fallback decision.
	replacement := filepath.Join(helperDir, "table.after")
	require.NoError(t, os.WriteFile(replacement, []byte(mountTableLine("/dev/sdb1", mountPoint, "ext4")), 0o600))
	script := "#!/bin/sh\n/bin/cp " + replacement + " " + table + "\nexit 1\n"
	require.NoError(t, os.WriteFile(filepath.Join(helperDir, "fusermount3"), []byte(script), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	onlyMountHelpersIn(t, helperDir)

	method, err := detachFUSEMount(context.Background(), mountPoint, fuseTypeFor(filesystemName))
	require.ErrorIs(t, err, errNoFUSEMount)
	require.ErrorContains(t, err, "fusermount3 -u -z", "the helper's failure stays in the chain")
	require.NotContains(t, err.Error(), lazyUnmountMethod, "the kernel fallback must not run once the FUSE mount is gone")
	require.Empty(t, method)
}

// TestDetachAllReinspectsWhenMountVanishes: a stale mount that disappears
// between the inspection and the detach is neither counted nor an error.
func TestDetachAllReinspectsWhenMountVanishes(t *testing.T) {
	mountPoint := t.TempDir()
	tables := []string{
		writeMountTable(t, fuseTableLine(mountPoint)),
		writeMountTable(t),
	}
	inspector := &staleMountInspector{mountTable: tables[0], stat: statReturning(syscall.ENOTCONN)}
	calls := 0
	detach := func(context.Context, string) (string, error) {
		calls++
		inspector.mountTable = tables[1]
		return "", errors.Wrap(errNoFUSEMount, mountPoint)
	}

	detached, err := inspector.detachAll(context.Background(), mountPoint, slog.Default(), detach)
	require.NoError(t, err)
	require.Zero(t, detached)
	require.Equal(t, 1, calls)
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

// TestFUSETypeFor pins that the type a mount must carry to be detached is
// derived from the mount Name (the subtype) the server mounts with, so a
// mount made with a custom Name is recognised and every other subtype is
// not. TestMountContextTypeFollowsMountName checks the same against a real
// kernel mount.
func TestFUSETypeFor(t *testing.T) {
	require.Equal(t, "fuse.elastickv", fuseTypeFor(filesystemName))
	require.Equal(t, "fuse.custom", fuseTypeFor("custom"))
	require.Equal(t, "fuse.elastickv", (&staleMountInspector{}).expectedType())

	mountPoint := t.TempDir()
	inspector := &staleMountInspector{
		mountTable: writeMountTable(t, mountTableLine("custom", mountPoint, "fuse.custom")),
		stat:       statReturning(syscall.ENOTCONN),
		fsType:     fuseTypeFor("custom"),
	}
	_, stale, err := inspector.staleFUSEMount(context.Background(), mountPoint)
	require.NoError(t, err)
	require.True(t, stale, "a dead mount of the configured Name is ours")
	inspector.mountTable = writeMountTable(t, fuseTableLine(mountPoint))
	_, stale, err = inspector.staleFUSEMount(context.Background(), mountPoint)
	require.NoError(t, err)
	require.False(t, stale, "the default Name is foreign to a server mounting with another one")
}

// TestTopmostMountStopsWhenResolutionBlocks stands in a mount-point
// resolution that never returns, as EvalSymlinks does when a parent
// directory sits on a wedged FUSE or network filesystem, and pins that the
// inspection and the detach both give up on cancellation or after the
// resolution bound instead of holding the shutdown.
func TestTopmostMountStopsWhenResolutionBlocks(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	oldResolve := mountPointCandidatesFn
	mountPointCandidatesFn = func(string) (map[string]bool, error) {
		<-release
		return nil, nil
	}
	t.Cleanup(func() { mountPointCandidatesFn = oldResolve })
	mountPoint := t.TempDir()
	table := writeMountTable(t, fuseTableLine(mountPoint))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := topmostMount(ctx, table, mountPoint)
	var resolveErr *mountProbeError
	require.ErrorAs(t, err, &resolveErr)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "resolve FUSE mount point")

	oldTimeout := mountPointResolveTimeout
	mountPointResolveTimeout = 20 * time.Millisecond
	t.Cleanup(func() { mountPointResolveTimeout = oldTimeout })
	start := time.Now()
	_, _, err = topmostMount(context.Background(), table, mountPoint)
	require.ErrorAs(t, err, &resolveErr)
	require.ErrorContains(t, err, "unresponsive filesystem")
	require.Less(t, time.Since(start), 2*time.Second)

	// Neither caller reads the failure as "nothing there": the startup
	// inspection reports it and the detach refuses to go on.
	inspector := &staleMountInspector{mountTable: table, stat: statReturning(syscall.ENOTCONN)}
	_, stale, err := inspector.staleFUSEMount(context.Background(), mountPoint)
	require.ErrorAs(t, err, &resolveErr)
	require.False(t, stale)
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	method, err := detachFUSEMount(context.Background(), mountPoint, fuseTypeFor(filesystemName))
	require.ErrorAs(t, err, &resolveErr)
	require.Empty(t, method)
}

// writeRecordingMountHelper puts a fusermount3 stand-in on PATH that records
// having run by creating the returned marker file.
func writeRecordingMountHelper(t *testing.T) string {
	t.Helper()
	helperDir := t.TempDir()
	marker := filepath.Join(helperDir, "ran")
	t.Setenv("ELASTICKV_TEST_DETACH_MARKER", marker)
	// Shell redirection keeps this helper portable across Linux and macOS.
	script := "#!/bin/sh\n: > \"$ELASTICKV_TEST_DETACH_MARKER\"\nexit 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(helperDir, "fusermount3"), []byte(script), 0o700)) //nolint:gosec // the helper stand-in must be executable.
	onlyMountHelpersIn(t, helperDir)
	return marker
}

// TestReleaseAbandonedDetachesWhenUnmountHangs: an abandoned mount whose
// plain unmount never returns (a hung helper, or an event loop stuck in a
// backend request) is detached lazily once the unmount overran its bound,
// instead of leaving the kernel mount attached to a dead server.
func TestReleaseAbandonedDetachesWhenUnmountHangs(t *testing.T) {
	old := abandonedUnmountTimeout
	abandonedUnmountTimeout = 50 * time.Millisecond
	t.Cleanup(func() { abandonedUnmountTimeout = old })
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	marker := writeRecordingMountHelper(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	start := time.Now()
	releaseAbandoned(func() error {
		<-release
		return nil
	}, mountPoint, fuseTypeFor(filesystemName))
	require.Less(t, time.Since(start), 5*time.Second, "the release must not wait on the hung unmount")
	require.FileExists(t, marker, "the lazy detach must run once the unmount overran its bound")
}

// TestReleaseAbandonedFallsBackOnlyWhenUnmountFails: a clean unmount ends
// the release without any detach; a failed one is followed by the lazy
// detach.
func TestReleaseAbandonedFallsBackOnlyWhenUnmountFails(t *testing.T) {
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	marker := writeRecordingMountHelper(t)

	releaseAbandoned(func() error { return nil }, mountPoint, fuseTypeFor(filesystemName))
	require.NoFileExists(t, marker, "a clean unmount needs no detach")

	releaseAbandoned(func() error { return errors.New("device or resource busy") }, mountPoint, fuseTypeFor(filesystemName))
	require.FileExists(t, marker, "a failed unmount falls back to the lazy detach")
}

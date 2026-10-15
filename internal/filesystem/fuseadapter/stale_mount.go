package fuseadapter

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	// mountTablePath lists the mounts visible to this process. Entries are
	// whitespace separated with octal escapes for the characters that would
	// break the format (space, tab, newline, backslash).
	mountTablePath = "/proc/self/mounts"
	// mountTableMinFields is the number of leading columns this code reads
	// from a mounts entry (device, mount point, type); shorter rows are
	// malformed and skipped.
	mountTableMinFields = 3
	// mountTableOctalDigits is the width of a `\ooo` escape in the table.
	mountTableOctalDigits = 3
	// mountTableOctalBase is the radix of a `\ooo` escape in the table.
	mountTableOctalBase = 8
	// mountTableOctalBits is the bit size of the byte a `\ooo` escape encodes.
	mountTableOctalBits = 8
	// fuseTypePrefix is the type go-fuse mounts report in the mounts table
	// (`fuse.<FsName>`). A bare `fuse` type is the libfuse default.
	fuseTypePrefix = "fuse."
	// fuseType is the mounts-table type of a FUSE mount without an FsName.
	fuseType = "fuse"
	// detachCommandTimeout bounds one fusermount invocation. A dangling
	// mount does not need the server, so detaching it never blocks on I/O,
	// but a hung suid helper must not stall startup forever.
	detachCommandTimeout = 10 * time.Second
	// maxStaleMountDetaches bounds the detach loop. Each iteration removes
	// the topmost stale mount at the path; several only stack up when a
	// server was restarted over a dangling mount more than once.
	maxStaleMountDetaches = 8
	// maxMountPointSymlinkHops bounds symlink resolution of the mount point
	// itself, matching the kernel's MAXSYMLINKS.
	maxMountPointSymlinkHops = 40
	// staleProbeTimeout bounds the liveness stat on a FUSE mount. A dead
	// mount answers ENOTCONN at once and a healthy server answers quickly;
	// only a server that is alive but wedged makes stat block, and that mount
	// is not ours to detach, so startup fails with a clear error instead of
	// hanging in fusermount's own stat afterwards.
	staleProbeTimeout = 10 * time.Second
)

// mountProbeError reports that the liveness stat on a FUSE mount could not
// reach a verdict: the probe was cancelled (shutdown requested) or timed out
// (a live but unresponsive FUSE server still holds the mount point).
type mountProbeError struct {
	path string
	err  error
}

func (e *mountProbeError) Error() string {
	return "stat FUSE mount at " + e.path + ": " + e.err.Error()
}

func (e *mountProbeError) Unwrap() error { return e.err }

// mountTableEntry is one row of /proc/self/mounts.
type mountTableEntry struct {
	mountPoint string
	fsType     string
}

// mountTableUnavailableError reports that the mounts table could not be
// read at all (no /proc in a minimal container or chroot). It is not a
// stale-mount verdict: the caller skips the check and lets the real mount
// report any conflict, so /proc does not become a new hard prerequisite.
type mountTableUnavailableError struct {
	path string
	err  error
}

func (e *mountTableUnavailableError) Error() string {
	return "open mount table " + e.path + ": " + e.err.Error()
}

func (e *mountTableUnavailableError) Unwrap() error { return e.err }

// staleMountInspector locates dangling FUSE mounts at a path. The fields are
// swapped in tests so the decision logic can be exercised without a kernel
// mount; production code uses defaultStaleMountInspector.
type staleMountInspector struct {
	mountTable   string
	stat         func(string) error
	probeTimeout time.Duration
}

func defaultStaleMountInspector() *staleMountInspector {
	return &staleMountInspector{mountTable: mountTablePath, stat: statMountPoint, probeTimeout: staleProbeTimeout}
}

func statMountPoint(path string) error {
	var st syscall.Stat_t
	return errors.WithStack(syscall.Stat(path, &st))
}

// detachStaleMount removes a dangling FUSE mount left at mountPoint by a
// server that died without unmounting (SIGKILL, a crash, or a stop before
// the process installed signal handling). It reports how many mounts were
// detached. A path that holds no mount, a live FUSE mount (its server still
// answers stat), or any non-FUSE filesystem is left untouched: the mount
// table decides the type, never the error from stat alone.
func detachStaleMount(ctx context.Context, mountPoint string, logger *slog.Logger) (int, error) {
	return defaultStaleMountInspector().detachAll(ctx, mountPoint, logger, detachFUSEMount)
}

func (i *staleMountInspector) detachAll(
	ctx context.Context,
	mountPoint string,
	logger *slog.Logger,
	detach func(context.Context, string) (string, error),
) (int, error) {
	if logger == nil {
		logger = slog.Default()
	}
	detached := 0
	for {
		entry, stale, err := i.staleFUSEMount(ctx, mountPoint)
		var unavailable *mountTableUnavailableError
		if errors.As(err, &unavailable) {
			logger.Warn("mount table unavailable, skipping stale FUSE mount check",
				"mount_point", mountPoint,
				"err", err,
			)
			return detached, nil
		}
		if err != nil {
			return detached, err
		}
		if !stale {
			return detached, nil
		}
		if detached >= maxStaleMountDetaches {
			// Re-inspected after the last allowed detach, so exactly
			// maxStaleMountDetaches stacked mounts succeed; only a mount
			// that is still there is an error.
			return detached, errors.Errorf("more than %d stale FUSE mounts at %s", maxStaleMountDetaches, mountPoint)
		}
		method, err := detach(ctx, entry.mountPoint)
		if err != nil {
			return detached, errors.Wrapf(err, "detach stale FUSE mount at %s", entry.mountPoint)
		}
		detached++
		logger.Warn("detached stale filesystem FUSE mount left by a previous process",
			"mount_point", entry.mountPoint,
			"fs_type", entry.fsType,
			"method", method,
		)
	}
}

// staleFUSEMount reports whether the topmost mount at mountPoint is a FUSE
// mount whose server is gone. The topmost entry decides: a non-FUSE
// filesystem mounted over a dead FUSE mount is not touched either. The
// liveness stat is bounded by ctx and probeTimeout; when it cannot reach a
// verdict the error is a *mountProbeError.
func (i *staleMountInspector) staleFUSEMount(ctx context.Context, mountPoint string) (mountTableEntry, bool, error) {
	candidates, err := mountPointCandidates(mountPoint)
	if err != nil {
		return mountTableEntry{}, false, err
	}
	entries, err := readMountTable(i.mountTable)
	if err != nil {
		return mountTableEntry{}, false, err
	}
	var top mountTableEntry
	found := false
	for _, entry := range entries {
		if candidates[entry.mountPoint] {
			top = entry
			found = true
		}
	}
	if !found || !isFUSEType(top.fsType) {
		return mountTableEntry{}, false, nil
	}
	statErr := i.probe(ctx, top.mountPoint)
	var probeErr *mountProbeError
	if errors.As(statErr, &probeErr) {
		return mountTableEntry{}, false, statErr
	}
	if statErr == nil {
		return mountTableEntry{}, false, nil
	}
	if !errors.Is(statErr, syscall.ENOTCONN) {
		// A FUSE mount whose server is alive but refuses stat (for
		// example a permission error from default_permissions) is not
		// stale. Only the broken-transport errno proves the server died.
		return mountTableEntry{}, false, nil
	}
	return top, true, nil
}

// probe runs the liveness stat on a mount point the table reports as FUSE.
// Nothing can interrupt a stat blocked on a wedged FUSE server, so the call
// runs in its own goroutine and only the wait is bounded, by ctx and by
// probeTimeout; on cancellation or timeout the goroutine is left to finish
// whenever the kernel lets it and a *mountProbeError is returned instead of
// a verdict.
func (i *staleMountInspector) probe(ctx context.Context, path string) error {
	timeout := i.probeTimeout
	if timeout <= 0 {
		timeout = staleProbeTimeout
	}
	result := make(chan error, 1)
	go func() { result <- i.stat(path) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return errors.WithStack(&mountProbeError{path: path, err: ctx.Err()})
	case <-timer.C:
		return errors.WithStack(&mountProbeError{
			path: path,
			err:  errors.Newf("no answer within %s, a live but unresponsive FUSE server may still hold the mount point", timeout),
		})
	}
}

// mountPointCandidates returns the spellings of mountPoint that may appear
// in the mounts table: the cleaned absolute path, the path with a symlinked
// final component followed (the kernel mounts on the target and records the
// target), and each of those with a symlinked parent directory resolved. The
// mounted directory itself is never stat'ed because stat on a dangling mount
// fails.
func mountPointCandidates(mountPoint string) (map[string]bool, error) {
	abs, err := filepath.Abs(mountPoint)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve FUSE mount point %q", mountPoint)
	}
	candidates := map[string]bool{}
	for _, path := range []string{abs, resolveMountPointSymlinks(abs)} {
		candidates[path] = true
		candidates[filepath.Join(canonicalParent(path), filepath.Base(path))] = true
	}
	return candidates, nil
}

// resolveMountPointSymlinks follows a symlink used as the mount point itself
// using only Readlink. Readlink on a non-symlink fails with EINVAL from the
// dentry type alone, so unlike Lstat it never sends a getattr through a
// FUSE mount covering the path: a dead mount cannot fail it and a live but
// wedged server cannot block it (that case is left to the bounded probe).
// A relative link target is interpreted from the canonical parent
// directory, as the kernel's pathname resolution does, so a link under a
// symlinked parent lands where the kernel mounted. It returns path unchanged
// when it is not a symlink or cannot be read.
func resolveMountPointSymlinks(path string) string {
	for hop := 0; hop < maxMountPointSymlinkHops; hop++ {
		target, err := os.Readlink(path)
		if err != nil {
			return path
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(canonicalParent(path), target)
		}
		path = filepath.Clean(target)
	}
	return path
}

// canonicalParent resolves symlinks in the directory holding path without
// ever stat'ing path itself (which may be a dead mount). It falls back to the
// lexical parent when resolution fails.
func canonicalParent(path string) string {
	parent := filepath.Dir(path)
	if resolved, err := filepath.EvalSymlinks(parent); err == nil {
		return resolved
	}
	return parent
}

func isFUSEType(fsType string) bool {
	return fsType == fuseType || strings.HasPrefix(fsType, fuseTypePrefix)
}

func readMountTable(path string) ([]mountTableEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(&mountTableUnavailableError{path: path, err: err})
	}
	defer file.Close()
	var entries []mountTableEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < mountTableMinFields {
			continue
		}
		entries = append(entries, mountTableEntry{
			mountPoint: unescapeMountTableField(fields[1]),
			fsType:     fields[2],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "read mount table %s", path)
	}
	return entries, nil
}

// unescapeMountTableField decodes the `\ooo` octal escapes the kernel uses
// for space, tab, newline, and backslash in mount table columns.
func unescapeMountTableField(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var out strings.Builder
	for index := 0; index < len(field); index++ {
		if field[index] != '\\' || index+mountTableOctalDigits >= len(field) {
			out.WriteByte(field[index])
			continue
		}
		code, err := strconv.ParseUint(field[index+1:index+1+mountTableOctalDigits], mountTableOctalBase, mountTableOctalBits)
		if err != nil {
			out.WriteByte(field[index])
			continue
		}
		out.WriteByte(byte(code))
		index += mountTableOctalDigits
	}
	return out.String()
}

// mountHelperFallbackDir mirrors go-fuse's helper lookup: when PATH does not
// hold fusermount3/fusermount, /bin is tried, so a service started with a
// restricted PATH detaches with the same helper go-fuse mounted with. A
// variable so tests can point it at a directory they control.
var mountHelperFallbackDir = "/bin"

// lookupMountHelper finds a fusermount helper the way go-fuse does: PATH
// first, then mountHelperFallbackDir.
func lookupMountHelper(name string) (string, error) {
	if path, err := exec.LookPath(name); err == nil {
		return path, nil
	}
	path, err := exec.LookPath(filepath.Join(mountHelperFallbackDir, name))
	if err != nil {
		return "", errors.Wrapf(err, "locate mount helper %s", name)
	}
	return path, nil
}

// detachFUSEMount lazily detaches the FUSE mount at mountPoint through the
// same helpers go-fuse's own unmount uses: the suid fusermount3 (or legacy
// fusermount) binary with `-u -z`, which works for unprivileged users, and
// otherwise a direct lazy unmount(2), the equivalent of `umount -l`, which
// needs CAP_SYS_ADMIN (lazyUnmount, Linux only). It returns a description of
// the mechanism used. A cancelled ctx is reported as the cancellation, not
// as the EPERM the privileged fallback would produce for an unprivileged
// server, so a shutdown requested mid-detach still exits cleanly.
func detachFUSEMount(ctx context.Context, mountPoint string) (string, error) {
	var helperErr error
	for _, helper := range []string{"fusermount3", "fusermount"} {
		bin, err := lookupMountHelper(helper)
		if err != nil {
			continue
		}
		cmdCtx, cancel := context.WithTimeout(ctx, detachCommandTimeout)
		//nolint:gosec // bin comes from LookPath over two fixed helper names and mountPoint is the operator's configured path.
		cmd := exec.CommandContext(cmdCtx, bin, "-u", "-z", mountPoint)
		output, err := cmd.CombinedOutput()
		cancel()
		if err == nil {
			return helper + " -u -z", nil
		}
		helperErr = errors.Wrapf(err, "%s -u -z %s: %s", helper, mountPoint, strings.TrimSpace(string(output)))
	}
	if err := ctx.Err(); err != nil {
		if helperErr != nil {
			return "", errors.Wrapf(err, "detach cancelled after %s", helperErr.Error())
		}
		return "", errors.Wrap(err, "detach cancelled")
	}
	if err := lazyUnmount(mountPoint); err != nil {
		if helperErr != nil {
			return "", errors.Wrapf(err, "%s %s after %s", lazyUnmountMethod, mountPoint, helperErr.Error())
		}
		return "", errors.Wrapf(err, "%s %s", lazyUnmountMethod, mountPoint)
	}
	return lazyUnmountMethod, nil
}

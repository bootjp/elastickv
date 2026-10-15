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
)

// mountTableEntry is one row of /proc/self/mounts.
type mountTableEntry struct {
	mountPoint string
	fsType     string
}

// staleMountInspector locates dangling FUSE mounts at a path. The fields are
// swapped in tests so the decision logic can be exercised without a kernel
// mount; production code uses defaultStaleMountInspector.
type staleMountInspector struct {
	mountTable string
	stat       func(string) error
}

func defaultStaleMountInspector() *staleMountInspector {
	return &staleMountInspector{mountTable: mountTablePath, stat: statMountPoint}
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
	for detached < maxStaleMountDetaches {
		entry, stale, err := i.staleFUSEMount(mountPoint)
		if err != nil {
			return detached, err
		}
		if !stale {
			return detached, nil
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
	return detached, errors.Errorf("more than %d stale FUSE mounts at %s", maxStaleMountDetaches, mountPoint)
}

// staleFUSEMount reports whether the topmost mount at mountPoint is a FUSE
// mount whose server is gone. The topmost entry decides: a non-FUSE
// filesystem mounted over a dead FUSE mount is not touched either.
func (i *staleMountInspector) staleFUSEMount(mountPoint string) (mountTableEntry, bool, error) {
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
	statErr := i.stat(top.mountPoint)
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

// mountPointCandidates returns the spellings of mountPoint that may appear
// in the mounts table: the cleaned absolute path and, when the parent
// directory resolves differently through symlinks, the resolved one. The
// mount point itself is never resolved because stat on a dangling mount
// fails.
func mountPointCandidates(mountPoint string) (map[string]bool, error) {
	abs, err := filepath.Abs(mountPoint)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve FUSE mount point %q", mountPoint)
	}
	candidates := map[string]bool{abs: true}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err == nil {
		candidates[filepath.Join(parent, filepath.Base(abs))] = true
	}
	return candidates, nil
}

func isFUSEType(fsType string) bool {
	return fsType == fuseType || strings.HasPrefix(fsType, fuseTypePrefix)
}

func readMountTable(path string) ([]mountTableEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "open mount table %s", path)
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

// detachFUSEMount lazily detaches the FUSE mount at mountPoint through the
// same helpers go-fuse's own unmount uses: the suid fusermount3 (or legacy
// fusermount) binary with `-u -z`, which works for unprivileged users, and
// otherwise a direct lazy unmount(2), the equivalent of `umount -l`, which
// needs CAP_SYS_ADMIN. It returns a description of the mechanism used.
func detachFUSEMount(ctx context.Context, mountPoint string) (string, error) {
	var helperErr error
	for _, helper := range []string{"fusermount3", "fusermount"} {
		bin, err := exec.LookPath(helper)
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
	if err := syscall.Unmount(mountPoint, syscall.MNT_DETACH); err != nil {
		if helperErr != nil {
			return "", errors.Wrapf(err, "umount(MNT_DETACH) %s after %s", mountPoint, helperErr.Error())
		}
		return "", errors.Wrapf(err, "umount(MNT_DETACH) %s", mountPoint)
	}
	return "umount(MNT_DETACH)", nil
}

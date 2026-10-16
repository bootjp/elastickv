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
	// fuseTypePrefix starts the type go-fuse mounts report in the mounts
	// table (`fuse.<Name>`); fuseTypeFor builds the full type. Only a
	// mount of this server's own type is ever detached: a bare `fuse` or a
	// foreign `fuse.<other>` mount at the path belongs to someone else.
	fuseTypePrefix = "fuse."
	// detachCommandTimeout bounds one fusermount invocation. A dangling
	// mount does not need the server, so detaching it never blocks on I/O,
	// but a hung suid helper must not stall startup forever.
	detachCommandTimeout = 10 * time.Second
	// detachHelperBudgetShare is the fraction (1/n) of the caller's remaining
	// deadline one helper invocation may use in total, including the
	// WaitDelay below, so a hung helper cannot spend the whole budget and
	// starve the privileged unmount(2) fallback.
	detachHelperBudgetShare = 2
	// detachHelperWaitDelay bounds how long a killed helper's output pipes
	// are waited for (exec.Cmd.WaitDelay). It is reserved out of the
	// helper's share: os/exec waits for the pipes after the context has
	// killed the helper, on top of the context timeout.
	detachHelperWaitDelay = 500 * time.Millisecond
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

// mountPointResolveTimeout bounds the symlink resolution of the mount
// point's parent directories, which stats each path component and can
// block when a parent sits on an unresponsive FUSE or network filesystem.
// A variable so tests can shorten it.
var mountPointResolveTimeout = staleProbeTimeout

// mountProbeError reports that an inspection of the mount point could not
// reach a verdict: the liveness stat on a FUSE mount, or the resolution of
// the mount point's parent directories, was cancelled (shutdown requested)
// or timed out (a live but unresponsive FUSE server still holds the mount
// point, or a parent directory sits on an unresponsive filesystem). op names
// the step that gave up.
type mountProbeError struct {
	op   string
	path string
	err  error
}

const (
	probeOpStat    = "stat FUSE mount at"
	probeOpResolve = "resolve FUSE mount point"
)

func (e *mountProbeError) Error() string {
	op := e.op
	if op == "" {
		op = probeOpStat
	}
	return op + " " + e.path + ": " + e.err.Error()
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
	// fsType is the mounts-table type of the mounts this server makes;
	// only a dead mount of that type is detached. Empty means the default
	// mount name's type.
	fsType string
}

func defaultStaleMountInspector(mountName string) *staleMountInspector {
	return &staleMountInspector{
		mountTable:   mountTablePath,
		stat:         statMountPoint,
		probeTimeout: staleProbeTimeout,
		fsType:       fuseTypeFor(mountName),
	}
}

// expectedType is the mounts-table type a mount must have to be ours.
func (i *staleMountInspector) expectedType() string {
	if i.fsType == "" {
		return fuseTypeFor(filesystemName)
	}
	return i.fsType
}

func statMountPoint(path string) error {
	var st syscall.Stat_t
	return errors.WithStack(syscall.Stat(path, &st))
}

// detachStaleMount removes a dangling FUSE mount left at mountPoint by a
// server that died without unmounting (SIGKILL, a crash, or a stop before
// the process installed signal handling). mountName is the Name (subtype)
// this server mounts with, so only a mount of the same `fuse.<Name>` type
// is a candidate. It reports how many mounts were detached. A path that holds
// no mount, a live mount (its server still answers stat), or a filesystem
// of any other type, FUSE or not, is left untouched: the mount table
// decides the type, never the error from stat alone.
func detachStaleMount(ctx context.Context, mountPoint, mountName string, logger *slog.Logger) (int, error) {
	inspector := defaultStaleMountInspector(mountName)
	return inspector.detachAll(ctx, mountPoint, logger, func(ctx context.Context, path string) (string, error) {
		return detachFUSEMount(ctx, path, inspector.fsType)
	})
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
		if errors.Is(err, errNoFUSEMount) {
			// The mount went away between the inspection and the detach
			// (another process, or the kernel finishing an unmount);
			// inspect again rather than count or report it.
			continue
		}
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

// topmostMount returns the last mounts-table entry for mountPoint (the one
// the kernel currently exposes there), read from tablePath. found is false
// when nothing is mounted at the path. Resolving the mount point's
// spellings is bounded by ctx and mountPointResolveTimeout; when it cannot
// finish the error is a *mountProbeError.
func topmostMount(ctx context.Context, tablePath, mountPoint string) (top mountTableEntry, found bool, err error) {
	candidates, err := resolveMountPointCandidates(ctx, mountPoint)
	if err != nil {
		return mountTableEntry{}, false, err
	}
	entries, err := readMountTable(tablePath)
	if err != nil {
		return mountTableEntry{}, false, err
	}
	for _, entry := range entries {
		if candidates[entry.mountPoint] {
			top = entry
			found = true
		}
	}
	return top, found, nil
}

// errNoFUSEMount reports that the path currently holds no FUSE mount of this
// server's type: either nothing is mounted there or the topmost filesystem
// is of another type, FUSE or not. A detach stops at that point so it can
// never remove a foreign filesystem.
var errNoFUSEMount = errors.New("no FUSE mount of this server's type at the path")

// detachMountTable is the mounts table detachFUSEMount checks before it
// touches anything. A variable so tests can point it at a table they wrote.
var detachMountTable = mountTablePath

// requireFUSEMount fails unless the topmost mount at mountPoint is a FUSE
// mount of type fsType: errNoFUSEMount when the path holds nothing or a
// filesystem of any other type, a wrapped *mountTableUnavailableError when
// that cannot be verified.
func requireFUSEMount(ctx context.Context, mountPoint, fsType string) error {
	top, found, err := topmostMount(ctx, detachMountTable, mountPoint)
	if err != nil {
		return errors.Wrapf(err, "verify FUSE mount at %s", mountPoint)
	}
	if !found || top.fsType != fsType {
		return errors.Wrapf(errNoFUSEMount, "%s", mountPoint)
	}
	return nil
}

// staleFUSEMount reports whether the topmost mount at mountPoint is a FUSE
// mount of this server's type whose server is gone. The topmost entry
// decides: any other filesystem mounted over a dead mount is not touched
// either. The liveness stat is bounded by ctx and probeTimeout; when it
// cannot reach a verdict the error is a *mountProbeError.
func (i *staleMountInspector) staleFUSEMount(ctx context.Context, mountPoint string) (mountTableEntry, bool, error) {
	top, found, err := topmostMount(ctx, i.mountTable, mountPoint)
	if err != nil {
		return mountTableEntry{}, false, err
	}
	if !found || top.fsType != i.expectedType() {
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
		return errors.WithStack(&mountProbeError{op: probeOpStat, path: path, err: ctx.Err()})
	case <-timer.C:
		return errors.WithStack(&mountProbeError{
			op:   probeOpStat,
			path: path,
			err:  errors.Newf("no answer within %s, a live but unresponsive FUSE server may still hold the mount point", timeout),
		})
	}
}

// mountPointCandidatesFn resolves a mount point's spellings; a variable so
// tests can stand in a resolution that blocks, as it does when a parent
// directory sits on an unresponsive filesystem.
var mountPointCandidatesFn = mountPointCandidates

// resolveMountPointCandidates runs mountPointCandidates with a bound. The
// parent canonicalization stats each path component, and nothing can
// interrupt a stat blocked on a wedged FUSE or network filesystem, so the
// call runs in its own goroutine and only the wait is bounded, by ctx and
// by mountPointResolveTimeout; on cancellation or timeout the goroutine is
// left to finish whenever the kernel lets it and a *mountProbeError is
// returned instead of a verdict.
func resolveMountPointCandidates(ctx context.Context, mountPoint string) (map[string]bool, error) {
	type resolution struct {
		candidates map[string]bool
		err        error
	}
	results := make(chan resolution, 1)
	resolve := mountPointCandidatesFn
	go func() {
		candidates, err := resolve(mountPoint)
		results <- resolution{candidates: candidates, err: err}
	}()
	timer := time.NewTimer(mountPointResolveTimeout)
	defer timer.Stop()
	select {
	case res := <-results:
		return res.candidates, res.err
	case <-ctx.Done():
		return nil, errors.WithStack(&mountProbeError{op: probeOpResolve, path: mountPoint, err: ctx.Err()})
	case <-timer.C:
		return nil, errors.WithStack(&mountProbeError{
			op:   probeOpResolve,
			path: mountPoint,
			err:  errors.Newf("no answer within %s, a parent directory may sit on an unresponsive filesystem", mountPointResolveTimeout),
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

// fuseTypeFor returns the mounts-table type go-fuse records for a mount
// made with MountOptions.Name: go-fuse passes Name as the subtype
// (`fuse.<Name>`), while FsName only fills the source column.
func fuseTypeFor(mountName string) string {
	return fuseTypePrefix + mountName
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

// lookupMountHelperFn locates a helper; a variable so tests can stand in a
// lookup that blocks, as exec.LookPath does when a PATH directory sits on an
// unresponsive filesystem.
var lookupMountHelperFn = lookupMountHelper

// lookupMountHelperContext runs the helper lookup under the same share of
// ctx's deadline a helper invocation gets. exec.LookPath stats each PATH
// directory and nothing can interrupt a stat blocked on a wedged FUSE or
// network filesystem, so the lookup runs in its own goroutine and only the
// wait is bounded; a lookup that does not answer in time counts as the
// helper not being found, which keeps the kernel fallback reachable.
func lookupMountHelperContext(ctx context.Context, name string) (string, error) {
	type lookupResult struct {
		path string
		err  error
	}
	results := make(chan lookupResult, 1)
	lookup := lookupMountHelperFn
	go func() {
		path, err := lookup(name)
		results <- lookupResult{path: path, err: err}
	}()
	timer := time.NewTimer(detachHelperTimeout(ctx))
	defer timer.Stop()
	select {
	case res := <-results:
		return res.path, res.err
	case <-ctx.Done():
		return "", errors.Wrapf(ctx.Err(), "locate mount helper %s", name)
	case <-timer.C:
		return "", errors.Errorf("locate mount helper %s: no answer within its share of the detach budget", name)
	}
}

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

// detachHelperTimeout bounds one helper invocation's context:
// detachCommandTimeout, or a share of whatever is left of the caller's
// deadline, so that when the helper hangs the privileged unmount(2)
// fallback still runs inside the caller's budget instead of being skipped
// behind an expired context. The share covers the whole invocation: the
// pipe wait that os/exec adds after killing the helper (WaitDelay) is
// taken out of it, so a helper that leaves a child holding its output
// pipes still returns within the share. A share no larger than that wait
// yields a zero or negative timeout, which makes exec skip the helper
// without starting it.
func detachHelperTimeout(ctx context.Context) time.Duration {
	timeout := detachCommandTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if share := time.Until(deadline) / detachHelperBudgetShare; share < timeout {
			timeout = share
		}
	}
	return timeout - detachHelperWaitDelay
}

// detachFUSEMount lazily detaches the FUSE mount of type fsType at
// mountPoint through the same helpers go-fuse's own unmount uses: the suid fusermount3 (or legacy
// fusermount) binary with `-u -z`, which works for unprivileged users, and
// otherwise a direct lazy unmount(2), the equivalent of `umount -l`, which
// needs CAP_SYS_ADMIN (lazyUnmount, Linux only). It returns a description of
// the mechanism used. A cancelled ctx is reported as the cancellation, not
// as the EPERM the privileged fallback would produce for an unprivileged
// server, so a shutdown requested mid-detach still exits cleanly.
//
// It only ever acts on a mount of this server's own type. The mounts table
// is checked before the helpers run and again right before the kernel
// fallback: the helpers refuse a non-FUSE mount on their own but accept any
// FUSE filesystem, unmount(2) accepts anything, and the path may no longer
// hold our mount by then (go-fuse's own Unmount can have removed it while
// still blocked on its event loop; another process may have detached it).
// Such a path yields errNoFUSEMount and nothing is touched, so a filesystem
// mounted underneath the mount point, another FUSE filesystem included, is
// never detached.
func detachFUSEMount(ctx context.Context, mountPoint, fsType string) (string, error) {
	if err := requireFUSEMount(ctx, mountPoint, fsType); err != nil {
		return "", err
	}
	method, helperErr := detachWithHelpers(ctx, mountPoint, fsType)
	if method != "" {
		return method, nil
	}
	if errors.Is(helperErr, errNoFUSEMount) {
		return "", helperErr
	}
	return detachWithKernel(ctx, mountPoint, fsType, helperErr)
}

// detachMountHelpers are the helpers go-fuse's own unmount uses, in order.
var detachMountHelpers = []string{"fusermount3", "fusermount"}

// detachWithHelpers runs `fusermount3 -u -z`, then `fusermount -u -z`, each
// under its share of the deadline, as is the lookup of each helper. It
// returns the mechanism on success;
// otherwise the last helper's failure, or nil when no helper was found.
// Before a helper runs after a failed one, the mounts table is checked
// again: the failed helper may still have detached our mount (it reports
// an error when, for example, a descendant keeps its output pipe open past
// WaitDelay), and the next helper accepts any FUSE filesystem, so it must
// not run against one that is now exposed. Such a path yields
// errNoFUSEMount with the earlier failure kept in the chain.
func detachWithHelpers(ctx context.Context, mountPoint, fsType string) (string, error) {
	var helperErr error
	for _, helper := range detachMountHelpers {
		bin, err := lookupMountHelperContext(ctx, helper)
		if err != nil {
			continue
		}
		if helperErr != nil {
			if err := requireFUSEMount(ctx, mountPoint, fsType); err != nil {
				return "", afterHelper(err, helperErr)
			}
		}
		if err := runDetachHelper(ctx, bin, mountPoint); err != nil {
			helperErr = errors.Wrapf(err, "%s -u -z %s", helper, mountPoint)
			continue
		}
		return helper + " -u -z", nil
	}
	return "", helperErr
}

// runDetachHelper runs one helper binary with `-u -z mountPoint` under its
// share of ctx's deadline, reporting its output alongside a failure.
func runDetachHelper(ctx context.Context, bin, mountPoint string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, detachHelperTimeout(ctx))
	defer cancel()
	//nolint:gosec // bin comes from LookPath over two fixed helper names and mountPoint is the operator's configured path.
	cmd := exec.CommandContext(cmdCtx, bin, "-u", "-z", mountPoint)
	// After the context kills the helper, do not wait on its output pipes
	// for longer than this: a child the helper left behind could otherwise
	// hold them open past the budget.
	cmd.WaitDelay = detachHelperWaitDelay
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrap(err, strings.TrimSpace(string(output)))
	}
	return nil
}

// detachWithKernel is the privileged fallback after the helpers: it stops
// on a cancelled context, re-verifies that the path still holds a FUSE
// mount of type fsType, and only then calls unmount(2) with MNT_DETACH.
// helperErr, when set, is kept in the error chain so the log shows why the
// helpers did not do the job.
func detachWithKernel(ctx context.Context, mountPoint, fsType string, helperErr error) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", afterHelper(errors.Wrap(err, "detach cancelled"), helperErr)
	}
	// Re-check right before the privileged fallback: a helper may have
	// taken long enough for the mount to disappear, and unmount(2) would
	// otherwise detach whatever filesystem is now exposed at the path.
	if err := requireFUSEMount(ctx, mountPoint, fsType); err != nil {
		return "", afterHelper(err, helperErr)
	}
	if err := lazyUnmount(mountPoint); err != nil {
		return "", afterHelper(errors.Wrapf(err, "%s %s", lazyUnmountMethod, mountPoint), helperErr)
	}
	return lazyUnmountMethod, nil
}

// afterHelper records the helpers' failure alongside err.
func afterHelper(err, helperErr error) error {
	if helperErr == nil {
		return err
	}
	return errors.Wrapf(err, "after %s", helperErr.Error())
}

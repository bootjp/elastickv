//go:build unix

package backup

import (
	"errors"
	"os"
	"syscall"

	cockroachdberr "github.com/cockroachdb/errors"
)

// openSidecarFile opens path for write while refusing symlink,
// hard-link, and non-regular-file (FIFO / socket / device) clobber
// attacks.
//
//   - O_NOFOLLOW makes the kernel return ELOOP atomically if the path
//     is a symbolic link — closing the TOCTOU race a separate
//     Lstat-then-Create pattern would have.
//   - O_NONBLOCK guarantees the open does not hang on a pre-existing
//     FIFO that has no reader (POSIX: O_WRONLY|O_NONBLOCK on a
//     reader-less FIFO returns ENXIO immediately). Without this, a
//     stale or adversarial mkfifo at strings_ttl.jsonl would block
//     the first TTL write indefinitely; the symlink and hard-link
//     guards do not catch this case (`mkfifo` produces nlink=1 and
//     is not a symlink). Codex P2 round 11.
//   - To also refuse hard links to files outside the dump tree, we
//     open WITHOUT O_TRUNC, fstat() the descriptor to check the
//     link count, and only call Truncate(0) if Nlink == 1 AND the
//     file is a regular file. An adversary that pre-created
//     strings_ttl.jsonl as a hard link to /etc/passwd (or any other
//     writable file outside the dump tree) would otherwise see the
//     inode truncated on openSidecarFile despite the symlink guard.
//     Codex P2 round 9.
//
// The Windows build (open_nofollow_windows.go) keeps the simpler
// Lstat-then-OpenFile guard because Windows's
// SeCreateSymbolicLinkPrivilege already raises the bar for the
// equivalent attack and Windows has no FIFO concept.
func openSidecarFile(path string) (*os.File, error) {
	// Note: NO O_TRUNC here — we truncate after the link-count check.
	const flag = os.O_WRONLY | os.O_CREATE | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	f, err := os.OpenFile(path, flag, 0o600) //nolint:gosec,mnd // path is composed from output-root + fixed file name; 0600 is the standard owner-only mode
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, cockroachdberr.WithStack(cockroachdberr.Wrapf(err,
				"backup: refusing to overwrite symlink at %s", path))
		}
		// ENXIO surfaces when the path is a FIFO with no reader;
		// because O_NONBLOCK turned the would-be hang into an
		// immediate error, surface it with a stable message
		// rather than letting the bare syscall errno leak out.
		if errors.Is(err, syscall.ENXIO) {
			return nil, cockroachdberr.WithStack(cockroachdberr.Wrapf(err,
				"backup: refusing to write to FIFO at %s", path))
		}
		return nil, cockroachdberr.WithStack(err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, cockroachdberr.WithStack(err)
	}
	// Refuse non-regular files. A reader-attached FIFO (where the
	// O_NONBLOCK open succeeded), a socket, or a character/block
	// device would all otherwise be silently written into and
	// `f.Truncate(0)` would be a no-op or fail in a confusing way.
	// Codex P2 round 11.
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, cockroachdberr.WithStack(cockroachdberr.Newf(
			"backup: refusing to write to non-regular file at %s (mode=%s)", path, info.Mode()))
	}
	if sysStat, ok := info.Sys().(*syscall.Stat_t); ok && sysStat.Nlink > 1 {
		_ = f.Close()
		return nil, cockroachdberr.WithStack(cockroachdberr.Newf(
			"backup: refusing to overwrite hard-linked file at %s (nlink=%d)", path, sysStat.Nlink))
	}
	if err := f.Truncate(0); err != nil {
		_ = f.Close()
		return nil, cockroachdberr.WithStack(err)
	}
	return f, nil
}

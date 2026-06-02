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
// refuseHardLink rejects a file whose inode carries more than one
// link. A hard link can name an inode living OUTSIDE the dump subtree
// while still presenting as a regular file — so both the IsRegular
// check and the os.Root symlink-escape guard pass — letting a crafted
// dump pull external bytes into the snapshot. This is the read-side
// analogue of openSidecarFile's write-side Nlink defense (codex P2 on
// PR #828). info must come from an Lstat/Fstat of the target.
func refuseHardLink(info os.FileInfo, path string) error {
	sysStat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if sysStat.Nlink > 1 {
		return cockroachdberr.Wrapf(ErrRedisEncodeHardLink, "%s (nlink=%d)", path, sysStat.Nlink)
	}
	return nil
}

func openSidecarFile(path string) (*os.File, error) {
	// Note: NO O_TRUNC here — we truncate after the link-count check.
	const flag = os.O_WRONLY | os.O_CREATE | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	f, err := os.OpenFile(path, flag, sidecarFileMode) //nolint:gosec // path is composed from output-root + fixed file name; sidecarFileMode is the standard owner-only mode used here and by the post-Truncate Chmod below
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
	// Enforce 0o600 on the descriptor. The flags-arg mode (0o600
	// above) is applied by the kernel ONLY on file creation; if
	// path already existed, its pre-existing perms are kept. An
	// older encoder writing 0o644 would otherwise leave the
	// sidecar's source path / cluster ID / SHA256 world-readable
	// after re-encode (claude / codex P2 v31 observation on PR #904).
	if err := f.Chmod(sidecarFileMode); err != nil {
		_ = f.Close()
		return nil, cockroachdberr.WithStack(err)
	}
	return f, nil
}

// sidecarFileMode is the file mode openSidecarFile enforces — owner
// read/write only. Pulled into a named const so the truncate-then-
// chmod step here matches the OpenFile flag-arg mode above; a future
// edit that widens one must touch both.
const sidecarFileMode os.FileMode = 0o600

//go:build !unix && !windows

package backup

import (
	"os"

	cockroachdberr "github.com/cockroachdb/errors"
)

// openSidecarFile is the fallback for non-unix/non-windows targets
// (js, wasip1, plan9). syscall.O_NOFOLLOW and the unix nlink-check
// path are unavailable; we keep a Lstat-then-OpenFile guard to at
// least refuse pre-existing symlinks. The remaining TOCTOU window
// is acceptable here because dump tooling on those targets is
// offline / sandboxed and the threat model that motivated the unix
// hardening (a local adversary swapping the path between syscalls)
// does not apply. Codex P2 round 10.
// refuseHardLink is a no-op on this platform: syscall.Stat_t.Nlink is
// unavailable, and the threat model that motivates the unix check
// (a local adversary hard-linking external inodes into the dump tree)
// does not apply to the offline/sandboxed tooling these targets run.
// Matches openSidecarFile's platform-specific posture. Codex P2 on
// PR #828.
func refuseHardLink(_ os.FileInfo, _ string) error { return nil }

func openSidecarFile(path string) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, cockroachdberr.WithStack(cockroachdberr.Newf(
			"backup: refusing to overwrite symlink at %s", path))
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600) //nolint:gosec,mnd // path is composed from output-root + fixed file name; 0600 is the standard owner-only mode
	if err != nil {
		return nil, cockroachdberr.WithStack(err)
	}
	return f, nil
}

//go:build windows

package backup

import (
	"os"

	cockroachdberr "github.com/cockroachdb/errors"
)

// openSidecarFile is the Windows counterpart to the unix
// open_nofollow_unix.go variant. syscall.O_NOFOLLOW is not defined on
// Windows and the platform's symlink/permission model is materially
// different (junction points, ACLs, SeCreateSymbolicLinkPrivilege),
// so we keep the simpler Lstat-then-OpenFile guard. The remaining
// TOCTOU window is acceptable here because mounting a successful
// attack on the dump tree on Windows already requires the attacker to
// hold write access to the output directory plus the symlink-create
// privilege, which is a much higher bar than the unix case.
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

//go:build unix

package backup

import (
	"errors"
	"os"
	"syscall"

	cockroachdberr "github.com/cockroachdb/errors"
)

// openSidecarFile opens path for write with truncate+create semantics
// and atomically refuses symlinks. The unix build leans on
// syscall.O_NOFOLLOW so the kernel returns ELOOP if path is a
// symlink, eliminating the TOCTOU race a separate Lstat-then-Create
// pattern would have. The Windows build (open_nofollow_windows.go)
// implements this with the best alternative available there.
func openSidecarFile(path string) (*os.File, error) {
	const flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | syscall.O_NOFOLLOW
	f, err := os.OpenFile(path, flag, 0o600) //nolint:gosec,mnd // path is composed from output-root + fixed file name; 0600 is the standard owner-only mode
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, cockroachdberr.WithStack(cockroachdberr.Wrapf(err,
				"backup: refusing to overwrite symlink at %s", path))
		}
		return nil, cockroachdberr.WithStack(err)
	}
	return f, nil
}

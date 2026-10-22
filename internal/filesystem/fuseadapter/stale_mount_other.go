//go:build !linux

package fuseadapter

import "github.com/cockroachdb/errors"

// lazyUnmountMethod names the privileged fallback in detach logs.
const lazyUnmountMethod = "lazy unmount"

// lazyUnmount has no portable implementation: MNT_DETACH is Linux-only.
// Stale-mount detection itself already depends on /proc/self/mounts and is
// skipped where that table is missing, so on other platforms the fusermount
// helpers are the only detach mechanism and this fallback reports that.
func lazyUnmount(string) error {
	return errors.New("lazy unmount is only implemented on Linux")
}

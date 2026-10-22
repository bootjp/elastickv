//go:build linux

package fuseadapter

import (
	"syscall"

	"github.com/cockroachdb/errors"
)

// lazyUnmountMethod names the privileged fallback in detach logs.
const lazyUnmountMethod = "umount(MNT_DETACH)"

// lazyUnmount is the privileged fallback behind fusermount: a lazy
// unmount(2), the equivalent of `umount -l`, which needs CAP_SYS_ADMIN.
// MNT_DETACH is Linux-only, which is why this lives behind a build tag.
func lazyUnmount(mountPoint string) error {
	return errors.WithStack(syscall.Unmount(mountPoint, syscall.MNT_DETACH))
}

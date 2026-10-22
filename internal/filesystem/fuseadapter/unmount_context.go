package fuseadapter

import (
	"context"

	"github.com/cockroachdb/errors"
)

// UnmountContext bounds and reaps the path-based helper before returning.
// Unlike go-fuse's Unmount, cancellation never leaves a helper that could
// later unmount a new filesystem at the same path.
func (s *Server) UnmountContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if s.adapter != nil {
		s.adapter.Close()
	}
	if s.inner == nil || s.isReleased() || s.mountPoint == "" {
		return nil
	}
	return unmountFUSEMount(ctx, s.mountPoint, s.fsType)
}

func unmountFUSEMount(ctx context.Context, mountPoint, fsType string) error {
	if err := verifyOrdinaryUnmount(ctx, mountPoint, fsType); err != nil {
		if errors.Is(err, errNoFUSEMount) {
			return nil
		}
		return errors.WithStack(err)
	}
	var helperErr error
	for _, helper := range []string{"fusermount3", "fusermount", "umount"} {
		bin, err := lookupMountHelperContext(ctx, helper)
		if err != nil {
			helperErr = err
			continue
		}
		if err := verifyOrdinaryUnmount(ctx, mountPoint, fsType); err != nil {
			return errors.WithStack(err)
		}
		args := []string{mountPoint}
		if helper != "umount" {
			args = append([]string{"-u"}, args...)
		}
		if err := runMountHelper(ctx, bin, args); err != nil {
			helperErr = err
			continue
		}
		return nil
	}
	return errors.Wrap(helperErr, "ordinary FUSE unmount helper failed")
}

func verifyOrdinaryUnmount(ctx context.Context, mountPoint, fsType string) error {
	err := requireFUSEMount(ctx, mountPoint, fsType)
	var unavailable *mountTableUnavailableError
	if errors.As(err, &unavailable) {
		// Startup permits this platform/container configuration. Only ordinary
		// unmount is allowed without the table; lazy detach stays type-guarded.
		return nil
	}
	return errors.WithStack(err)
}

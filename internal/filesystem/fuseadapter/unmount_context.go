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
	if err := requireFUSEMount(ctx, mountPoint, fsType); err != nil {
		if errors.Is(err, errNoFUSEMount) {
			return nil
		}
		return errors.WithStack(err)
	}
	var helperErr error
	for _, helper := range detachMountHelpers {
		bin, err := lookupMountHelperContext(ctx, helper)
		if err != nil {
			helperErr = err
			continue
		}
		if err := requireFUSEMount(ctx, mountPoint, fsType); err != nil {
			return errors.WithStack(err)
		}
		if err := runUnmountHelper(ctx, bin, mountPoint, false); err != nil {
			helperErr = err
			continue
		}
		return nil
	}
	return errors.Wrap(helperErr, "ordinary FUSE unmount helper failed")
}

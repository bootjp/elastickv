//go:build !linux

package fuseadapter

import "github.com/hanwen/go-fuse/v2/fuse"

func callerInGroup(fuse.Caller, uint32) (bool, bool) {
	// No authenticated supplementary-group source is available on this platform.
	return false, false
}

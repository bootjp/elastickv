//go:build windows

package backup

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// isDiskFullError reports whether err is a Windows disk-full failure.
// We accept both ERROR_DISK_FULL (Win32 errno 112) and
// ERROR_HANDLE_DISK_FULL (39): the kernel returns 112 for write
// failures driven by free-space exhaustion and 39 for the legacy
// handle-level variant some FS drivers still surface. We also keep
// syscall.ENOSPC in the chain because Go's os package occasionally
// translates platform errors into the POSIX value before returning.
func isDiskFullError(err error) bool {
	switch {
	case errors.Is(err, windows.ERROR_DISK_FULL),
		errors.Is(err, windows.ERROR_HANDLE_DISK_FULL),
		errors.Is(err, syscall.ENOSPC):
		return true
	}
	return false
}

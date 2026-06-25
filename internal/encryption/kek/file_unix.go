//go:build !windows

package kek

import "syscall"

// kekOpenExtraFlags carries O_NONBLOCK on unix so a path that points
// at a FIFO with no writer (one of the DoS shapes the §5.1
// fail-closed-on-bad-KEK rule defends against) does not hang
// NewFileWrapper at the os.OpenFile call before the regular-file
// stat check can run. The non-blocking flag has no effect on
// regular files.
const kekOpenExtraFlags = syscall.O_NONBLOCK

//go:build !darwin

package fuse

import (
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

func pollHack(mountPoint string) error {
	var fd int
	err := handleEINTR(func() error {
		var openErr error
		fd, openErr = syscall.Open(filepath.Join(mountPoint, pollHackName), syscall.O_RDONLY, 0)
		return openErr
	})
	if err != nil {
		return err
	}
	pollData := []unix.PollFd{{
		Fd:     int32(fd),
		Events: unix.POLLIN | unix.POLLPRI | unix.POLLOUT,
	}}

	// Trigger _OP_POLL, so we can say ENOSYS. We don't care about
	// the return value.
	unix.Poll(pollData, 0)
	syscall.Close(fd)
	return nil
}

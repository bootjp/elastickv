//go:build linux || darwin || freebsd

package fuseadapter

import (
	"os/exec"
	"syscall"

	"github.com/cockroachdb/errors"
)

func isolateMountHelper(cmd *exec.Cmd) func() {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	kill := func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return errors.WithStack(err)
	}
	cmd.Cancel = kill
	return func() { _ = kill() }
}

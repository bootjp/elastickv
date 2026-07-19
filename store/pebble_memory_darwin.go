//go:build darwin

package store

import (
	"math"

	"golang.org/x/sys/unix"
)

func platformMemoryBudgetBytes() int64 {
	total, err := unix.SysctlUint64("hw.memsize")
	if err != nil || total > math.MaxInt64 {
		return 0
	}
	return int64(total)
}

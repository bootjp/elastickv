//go:build linux

package store

import (
	"math"
	"os"

	"golang.org/x/sys/unix"
)

var rootCgroupMemoryLimitPaths = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

func platformMemoryBudgetBytes() int64 {
	return minPositiveMemoryBytes(systemMemoryBytes(), cgroupMemoryLimitBytes())
}

func systemMemoryBytes() int64 {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0
	}
	total := info.Totalram * uint64(info.Unit)
	if total > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(total)
}

func cgroupMemoryLimitBytes() int64 {
	paths := rootCgroupMemoryLimitPaths
	if procSelfCgroup, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		paths = append(cgroupMemoryLimitPaths(procSelfCgroup), paths...)
	}
	var limit int64
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		limit = minPositiveMemoryBytes(limit, parseMemoryLimitBytes(raw))
	}
	return limit
}

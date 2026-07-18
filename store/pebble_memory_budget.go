package store

import (
	"math"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

const cgroupFieldCount = 3

// effectiveMemoryBudgetBytes returns the smallest process or platform memory
// limit visible to the node. GOMEMLIMIT is included because production may
// deliberately give elastickv a smaller budget than its container or host.
func effectiveMemoryBudgetBytes() int64 {
	runtimeLimit := debug.SetMemoryLimit(-1)
	if runtimeLimit <= 0 || runtimeLimit == math.MaxInt64 {
		runtimeLimit = 0
	}
	return minPositiveMemoryBytes(runtimeLimit, platformMemoryBudgetBytes())
}

func minPositiveMemoryBytes(values ...int64) int64 {
	var minimum int64
	for _, value := range values {
		if value <= 0 || minimum != 0 && value >= minimum {
			continue
		}
		minimum = value
	}
	return minimum
}

func parseMemoryLimitBytes(raw []byte) int64 {
	value := strings.TrimSpace(string(raw))
	if value == "" || value == "max" {
		return 0
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed > math.MaxInt64 {
		return 0
	}
	return int64(parsed)
}

func cgroupMemoryLimitPaths(procSelfCgroup []byte) []string {
	var paths []string
	for _, line := range strings.Split(strings.TrimSpace(string(procSelfCgroup)), "\n") {
		fields := strings.SplitN(line, ":", cgroupFieldCount)
		if len(fields) != cgroupFieldCount {
			continue
		}
		relative := strings.TrimPrefix(filepath.Clean(fields[2]), string(filepath.Separator))
		if fields[0] == "0" && fields[1] == "" {
			paths = append(paths, filepath.Join("/sys/fs/cgroup", relative, "memory.max"))
			continue
		}
		for _, controller := range strings.Split(fields[1], ",") {
			if controller == "memory" {
				paths = append(paths, filepath.Join("/sys/fs/cgroup/memory", relative, "memory.limit_in_bytes"))
				break
			}
		}
	}
	return paths
}

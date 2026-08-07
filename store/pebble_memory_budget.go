package store

import (
	"math"
	"path/filepath"
	"strconv"
	"strings"
)

const cgroupFieldCount = 3

// effectiveMemoryBudgetBytes returns the hard platform memory capacity visible
// to the node. GOMEMLIMIT is a Go heap soft limit, not a total RSS budget, and
// Pebble's manually allocated block cache must not be sized as a fraction of it.
func effectiveMemoryBudgetBytes() int64 {
	return platformMemoryBudgetBytes()
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
		cleaned := filepath.Clean(string(filepath.Separator) + fields[2])
		relative := strings.TrimPrefix(cleaned, string(filepath.Separator))
		if fields[0] == "0" && fields[1] == "" {
			paths = append(paths, cgroupAncestorLimitPaths("/sys/fs/cgroup", relative, "memory.max")...)
			continue
		}
		for _, controller := range strings.Split(fields[1], ",") {
			if controller == "memory" {
				paths = append(paths, cgroupAncestorLimitPaths(
					"/sys/fs/cgroup/memory",
					relative,
					"memory.limit_in_bytes",
				)...)
				break
			}
		}
	}
	return paths
}

func cgroupAncestorLimitPaths(root, relative, filename string) []string {
	root = filepath.Clean(root)
	dir := filepath.Join(root, relative)
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil
	}

	var paths []string
	for {
		paths = append(paths, filepath.Join(dir, filename))
		if dir == root {
			return paths
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return paths
		}
		dir = parent
	}
}

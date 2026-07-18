//go:build !darwin && !linux

package store

func platformMemoryBudgetBytes() int64 {
	return 0
}

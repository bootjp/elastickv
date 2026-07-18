package kv

import "github.com/cockroachdb/pebble/v2"

type lsmBackpressureLimits struct {
	maxL0Files      int64
	maxL0Sublevels  int32
	maxLSMDebtBytes uint64
}

func lsmWriteBackpressured(snap *pebble.Metrics, limits lsmBackpressureLimits) bool {
	if snap == nil {
		return false
	}
	if limits.maxL0Sublevels > 0 && snap.Levels[0].Sublevels >= limits.maxL0Sublevels {
		return true
	}
	if !pebbleCompactionActive(snap) {
		return false
	}
	if limits.maxL0Files > 0 && snap.Levels[0].TablesCount >= limits.maxL0Files {
		return true
	}
	if limits.maxLSMDebtBytes > 0 && snap.Compact.EstimatedDebt >= limits.maxLSMDebtBytes {
		return true
	}
	return false
}

func pebbleCompactionActive(snap *pebble.Metrics) bool {
	return snap.Compact.NumInProgress > 0 || snap.Compact.InProgressBytes > 0
}

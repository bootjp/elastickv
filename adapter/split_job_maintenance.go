package adapter

import "github.com/bootjp/elastickv/distribution"

const (
	onlineSplitMaintenanceMaxVersions = uint32(256)
	onlineSplitMaintenanceMaxBytes    = uint64(512 * 1024)
)

// Keep each online maintenance proposal short enough for ordinary Raft data
// proposals to interleave. Scan bounds and oversized-row progress stay intact.
func splitMaintenanceMaxVersions(job distribution.SplitJob) uint32 {
	if job.OnlineCaptureVersion != 0 {
		return onlineSplitMaintenanceMaxVersions
	}
	return defaultSplitPromotionMaxVersions
}

func splitMaintenanceMaxBytes(job distribution.SplitJob) uint64 {
	if job.OnlineCaptureVersion != 0 {
		return onlineSplitMaintenanceMaxBytes
	}
	return defaultSplitPromotionMaxBytes
}

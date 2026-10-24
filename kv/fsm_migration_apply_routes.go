package kv

import (
	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
)

type migrationApplyRange struct{ start, end []byte }

func migrationApplyFootprint(req *pb.Request) []migrationApplyRange {
	ranges := make([]migrationApplyRange, 0, len(req.GetMutations())+len(req.GetReadKeys()))
	for _, mut := range req.GetMutations() {
		if mut == nil || isTxnInternalKey(mut.GetKey()) {
			continue
		}
		var start, end []byte
		if mut.GetOp() == pb.Op_DEL_PREFIX {
			start, end = routePrefixRange(mut.GetKey())
		} else {
			start, end = readinessRouteRangeForScan(mut.GetKey(), nextScanCursor(mut.GetKey()))
		}
		ranges = append(ranges, migrationApplyRange{start, end})
	}
	for _, key := range req.GetReadKeys() {
		if _, raw, staged := distribution.MigrationStagedDataKeyParts(key); staged {
			key = raw
		}
		if !isTxnInternalKey(key) {
			start, end := readinessRouteRangeForScan(key, nextScanCursor(key))
			ranges = append(ranges, migrationApplyRange{start, end})
		}
	}
	return ranges
}

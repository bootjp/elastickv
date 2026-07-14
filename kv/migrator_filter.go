package kv

import (
	"bytes"

	"github.com/bootjp/elastickv/internal/s3keys"
)

// RouteKeyFilter returns the migration export predicate for raw MVCC keys.
// rangeEnd nil or empty means +infinity, matching the route descriptor wire
// convention.
func RouteKeyFilter(rangeStart, rangeEnd []byte) func([]byte) bool {
	start := bytes.Clone(rangeStart)
	end := bytes.Clone(rangeEnd)
	return func(rawKey []byte) bool {
		if s3BucketAuxiliaryRouteInRange(rawKey, start, end) {
			return true
		}
		rkey := routeKey(rawKey)
		if bytes.Compare(rkey, start) < 0 {
			return false
		}
		if len(end) > 0 && bytes.Compare(rkey, end) >= 0 {
			return false
		}
		return true
	}
}

func s3BucketAuxiliaryRouteInRange(rawKey, routeStart, routeEnd []byte) bool {
	bucket, ok := s3keys.ParseBucketMetaKey(rawKey)
	if !ok {
		bucket, ok = s3keys.ParseBucketGenerationKey(rawKey)
	}
	if !ok {
		return false
	}
	bucketRouteStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	return migrationRouteRangesIntersect(routeStart, routeEnd, bucketRouteStart, prefixScanEnd(bucketRouteStart))
}

func migrationRouteRangesIntersect(aStart, aEnd, bStart, bEnd []byte) bool {
	if len(aEnd) > 0 && bytes.Compare(aEnd, bStart) <= 0 {
		return false
	}
	if len(bEnd) > 0 && bytes.Compare(bEnd, aStart) <= 0 {
		return false
	}
	return true
}

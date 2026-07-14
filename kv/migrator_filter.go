package kv

import (
	"bytes"

	"github.com/bootjp/elastickv/internal/s3keys"
)

// RouteKeyFilter returns the migration export predicate for raw MVCC keys.
// rangeEnd nil or empty means +infinity, matching the route descriptor wire
// convention.
func RouteKeyFilter(rangeStart, rangeEnd []byte) func([]byte) bool {
	return RouteKeyFilterForGroup(rangeStart, rangeEnd, 0, nil)
}

// RouteKeyFilterForGroup returns the migration export predicate for a source
// route and group. Partition-resolved keyspaces such as HT-FIFO SQS are matched
// by resolver group instead of the byte-range route key.
func RouteKeyFilterForGroup(rangeStart, rangeEnd []byte, sourceGroupID uint64, resolver PartitionResolver) func([]byte) bool {
	start := bytes.Clone(rangeStart)
	end := bytes.Clone(rangeEnd)
	return func(rawKey []byte) bool {
		if resolver != nil {
			if gid, ok := resolver.ResolveGroup(rawKey); ok {
				return gid == sourceGroupID
			}
			if resolver.RecognisesPartitionedKey(rawKey) {
				return false
			}
		}
		if s3BucketAuxiliaryRouteInRange(rawKey, start, end) {
			return true
		}
		rkey := routeKey(rawKey)
		return keyInMigrationRouteRange(rkey, start, end)
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
	if keyInMigrationRouteRange(rawKey, routeStart, routeEnd) {
		return true
	}
	bucketRouteStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	return migrationRouteRangesIntersect(routeStart, routeEnd, bucketRouteStart, prefixScanEnd(bucketRouteStart))
}

func keyInMigrationRouteRange(key, routeStart, routeEnd []byte) bool {
	if key == nil {
		return false
	}
	if bytes.Compare(key, routeStart) < 0 {
		return false
	}
	return len(routeEnd) == 0 || bytes.Compare(key, routeEnd) < 0
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

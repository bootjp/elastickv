package kv

import (
	"bytes"

	"github.com/bootjp/elastickv/distribution"
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
			// Transaction-family brackets carry the user key wrapped in
			// !txn|int| / !txn|cmt| / ... . The resolver only recognises bare
			// partitioned-family prefixes, so probing the wrapper would answer
			// "not mine" and drop the key onto routeKey's !sqs|route|global
			// collapse -- a different verdict from the one the embedded row
			// gets in its own data bracket, which is how a migration ends up
			// copying SQS rows while leaving their intents behind.
			probe := partitionResolverProbeKey(rawKey)
			if gid, ok := resolver.ResolveGroup(probe); ok {
				return gid == sourceGroupID
			}
			if resolver.RecognisesPartitionedKey(probe) {
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

// partitionResolverProbeKey returns the key a PartitionResolver should be asked
// about: the embedded user key for a transaction-internal wrapper, the key
// itself otherwise. Route ownership of a txn key is the ownership of the user
// key it locks, so both must be resolved the same way.
func partitionResolverProbeKey(rawKey []byte) []byte {
	if embedded, ok := txnRouteKey(rawKey); ok {
		return embedded
	}
	return rawKey
}

func s3BucketAuxiliaryRouteInRange(rawKey, routeStart, routeEnd []byte) bool {
	bucketRouteStart, bucketRouteEnd, ok := s3BucketAuxiliaryRouteRange(rawKey)
	if !ok {
		return false
	}
	if keyInMigrationRouteRange(rawKey, routeStart, routeEnd) {
		return true
	}
	return migrationRouteRangesIntersect(routeStart, routeEnd, bucketRouteStart, bucketRouteEnd)
}

func s3BucketAuxiliaryRouteRange(rawKey []byte) ([]byte, []byte, bool) {
	bucket, ok := s3keys.ParseBucketMetaKey(rawKey)
	if !ok {
		bucket, ok = s3keys.ParseBucketGenerationKey(rawKey)
	}
	if !ok {
		return nil, nil, false
	}
	bucketRouteStart := s3keys.RoutePrefixForBucketAnyGeneration(bucket)
	return bucketRouteStart, prefixScanEnd(bucketRouteStart), true
}

func s3BucketAuxiliaryOwnerRoute(rawKey []byte, routes []distribution.Route) (distribution.Route, bool) {
	start, end, ok := s3BucketAuxiliaryRouteRange(rawKey)
	if !ok {
		return distribution.Route{}, false
	}
	return s3BucketAuxiliaryOwnerRouteFromRange(start, end, routes)
}

func s3BucketAuxiliaryOwnerRouteFromRange(start []byte, end []byte, routes []distribution.Route) (distribution.Route, bool) {
	for _, route := range routes {
		if migrationRouteRangesIntersect(route.Start, route.End, start, end) {
			return route, true
		}
	}
	return distribution.Route{}, false
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

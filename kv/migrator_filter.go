package kv

import "bytes"

// RouteKeyFilter returns the migration export predicate for raw MVCC keys.
// rangeEnd nil or empty means +infinity, matching the route descriptor wire
// convention.
func RouteKeyFilter(rangeStart, rangeEnd []byte) func([]byte) bool {
	start := bytes.Clone(rangeStart)
	end := bytes.Clone(rangeEnd)
	return func(rawKey []byte) bool {
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

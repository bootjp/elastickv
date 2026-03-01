package kv

import "github.com/bootjp/elastickv/store"

// routeKey normalizes internal keys (e.g., list metadata/items) to the logical
// user key used for shard routing.
func routeKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	if embedded, ok := txnRouteKey(key); ok {
		// Transaction internal keys embed the logical key after the prefix.
		if user := store.ExtractListUserKey(embedded); user != nil {
			return user
		}
		return embedded
	}
	if user := store.ExtractListUserKey(key); user != nil {
		return user
	}
	return key
}

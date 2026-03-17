package kv

import (
	"bytes"

	"github.com/bootjp/elastickv/store"
)

const redisInternalRoutePrefix = "!redis|"

var redisInternalRoutePrefixBytes = []byte(redisInternalRoutePrefix)

// routeKey normalizes internal keys (e.g., list metadata/items) to the logical
// user key used for shard routing.
func routeKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	if user := redisRouteKey(key); user != nil {
		return user
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

func redisRouteKey(key []byte) []byte {
	if !bytes.HasPrefix(key, redisInternalRoutePrefixBytes) {
		return nil
	}
	rest := key[len(redisInternalRoutePrefix):]
	sep := bytes.IndexByte(rest, '|')
	if sep < 0 || sep+1 >= len(rest) {
		return nil
	}
	return rest[sep+1:]
}

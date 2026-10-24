package distribution

import "bytes"

// IsCatalogOwnershipPartitionKey includes malformed keys in the reserved ledger
// namespace so that authorization never depends on successful key decoding.
func IsCatalogOwnershipPartitionKey(key []byte) bool {
	return bytes.HasPrefix(key, []byte(ownershipPartitionCatalogPrefix))
}

func CatalogOwnershipPartitionPrefixIntersects(prefix []byte) bool {
	reserved := []byte(ownershipPartitionCatalogPrefix)
	return bytes.HasPrefix(prefix, reserved) || bytes.HasPrefix(reserved, prefix)
}
